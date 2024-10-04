package starlark

import (
	"fmt"
	"sort"
	"strings"

	model_core "github.com/buildbarn/bb-playground/pkg/model/core"
	model_starlark_pb "github.com/buildbarn/bb-playground/pkg/proto/model/starlark"
	"github.com/buildbarn/bb-playground/pkg/storage/dag"

	"go.starlark.net/starlark"
)

type strukt struct {
	fields     map[string]starlark.Value
	fieldNames []string
}

var (
	_ starlark.HasAttrs = &strukt{}
	_ EncodableValue    = &strukt{}
)

func NewStruct(fields map[string]starlark.Value) starlark.Value {
	return &strukt{
		fields: fields,
	}
}

func (s *strukt) String() string {
	if len(s.fields) == 0 {
		return "struct()"
	}

	var out strings.Builder
	separator := "struct("
	for _, key := range s.AttrNames() {
		out.WriteString(separator)
		separator = ", "

		out.WriteString(key)
		out.WriteString(" = ")
		out.WriteString(s.fields[key].String())
	}
	return out.String()
}

func (s *strukt) Type() string {
	return "struct"
}

func (s *strukt) Freeze() {
	for _, value := range s.fields {
		value.Freeze()
	}
}

func (s *strukt) Truth() starlark.Bool {
	return starlark.True
}

func (s *strukt) Hash() (uint32, error) {
	h := uint32(0xacf4219d)
	for key, value := range s.fields {
		keyHash, err := starlark.String(key).Hash()
		if err != nil {
			return 0, err
		}
		valueHash, err := value.Hash()
		if err != nil {
			return 0, err
		}
		h ^= keyHash * valueHash
	}
	return h, nil
}

func (s *strukt) Attr(name string) (starlark.Value, error) {
	return s.fields[name], nil
}

func (s *strukt) AttrNames() []string {
	keys := make([]string, 0, len(s.fields))
	for key := range s.fields {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

func (s *strukt) EncodeValue(path map[starlark.Value]struct{}, options *ValueEncodingOptions) (model_core.PatchedMessage[*model_starlark_pb.Value, dag.ObjectContentsWalker], bool, error) {
	keys := make([]string, 0, len(s.fields))
	for key := range s.fields {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	fields := make([]*model_starlark_pb.StructField, 0, len(keys))
	patcher := model_core.NewReferenceMessagePatcher[dag.ObjectContentsWalker]()
	needsCode := false
	for _, key := range keys {
		encodedValue, valueNeedsCode, err := EncodeValue(s.fields[key], path, options)
		if err != nil {
			return model_core.PatchedMessage[*model_starlark_pb.Value, dag.ObjectContentsWalker]{}, false, fmt.Errorf("field %#v: %w", key, err)
		}
		fields = append(fields, &model_starlark_pb.StructField{
			Key:   key,
			Value: encodedValue.Message,
		})
		patcher.Merge(encodedValue.Patcher)
		needsCode = needsCode || valueNeedsCode
	}

	return model_core.PatchedMessage[*model_starlark_pb.Value, dag.ObjectContentsWalker]{
		Message: &model_starlark_pb.Value{
			Kind: &model_starlark_pb.Value_Struct{
				Struct: &model_starlark_pb.Struct{
					Fields: fields,
				},
			},
		},
		Patcher: patcher,
	}, needsCode, nil
}
