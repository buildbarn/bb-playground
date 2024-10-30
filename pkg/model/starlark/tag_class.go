package starlark

import (
	pg_label "github.com/buildbarn/bb-playground/pkg/label"
	model_core "github.com/buildbarn/bb-playground/pkg/model/core"
	model_starlark_pb "github.com/buildbarn/bb-playground/pkg/proto/model/starlark"
	"github.com/buildbarn/bb-playground/pkg/storage/dag"

	"go.starlark.net/starlark"
)

type TagClass struct {
	definition TagClassDefinition
}

var (
	_ starlark.Value = &TagClass{}
	_ EncodableValue = &rule{}
)

func NewTagClass(definition TagClassDefinition) starlark.Value {
	return &TagClass{
		definition: definition,
	}
}

func (TagClass) String() string {
	return "<tag_class>"
}

func (TagClass) Type() string {
	return "tag_class"
}

func (TagClass) Freeze() {}

func (TagClass) Truth() starlark.Bool {
	return starlark.True
}

func (TagClass) Hash() (uint32, error) {
	// TODO
	return 0, nil
}

func (tc *TagClass) EncodeValue(path map[starlark.Value]struct{}, currentIdentifier *pg_label.CanonicalStarlarkIdentifier, options *ValueEncodingOptions) (model_core.PatchedMessage[*model_starlark_pb.Value, dag.ObjectContentsWalker], bool, error) {
	return tc.definition.Encode(path, options)
}

type TagClassDefinition interface {
	Encode(path map[starlark.Value]struct{}, options *ValueEncodingOptions) (model_core.PatchedMessage[*model_starlark_pb.Value, dag.ObjectContentsWalker], bool, error)
}

type starlarkTagClassDefinition struct {
	attrs map[pg_label.StarlarkIdentifier]*Attr
}

func NewStarlarkTagClassDefinition(attrs map[pg_label.StarlarkIdentifier]*Attr) TagClassDefinition {
	return &starlarkTagClassDefinition{
		attrs: attrs,
	}
}

func (tcd *starlarkTagClassDefinition) Encode(path map[starlark.Value]struct{}, options *ValueEncodingOptions) (model_core.PatchedMessage[*model_starlark_pb.Value, dag.ObjectContentsWalker], bool, error) {
	encodedAttrs, needsCode, err := encodeNamedAttrs(tcd.attrs, path, options)
	if err != nil {
		return model_core.PatchedMessage[*model_starlark_pb.Value, dag.ObjectContentsWalker]{}, false, nil
	}
	return model_core.PatchedMessage[*model_starlark_pb.Value, dag.ObjectContentsWalker]{
		Message: &model_starlark_pb.Value{
			Kind: &model_starlark_pb.Value_TagClass{
				TagClass: &model_starlark_pb.TagClass{
					Attrs: encodedAttrs.Message,
				},
			},
		},
		Patcher: encodedAttrs.Patcher,
	}, needsCode, nil
}
