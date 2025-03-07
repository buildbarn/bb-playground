package starlark

import (
	"errors"
	"fmt"

	pg_label "github.com/buildbarn/bonanza/pkg/label"
	model_core "github.com/buildbarn/bonanza/pkg/model/core"
	model_starlark_pb "github.com/buildbarn/bonanza/pkg/proto/model/starlark"
	"github.com/buildbarn/bonanza/pkg/starlark/unpack"
	"github.com/buildbarn/bonanza/pkg/storage/object"

	"go.starlark.net/starlark"
	"go.starlark.net/syntax"
)

type ToolchainType[TReference any, TMetadata model_core.CloneableReferenceMetadata] struct {
	toolchainType pg_label.ResolvedLabel
	mandatory     bool
}

var (
	_ starlark.HasAttrs                                                            = (*ToolchainType[object.LocalReference, model_core.CloneableReferenceMetadata])(nil)
	_ starlark.Value                                                               = (*ToolchainType[object.LocalReference, model_core.CloneableReferenceMetadata])(nil)
	_ EncodableValue[object.LocalReference, model_core.CloneableReferenceMetadata] = (*ToolchainType[object.LocalReference, model_core.CloneableReferenceMetadata])(nil)
)

func NewToolchainType[TReference any, TMetadata model_core.CloneableReferenceMetadata](toolchainType pg_label.ResolvedLabel, mandatory bool) *ToolchainType[TReference, TMetadata] {
	return &ToolchainType[TReference, TMetadata]{
		toolchainType: toolchainType,
		mandatory:     mandatory,
	}
}

func (ToolchainType[TReference, TMetadata]) String() string {
	return "<config_common.toolchain_type>"
}

func (ToolchainType[TReference, TMetadata]) Type() string {
	return "config_common.toolchain_type"
}

func (ToolchainType[TReference, TMetadata]) Freeze() {}

func (ToolchainType[TReference, TMetadata]) Truth() starlark.Bool {
	return starlark.True
}

func (ToolchainType[TReference, TMetadata]) Hash(thread *starlark.Thread) (uint32, error) {
	return 0, errors.New("config_common.toolchain_type cannot be hashed")
}

func (tt *ToolchainType[TReference, TMetadata]) Attr(thread *starlark.Thread, name string) (starlark.Value, error) {
	switch name {
	case "mandatory":
		return starlark.Bool(tt.mandatory), nil
	case "toolchain_type":
		return NewLabel[TReference, TMetadata](tt.toolchainType), nil
	default:
		return nil, nil
	}
}

func (tt *ToolchainType[TReference, TMetadata]) Encode() *model_starlark_pb.ToolchainType {
	return &model_starlark_pb.ToolchainType{
		ToolchainType: tt.toolchainType.String(),
		Mandatory:     tt.mandatory,
	}
}

func (tt *ToolchainType[TReference, TMetadata]) Merge(other *ToolchainType[TReference, TMetadata]) *ToolchainType[TReference, TMetadata] {
	return &ToolchainType[TReference, TMetadata]{
		toolchainType: tt.toolchainType,
		mandatory:     tt.mandatory || other.mandatory,
	}
}

var toolchainTypeAttrNames = []string{
	"mandatory",
	"toolchain_type",
}

func (tt *ToolchainType[TReference, TMetadata]) AttrNames() []string {
	return toolchainTypeAttrNames
}

func (tt *ToolchainType[TReference, TMetadata]) EncodeValue(path map[starlark.Value]struct{}, currentIdentifier *pg_label.CanonicalStarlarkIdentifier, options *ValueEncodingOptions[TReference, TMetadata]) (model_core.PatchedMessage[*model_starlark_pb.Value, TMetadata], bool, error) {
	return model_core.NewSimplePatchedMessage[TMetadata](
		&model_starlark_pb.Value{
			Kind: &model_starlark_pb.Value_ToolchainType{
				ToolchainType: tt.Encode(),
			},
		},
	), false, nil
}

type toolchainTypeUnpackerInto[TReference any, TMetadata model_core.CloneableReferenceMetadata] struct{}

func NewToolchainTypeUnpackerInto[TReference any, TMetadata model_core.CloneableReferenceMetadata]() unpack.UnpackerInto[*ToolchainType[TReference, TMetadata]] {
	return toolchainTypeUnpackerInto[TReference, TMetadata]{}
}

func (toolchainTypeUnpackerInto[TReference, TMetadata]) UnpackInto(thread *starlark.Thread, v starlark.Value, dst **ToolchainType[TReference, TMetadata]) error {
	switch typedV := v.(type) {
	case starlark.String, label[TReference, TMetadata]:
		var l pg_label.ResolvedLabel
		if err := NewLabelOrStringUnpackerInto[TReference, TMetadata](CurrentFilePackage(thread, 1)).UnpackInto(thread, v, &l); err != nil {
			return err
		}
		*dst = NewToolchainType[TReference, TMetadata](l, true)
		return nil
	case *ToolchainType[TReference, TMetadata]:
		*dst = typedV
		return nil
	default:
		return fmt.Errorf("got %s, want config_common.toolchain_type, Label or str", v.Type())
	}
}

func (ui toolchainTypeUnpackerInto[TReference, TMetadata]) Canonicalize(thread *starlark.Thread, v starlark.Value) (starlark.Value, error) {
	var tt *ToolchainType[TReference, TMetadata]
	if err := ui.UnpackInto(thread, v, &tt); err != nil {
		return nil, err
	}
	return tt, nil
}

func (toolchainTypeUnpackerInto[TReference, TMetadata]) GetConcatenationOperator() syntax.Token {
	return 0
}
