package starlark

import (
	"errors"
	"fmt"

	pg_label "github.com/buildbarn/bb-playground/pkg/label"
	model_core "github.com/buildbarn/bb-playground/pkg/model/core"
	model_starlark_pb "github.com/buildbarn/bb-playground/pkg/proto/model/starlark"
	"github.com/buildbarn/bb-playground/pkg/starlark/unpack"
	"github.com/buildbarn/bb-playground/pkg/storage/dag"

	"go.starlark.net/starlark"
	"go.starlark.net/syntax"
)

type ToolchainType struct {
	toolchainType pg_label.ResolvedLabel
	mandatory     bool
}

var (
	_ starlark.HasAttrs = &ToolchainType{}
	_ starlark.Value    = &ToolchainType{}
	_ EncodableValue    = &ToolchainType{}
)

func NewToolchainType(toolchainType pg_label.ResolvedLabel, mandatory bool) *ToolchainType {
	return &ToolchainType{
		toolchainType: toolchainType,
		mandatory:     mandatory,
	}
}

func (ToolchainType) String() string {
	return "<config_common.toolchain_type>"
}

func (ToolchainType) Type() string {
	return "config_common.toolchain_type"
}

func (ToolchainType) Freeze() {}

func (ToolchainType) Truth() starlark.Bool {
	return starlark.True
}

func (ToolchainType) Hash() (uint32, error) {
	return 0, errors.New("config_common.toolchain_type cannot be hashed")
}

func (tt *ToolchainType) Attr(thread *starlark.Thread, name string) (starlark.Value, error) {
	switch name {
	case "mandatory":
		return starlark.Bool(tt.mandatory), nil
	case "toolchain_type":
		return NewLabel(tt.toolchainType), nil
	default:
		return nil, nil
	}
}

func (tt *ToolchainType) Encode() *model_starlark_pb.ToolchainType {
	return &model_starlark_pb.ToolchainType{
		ToolchainType: tt.toolchainType.String(),
		Mandatory:     tt.mandatory,
	}
}

func (tt *ToolchainType) Merge(other *ToolchainType) *ToolchainType {
	return &ToolchainType{
		toolchainType: tt.toolchainType,
		mandatory:     tt.mandatory || other.mandatory,
	}
}

var toolchainTypeAttrNames = []string{
	"mandatory",
	"toolchain_type",
}

func (tt *ToolchainType) AttrNames() []string {
	return toolchainTypeAttrNames
}

func (tt *ToolchainType) EncodeValue(path map[starlark.Value]struct{}, currentIdentifier *pg_label.CanonicalStarlarkIdentifier, options *ValueEncodingOptions) (model_core.PatchedMessage[*model_starlark_pb.Value, dag.ObjectContentsWalker], bool, error) {
	return model_core.NewSimplePatchedMessage[dag.ObjectContentsWalker](
		&model_starlark_pb.Value{
			Kind: &model_starlark_pb.Value_ToolchainType{
				ToolchainType: tt.Encode(),
			},
		},
	), false, nil
}

type toolchainTypeUnpackerInto struct{}

var ToolchainTypeUnpackerInto unpack.UnpackerInto[*ToolchainType] = toolchainTypeUnpackerInto{}

func (toolchainTypeUnpackerInto) UnpackInto(thread *starlark.Thread, v starlark.Value, dst **ToolchainType) error {
	switch typedV := v.(type) {
	case starlark.String, label:
		var l pg_label.ResolvedLabel
		if err := NewLabelOrStringUnpackerInto(CurrentFilePackage(thread, 1)).UnpackInto(thread, v, &l); err != nil {
			return err
		}
		*dst = NewToolchainType(l, true)
		return nil
	case *ToolchainType:
		*dst = typedV
		return nil
	default:
		return fmt.Errorf("got %s, want config_common.toolchain_type, Label or str", v.Type())
	}
}

func (ui toolchainTypeUnpackerInto) Canonicalize(thread *starlark.Thread, v starlark.Value) (starlark.Value, error) {
	var tt *ToolchainType
	if err := ui.UnpackInto(thread, v, &tt); err != nil {
		return nil, err
	}
	return tt, nil
}

func (toolchainTypeUnpackerInto) GetConcatenationOperator() syntax.Token {
	return 0
}
