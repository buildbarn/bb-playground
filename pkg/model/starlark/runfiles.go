package starlark

import (
	pg_label "github.com/buildbarn/bb-playground/pkg/label"
	model_core "github.com/buildbarn/bb-playground/pkg/model/core"
	model_starlark_pb "github.com/buildbarn/bb-playground/pkg/proto/model/starlark"
	"github.com/buildbarn/bb-playground/pkg/storage/dag"

	"go.starlark.net/starlark"
)

type Runfiles struct{}

var _ EncodableValue = (*Runfiles)(nil)

func NewRunfiles() *Runfiles {
	return &Runfiles{}
}

func (Runfiles) String() string {
	return "TODO"
}

func (Runfiles) Type() string {
	return "runfiles"
}

func (Runfiles) Freeze() {
}

func (Runfiles) Truth() starlark.Bool {
	panic("TODO")
}

func (Runfiles) Hash() (uint32, error) {
	return 0, nil
}

func (Runfiles) EncodeValue(path map[starlark.Value]struct{}, currentIdentifier *pg_label.CanonicalStarlarkIdentifier, options *ValueEncodingOptions) (model_core.PatchedMessage[*model_starlark_pb.Value, dag.ObjectContentsWalker], bool, error) {
	// TODO
	needsCode := false
	return model_core.NewSimplePatchedMessage[dag.ObjectContentsWalker](
		&model_starlark_pb.Value{
			Kind: &model_starlark_pb.Value_Runfiles{
				Runfiles: &model_starlark_pb.Runfiles{},
			},
		},
	), needsCode, nil
}
