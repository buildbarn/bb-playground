package starlark

import (
	pg_label "github.com/buildbarn/bb-playground/pkg/label"
	model_core "github.com/buildbarn/bb-playground/pkg/model/core"
	model_starlark_pb "github.com/buildbarn/bb-playground/pkg/proto/model/starlark"
	"github.com/buildbarn/bb-playground/pkg/storage/dag"

	"go.starlark.net/starlark"
)

type Depset struct{}

var _ EncodableValue = (*Depset)(nil)

func NewDepset() *Depset {
	return &Depset{}
}

func (Depset) String() string {
	return "TODO"
}

func (Depset) Type() string {
	return "depset"
}

func (Depset) Freeze() {
}

func (Depset) Truth() starlark.Bool {
	panic("TODO")
}

func (Depset) Hash() (uint32, error) {
	return 0, nil
}

func (Depset) EncodeValue(path map[starlark.Value]struct{}, currentIdentifier *pg_label.CanonicalStarlarkIdentifier, options *ValueEncodingOptions) (model_core.PatchedMessage[*model_starlark_pb.Value, dag.ObjectContentsWalker], bool, error) {
	// TODO
	needsCode := false
	return model_core.NewSimplePatchedMessage[dag.ObjectContentsWalker](
		&model_starlark_pb.Value{
			Kind: &model_starlark_pb.Value_Depset{
				Depset: &model_starlark_pb.Depset{},
			},
		},
	), needsCode, nil
}
