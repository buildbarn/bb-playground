package starlark

import (
	model_core "github.com/buildbarn/bb-playground/pkg/model/core"
	model_starlark_pb "github.com/buildbarn/bb-playground/pkg/proto/model/starlark"
	"github.com/buildbarn/bb-playground/pkg/storage/dag"

	"go.starlark.net/starlark"
)

type depset struct{}

var _ EncodableValue = &strukt{}

func NewDepset() starlark.Value {
	return &depset{}
}

func (d *depset) String() string {
	return "TODO"
}

func (d *depset) Type() string {
	return "depset"
}

func (d *depset) Freeze() {
}

func (d *depset) Truth() starlark.Bool {
	panic("TODO")
}

func (d *depset) Hash() (uint32, error) {
	return 0, nil
}

func (d *depset) EncodeValue(path map[starlark.Value]struct{}, options *ValueEncodingOptions) (model_core.PatchedMessage[*model_starlark_pb.Value, dag.ObjectContentsWalker], bool, error) {
	// TODO
	needsCode := false
	return model_core.PatchedMessage[*model_starlark_pb.Value, dag.ObjectContentsWalker]{
		Message: &model_starlark_pb.Value{
			Kind: &model_starlark_pb.Value_Depset{
				Depset: &model_starlark_pb.Depset{},
			},
		},
		Patcher: model_core.NewReferenceMessagePatcher[dag.ObjectContentsWalker](),
	}, needsCode, nil
}
