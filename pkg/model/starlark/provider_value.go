package starlark

import (
	"fmt"

	pg_label "github.com/buildbarn/bb-playground/pkg/label"
	model_core "github.com/buildbarn/bb-playground/pkg/model/core"
	model_starlark_pb "github.com/buildbarn/bb-playground/pkg/proto/model/starlark"
	"github.com/buildbarn/bb-playground/pkg/storage/dag"

	"go.starlark.net/starlark"
)

type providerValue struct {
	filename pg_label.CanonicalLabel
	name     string
	init     starlark.Value
}

var (
	_ EncodableValue    = &providerValue{}
	_ starlark.Callable = &providerValue{}
)

func NewProviderValue(filename pg_label.CanonicalLabel, name string, init starlark.Value) starlark.Value {
	return &providerValue{
		filename: filename,
		name:     name,
		init:     init,
	}
}

func (pv *providerValue) String() string {
	return "<provider>"
}

func (pv *providerValue) Type() string {
	return "provider"
}

func (pv *providerValue) Freeze() {}

func (pv *providerValue) Truth() starlark.Bool {
	return starlark.True
}

func (pv *providerValue) Hash() (uint32, error) {
	// TODO
	return 0, nil
}

func (pv *providerValue) Name() string {
	return pv.name
}

func (pv *providerValue) CallInternal(thread *starlark.Thread, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
	// TODO: WE SHOULD VALIDATE FIELDS HERE!
	if len(args) > 0 {
		return nil, fmt.Errorf("%s: got %d positional arguments, want 0", pv.name, len(args))
	}
	fields := make(map[string]starlark.Value, len(kwargs))
	for _, kwarg := range kwargs {
		fields[string(kwarg[0].(starlark.String))] = kwarg[1]
	}
	return NewStruct(fields), nil
}

func (pv *providerValue) EncodeValue(path map[starlark.Value]struct{}, options *ValueEncodingOptions) (model_core.PatchedMessage[*model_starlark_pb.Value, dag.ObjectContentsWalker], bool, error) {
	// TODO!
	needsCode := false
	return model_core.PatchedMessage[*model_starlark_pb.Value, dag.ObjectContentsWalker]{
		Message: &model_starlark_pb.Value{
			Kind: &model_starlark_pb.Value_Provider{
				Provider: &model_starlark_pb.IdentifierReference{
					Filename: pv.filename.String(),
					Name:     pv.name,
				},
			},
		},
		Patcher: model_core.NewReferenceMessagePatcher[dag.ObjectContentsWalker](),
	}, needsCode, nil
}
