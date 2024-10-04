package starlark

import (
	"errors"
	"fmt"
	"sync/atomic"

	pg_label "github.com/buildbarn/bb-playground/pkg/label"
	model_core "github.com/buildbarn/bb-playground/pkg/model/core"
	model_starlark_pb "github.com/buildbarn/bb-playground/pkg/proto/model/starlark"
	"github.com/buildbarn/bb-playground/pkg/storage/dag"

	"go.starlark.net/starlark"
)

type indirectFunction struct {
	filename pg_label.CanonicalLabel
	name     string

	actualFunction atomic.Pointer[starlark.Callable]
}

var (
	_ EncodableValue    = &indirectFunction{}
	_ starlark.Callable = &indirectFunction{}
)

func NewIndirectFunction(filename pg_label.CanonicalLabel, name string) starlark.Value {
	return &indirectFunction{
		filename: filename,
		name:     name,
	}
}

func (f *indirectFunction) String() string {
	return fmt.Sprintf("<function %s>", f.name)
}

func (f *indirectFunction) Type() string {
	return "function"
}

func (f *indirectFunction) Freeze() {}

func (f *indirectFunction) Truth() starlark.Bool {
	return starlark.True
}

func (f *indirectFunction) Hash() (uint32, error) {
	// TODO
	return 0, nil
}

func (f *indirectFunction) Name() string {
	return f.name
}

type FunctionResolver = func(filename pg_label.CanonicalLabel, name string) (starlark.Callable, error)

const FunctionResolverKey = "function_resolver"

func (f *indirectFunction) CallInternal(thread *starlark.Thread, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
	if actualFunction := f.actualFunction.Load(); actualFunction != nil {
		return (*actualFunction).CallInternal(thread, args, kwargs)
	}

	functionResolver := thread.Local(FunctionResolverKey)
	if functionResolver == nil {
		return nil, errors.New("indirect functions cannot be resolved from within this context")
	}
	callable, err := functionResolver.(FunctionResolver)(f.filename, f.name)
	if err != nil {
		return nil, err
	}
	f.actualFunction.Store(&callable)
	return callable.CallInternal(thread, args, kwargs)
}

func (f *indirectFunction) EncodeValue(path map[starlark.Value]struct{}, options *ValueEncodingOptions) (model_core.PatchedMessage[*model_starlark_pb.Value, dag.ObjectContentsWalker], bool, error) {
	return model_core.PatchedMessage[*model_starlark_pb.Value, dag.ObjectContentsWalker]{
		Message: &model_starlark_pb.Value{
			Kind: &model_starlark_pb.Value_Function{
				Function: &model_starlark_pb.Function{
					Filename: f.filename.String(),
					Name:     f.name,
				},
			},
		},
		Patcher: model_core.NewReferenceMessagePatcher[dag.ObjectContentsWalker](),
	}, false, nil
}
