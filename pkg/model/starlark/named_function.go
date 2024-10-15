package starlark

import (
	"errors"
	"fmt"
	"sync/atomic"

	pg_label "github.com/buildbarn/bb-playground/pkg/label"
	model_core "github.com/buildbarn/bb-playground/pkg/model/core"
	model_starlark_pb "github.com/buildbarn/bb-playground/pkg/proto/model/starlark"
	"github.com/buildbarn/bb-playground/pkg/starlark/unpack"
	"github.com/buildbarn/bb-playground/pkg/storage/dag"

	"go.starlark.net/starlark"
	"go.starlark.net/syntax"
)

type CallableWithPosition interface {
	starlark.Callable
	Position() syntax.Position
}

type NamedFunction struct {
	identifier     pg_label.CanonicalStarlarkIdentifier
	actualFunction atomic.Pointer[CallableWithPosition]
}

var (
	_ EncodableValue       = &NamedFunction{}
	_ CallableWithPosition = &NamedFunction{}
)

func NewNamedFunction(identifier pg_label.CanonicalStarlarkIdentifier, actualFunction CallableWithPosition) *NamedFunction {
	f := &NamedFunction{
		identifier: identifier,
	}
	if actualFunction != nil {
		f.actualFunction.Store(&actualFunction)
	}
	return f
}

func (f *NamedFunction) String() string {
	return fmt.Sprintf("<function %s>", f.Name())
}

func (f *NamedFunction) Type() string {
	return "function"
}

func (f *NamedFunction) Freeze() {}

func (f *NamedFunction) Truth() starlark.Bool {
	return starlark.True
}

func (f *NamedFunction) Hash() (uint32, error) {
	// TODO
	return 0, nil
}

func (f *NamedFunction) Name() string {
	return f.identifier.GetStarlarkIdentifier().String()
}

func (f *NamedFunction) Position() syntax.Position {
	if actualFunction := f.actualFunction.Load(); actualFunction != nil {
		// We've already loaded the function. Call into its
		// Position() function, so that we get accurate line
		// numbers.
		return (*actualFunction).Position()
	}

	// At least provide the filename.
	filename := f.identifier.GetCanonicalLabel().String()
	return syntax.MakePosition(&filename, 0, 0)
}

type FunctionResolver = func(identifier pg_label.CanonicalStarlarkIdentifier) (CallableWithPosition, error)

const FunctionResolverKey = "function_resolver"

func (f *NamedFunction) CallInternal(thread *starlark.Thread, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
	if actualFunction := f.actualFunction.Load(); actualFunction != nil {
		return (*actualFunction).CallInternal(thread, args, kwargs)
	}

	functionResolver := thread.Local(FunctionResolverKey)
	if functionResolver == nil {
		return nil, errors.New("indirect functions cannot be resolved from within this context")
	}
	callable, err := functionResolver.(FunctionResolver)(f.identifier)
	if err != nil {
		return nil, err
	}
	f.actualFunction.Store(&callable)
	return callable.CallInternal(thread, args, kwargs)
}

func (f *NamedFunction) EncodeValue(path map[starlark.Value]struct{}, currentIdentifier *pg_label.CanonicalStarlarkIdentifier, options *ValueEncodingOptions) (model_core.PatchedMessage[*model_starlark_pb.Value, dag.ObjectContentsWalker], bool, error) {
	return model_core.PatchedMessage[*model_starlark_pb.Value, dag.ObjectContentsWalker]{
		Message: &model_starlark_pb.Value{
			Kind: &model_starlark_pb.Value_Function{
				Function: f.identifier.String(),
			},
		},
		Patcher: model_core.NewReferenceMessagePatcher[dag.ObjectContentsWalker](),
	}, f.identifier.GetCanonicalLabel() == options.CurrentFilename, nil
}

type namedFunctionUnpackerInto struct{}

var NamedFunctionUnpackerInto unpack.UnpackerInto[*NamedFunction] = namedFunctionUnpackerInto{}

func (namedFunctionUnpackerInto) UnpackInto(thread *starlark.Thread, v starlark.Value, dst **NamedFunction) error {
	switch typedV := v.(type) {
	case *starlark.Function:
		*dst = NewNamedFunction(
			pg_label.MustNewCanonicalLabel(typedV.Position().Filename()).
				AppendStarlarkIdentifier(pg_label.MustNewStarlarkIdentifier(typedV.Name())),
			typedV,
		)
		return nil
	case *NamedFunction:
		*dst = typedV
		return nil
	default:
		return fmt.Errorf("got %s, want function", v.Type())
	}
}

func (ui namedFunctionUnpackerInto) Canonicalize(thread *starlark.Thread, v starlark.Value) (starlark.Value, error) {
	var f *NamedFunction
	if err := ui.UnpackInto(thread, v, &f); err != nil {
		return nil, err
	}
	return f, nil
}

func (namedFunctionUnpackerInto) GetConcatenationOperator() syntax.Token {
	return 0
}
