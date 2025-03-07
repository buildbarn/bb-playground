package starlark

import (
	"errors"
	"fmt"
	"sync/atomic"

	pg_label "github.com/buildbarn/bonanza/pkg/label"
	model_core "github.com/buildbarn/bonanza/pkg/model/core"
	model_starlark_pb "github.com/buildbarn/bonanza/pkg/proto/model/starlark"
	"github.com/buildbarn/bonanza/pkg/starlark/unpack"
	"github.com/buildbarn/bonanza/pkg/storage/object"

	"go.starlark.net/starlark"
	"go.starlark.net/syntax"
)

type NamedFunction[TReference any, TMetadata model_core.CloneableReferenceMetadata] struct {
	NamedFunctionDefinition[TReference, TMetadata]
}

var _ EncodableValue[object.LocalReference, model_core.CloneableReferenceMetadata] = (*NamedFunction[object.LocalReference, model_core.CloneableReferenceMetadata])(nil)

func NewNamedFunction[TReference any, TMetadata model_core.CloneableReferenceMetadata](definition NamedFunctionDefinition[TReference, TMetadata]) NamedFunction[TReference, TMetadata] {
	return NamedFunction[TReference, TMetadata]{
		NamedFunctionDefinition: definition,
	}
}

func (f NamedFunction[TReference, TMetadata]) String() string {
	return fmt.Sprintf("<function %s>", f.Name())
}

func (f NamedFunction[TReference, TMetadata]) Type() string {
	return "function"
}

func (f NamedFunction[TReference, TMetadata]) Freeze() {}

func (f NamedFunction[TReference, TMetadata]) Truth() starlark.Bool {
	return starlark.True
}

func (f NamedFunction[TReference, TMetadata]) Hash(thread *starlark.Thread) (uint32, error) {
	return 0, errors.New("function cannot be hashed")
}

func (f NamedFunction[TReference, TMetadata]) EncodeValue(path map[starlark.Value]struct{}, currentIdentifier *pg_label.CanonicalStarlarkIdentifier, options *ValueEncodingOptions[TReference, TMetadata]) (model_core.PatchedMessage[*model_starlark_pb.Value, TMetadata], bool, error) {
	function, needsCode, err := f.NamedFunctionDefinition.Encode(path, options)
	if err != nil {
		return model_core.PatchedMessage[*model_starlark_pb.Value, TMetadata]{}, false, err
	}
	return model_core.NewPatchedMessage(
		&model_starlark_pb.Value{
			Kind: &model_starlark_pb.Value_Function{
				Function: function.Message,
			},
		},
		function.Patcher,
	), needsCode, nil
}

type NamedFunctionDefinition[TReference any, TMetadata model_core.CloneableReferenceMetadata] interface {
	CallInternal(thread *starlark.Thread, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error)
	Encode(path map[starlark.Value]struct{}, options *ValueEncodingOptions[TReference, TMetadata]) (model_core.PatchedMessage[*model_starlark_pb.Function, TMetadata], bool, error)
	Name() string
	Position() syntax.Position
}

type starlarkNamedFunctionDefinition[TReference any, TMetadata model_core.CloneableReferenceMetadata] struct {
	*starlark.Function
}

func NewStarlarkNamedFunctionDefinition[TReference any, TMetadata model_core.CloneableReferenceMetadata](function *starlark.Function) NamedFunctionDefinition[TReference, TMetadata] {
	return starlarkNamedFunctionDefinition[TReference, TMetadata]{
		Function: function,
	}
}

func (d starlarkNamedFunctionDefinition[TReference, TMetadata]) Encode(path map[starlark.Value]struct{}, options *ValueEncodingOptions[TReference, TMetadata]) (model_core.PatchedMessage[*model_starlark_pb.Function, TMetadata], bool, error) {
	patcher := model_core.NewReferenceMessagePatcher[TMetadata]()
	position := d.Function.Position()
	filename := position.Filename()
	needsCode := filename == options.CurrentFilename.String()
	name := d.Function.Name()

	var closure *model_starlark_pb.Function_Closure
	if position.Col != 1 || name == "lambda" {
		if _, ok := path[d]; ok {
			return model_core.PatchedMessage[*model_starlark_pb.Function, TMetadata]{}, false, errors.New("value is defined recursively")
		}
		path[d] = struct{}{}
		defer delete(path, d)

		numRawDefaults := d.Function.NumRawDefaults()
		defaultParameters := make([]*model_starlark_pb.Function_Closure_DefaultParameter, 0, numRawDefaults)
		for index := 0; index < numRawDefaults; index++ {
			if defaultValue := d.Function.RawDefault(index); defaultValue != nil {
				encodedDefaultValue, defaultValueNeedsCode, err := EncodeValue[TReference, TMetadata](defaultValue, path, nil, options)
				if err != nil {
					return model_core.PatchedMessage[*model_starlark_pb.Function, TMetadata]{}, false, fmt.Errorf("default parameter %d: %w", index, err)
				}
				defaultParameters = append(defaultParameters, &model_starlark_pb.Function_Closure_DefaultParameter{
					Value: encodedDefaultValue.Message,
				})
				patcher.Merge(encodedDefaultValue.Patcher)
				needsCode = needsCode || defaultValueNeedsCode
			} else {
				defaultParameters = append(defaultParameters, &model_starlark_pb.Function_Closure_DefaultParameter{})
			}
		}

		numFreeVars := d.Function.NumFreeVars()
		freeVars := make([]*model_starlark_pb.Value, 0, numFreeVars)
		for index := 0; index < numFreeVars; index++ {
			_, freeVar := d.Function.FreeVar(index)
			encodedFreeVar, freeVarNeedsCode, err := EncodeValue[TReference, TMetadata](freeVar, path, nil, options)
			if err != nil {
				return model_core.PatchedMessage[*model_starlark_pb.Function, TMetadata]{}, false, fmt.Errorf("free variable %d: %w", index, err)
			}
			freeVars = append(freeVars, encodedFreeVar.Message)
			patcher.Merge(encodedFreeVar.Patcher)
			needsCode = needsCode || freeVarNeedsCode
		}

		closure = &model_starlark_pb.Function_Closure{
			Index:             d.Function.Index(),
			DefaultParameters: defaultParameters,
			FreeVariables:     freeVars,
		}
	}

	return model_core.NewPatchedMessage(
		&model_starlark_pb.Function{
			Filename: filename,
			Line:     position.Line,
			Column:   position.Col,
			Name:     d.Function.Name(),
			Closure:  closure,
		},
		patcher,
	), needsCode, nil
}

type FunctionFactoryResolver = func(filename pg_label.CanonicalLabel) (*starlark.FunctionFactory, error)

const FunctionFactoryResolverKey = "function_factory_resolver"

type protoNamedFunctionDefinition[TReference object.BasicReference, TMetadata model_core.CloneableReferenceMetadata] struct {
	message  model_core.Message[*model_starlark_pb.Function, TReference]
	function atomic.Pointer[starlark.Function]
}

func NewProtoNamedFunctionDefinition[TReference object.BasicReference, TMetadata model_core.CloneableReferenceMetadata](message model_core.Message[*model_starlark_pb.Function, TReference]) NamedFunctionDefinition[TReference, TMetadata] {
	return &protoNamedFunctionDefinition[TReference, TMetadata]{
		message: message,
	}
}

func (d *protoNamedFunctionDefinition[TReference, TMetadata]) CallInternal(thread *starlark.Thread, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
	function := d.function.Load()
	if function == nil {
		functionFactoryResolver := thread.Local(FunctionFactoryResolverKey)
		if functionFactoryResolver == nil {
			return nil, errors.New("indirect functions cannot be resolved from within this context")
		}
		definition := d.message.Message
		if definition == nil {
			return nil, errors.New("no function message present")
		}
		filename, err := pg_label.NewCanonicalLabel(definition.Filename)
		if err != nil {
			return nil, fmt.Errorf("invalid filename %#v: %w", definition.Filename, err)
		}
		functionFactory, err := functionFactoryResolver.(FunctionFactoryResolver)(filename)
		if err != nil {
			return nil, err
		}

		if closure := definition.Closure; closure == nil {
			function, err = functionFactory.NewFunctionByName(definition.Name)
			if err != nil {
				return nil, err
			}
		} else {
			options := thread.Local(ValueDecodingOptionsKey).(*ValueDecodingOptions[TReference])
			freeVariables := make(starlark.Tuple, 0, len(closure.FreeVariables))
			for index, freeVariable := range closure.FreeVariables {
				value, err := DecodeValue[TReference, TMetadata](model_core.NewNestedMessage(d.message, freeVariable), nil, options)
				if err != nil {
					return nil, fmt.Errorf("invalid free variable %d: %w", index, err)
				}
				freeVariables = append(freeVariables, value)
			}

			defaultParameters := make(starlark.Tuple, len(closure.DefaultParameters))
			for index, defaultParameter := range closure.DefaultParameters {
				if defaultParameter.Value != nil {
					value, err := DecodeValue[TReference, TMetadata](model_core.NewNestedMessage(d.message, defaultParameter.Value), nil, options)
					if err != nil {
						return nil, fmt.Errorf("invalid default parameter %d: %w", index, err)
					}
					defaultParameters[index] = value
				}
			}

			function, err = functionFactory.NewFunctionByIndex(closure.Index, defaultParameters, freeVariables)
			if err != nil {
				return nil, err
			}
		}

		d.function.Store(function)
	}
	return function.CallInternal(thread, args, kwargs)
}

func (d *protoNamedFunctionDefinition[TReference, TMetadata]) Encode(path map[starlark.Value]struct{}, options *ValueEncodingOptions[TReference, TMetadata]) (model_core.PatchedMessage[*model_starlark_pb.Function, TMetadata], bool, error) {
	return model_core.NewPatchedMessageFromExistingCaptured(options.ObjectCapturer, d.message), false, nil
}

func (d *protoNamedFunctionDefinition[TReference, TMetadata]) Name() string {
	if m := d.message.Message; m != nil {
		return m.Name
	}
	return "unknown"
}

func (d *protoNamedFunctionDefinition[TReference, TMetadata]) Position() syntax.Position {
	if m := d.message.Message; m != nil {
		return syntax.MakePosition(&m.Filename, m.Line, m.Column)
	}
	return syntax.MakePosition(nil, 0, 0)
}

type namedFunctionUnpackerInto[TReference any, TMetadata model_core.CloneableReferenceMetadata] struct{}

func NewNamedFunctionUnpackerInto[TReference any, TMetadata model_core.CloneableReferenceMetadata]() unpack.UnpackerInto[NamedFunction[TReference, TMetadata]] {
	return namedFunctionUnpackerInto[TReference, TMetadata]{}
}

func (namedFunctionUnpackerInto[TReference, TMetadata]) UnpackInto(thread *starlark.Thread, v starlark.Value, dst *NamedFunction[TReference, TMetadata]) error {
	switch typedV := v.(type) {
	case *starlark.Function:
		*dst = NewNamedFunction(NewStarlarkNamedFunctionDefinition[TReference, TMetadata](typedV))
		return nil
	case NamedFunction[TReference, TMetadata]:
		*dst = typedV
		return nil
	default:
		return fmt.Errorf("got %s, want function", v.Type())
	}
}

func (ui namedFunctionUnpackerInto[TReference, TMetadata]) Canonicalize(thread *starlark.Thread, v starlark.Value) (starlark.Value, error) {
	var f NamedFunction[TReference, TMetadata]
	if err := ui.UnpackInto(thread, v, &f); err != nil {
		return nil, err
	}
	return f, nil
}

func (namedFunctionUnpackerInto[TReference, TMetadata]) GetConcatenationOperator() syntax.Token {
	return 0
}
