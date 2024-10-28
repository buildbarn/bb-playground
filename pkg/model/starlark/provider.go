package starlark

import (
	"errors"
	"fmt"

	pg_label "github.com/buildbarn/bb-playground/pkg/label"
	model_core "github.com/buildbarn/bb-playground/pkg/model/core"
	model_starlark_pb "github.com/buildbarn/bb-playground/pkg/proto/model/starlark"
	"github.com/buildbarn/bb-playground/pkg/storage/dag"

	"go.starlark.net/starlark"
	"go.starlark.net/starlarkstruct"
)

type Provider struct {
	LateNamedValue
	initFunction *NamedFunction
}

var (
	_ EncodableValue    = &Provider{}
	_ NamedGlobal       = &Provider{}
	_ starlark.Callable = &Provider{}
)

func NewProvider(identifier *pg_label.CanonicalStarlarkIdentifier, initFunction *NamedFunction) starlark.Value {
	return &Provider{
		LateNamedValue: LateNamedValue{
			Identifier: identifier,
		},
		initFunction: initFunction,
	}
}

func (p *Provider) String() string {
	return "<provider>"
}

func (p *Provider) Type() string {
	return "provider"
}

func (p *Provider) Freeze() {}

func (p *Provider) Truth() starlark.Bool {
	return starlark.True
}

func (p *Provider) Hash() (uint32, error) {
	// TODO
	return 0, nil
}

func (p *Provider) Name() string {
	return "provider"
}

func (p *Provider) CallInternal(thread *starlark.Thread, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
	// TODO: WE SHOULD VALIDATE FIELDS HERE!
	if len(args) > 0 {
		return nil, fmt.Errorf("%s: got %d positional arguments, want 0", p.Name(), len(args))
	}
	fields := make(starlark.StringDict, len(kwargs))
	for _, kwarg := range kwargs {
		fields[string(kwarg[0].(starlark.String))] = kwarg[1]
	}
	return ProviderInstance{
		Struct:   starlarkstruct.FromStringDict(starlarkstruct.Default, fields),
		provider: p,
	}, nil
}

func (p *Provider) EncodeValue(path map[starlark.Value]struct{}, currentIdentifier *pg_label.CanonicalStarlarkIdentifier, options *ValueEncodingOptions) (model_core.PatchedMessage[*model_starlark_pb.Value, dag.ObjectContentsWalker], bool, error) {
	if p.Identifier == nil {
		return model_core.PatchedMessage[*model_starlark_pb.Value, dag.ObjectContentsWalker]{}, false, errors.New("provider does not have a name")
	}
	var initFunctionIdentifier string
	if p.initFunction != nil {
		initFunctionIdentifier = p.initFunction.identifier.String()
	}
	// TODO!
	needsCode := false
	return model_core.PatchedMessage[*model_starlark_pb.Value, dag.ObjectContentsWalker]{
		Message: &model_starlark_pb.Value{
			Kind: &model_starlark_pb.Value_Provider{
				Provider: &model_starlark_pb.Provider{
					ProviderIdentifier:     p.Identifier.String(),
					InitFunctionIdentifier: initFunctionIdentifier,
				},
			},
		},
		Patcher: model_core.NewReferenceMessagePatcher[dag.ObjectContentsWalker](),
	}, needsCode, nil
}

type ProviderInstance struct {
	*starlarkstruct.Struct
	provider *Provider
}

func NewProviderInstance(strukt *starlarkstruct.Struct, providerIdentifier pg_label.CanonicalStarlarkIdentifier) ProviderInstance {
	return ProviderInstance{
		Struct: strukt,
		provider: &Provider{
			LateNamedValue: LateNamedValue{
				Identifier: &providerIdentifier,
			},
		},
	}
}

func (pi ProviderInstance) EncodeStruct(path map[starlark.Value]struct{}, options *ValueEncodingOptions) (model_core.PatchedMessage[*model_starlark_pb.Struct, dag.ObjectContentsWalker], bool, error) {
	providerIdentifier := pi.provider.Identifier
	if providerIdentifier == nil {
		return model_core.PatchedMessage[*model_starlark_pb.Struct, dag.ObjectContentsWalker]{}, false, errors.New("provider does not have a name")
	}
	return encodeStruct(pi.Struct, path, providerIdentifier.String(), options)
}

func (pi ProviderInstance) EncodeValue(path map[starlark.Value]struct{}, currentIdentifier *pg_label.CanonicalStarlarkIdentifier, options *ValueEncodingOptions) (model_core.PatchedMessage[*model_starlark_pb.Value, dag.ObjectContentsWalker], bool, error) {
	encodedStruct, needsCode, err := pi.EncodeStruct(path, options)
	if err != nil {
		return model_core.PatchedMessage[*model_starlark_pb.Value, dag.ObjectContentsWalker]{}, false, err
	}
	return model_core.PatchedMessage[*model_starlark_pb.Value, dag.ObjectContentsWalker]{
		Message: &model_starlark_pb.Value{
			Kind: &model_starlark_pb.Value_Struct{
				Struct: encodedStruct.Message,
			},
		},
		Patcher: encodedStruct.Patcher,
	}, needsCode, nil
}
