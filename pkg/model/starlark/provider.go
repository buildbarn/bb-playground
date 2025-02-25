package starlark

import (
	"errors"
	"fmt"
	"sort"
	"strings"

	pg_label "github.com/buildbarn/bonanza/pkg/label"
	model_core "github.com/buildbarn/bonanza/pkg/model/core"
	model_starlark_pb "github.com/buildbarn/bonanza/pkg/proto/model/starlark"
	"github.com/buildbarn/bonanza/pkg/storage/dag"

	"go.starlark.net/starlark"
)

type ProviderInstanceProperties struct {
	LateNamedValue
	dictLike bool
}

func (pip *ProviderInstanceProperties) Encode() (*model_starlark_pb.Provider_InstanceProperties, error) {
	if pip.Identifier == nil {
		return nil, errors.New("provider does not have a name")
	}
	return &model_starlark_pb.Provider_InstanceProperties{
		ProviderIdentifier: pip.Identifier.String(),
		DictLike:           pip.dictLike,
	}, nil
}

func NewProviderInstanceProperties(identifier *pg_label.CanonicalStarlarkIdentifier, dictLike bool) *ProviderInstanceProperties {
	return &ProviderInstanceProperties{
		LateNamedValue: LateNamedValue{
			Identifier: identifier,
		},
		dictLike: dictLike,
	}
}

type Provider struct {
	*ProviderInstanceProperties
	fields       []string
	initFunction *NamedFunction
}

var (
	_ EncodableValue          = &Provider{}
	_ NamedGlobal             = &Provider{}
	_ starlark.Callable       = &Provider{}
	_ starlark.TotallyOrdered = &Provider{}
)

func NewProvider(instanceProperties *ProviderInstanceProperties, fields []string, initFunction *NamedFunction) *Provider {
	return &Provider{
		ProviderInstanceProperties: instanceProperties,
		fields:                     fields,
		initFunction:               initFunction,
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

func (p *Provider) Hash(thread *starlark.Thread) (uint32, error) {
	if p.Identifier == nil {
		return 0, errors.New("provider without a name cannot be hashed")
	}
	return starlark.String(p.Identifier.String()).Hash(thread)
}

func (p *Provider) Cmp(other starlark.Value, depth int) (int, error) {
	pOther := other.(*Provider)
	if p.Identifier == nil || pOther.Identifier == nil {
		return 0, errors.New("provider without a name cannot be compared")
	}
	return strings.Compare(p.Identifier.String(), pOther.Identifier.String()), nil
}

func (p *Provider) Name() string {
	return "provider"
}

func (p *Provider) CallInternal(thread *starlark.Thread, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
	var fields map[string]any
	if p.initFunction == nil {
		// Trivially constructible provider.
		if len(args) > 0 {
			return nil, fmt.Errorf("%s: got %d positional arguments, want 0", p.Name(), len(args))
		}
		fields = make(map[string]any, len(kwargs))
		for _, kwarg := range kwargs {
			field := string(kwarg[0].(starlark.String))
			if len(p.fields) > 0 {
				if _, ok := sort.Find(
					len(p.fields),
					func(i int) int { return strings.Compare(field, p.fields[i]) },
				); !ok {
					return nil, fmt.Errorf("field %#v is not in the allowed set of fields for this provider", field)
				}
			}
			fields[field] = kwarg[1]
		}
	} else {
		// Provider has a custom init function.
		result, err := starlark.Call(thread, p.initFunction, args, kwargs)
		if err != nil {
			return nil, err
		}
		mapping, ok := result.(starlark.IterableMapping)
		if !ok {
			return nil, fmt.Errorf("init function returned %s, want dict", result.Type())
		}
		fields = map[string]any{}
		for key, value := range starlark.Entries(thread, mapping) {
			keyStr, ok := starlark.AsString(key)
			if !ok {
				return nil, fmt.Errorf("init function returned dict containing key of type %s, want string", key.Type())
			}
			fields[keyStr] = value
		}
	}

	return NewStructFromDict(p.ProviderInstanceProperties, fields), nil
}

func (p *Provider) EncodeValue(path map[starlark.Value]struct{}, currentIdentifier *pg_label.CanonicalStarlarkIdentifier, options *ValueEncodingOptions) (model_core.PatchedMessage[*model_starlark_pb.Value, dag.ObjectContentsWalker], bool, error) {
	instanceProperties, err := p.ProviderInstanceProperties.Encode()
	if err != nil {
		return model_core.PatchedMessage[*model_starlark_pb.Value, dag.ObjectContentsWalker]{}, false, err
	}

	provider := &model_starlark_pb.Provider{
		InstanceProperties: instanceProperties,
	}
	patcher := model_core.NewReferenceMessagePatcher[dag.ObjectContentsWalker]()
	needsCode := false

	if p.initFunction != nil {
		initFunction, initFunctionNeedsCode, err := p.initFunction.Encode(path, options)
		if err != nil {
			return model_core.PatchedMessage[*model_starlark_pb.Value, dag.ObjectContentsWalker]{}, false, err
		}
		provider.InitFunction = initFunction.Message
		patcher.Merge(initFunction.Patcher)
		needsCode = needsCode || initFunctionNeedsCode
	}

	return model_core.NewPatchedMessage(
		&model_starlark_pb.Value{
			Kind: &model_starlark_pb.Value_Provider{
				Provider: provider,
			},
		},
		patcher,
	), needsCode, nil
}
