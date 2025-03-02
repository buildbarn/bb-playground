package starlark

import (
	"errors"
	"fmt"
	"slices"
	"sort"

	pg_label "github.com/buildbarn/bonanza/pkg/label"
	model_core "github.com/buildbarn/bonanza/pkg/model/core"
	model_starlark_pb "github.com/buildbarn/bonanza/pkg/proto/model/starlark"

	"go.starlark.net/starlark"
)

type Subrule struct {
	LateNamedValue
	definition SubruleDefinition
}

var (
	_ starlark.Callable = &Subrule{}
	_ EncodableValue    = &Subrule{}
	_ NamedGlobal       = &Subrule{}
)

func NewSubrule(identifier *pg_label.CanonicalStarlarkIdentifier, definition SubruleDefinition) starlark.Value {
	return &Subrule{
		LateNamedValue: LateNamedValue{
			Identifier: identifier,
		},
		definition: definition,
	}
}

func (Subrule) String() string {
	return "<subrule>"
}

func (Subrule) Type() string {
	return "subrule"
}

func (Subrule) Freeze() {}

func (Subrule) Truth() starlark.Bool {
	return starlark.True
}

func (Subrule) Hash(thread *starlark.Thread) (uint32, error) {
	return 0, errors.New("subrule cannot be hashed")
}

func (sr *Subrule) Name() string {
	if sr.Identifier == nil {
		return "subrule"
	}
	return sr.Identifier.GetStarlarkIdentifier().String()
}

const SubruleInvokerKey = "subrule_invoker"

type SubruleInvoker = func(subruleIdentifier pg_label.CanonicalStarlarkIdentifier, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error)

func (sr *Subrule) CallInternal(thread *starlark.Thread, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
	subruleInvoker := thread.Local(SubruleInvokerKey)
	if subruleInvoker == nil {
		return nil, errors.New("subrules cannot be invoked from within this context")
	}
	if sr.Identifier == nil {
		return nil, errors.New("subrule does not have a name")
	}
	return subruleInvoker.(SubruleInvoker)(*sr.Identifier, args, kwargs)
}

func (sr *Subrule) EncodeValue(path map[starlark.Value]struct{}, currentIdentifier *pg_label.CanonicalStarlarkIdentifier, options *ValueEncodingOptions) (model_core.PatchedMessage[*model_starlark_pb.Value, model_core.CreatedObjectTree], bool, error) {
	if sr.Identifier == nil {
		return model_core.PatchedMessage[*model_starlark_pb.Value, model_core.CreatedObjectTree]{}, false, errors.New("subrule does not have a name")
	}
	if currentIdentifier == nil || *currentIdentifier != *sr.Identifier {
		// Not the canonical identifier under which this subrule
		// is known. Emit a reference.
		return model_core.NewSimplePatchedMessage[model_core.CreatedObjectTree](
			&model_starlark_pb.Value{
				Kind: &model_starlark_pb.Value_Subrule{
					Subrule: &model_starlark_pb.Subrule{
						Kind: &model_starlark_pb.Subrule_Reference{
							Reference: sr.Identifier.String(),
						},
					},
				},
			},
		), false, nil
	}

	definition, needsCode, err := sr.definition.Encode(path, options)
	if err != nil {
		return model_core.PatchedMessage[*model_starlark_pb.Value, model_core.CreatedObjectTree]{}, false, err
	}
	return model_core.NewPatchedMessage(
		&model_starlark_pb.Value{
			Kind: &model_starlark_pb.Value_Subrule{
				Subrule: &model_starlark_pb.Subrule{
					Kind: &model_starlark_pb.Subrule_Definition_{
						Definition: definition.Message,
					},
				},
			},
		},
		definition.Patcher,
	), needsCode, nil
}

type SubruleDefinition interface {
	Encode(path map[starlark.Value]struct{}, options *ValueEncodingOptions) (model_core.PatchedMessage[*model_starlark_pb.Subrule_Definition, model_core.CreatedObjectTree], bool, error)
}

type starlarkSubruleDefinition struct {
	attrs          map[pg_label.StarlarkIdentifier]*Attr
	implementation NamedFunction
	subrules       []*Subrule
}

func NewStarlarkSubruleDefinition(
	attrs map[pg_label.StarlarkIdentifier]*Attr,
	implementation NamedFunction,
	subrules []*Subrule,
) SubruleDefinition {
	return &starlarkSubruleDefinition{
		attrs:          attrs,
		implementation: implementation,
		subrules:       subrules,
	}
}

func (sd *starlarkSubruleDefinition) Encode(path map[starlark.Value]struct{}, options *ValueEncodingOptions) (model_core.PatchedMessage[*model_starlark_pb.Subrule_Definition, model_core.CreatedObjectTree], bool, error) {
	patcher := model_core.NewReferenceMessagePatcher[model_core.CreatedObjectTree]()

	implementation, needsCode, err := sd.implementation.Encode(path, options)
	if err != nil {
		return model_core.PatchedMessage[*model_starlark_pb.Subrule_Definition, model_core.CreatedObjectTree]{}, false, err
	}
	patcher.Merge(implementation.Patcher)

	namedAttrs, namedAttrsNeedCode, err := encodeNamedAttrs(sd.attrs, path, options)
	if err != nil {
		return model_core.PatchedMessage[*model_starlark_pb.Subrule_Definition, model_core.CreatedObjectTree]{}, false, err
	}
	needsCode = needsCode || namedAttrsNeedCode
	patcher.Merge(namedAttrs.Patcher)

	subruleIdentifiers := make([]string, 0, len(sd.subrules))
	for i, subrule := range sd.subrules {
		if subrule.Identifier == nil {
			return model_core.PatchedMessage[*model_starlark_pb.Subrule_Definition, model_core.CreatedObjectTree]{}, false, fmt.Errorf("subrule at index %d does not have an identifier", i)
		}
		subruleIdentifiers = append(subruleIdentifiers, subrule.Identifier.String())
	}
	sort.Strings(subruleIdentifiers)

	return model_core.NewPatchedMessage(
		&model_starlark_pb.Subrule_Definition{
			Attrs:              namedAttrs.Message,
			Implementation:     implementation.Message,
			SubruleIdentifiers: slices.Compact(subruleIdentifiers),
		},
		patcher,
	), needsCode, nil
}

type protoSubruleDefinition struct {
	message model_core.Message[*model_starlark_pb.Subrule_Definition]
}

func NewProtoSubruleDefinition(message model_core.Message[*model_starlark_pb.Subrule_Definition]) SubruleDefinition {
	return &protoSubruleDefinition{
		message: message,
	}
}

func (sd *protoSubruleDefinition) Encode(path map[starlark.Value]struct{}, options *ValueEncodingOptions) (model_core.PatchedMessage[*model_starlark_pb.Subrule_Definition, model_core.CreatedObjectTree], bool, error) {
	panic("rule definition was already encoded previously")
}
