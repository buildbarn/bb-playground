package starlark

import (
	"errors"
	"fmt"
	"slices"
	"sort"

	pg_label "github.com/buildbarn/bonanza/pkg/label"
	model_core "github.com/buildbarn/bonanza/pkg/model/core"
	model_starlark_pb "github.com/buildbarn/bonanza/pkg/proto/model/starlark"
	"github.com/buildbarn/bonanza/pkg/storage/object"

	"go.starlark.net/starlark"
)

type Subrule[TReference any, TMetadata model_core.CloneableReferenceMetadata] struct {
	LateNamedValue
	definition SubruleDefinition[TReference, TMetadata]
}

var (
	_ starlark.Callable                                                            = (*Subrule[object.LocalReference, model_core.CloneableReferenceMetadata])(nil)
	_ EncodableValue[object.LocalReference, model_core.CloneableReferenceMetadata] = (*Subrule[object.LocalReference, model_core.CloneableReferenceMetadata])(nil)
	_ NamedGlobal                                                                  = (*Subrule[object.LocalReference, model_core.CloneableReferenceMetadata])(nil)
)

func NewSubrule[TReference any, TMetadata model_core.CloneableReferenceMetadata](identifier *pg_label.CanonicalStarlarkIdentifier, definition SubruleDefinition[TReference, TMetadata]) starlark.Value {
	return &Subrule[TReference, TMetadata]{
		LateNamedValue: LateNamedValue{
			Identifier: identifier,
		},
		definition: definition,
	}
}

func (Subrule[TReference, TMetadata]) String() string {
	return "<subrule>"
}

func (Subrule[TReference, TMetadata]) Type() string {
	return "subrule"
}

func (Subrule[TReference, TMetadata]) Freeze() {}

func (Subrule[TReference, TMetadata]) Truth() starlark.Bool {
	return starlark.True
}

func (Subrule[TReference, TMetadata]) Hash(thread *starlark.Thread) (uint32, error) {
	return 0, errors.New("subrule cannot be hashed")
}

func (sr *Subrule[TReference, TMetadata]) Name() string {
	if sr.Identifier == nil {
		return "subrule"
	}
	return sr.Identifier.GetStarlarkIdentifier().String()
}

const SubruleInvokerKey = "subrule_invoker"

type SubruleInvoker = func(subruleIdentifier pg_label.CanonicalStarlarkIdentifier, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error)

func (sr *Subrule[TReference, TMetadata]) CallInternal(thread *starlark.Thread, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
	subruleInvoker := thread.Local(SubruleInvokerKey)
	if subruleInvoker == nil {
		return nil, errors.New("subrules cannot be invoked from within this context")
	}
	if sr.Identifier == nil {
		return nil, errors.New("subrule does not have a name")
	}
	return subruleInvoker.(SubruleInvoker)(*sr.Identifier, args, kwargs)
}

func (sr *Subrule[TReference, TMetadata]) EncodeValue(path map[starlark.Value]struct{}, currentIdentifier *pg_label.CanonicalStarlarkIdentifier, options *ValueEncodingOptions[TReference, TMetadata]) (model_core.PatchedMessage[*model_starlark_pb.Value, TMetadata], bool, error) {
	if sr.Identifier == nil {
		return model_core.PatchedMessage[*model_starlark_pb.Value, TMetadata]{}, false, errors.New("subrule does not have a name")
	}
	if currentIdentifier == nil || *currentIdentifier != *sr.Identifier {
		// Not the canonical identifier under which this subrule
		// is known. Emit a reference.
		return model_core.NewSimplePatchedMessage[TMetadata](
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
		return model_core.PatchedMessage[*model_starlark_pb.Value, TMetadata]{}, false, err
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

type SubruleDefinition[TReference any, TMetadata model_core.CloneableReferenceMetadata] interface {
	Encode(path map[starlark.Value]struct{}, options *ValueEncodingOptions[TReference, TMetadata]) (model_core.PatchedMessage[*model_starlark_pb.Subrule_Definition, TMetadata], bool, error)
}

type starlarkSubruleDefinition[TReference any, TMetadata model_core.CloneableReferenceMetadata] struct {
	attrs          map[pg_label.StarlarkIdentifier]*Attr[TReference, TMetadata]
	implementation NamedFunction[TReference, TMetadata]
	subrules       []*Subrule[TReference, TMetadata]
}

func NewStarlarkSubruleDefinition[TReference any, TMetadata model_core.CloneableReferenceMetadata](
	attrs map[pg_label.StarlarkIdentifier]*Attr[TReference, TMetadata],
	implementation NamedFunction[TReference, TMetadata],
	subrules []*Subrule[TReference, TMetadata],
) SubruleDefinition[TReference, TMetadata] {
	return &starlarkSubruleDefinition[TReference, TMetadata]{
		attrs:          attrs,
		implementation: implementation,
		subrules:       subrules,
	}
}

func (sd *starlarkSubruleDefinition[TReference, TMetadata]) Encode(path map[starlark.Value]struct{}, options *ValueEncodingOptions[TReference, TMetadata]) (model_core.PatchedMessage[*model_starlark_pb.Subrule_Definition, TMetadata], bool, error) {
	patcher := model_core.NewReferenceMessagePatcher[TMetadata]()

	implementation, needsCode, err := sd.implementation.Encode(path, options)
	if err != nil {
		return model_core.PatchedMessage[*model_starlark_pb.Subrule_Definition, TMetadata]{}, false, err
	}
	patcher.Merge(implementation.Patcher)

	namedAttrs, namedAttrsNeedCode, err := encodeNamedAttrs(sd.attrs, path, options)
	if err != nil {
		return model_core.PatchedMessage[*model_starlark_pb.Subrule_Definition, TMetadata]{}, false, err
	}
	needsCode = needsCode || namedAttrsNeedCode
	patcher.Merge(namedAttrs.Patcher)

	subruleIdentifiers := make([]string, 0, len(sd.subrules))
	for i, subrule := range sd.subrules {
		if subrule.Identifier == nil {
			return model_core.PatchedMessage[*model_starlark_pb.Subrule_Definition, TMetadata]{}, false, fmt.Errorf("subrule at index %d does not have an identifier", i)
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

type protoSubruleDefinition[TReference any, TMetadata model_core.CloneableReferenceMetadata] struct {
	message model_core.Message[*model_starlark_pb.Subrule_Definition, TReference]
}

func NewProtoSubruleDefinition[TReference any, TMetadata model_core.CloneableReferenceMetadata](message model_core.Message[*model_starlark_pb.Subrule_Definition, TReference]) SubruleDefinition[TReference, TMetadata] {
	return &protoSubruleDefinition[TReference, TMetadata]{
		message: message,
	}
}

func (sd *protoSubruleDefinition[TReference, TMetadata]) Encode(path map[starlark.Value]struct{}, options *ValueEncodingOptions[TReference, TMetadata]) (model_core.PatchedMessage[*model_starlark_pb.Subrule_Definition, TMetadata], bool, error) {
	panic("rule definition was already encoded previously")
}
