package starlark

import (
	"errors"
	"fmt"

	pg_label "github.com/buildbarn/bonanza/pkg/label"
	model_core "github.com/buildbarn/bonanza/pkg/model/core"
	model_starlark_pb "github.com/buildbarn/bonanza/pkg/proto/model/starlark"
	"github.com/buildbarn/bonanza/pkg/starlark/unpack"
	"github.com/buildbarn/bonanza/pkg/storage/dag"

	"google.golang.org/protobuf/types/known/emptypb"

	"go.starlark.net/starlark"
	"go.starlark.net/syntax"
)

type Transition struct {
	TransitionDefinition
}

var (
	_ EncodableValue = &Transition{}
	_ NamedGlobal    = &Transition{}
)

func NewTransition(definition TransitionDefinition) starlark.Value {
	return &Transition{
		TransitionDefinition: definition,
	}
}

func (Transition) String() string {
	return "<transition>"
}

func (Transition) Type() string {
	return "transition"
}

func (Transition) Freeze() {}

func (Transition) Truth() starlark.Bool {
	return starlark.True
}

func (Transition) Hash(thread *starlark.Thread) (uint32, error) {
	return 0, errors.New("transition cannot be hashed")
}

type TransitionDefinition interface {
	EncodableValue
	AssignIdentifier(identifier pg_label.CanonicalStarlarkIdentifier)
	EncodeReference() (*model_starlark_pb.Transition_Reference, error)
	GetUserDefinedTransitionIdentifier() (string, error)
}

type referenceTransitionDefinition struct {
	reference *model_starlark_pb.Transition_Reference
}

func NewReferenceTransitionDefinition(reference *model_starlark_pb.Transition_Reference) TransitionDefinition {
	return &referenceTransitionDefinition{
		reference: reference,
	}
}

func (referenceTransitionDefinition) AssignIdentifier(identifier pg_label.CanonicalStarlarkIdentifier) {
}

func (td *referenceTransitionDefinition) EncodeReference() (*model_starlark_pb.Transition_Reference, error) {
	return td.reference, nil
}

func (td *referenceTransitionDefinition) GetUserDefinedTransitionIdentifier() (string, error) {
	userDefined, ok := td.reference.Kind.(*model_starlark_pb.Transition_Reference_UserDefined)
	if !ok {
		return "", errors.New("transition is not a user-defined transition")
	}
	return userDefined.UserDefined, nil
}

func (td *referenceTransitionDefinition) EncodeValue(path map[starlark.Value]struct{}, currentIdentifier *pg_label.CanonicalStarlarkIdentifier, options *ValueEncodingOptions) (model_core.PatchedMessage[*model_starlark_pb.Value, dag.ObjectContentsWalker], bool, error) {
	return model_core.NewSimplePatchedMessage[dag.ObjectContentsWalker](
		&model_starlark_pb.Value{
			Kind: &model_starlark_pb.Value_Transition{
				Transition: &model_starlark_pb.Transition{
					Kind: &model_starlark_pb.Transition_Reference_{
						Reference: td.reference,
					},
				},
			},
		},
	), false, nil
}

var (
	DefaultExecGroupTransitionDefinition = NewReferenceTransitionDefinition(
		&model_starlark_pb.Transition_Reference{
			Kind: &model_starlark_pb.Transition_Reference_ExecGroup{
				ExecGroup: "",
			},
		},
	)
	NoneTransitionDefinition = NewReferenceTransitionDefinition(
		&model_starlark_pb.Transition_Reference{
			Kind: &model_starlark_pb.Transition_Reference_None{
				None: &emptypb.Empty{},
			},
		},
	)
	TargetTransitionDefinition = NewReferenceTransitionDefinition(
		&model_starlark_pb.Transition_Reference{
			Kind: &model_starlark_pb.Transition_Reference_Target{
				Target: &emptypb.Empty{},
			},
		},
	)
	UnconfiguredTransitionDefinition = NewReferenceTransitionDefinition(
		&model_starlark_pb.Transition_Reference{
			Kind: &model_starlark_pb.Transition_Reference_Unconfigured{
				Unconfigured: &emptypb.Empty{},
			},
		},
	)
)

type userDefinedTransitionDefinition struct {
	LateNamedValue

	implementation NamedFunction
	inputs         []string
	outputs        []string
}

func NewUserDefinedTransitionDefinition(identifier *pg_label.CanonicalStarlarkIdentifier, implementation NamedFunction, inputs, outputs []string) TransitionDefinition {
	return &userDefinedTransitionDefinition{
		LateNamedValue: LateNamedValue{
			Identifier: identifier,
		},
		implementation: implementation,
		inputs:         inputs,
		outputs:        outputs,
	}
}

func (td *userDefinedTransitionDefinition) EncodeReference() (*model_starlark_pb.Transition_Reference, error) {
	if td.Identifier == nil {
		return nil, errors.New("transition does not have a name")
	}
	return &model_starlark_pb.Transition_Reference{
		Kind: &model_starlark_pb.Transition_Reference_UserDefined{
			UserDefined: td.Identifier.String(),
		},
	}, nil
}

func (td *userDefinedTransitionDefinition) GetUserDefinedTransitionIdentifier() (string, error) {
	if td.Identifier == nil {
		return "", errors.New("transition does not have a name")
	}
	return td.Identifier.String(), nil
}

func (td *userDefinedTransitionDefinition) EncodeValue(path map[starlark.Value]struct{}, currentIdentifier *pg_label.CanonicalStarlarkIdentifier, options *ValueEncodingOptions) (model_core.PatchedMessage[*model_starlark_pb.Value, dag.ObjectContentsWalker], bool, error) {
	if td.Identifier == nil {
		return model_core.PatchedMessage[*model_starlark_pb.Value, dag.ObjectContentsWalker]{}, false, errors.New("transition does not have a name")
	}
	if currentIdentifier == nil || *currentIdentifier != *td.Identifier {
		// Not the canonical identifier under which this
		// transition is known. Emit a reference.
		return model_core.NewSimplePatchedMessage[dag.ObjectContentsWalker](
			&model_starlark_pb.Value{
				Kind: &model_starlark_pb.Value_Transition{
					Transition: &model_starlark_pb.Transition{
						Kind: &model_starlark_pb.Transition_Reference_{
							Reference: &model_starlark_pb.Transition_Reference{
								Kind: &model_starlark_pb.Transition_Reference_UserDefined{
									UserDefined: td.Identifier.String(),
								},
							},
						},
					},
				},
			},
		), false, nil
	}

	implementation, needsCode, err := td.implementation.Encode(path, options)
	if err != nil {
		return model_core.PatchedMessage[*model_starlark_pb.Value, dag.ObjectContentsWalker]{}, false, err
	}
	return model_core.NewPatchedMessage(
		&model_starlark_pb.Value{
			Kind: &model_starlark_pb.Value_Transition{
				Transition: &model_starlark_pb.Transition{
					Kind: &model_starlark_pb.Transition_Definition_{
						Definition: &model_starlark_pb.Transition_Definition{
							Implementation: implementation.Message,
							Inputs:         td.inputs,
							Outputs:        td.outputs,
						},
					},
				},
			},
		},
		implementation.Patcher,
	), needsCode, nil
}

type transitionDefinitionUnpackerInto struct{}

var TransitionDefinitionUnpackerInto unpack.UnpackerInto[TransitionDefinition] = transitionDefinitionUnpackerInto{}

func (transitionDefinitionUnpackerInto) UnpackInto(thread *starlark.Thread, v starlark.Value, dst *TransitionDefinition) error {
	switch typedV := v.(type) {
	case starlark.String:
		switch typedV {
		case "exec":
			*dst = DefaultExecGroupTransitionDefinition
		case "target":
			*dst = TargetTransitionDefinition
		default:
			return fmt.Errorf("got %#v, want \"exec\" or \"target\"", typedV)
		}
		return nil
	case *Transition:
		*dst = typedV.TransitionDefinition
		return nil
	default:
		return fmt.Errorf("got %s, want transition or str", v.Type())
	}
}

func (ui transitionDefinitionUnpackerInto) Canonicalize(thread *starlark.Thread, v starlark.Value) (starlark.Value, error) {
	var td TransitionDefinition
	if err := ui.UnpackInto(thread, v, &td); err != nil {
		return nil, err
	}
	return NewTransition(td), nil
}

func (transitionDefinitionUnpackerInto) GetConcatenationOperator() syntax.Token {
	return 0
}
