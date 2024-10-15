package starlark

import (
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"

	pg_label "github.com/buildbarn/bb-playground/pkg/label"
	model_core "github.com/buildbarn/bb-playground/pkg/model/core"
	model_starlark_pb "github.com/buildbarn/bb-playground/pkg/proto/model/starlark"
	"github.com/buildbarn/bb-playground/pkg/starlark/unpack"
	"github.com/buildbarn/bb-playground/pkg/storage/dag"

	"go.starlark.net/starlark"
	"go.starlark.net/syntax"
)

var defaultConditionLabel = pg_label.MustNewCanonicalLabel("@@todo+//conditions:default")

type selectGroup struct {
	conditions   map[pg_label.CanonicalLabel]starlark.Value
	noMatchError string
}

type Select struct {
	groups                []selectGroup
	concatenationOperator syntax.Token
}

var (
	_ starlark.Value     = &Select{}
	_ starlark.HasBinary = &Select{}
	_ EncodableValue     = &rule{}
)

func NewSelect(conditions map[pg_label.CanonicalLabel]starlark.Value, noMatchError string) *Select {
	return &Select{
		groups: []selectGroup{{
			conditions:   conditions,
			noMatchError: noMatchError,
		}},
	}
}

func (Select) String() string {
	return "<select>"
}

func (Select) Type() string {
	return "select"
}

func (Select) Freeze() {}

func (Select) Truth() starlark.Bool {
	return starlark.True
}

func (Select) Hash() (uint32, error) {
	// TODO!
	return 0, nil
}

func (s *Select) validateConcatenationOperator(op syntax.Token) error {
	if s.concatenationOperator != 0 && op != s.concatenationOperator {
		return fmt.Errorf("cannot perform select %s select %s select", s.concatenationOperator, op)
	}
	return nil
}

func (s *Select) Binary(op syntax.Token, y starlark.Value, side starlark.Side) (starlark.Value, error) {
	if op != syntax.PLUS && op != syntax.PIPE {
		return nil, errors.New("select only supports operators + and |")
	}
	if err := s.validateConcatenationOperator(op); err != nil {
		return nil, err
	}
	var newGroups []selectGroup
	switch other := y.(type) {
	case *Select:
		if err := other.validateConcatenationOperator(op); err != nil {
			return nil, err
		}
		if side == starlark.Left {
			newGroups = append(append([]selectGroup(nil), s.groups...), other.groups...)
		} else {
			newGroups = append(append([]selectGroup(nil), other.groups...), s.groups...)
		}
	default:
		newGroup := selectGroup{
			conditions: map[pg_label.CanonicalLabel]starlark.Value{
				defaultConditionLabel: y,
			},
		}
		if side == starlark.Left {
			newGroups = append(append([]selectGroup(nil), s.groups...), newGroup)
		} else {
			newGroups = append([]selectGroup{newGroup}, s.groups...)
		}
	}
	return &Select{
		groups:                newGroups,
		concatenationOperator: op,
	}, nil
}

func (s *Select) EncodeGroups(path map[starlark.Value]struct{}, options *ValueEncodingOptions) (model_core.PatchedMessage[[]*model_starlark_pb.Select_Group, dag.ObjectContentsWalker], bool, error) {
	groups := make([]*model_starlark_pb.Select_Group, 0, len(s.groups))
	patcher := model_core.NewReferenceMessagePatcher[dag.ObjectContentsWalker]()
	needsCode := false

	for _, group := range s.groups {
		encodedGroup := model_starlark_pb.Select_Group{
			Conditions:   make([]*model_starlark_pb.Select_Condition, 0, len(group.conditions)),
			NoMatchError: group.noMatchError,
		}
		for _, condition := range slices.SortedFunc(
			maps.Keys(group.conditions),
			func(a, b pg_label.CanonicalLabel) int { return strings.Compare(a.String(), b.String()) },
		) {
			value, valueNeedsCode, err := EncodeValue(group.conditions[condition], path, nil, options)
			if err != nil {
				return model_core.PatchedMessage[[]*model_starlark_pb.Select_Group, dag.ObjectContentsWalker]{}, false, err
			}
			encodedGroup.Conditions = append(encodedGroup.Conditions, &model_starlark_pb.Select_Condition{
				ConditionIdentifier: condition.String(),
				Value:               value.Message,
			})
			patcher.Merge(value.Patcher)
			needsCode = needsCode || valueNeedsCode
		}
		groups = append(groups, &encodedGroup)
	}

	return model_core.PatchedMessage[[]*model_starlark_pb.Select_Group, dag.ObjectContentsWalker]{
		Message: groups,
		Patcher: patcher,
	}, needsCode, nil
}

func (s *Select) EncodeValue(path map[starlark.Value]struct{}, currentIdentifier *pg_label.CanonicalStarlarkIdentifier, options *ValueEncodingOptions) (model_core.PatchedMessage[*model_starlark_pb.Value, dag.ObjectContentsWalker], bool, error) {
	groups, needsCode, err := s.EncodeGroups(path, options)
	if err != nil {
		return model_core.PatchedMessage[*model_starlark_pb.Value, dag.ObjectContentsWalker]{}, false, err
	}

	concatenationOperator := model_starlark_pb.Select_NONE
	switch s.concatenationOperator {
	case syntax.PIPE:
		concatenationOperator = model_starlark_pb.Select_PIPE
	case syntax.PLUS:
		concatenationOperator = model_starlark_pb.Select_PLUS
	}

	return model_core.PatchedMessage[*model_starlark_pb.Value, dag.ObjectContentsWalker]{
		Message: &model_starlark_pb.Value{
			Kind: &model_starlark_pb.Value_Select{
				Select: &model_starlark_pb.Select{
					Groups:                groups.Message,
					ConcatenationOperator: concatenationOperator,
				},
			},
		},
		Patcher: groups.Patcher,
	}, needsCode, nil
}

type selectUnpackerInto struct {
	valueUnpackerInto unpack.Canonicalizer
}

func NewSelectUnpackerInto(valueUnpackerInto unpack.Canonicalizer) unpack.UnpackerInto[*Select] {
	return &selectUnpackerInto{
		valueUnpackerInto: valueUnpackerInto,
	}
}

func (ui *selectUnpackerInto) UnpackInto(thread *starlark.Thread, v starlark.Value, dst **Select) error {
	switch typedV := v.(type) {
	case *Select:
		if actual := typedV.concatenationOperator; actual != 0 {
			if expected := ui.valueUnpackerInto.GetConcatenationOperator(); actual != expected {
				if expected == 0 {
					return errors.New("values of this attribute type cannot be concatenated")
				}
				return fmt.Errorf("values of this attribute type need to be concatenated with %s, not %s", expected, actual)
			}
		}

		canonicalizedGroups := make([]selectGroup, 0, len(typedV.groups))
		for _, group := range typedV.groups {
			canonicalizedConditions := make(map[pg_label.CanonicalLabel]starlark.Value, len(group.conditions))
			for condition, value := range group.conditions {
				canonicalValue, err := ui.valueUnpackerInto.Canonicalize(thread, value)
				if err != nil {
					return fmt.Errorf("canonicalizing condition %#v: %w", condition.String(), err)
				}
				canonicalizedConditions[condition] = canonicalValue
			}
			canonicalizedGroups = append(canonicalizedGroups, selectGroup{
				conditions:   canonicalizedConditions,
				noMatchError: group.noMatchError,
			})
		}
		*dst = &Select{
			groups:                canonicalizedGroups,
			concatenationOperator: typedV.concatenationOperator,
		}
	default:
		canonicalV, err := ui.valueUnpackerInto.Canonicalize(thread, v)
		if err != nil {
			return err
		}
		*dst = &Select{
			groups: []selectGroup{{
				conditions: map[pg_label.CanonicalLabel]starlark.Value{
					defaultConditionLabel: canonicalV,
				},
			}},
		}
	}
	return nil
}

func (ui *selectUnpackerInto) Canonicalize(thread *starlark.Thread, v starlark.Value) (starlark.Value, error) {
	var s *Select
	if err := ui.UnpackInto(thread, v, &s); err != nil {
		return nil, err
	}
	return s, nil
}

func (selectUnpackerInto) GetConcatenationOperator() syntax.Token {
	return 0
}
