package starlark

import (
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"

	pg_label "github.com/buildbarn/bonanza/pkg/label"
	model_core "github.com/buildbarn/bonanza/pkg/model/core"
	model_starlark_pb "github.com/buildbarn/bonanza/pkg/proto/model/starlark"
	"github.com/buildbarn/bonanza/pkg/starlark/unpack"
	"github.com/buildbarn/bonanza/pkg/storage/object"

	"go.starlark.net/starlark"
	"go.starlark.net/syntax"
)

type SelectGroup struct {
	conditions   map[pg_label.ResolvedLabel]starlark.Value
	defaultValue starlark.Value
	noMatchError string
}

func NewSelectGroup(conditions map[pg_label.ResolvedLabel]starlark.Value, defaultValue starlark.Value, noMatchError string) SelectGroup {
	return SelectGroup{
		conditions:   conditions,
		defaultValue: defaultValue,
		noMatchError: noMatchError,
	}
}

type Select[TReference any, TMetadata model_core.CloneableReferenceMetadata] struct {
	groups                []SelectGroup
	concatenationOperator syntax.Token
}

var (
	_ EncodableValue[object.LocalReference, model_core.CloneableReferenceMetadata] = (*Select[object.LocalReference, model_core.CloneableReferenceMetadata])(nil)
	_ HasLabels                                                                    = (*Select[object.LocalReference, model_core.CloneableReferenceMetadata])(nil)
	_ starlark.HasBinary                                                           = (*Select[object.LocalReference, model_core.CloneableReferenceMetadata])(nil)
	_ starlark.Value                                                               = (*Select[object.LocalReference, model_core.CloneableReferenceMetadata])(nil)
)

func NewSelect[TReference any, TMetadata model_core.CloneableReferenceMetadata](groups []SelectGroup, concatenationOperator syntax.Token) *Select[TReference, TMetadata] {
	return &Select[TReference, TMetadata]{
		groups:                groups,
		concatenationOperator: concatenationOperator,
	}
}

func (Select[TReference, TMetadata]) String() string {
	return "<select>"
}

func (Select[TReference, TMetadata]) Type() string {
	return "select"
}

func (Select[TReference, TMetadata]) Freeze() {}

func (Select[TReference, TMetadata]) Truth() starlark.Bool {
	return starlark.True
}

func (Select[TReference, TMetadata]) Hash(thread *starlark.Thread) (uint32, error) {
	return 0, errors.New("select cannot be hashed")
}

func (s *Select[TReference, TMetadata]) validateConcatenationOperator(op syntax.Token) error {
	if s.concatenationOperator != 0 && op != s.concatenationOperator {
		return fmt.Errorf("cannot perform select %s select %s select", s.concatenationOperator, op)
	}
	return nil
}

func (s *Select[TReference, TMetadata]) Binary(thread *starlark.Thread, op syntax.Token, y starlark.Value, side starlark.Side) (starlark.Value, error) {
	if op != syntax.PLUS && op != syntax.PIPE {
		return nil, errors.New("select only supports operators + and |")
	}
	if err := s.validateConcatenationOperator(op); err != nil {
		return nil, err
	}
	var newGroups []SelectGroup
	switch other := y.(type) {
	case *Select[TReference, TMetadata]:
		if err := other.validateConcatenationOperator(op); err != nil {
			return nil, err
		}
		if side == starlark.Left {
			newGroups = append(append([]SelectGroup(nil), s.groups...), other.groups...)
		} else {
			newGroups = append(append([]SelectGroup(nil), other.groups...), s.groups...)
		}
	default:
		newGroup := NewSelectGroup(nil, y, "")
		if side == starlark.Left {
			newGroups = append(append([]SelectGroup(nil), s.groups...), newGroup)
		} else {
			newGroups = append([]SelectGroup{newGroup}, s.groups...)
		}
	}
	return &Select[TReference, TMetadata]{
		groups:                newGroups,
		concatenationOperator: op,
	}, nil
}

func (s *Select[TReference, TMetadata]) EncodeGroups(path map[starlark.Value]struct{}, options *ValueEncodingOptions[TReference, TMetadata]) (model_core.PatchedMessage[[]*model_starlark_pb.Select_Group, TMetadata], bool, error) {
	groups := make([]*model_starlark_pb.Select_Group, 0, len(s.groups))
	patcher := model_core.NewReferenceMessagePatcher[TMetadata]()
	needsCode := false

	for _, group := range s.groups {
		encodedGroup := model_starlark_pb.Select_Group{
			Conditions: make([]*model_starlark_pb.Select_Condition, 0, len(group.conditions)),
		}

		for _, condition := range slices.SortedFunc(
			maps.Keys(group.conditions),
			func(a, b pg_label.ResolvedLabel) int { return strings.Compare(a.String(), b.String()) },
		) {
			value, valueNeedsCode, err := EncodeValue[TReference, TMetadata](group.conditions[condition], path, nil, options)
			if err != nil {
				return model_core.PatchedMessage[[]*model_starlark_pb.Select_Group, TMetadata]{}, false, err
			}

			encodedGroup.Conditions = append(encodedGroup.Conditions, &model_starlark_pb.Select_Condition{
				ConditionIdentifier: condition.String(),
				Value:               value.Message,
			})
			patcher.Merge(value.Patcher)
			needsCode = needsCode || valueNeedsCode
		}

		if group.defaultValue != nil {
			value, valueNeedsCode, err := EncodeValue[TReference, TMetadata](group.defaultValue, path, nil, options)
			if err != nil {
				return model_core.PatchedMessage[[]*model_starlark_pb.Select_Group, TMetadata]{}, false, err
			}
			needsCode = needsCode || valueNeedsCode

			encodedGroup.NoMatch = &model_starlark_pb.Select_Group_NoMatchValue{
				NoMatchValue: value.Message,
			}
			patcher.Merge(value.Patcher)
		} else if group.noMatchError != "" {
			encodedGroup.NoMatch = &model_starlark_pb.Select_Group_NoMatchError{
				NoMatchError: group.noMatchError,
			}
		}

		groups = append(groups, &encodedGroup)
	}

	return model_core.NewPatchedMessage(groups, patcher), needsCode, nil
}

func (s *Select[TReference, TMetadata]) EncodeValue(path map[starlark.Value]struct{}, currentIdentifier *pg_label.CanonicalStarlarkIdentifier, options *ValueEncodingOptions[TReference, TMetadata]) (model_core.PatchedMessage[*model_starlark_pb.Value, TMetadata], bool, error) {
	groups, needsCode, err := s.EncodeGroups(path, options)
	if err != nil {
		return model_core.PatchedMessage[*model_starlark_pb.Value, TMetadata]{}, false, err
	}

	concatenationOperator := model_starlark_pb.Select_NONE
	switch s.concatenationOperator {
	case syntax.PIPE:
		concatenationOperator = model_starlark_pb.Select_PIPE
	case syntax.PLUS:
		concatenationOperator = model_starlark_pb.Select_PLUS
	}

	return model_core.NewPatchedMessage(
		&model_starlark_pb.Value{
			Kind: &model_starlark_pb.Value_Select{
				Select: &model_starlark_pb.Select{
					Groups:                groups.Message,
					ConcatenationOperator: concatenationOperator,
				},
			},
		},
		groups.Patcher,
	), needsCode, nil
}

func (s *Select[TReference, TMetadata]) VisitLabels(thread *starlark.Thread, path map[starlark.Value]struct{}, visitor func(pg_label.ResolvedLabel) error) error {
	for _, sg := range s.groups {
		for conditionIdentifier, conditionValue := range sg.conditions {
			visitor(conditionIdentifier)
			if err := VisitLabels(thread, conditionValue, path, visitor); err != nil {
				return err
			}
		}
		if sg.defaultValue != nil {
			if err := VisitLabels(thread, sg.defaultValue, path, visitor); err != nil {
				return err
			}
		}
	}
	return nil
}

type selectUnpackerInto[TReference any, TMetadata model_core.CloneableReferenceMetadata] struct {
	valueUnpackerInto unpack.Canonicalizer
}

func NewSelectUnpackerInto[TReference any, TMetadata model_core.CloneableReferenceMetadata](valueUnpackerInto unpack.Canonicalizer) unpack.UnpackerInto[*Select[TReference, TMetadata]] {
	return &selectUnpackerInto[TReference, TMetadata]{
		valueUnpackerInto: valueUnpackerInto,
	}
}

func (ui *selectUnpackerInto[TReference, TMetadata]) UnpackInto(thread *starlark.Thread, v starlark.Value, dst **Select[TReference, TMetadata]) error {
	switch typedV := v.(type) {
	case *Select[TReference, TMetadata]:
		if actual := typedV.concatenationOperator; actual != 0 {
			if expected := ui.valueUnpackerInto.GetConcatenationOperator(); actual != expected {
				if expected == 0 {
					return errors.New("values of this attribute type cannot be concatenated")
				}
				return fmt.Errorf("values of this attribute type need to be concatenated with %s, not %s", expected, actual)
			}
		}

		canonicalizedGroups := make([]SelectGroup, 0, len(typedV.groups))
		for _, group := range typedV.groups {
			canonicalizedConditions := make(map[pg_label.ResolvedLabel]starlark.Value, len(group.conditions))
			for condition, value := range group.conditions {
				canonicalValue, err := ui.valueUnpackerInto.Canonicalize(thread, value)
				if err != nil {
					return fmt.Errorf("canonicalizing condition %#v: %w", condition.String(), err)
				}
				canonicalizedConditions[condition] = canonicalValue
			}

			var defaultValue starlark.Value
			if group.defaultValue != nil {
				var err error
				defaultValue, err = ui.valueUnpackerInto.Canonicalize(thread, group.defaultValue)
				if err != nil {
					return fmt.Errorf("canonicalizing default value: %w", err)
				}
			}

			canonicalizedGroups = append(canonicalizedGroups, NewSelectGroup(canonicalizedConditions, defaultValue, group.noMatchError))
		}
		*dst = &Select[TReference, TMetadata]{
			groups:                canonicalizedGroups,
			concatenationOperator: typedV.concatenationOperator,
		}
	default:
		canonicalValue, err := ui.valueUnpackerInto.Canonicalize(thread, v)
		if err != nil {
			return err
		}
		*dst = &Select[TReference, TMetadata]{
			groups: []SelectGroup{NewSelectGroup(nil, canonicalValue, "")},
		}
	}
	return nil
}

func (ui *selectUnpackerInto[TReference, TMetadata]) Canonicalize(thread *starlark.Thread, v starlark.Value) (starlark.Value, error) {
	var s *Select[TReference, TMetadata]
	if err := ui.UnpackInto(thread, v, &s); err != nil {
		return nil, err
	}
	return s, nil
}

func (selectUnpackerInto[TReference, TMetadata]) GetConcatenationOperator() syntax.Token {
	return 0
}
