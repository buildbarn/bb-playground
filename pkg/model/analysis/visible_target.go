package analysis

import (
	"context"
	"errors"
	"fmt"

	"github.com/buildbarn/bb-playground/pkg/evaluation"
	"github.com/buildbarn/bb-playground/pkg/label"
	model_core "github.com/buildbarn/bb-playground/pkg/model/core"
	model_analysis_pb "github.com/buildbarn/bb-playground/pkg/proto/model/analysis"
	model_starlark_pb "github.com/buildbarn/bb-playground/pkg/proto/model/starlark"
	"github.com/buildbarn/bb-playground/pkg/storage/dag"
)

type getValueFromSelectGroupEnvironment interface {
	GetSelectValue(*model_analysis_pb.Select_Key) model_core.Message[*model_analysis_pb.Select_Value]
}

func getValueFromSelectGroup(e getValueFromSelectGroupEnvironment, selectGroup *model_starlark_pb.Select_Group, permitNoMatch bool) (*model_starlark_pb.Value, error) {
	if len(selectGroup.Conditions) > 0 {
		conditionIdentifiers := make([]string, 0, len(selectGroup.Conditions))
		for _, condition := range selectGroup.Conditions {
			conditionIdentifiers = append(conditionIdentifiers, condition.ConditionIdentifier)
		}
		selectValue := e.GetSelectValue(&model_analysis_pb.Select_Key{
			ConditionIdentifiers: conditionIdentifiers,
		})
		if !selectValue.IsSet() {
			return nil, evaluation.ErrMissingDependency
		}
		if len(selectValue.Message.ConditionIndices) > 0 {
			return nil, errors.New("TODO: Implement conditions matching!")
		}
	}

	switch noMatch := selectGroup.NoMatch.(type) {
	case *model_starlark_pb.Select_Group_NoMatchValue:
		return noMatch.NoMatchValue, nil
	case *model_starlark_pb.Select_Group_NoMatchError:
		if permitNoMatch {
			return nil, nil
		}
		return nil, errors.New(noMatch.NoMatchError)
	case nil:
		if permitNoMatch {
			return nil, nil
		}
		return nil, errors.New("none of the conditions matched, and no default condition or no-match error is specified")
	default:
		return nil, errors.New("select group does not contain a valid no-match behavior")
	}
}

func checkVisibility(visibility model_core.Message[*model_starlark_pb.PackageGroup], fromPackage label.CanonicalPackage) error {
	// TODO: Implement actual visibility checks!
	return nil
}

func (c *baseComputer) ComputeVisibleTargetValue(ctx context.Context, key *model_analysis_pb.VisibleTarget_Key, e VisibleTargetEnvironment) (PatchedVisibleTargetValue, error) {
	fromPackage, err := label.NewCanonicalPackage(key.FromPackage)
	if err != nil {
		return PatchedVisibleTargetValue{}, fmt.Errorf("invalid from package: %w", err)
	}

	targetValue := e.GetTargetValue(&model_analysis_pb.Target_Key{
		Label: key.ToLabel,
	})
	if !targetValue.IsSet() {
		return PatchedVisibleTargetValue{}, evaluation.ErrMissingDependency
	}

	switch definition := targetValue.Message.Definition.GetKind().(type) {
	case *model_starlark_pb.Target_Definition_Alias:
		if err := checkVisibility(
			model_core.Message[*model_starlark_pb.PackageGroup]{
				Message:            definition.Alias.Visibility,
				OutgoingReferences: targetValue.OutgoingReferences,
			},
			fromPackage,
		); err != nil {
			return PatchedVisibleTargetValue{}, err
		}

		// If the actual target is a select(), evaluate it.
		actualSelectGroup := definition.Alias.Actual
		if actualSelectGroup == nil {
			return PatchedVisibleTargetValue{}, errors.New("alias has no actual target")
		}
		actualValue, err := getValueFromSelectGroup(e, actualSelectGroup, key.PermitAliasNoMatch)
		if err != nil {
			return PatchedVisibleTargetValue{}, err
		}
		if actualValue == nil {
			// None of the conditions match, and the caller
			// is fine with that.
			return model_core.NewSimplePatchedMessage[dag.ObjectContentsWalker](&model_analysis_pb.VisibleTarget_Value{}), nil
		}
		actualLabel, ok := actualValue.Kind.(*model_starlark_pb.Value_Label)
		if !ok {
			return PatchedVisibleTargetValue{}, errors.New("actual target of alias is not a label")
		}

		// The actual target may also be an alias.
		toLabel, err := label.NewCanonicalLabel(key.ToLabel)
		if err != nil {
			return PatchedVisibleTargetValue{}, fmt.Errorf("invalid to label: %w", err)
		}
		actualVisibleTargetValue := e.GetVisibleTargetValue(&model_analysis_pb.VisibleTarget_Key{
			FromPackage:        toLabel.GetCanonicalPackage().String(),
			ToLabel:            actualLabel.Label,
			PermitAliasNoMatch: key.PermitAliasNoMatch,
		})
		if !actualVisibleTargetValue.IsSet() {
			return PatchedVisibleTargetValue{}, evaluation.ErrMissingDependency
		}
		return model_core.NewSimplePatchedMessage[dag.ObjectContentsWalker](actualVisibleTargetValue.Message), nil
	case *model_starlark_pb.Target_Definition_RuleTarget:
		inheritableAttrs := definition.RuleTarget.InheritableAttrs
		if inheritableAttrs == nil {
			return PatchedVisibleTargetValue{}, errors.New("rule target has no inheritable attrs")
		}
		if err := checkVisibility(
			model_core.Message[*model_starlark_pb.PackageGroup]{
				Message:            inheritableAttrs.Visibility,
				OutgoingReferences: targetValue.OutgoingReferences,
			},
			fromPackage,
		); err != nil {
			return PatchedVisibleTargetValue{}, err
		}

		// Found the definitive target.
		return model_core.NewSimplePatchedMessage[dag.ObjectContentsWalker](
			&model_analysis_pb.VisibleTarget_Value{
				Label: key.ToLabel,
			},
		), nil
	case *model_starlark_pb.Target_Definition_SourceFileTarget:
		if err := checkVisibility(
			model_core.Message[*model_starlark_pb.PackageGroup]{
				Message:            definition.SourceFileTarget.Visibility,
				OutgoingReferences: targetValue.OutgoingReferences,
			},
			fromPackage,
		); err != nil {
			return PatchedVisibleTargetValue{}, err
		}

		// Found the definitive target.
		return model_core.NewSimplePatchedMessage[dag.ObjectContentsWalker](
			&model_analysis_pb.VisibleTarget_Value{
				Label: key.ToLabel,
			},
		), nil
	default:
		return PatchedVisibleTargetValue{}, errors.New("not a source file target, rule target or alias")
	}
}
