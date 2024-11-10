package analysis

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"

	"github.com/buildbarn/bb-playground/pkg/evaluation"
	"github.com/buildbarn/bb-playground/pkg/label"
	model_core "github.com/buildbarn/bb-playground/pkg/model/core"
	model_starlark "github.com/buildbarn/bb-playground/pkg/model/starlark"
	model_analysis_pb "github.com/buildbarn/bb-playground/pkg/proto/model/analysis"
	model_starlark_pb "github.com/buildbarn/bb-playground/pkg/proto/model/starlark"
	"github.com/buildbarn/bb-playground/pkg/starlark/unpack"
	"github.com/buildbarn/bb-playground/pkg/storage/dag"

	"go.starlark.net/starlark"
	"go.starlark.net/starlarkstruct"
)

func (c *baseComputer) ComputeConfiguredTargetValue(ctx context.Context, key *model_analysis_pb.ConfiguredTarget_Key, e ConfiguredTargetEnvironment) (PatchedConfiguredTargetValue, error) {
	targetLabel, err := label.NewCanonicalLabel(key.Label)
	if err != nil {
		return PatchedConfiguredTargetValue{}, fmt.Errorf("invalid target label: %w", err)
	}
	targetValue := e.GetTargetValue(&model_analysis_pb.Target_Key{
		Label: targetLabel.String(),
	})
	if !targetValue.IsSet() {
		return PatchedConfiguredTargetValue{}, evaluation.ErrMissingDependency
	}

	targetKind, ok := targetValue.Message.Definition.GetKind().(*model_starlark_pb.Target_Definition_RuleTarget)
	if !ok {
		return PatchedConfiguredTargetValue{}, errors.New("only rule targets can be configured")
	}
	ruleTarget := targetKind.RuleTarget

	configuredRule, gotConfiguredRule := e.GetConfiguredRuleObjectValue(&model_analysis_pb.ConfiguredRuleObject_Key{
		Identifier: ruleTarget.RuleIdentifier,
	})
	if !gotConfiguredRule {
		return PatchedConfiguredTargetValue{}, evaluation.ErrMissingDependency
	}

	allBuiltinsModulesNames := e.GetBuiltinsModuleNamesValue(&model_analysis_pb.BuiltinsModuleNames_Key{})
	if !allBuiltinsModulesNames.IsSet() {
		return PatchedConfiguredTargetValue{}, evaluation.ErrMissingDependency
	}

	attrs := starlark.StringDict{}
	attrValues := ruleTarget.AttrValues
	missingDependencies := false
	for _, publicAttr := range configuredRule.Attrs.Public {
		if len(attrValues) > 0 && attrValues[0].Name == publicAttr.Name {
			var partValues []starlark.Value
			for selectGroupIndex, selectGroup := range attrValues[0].ValueParts {
				if len(selectGroup.Conditions) > 0 {
					panic("TODO")
				}

				switch noMatch := selectGroup.NoMatch.(type) {
				case *model_starlark_pb.Select_Group_NoMatchValue:
					partValue, err := model_starlark.DecodeValue(
						model_core.Message[*model_starlark_pb.Value]{
							Message:            noMatch.NoMatchValue,
							OutgoingReferences: targetValue.OutgoingReferences,
						},
						/* currentIdentifier = */ nil,
						c.getValueDecodingOptions(ctx, func(canonicalLabel label.CanonicalLabel) (starlark.Value, error) {
							referencedTarget := e.GetConfiguredTargetValue(&model_analysis_pb.ConfiguredTarget_Key{
								Label: canonicalLabel.String(),
							})
							if !referencedTarget.IsSet() {
								missingDependencies = true
								return starlark.None, nil
							}
							providerInstances := make([]model_starlark.ProviderInstance, 0, len(referencedTarget.Message.ProviderInstances))
							for _, providerInstance := range referencedTarget.Message.ProviderInstances {
								pi, err := model_starlark.DecodeProviderInstance(
									model_core.Message[*model_starlark_pb.Struct]{
										Message:            providerInstance,
										OutgoingReferences: referencedTarget.OutgoingReferences,
									},
									c.getValueDecodingOptions(ctx, func(canonicalLabel label.CanonicalLabel) (starlark.Value, error) {
										return model_starlark.NewLabel(canonicalLabel), nil
									}),
								)
								if err != nil {
									return nil, err
								}
								providerInstances = append(providerInstances, pi)
							}
							return model_starlark.NewTargetReference(canonicalLabel, providerInstances), nil
						}),
					)
					if err != nil {
						return PatchedConfiguredTargetValue{}, err
					}
					partValues = append(partValues, partValue)
				case *model_starlark_pb.Select_Group_NoMatchError:
					panic("TODO")
				case nil:
					panic("TODO")
				default:
					return PatchedConfiguredTargetValue{}, fmt.Errorf("invalid type for no-match of group %d of attribute %s", selectGroupIndex, publicAttr.Name)
				}
			}

			if len(partValues) != 1 {
				panic("TODO: COMBINE AND VALIDATE!")
			}
			attrs[publicAttr.Name] = partValues[0]

			attrValues = attrValues[1:]
		} else if d := publicAttr.Default; d != nil {
			attrs[publicAttr.Name] = d
		} else {
			return PatchedConfiguredTargetValue{}, fmt.Errorf("missing value for mandatory attribute %#v", publicAttr.Name)
		}
	}
	if len(attrValues) > 0 {
		return PatchedConfiguredTargetValue{}, fmt.Errorf("unknown attribute %#v", attrValues[0].Name)
	}
	for name, value := range configuredRule.Attrs.Private {
		attrs[name] = value
	}

	if missingDependencies {
		return PatchedConfiguredTargetValue{}, evaluation.ErrMissingDependency
	}

	thread := c.newStarlarkThread(ctx, e, allBuiltinsModulesNames.Message.BuiltinsModuleNames)
	returnValue, err := starlark.Call(
		thread,
		configuredRule.Implementation,
		/* args = */ starlark.Tuple{
			starlarkstruct.FromStringDict(starlarkstruct.Default, starlark.StringDict{
				"attr":  starlarkstruct.FromStringDict(starlarkstruct.Default, attrs),
				"label": model_starlark.NewLabel(targetLabel),
			}),
		},
		/* kwargs = */ nil,
	)
	if err != nil {
		if !errors.Is(err, evaluation.ErrMissingDependency) {
			var evalErr *starlark.EvalError
			if errors.As(err, &evalErr) {
				panic(evalErr.Backtrace())
			} else {
				panic(err.Error())
			}
		}
		return PatchedConfiguredTargetValue{}, err
	}

	var providerInstances []model_starlark.ProviderInstance
	if err := unpack.IfNotNone(
		unpack.List(unpack.Type[model_starlark.ProviderInstance]("struct")),
	).UnpackInto(thread, returnValue, &providerInstances); err != nil {
		return PatchedConfiguredTargetValue{}, err
	}

	encodedProviderInstances := map[string]model_core.PatchedMessage[*model_starlark_pb.Struct, dag.ObjectContentsWalker]{}
	for _, providerInstance := range providerInstances {
		v, _, err := providerInstance.EncodeStruct(map[starlark.Value]struct{}{}, c.getValueEncodingOptions(targetLabel))
		if err != nil {
			return PatchedConfiguredTargetValue{}, err
		}
		providerIdentifier := v.Message.ProviderIdentifier
		if _, ok := encodedProviderInstances[providerIdentifier]; ok {
			return PatchedConfiguredTargetValue{}, fmt.Errorf("implementation function returned multiple structs for provider %#v", providerIdentifier)
		}
		encodedProviderInstances[v.Message.ProviderIdentifier] = v
	}

	patcher := model_core.NewReferenceMessagePatcher[dag.ObjectContentsWalker]()
	sortedProviderInstances := make([]*model_starlark_pb.Struct, 0, len(encodedProviderInstances))
	for _, providerIdentifier := range slices.Sorted(maps.Keys(encodedProviderInstances)) {
		providerInstance := encodedProviderInstances[providerIdentifier]
		sortedProviderInstances = append(sortedProviderInstances, providerInstance.Message)
		patcher.Merge(providerInstance.Patcher)
	}

	return model_core.PatchedMessage[*model_analysis_pb.ConfiguredTarget_Value, dag.ObjectContentsWalker]{
		Message: &model_analysis_pb.ConfiguredTarget_Value{
			ProviderInstances: sortedProviderInstances,
		},
		Patcher: patcher,
	}, nil
}
