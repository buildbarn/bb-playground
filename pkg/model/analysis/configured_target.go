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
		return model_core.NewSimplePatchedMessage[dag.ObjectContentsWalker](&model_analysis_pb.ConfiguredTarget_Value{
			Result: &model_analysis_pb.ConfiguredTarget_Value_Failure{
				Failure: fmt.Sprintf("invalid target label %#v: %s", key.Label, err),
			},
		}), nil
	}
	targetValue := e.GetTargetValue(&model_analysis_pb.Target_Key{
		Label: targetLabel.String(),
	})
	if !targetValue.IsSet() {
		return PatchedConfiguredTargetValue{}, evaluation.ErrMissingDependency
	}

	var targetDefinition model_core.Message[*model_starlark_pb.TargetDefinition]
	switch resultType := targetValue.Message.Result.(type) {
	case *model_analysis_pb.Target_Value_Success:
		targetDefinition = model_core.Message[*model_starlark_pb.TargetDefinition]{
			Message:            resultType.Success,
			OutgoingReferences: targetValue.OutgoingReferences,
		}
	case *model_analysis_pb.Target_Value_Failure:
		return model_core.NewSimplePatchedMessage[dag.ObjectContentsWalker](&model_analysis_pb.ConfiguredTarget_Value{
			Result: &model_analysis_pb.ConfiguredTarget_Value_Failure{
				Failure: resultType.Failure,
			},
		}), nil
	default:
		return PatchedConfiguredTargetValue{}, errors.New("package value has an unknown result type")
	}

	targetKind, ok := targetDefinition.Message.Kind.(*model_starlark_pb.TargetDefinition_RuleTarget)
	if !ok {
		return model_core.NewSimplePatchedMessage[dag.ObjectContentsWalker](&model_analysis_pb.ConfiguredTarget_Value{
			Result: &model_analysis_pb.ConfiguredTarget_Value_Failure{
				Failure: "Only rule targets can be configured",
			},
		}), nil
	}
	ruleTarget := targetKind.RuleTarget

	configuredRule, err := e.GetConfiguredRuleObjectValue(&model_analysis_pb.ConfiguredRuleObject_Key{
		Identifier: ruleTarget.RuleIdentifier,
	})
	if err != nil {
		return PatchedConfiguredTargetValue{}, err
	}

	allBuiltinsModulesNames := e.GetBuiltinsModuleNamesValue(&model_analysis_pb.BuiltinsModuleNames_Key{})
	if !allBuiltinsModulesNames.IsSet() {
		return PatchedConfiguredTargetValue{}, evaluation.ErrMissingDependency
	}

	attrs := starlark.StringDict{}
	attrValues := ruleTarget.AttrValues
	missingDependencies := false
	for _, publicAttr := range configuredRule.PublicAttrs {
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
							switch result := referencedTarget.Message.Result.(type) {
							case *model_analysis_pb.ConfiguredTarget_Value_Success_:
								providerInstances := make([]model_starlark.ProviderInstance, 0, len(result.Success.ProviderInstances))
								for _, providerInstance := range result.Success.ProviderInstances {
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
							case *model_analysis_pb.ConfiguredTarget_Value_Failure:
								return nil, fmt.Errorf("unable to obtain configured target %#v: %s", canonicalLabel.String(), result.Failure)
							default:
								return nil, errors.New("configured target value has an unknown result type")
							}
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
					return model_core.NewSimplePatchedMessage[dag.ObjectContentsWalker](&model_analysis_pb.ConfiguredTarget_Value{
						Result: &model_analysis_pb.ConfiguredTarget_Value_Failure{
							Failure: fmt.Sprintf("Invalid type for no-match of group %d of attribute %s", selectGroupIndex, publicAttr.Name),
						},
					}), nil
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
			return model_core.NewSimplePatchedMessage[dag.ObjectContentsWalker](&model_analysis_pb.ConfiguredTarget_Value{
				Result: &model_analysis_pb.ConfiguredTarget_Value_Failure{
					Failure: fmt.Sprintf("Missing value for mandatory attribute %#v", publicAttr.Name),
				},
			}), nil
		}
	}
	if len(attrValues) > 0 {
		return model_core.NewSimplePatchedMessage[dag.ObjectContentsWalker](&model_analysis_pb.ConfiguredTarget_Value{
			Result: &model_analysis_pb.ConfiguredTarget_Value_Failure{
				Failure: fmt.Sprintf("Unknown attribute %#v", attrValues[0].Name),
			},
		}), nil
	}
	for name, value := range configuredRule.PrivateAttrs {
		attrs[name] = value
	}

	if missingDependencies {
		return PatchedConfiguredTargetValue{}, evaluation.ErrMissingDependency
	}

	thread := c.newStarlarkThread(e, allBuiltinsModulesNames.Message.BuiltinsModuleNames)
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
			return model_core.NewSimplePatchedMessage[dag.ObjectContentsWalker](&model_analysis_pb.ConfiguredTarget_Value{
				Result: &model_analysis_pb.ConfiguredTarget_Value_Failure{
					Failure: fmt.Sprintf("Implementation function returned multiple structs for provider %#v", providerIdentifier),
				},
			}), nil
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
			Result: &model_analysis_pb.ConfiguredTarget_Value_Success_{
				Success: &model_analysis_pb.ConfiguredTarget_Value_Success{
					ProviderInstances: sortedProviderInstances,
				},
			},
		},
		Patcher: patcher,
	}, nil
}
