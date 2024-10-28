package analysis

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/buildbarn/bb-playground/pkg/evaluation"
	"github.com/buildbarn/bb-playground/pkg/label"
	model_core "github.com/buildbarn/bb-playground/pkg/model/core"
	model_starlark "github.com/buildbarn/bb-playground/pkg/model/starlark"
	model_analysis_pb "github.com/buildbarn/bb-playground/pkg/proto/model/analysis"
	model_starlark_pb "github.com/buildbarn/bb-playground/pkg/proto/model/starlark"
	"github.com/buildbarn/bb-playground/pkg/storage/dag"

	"go.starlark.net/starlark"
)

func (c *baseComputer) ComputeConfiguredRuleValue(ctx context.Context, key *model_analysis_pb.ConfiguredRule_Key, e ConfiguredRuleEnvironment) (PatchedConfiguredRuleValue, error) {
	ruleValue := e.GetCompiledBzlFileGlobalValue(&model_analysis_pb.CompiledBzlFileGlobal_Key{
		Identifier: key.Identifier,
	})
	if !ruleValue.IsSet() {
		return PatchedConfiguredRuleValue{}, evaluation.ErrMissingDependency
	}
	var ruleDefinition model_core.Message[*model_starlark_pb.Rule_Definition]
	switch resultType := ruleValue.Message.Result.(type) {
	case *model_analysis_pb.CompiledBzlFileGlobal_Value_Success:
		v, ok := resultType.Success.Kind.(*model_starlark_pb.Value_Rule)
		if !ok {
			return model_core.NewSimplePatchedMessage[dag.ObjectContentsWalker](&model_analysis_pb.ConfiguredRule_Value{
				Result: &model_analysis_pb.ConfiguredRule_Value_Failure{
					Failure: fmt.Sprintf("Global value %#v is not a rule", key.Identifier),
				},
			}), nil
		}
		d, ok := v.Rule.Kind.(*model_starlark_pb.Rule_Definition_)
		if !ok {
			return model_core.NewSimplePatchedMessage[dag.ObjectContentsWalker](&model_analysis_pb.ConfiguredRule_Value{
				Result: &model_analysis_pb.ConfiguredRule_Value_Failure{
					Failure: fmt.Sprintf("Global value %#v is not a rule definition", key.Identifier),
				},
			}), nil
		}
		ruleDefinition = model_core.Message[*model_starlark_pb.Rule_Definition]{
			Message:            d.Definition,
			OutgoingReferences: ruleValue.OutgoingReferences,
		}
	case *model_analysis_pb.CompiledBzlFileGlobal_Value_Failure:
		return model_core.NewSimplePatchedMessage[dag.ObjectContentsWalker](&model_analysis_pb.ConfiguredRule_Value{
			Result: &model_analysis_pb.ConfiguredRule_Value_Failure{
				Failure: resultType.Failure,
			},
		}), nil
	default:
		return model_core.NewSimplePatchedMessage[dag.ObjectContentsWalker](&model_analysis_pb.ConfiguredRule_Value{
			Result: &model_analysis_pb.ConfiguredRule_Value_Failure{
				Failure: fmt.Sprintf("Global value for rule %#v has an unknown result type", key.Identifier),
			},
		}), nil
	}

	missingResolvedToolchains := false
	for _, execGroup := range ruleDefinition.Message.ExecGroups {
		resolvedToolchains := e.GetResolvedToolchainsValue(&model_analysis_pb.ResolvedToolchains_Key{
			ExecGroup: execGroup.ExecGroup,
		})
		if !resolvedToolchains.IsSet() {
			missingResolvedToolchains = true
			continue
		}
		panic("TODO: Add resolved toolchain to configured rule")
	}
	if missingResolvedToolchains {
		return PatchedConfiguredRuleValue{}, evaluation.ErrMissingDependency
	}

	for _, attr := range ruleDefinition.Message.Attrs {
		if strings.HasPrefix(attr.Name, "_") {
			panic("TODO: Already evaluate private attributes!")
		}
	}

	patcher := model_core.NewReferenceMessagePatcher[dag.ObjectContentsWalker]()
	return model_core.PatchedMessage[*model_analysis_pb.ConfiguredRule_Value, dag.ObjectContentsWalker]{
		Message: &model_analysis_pb.ConfiguredRule_Value{
			Result: &model_analysis_pb.ConfiguredRule_Value_Success_{
				Success: &model_analysis_pb.ConfiguredRule_Value_Success{},
			},
		},
		Patcher: patcher,
	}, nil
}

type PublicAttr struct {
	Name     string
	Default  starlark.Value
	AttrType model_starlark.AttrType
}

type ConfiguredRule struct {
	Implementation starlark.Callable
	PublicAttrs    []PublicAttr
	PrivateAttrs   starlark.StringDict
}

func (c *baseComputer) ComputeConfiguredRuleObjectValue(ctx context.Context, key *model_analysis_pb.ConfiguredRuleObject_Key, e ConfiguredRuleObjectEnvironment) (*ConfiguredRule, error) {
	ruleValue := e.GetCompiledBzlFileGlobalValue(&model_analysis_pb.CompiledBzlFileGlobal_Key{
		Identifier: key.Identifier,
	})
	configuredRuleValue := e.GetConfiguredRuleValue(&model_analysis_pb.ConfiguredRule_Key{
		Identifier: key.Identifier,
	})
	if !ruleValue.IsSet() || !configuredRuleValue.IsSet() {
		return nil, evaluation.ErrMissingDependency
	}

	var ruleDefinition model_core.Message[*model_starlark_pb.Rule_Definition]
	switch resultType := ruleValue.Message.Result.(type) {
	case *model_analysis_pb.CompiledBzlFileGlobal_Value_Success:
		v, ok := resultType.Success.Kind.(*model_starlark_pb.Value_Rule)
		if !ok {
			return nil, fmt.Errorf("global value %#v is not a rule", key.Identifier)
		}
		d, ok := v.Rule.Kind.(*model_starlark_pb.Rule_Definition_)
		if !ok {
			return nil, fmt.Errorf("global value %#v is not a rule definition", key.Identifier)
		}
		ruleDefinition = model_core.Message[*model_starlark_pb.Rule_Definition]{
			Message:            d.Definition,
			OutgoingReferences: ruleValue.OutgoingReferences,
		}
	case *model_analysis_pb.CompiledBzlFileGlobal_Value_Failure:
		return nil, errors.New(resultType.Failure)
	default:
		return nil, fmt.Errorf("global value for rule %#v has an unknown result type", key.Identifier)
	}

	switch resultType := configuredRuleValue.Message.Result.(type) {
	case *model_analysis_pb.ConfiguredRule_Value_Success_:
		// TODO: Capture!
	case *model_analysis_pb.ConfiguredRule_Value_Failure:
		return nil, errors.New(resultType.Failure)
	default:
		return nil, errors.New("configured rule has an unknown result type")
	}

	implementationIdentifier, err := label.NewCanonicalStarlarkIdentifier(ruleDefinition.Message.Implementation)
	if err != nil {
		return nil, fmt.Errorf("invalid canonical Starlark identifier %#v: %w", ruleDefinition.Message.Implementation, err)
	}

	var publicAttrs []PublicAttr
	for _, namedAttr := range ruleDefinition.Message.Attrs {
		if !strings.HasPrefix(namedAttr.Name, "_") {
			attrType, err := model_starlark.DecodeAttrType(namedAttr.Attr)
			if err != nil {
				return nil, fmt.Errorf("invalid type for attribute %#v: %w", namedAttr.Name, err)
			}

			var defaultAttr starlark.Value
			if d := namedAttr.Attr.GetDefault(); d != nil {
				// TODO: Call into attr type to validate
				// the value!
				defaultAttr, err = model_starlark.DecodeValue(
					model_core.Message[*model_starlark_pb.Value]{
						Message:            d,
						OutgoingReferences: ruleDefinition.OutgoingReferences,
					},
					/* currentIdentifier = */ nil,
					c.getValueDecodingOptions(ctx, func(canonicalLabel label.CanonicalLabel) (starlark.Value, error) {
						panic("TODO")
					}),
				)
				if err != nil {
					return nil, fmt.Errorf("invalid default value for attribute %#v: %w", namedAttr.Name, err)
				}
			}
			publicAttrs = append(publicAttrs, PublicAttr{
				Name:     namedAttr.Name,
				Default:  defaultAttr,
				AttrType: attrType,
			})
		}
	}

	return &ConfiguredRule{
		Implementation: model_starlark.NewNamedFunction(implementationIdentifier, nil),
		// TODO: PrivateAttrs!
		PublicAttrs: publicAttrs,
	}, nil
}
