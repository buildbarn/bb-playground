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
	v, ok := ruleValue.Message.Global.GetKind().(*model_starlark_pb.Value_Rule)
	if !ok {
		return PatchedConfiguredRuleValue{}, errors.New("global value is not a rule")
	}
	d, ok := v.Rule.Kind.(*model_starlark_pb.Rule_Definition_)
	if !ok {
		return PatchedConfiguredRuleValue{}, errors.New("global value is not a rule definition")
	}
	ruleDefinition := model_core.Message[*model_starlark_pb.Rule_Definition]{
		Message:            d.Definition,
		OutgoingReferences: ruleValue.OutgoingReferences,
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
	return model_core.NewSimplePatchedMessage[dag.ObjectContentsWalker](&model_analysis_pb.ConfiguredRule_Value{}), nil
}

type PublicAttr struct {
	Name     string
	Default  starlark.Value
	AttrType model_starlark.AttrType
}

type AttrsDict struct {
	Public  []PublicAttr
	Private starlark.StringDict
}

func (c *baseComputer) decodeAttrsDict(ctx context.Context, encodedAttrs model_core.Message[[]*model_starlark_pb.NamedAttr]) (AttrsDict, error) {
	var attrsDict AttrsDict
	for _, namedAttr := range encodedAttrs.Message {
		if strings.HasPrefix(namedAttr.Name, "_") {
			// TODO: Deal with private attributes!
		} else {
			attrType, err := model_starlark.DecodeAttrType(namedAttr.Attr)
			if err != nil {
				return AttrsDict{}, fmt.Errorf("invalid type for attribute %#v: %w", namedAttr.Name, err)
			}

			var defaultAttr starlark.Value
			if d := namedAttr.Attr.GetDefault(); d != nil {
				// TODO: Call into attr type to validate
				// the value!
				defaultAttr, err = model_starlark.DecodeValue(
					model_core.Message[*model_starlark_pb.Value]{
						Message:            d,
						OutgoingReferences: encodedAttrs.OutgoingReferences,
					},
					/* currentIdentifier = */ nil,
					c.getValueDecodingOptions(ctx, func(canonicalLabel label.CanonicalLabel) (starlark.Value, error) {
						// TODO: We should probably not even attempt to
						// decode defaults for label values, as we don't
						// want to compute ConfiguredTargets
						// unconditionally.
						return starlark.None, nil
					}),
				)
				if err != nil {
					return AttrsDict{}, fmt.Errorf("invalid default value for attribute %#v: %w", namedAttr.Name, err)
				}
			}
			attrsDict.Public = append(attrsDict.Public, PublicAttr{
				Name:     namedAttr.Name,
				Default:  defaultAttr,
				AttrType: attrType,
			})
		}
	}
	return attrsDict, nil
}

type ConfiguredRule struct {
	Implementation starlark.Callable
	Attrs          AttrsDict
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

	v, ok := ruleValue.Message.Global.GetKind().(*model_starlark_pb.Value_Rule)
	if !ok {
		return nil, errors.New("global value is not a rule")
	}
	d, ok := v.Rule.Kind.(*model_starlark_pb.Rule_Definition_)
	if !ok {
		return nil, errors.New("global value is not a rule definition")
	}

	attrs, err := c.decodeAttrsDict(ctx, model_core.Message[[]*model_starlark_pb.NamedAttr]{
		Message:            d.Definition.Attrs,
		OutgoingReferences: ruleValue.OutgoingReferences,
	})
	if err != nil {
		return nil, err
	}

	return &ConfiguredRule{
		Implementation: model_starlark.NewNamedFunction(model_starlark.NewProtoNamedFunctionDefinition(
			model_core.Message[*model_starlark_pb.Function]{
				Message:            d.Definition.Implementation,
				OutgoingReferences: ruleValue.OutgoingReferences,
			},
		)),
		Attrs: attrs,
	}, nil
}
