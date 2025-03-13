package analysis

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/buildbarn/bonanza/pkg/evaluation"
	"github.com/buildbarn/bonanza/pkg/label"
	model_core "github.com/buildbarn/bonanza/pkg/model/core"
	"github.com/buildbarn/bonanza/pkg/model/core/btree"
	model_parser "github.com/buildbarn/bonanza/pkg/model/parser"
	model_starlark "github.com/buildbarn/bonanza/pkg/model/starlark"
	model_analysis_pb "github.com/buildbarn/bonanza/pkg/proto/model/analysis"
	model_core_pb "github.com/buildbarn/bonanza/pkg/proto/model/core"
	model_starlark_pb "github.com/buildbarn/bonanza/pkg/proto/model/starlark"
	"github.com/buildbarn/bonanza/pkg/storage/dag"
)

func (c *baseComputer[TReference, TMetadata]) getConfigurationByReference(ctx context.Context, configurationReference model_core.Message[*model_core_pb.Reference, TReference]) (model_core.Message[*model_analysis_pb.Configuration, TReference], error) {
	if configurationReference.Message == nil {
		// Empty configuration.
		return model_core.NewSimpleMessage[TReference](&model_analysis_pb.Configuration{}), nil
	}
	return model_parser.Dereference(ctx, c.configurationReader, configurationReference)
}

var commandLineOptionPlatformsLabel = label.MustNewCanonicalLabel("@@bazel_tools+//command_line_option:platforms")

func (c *baseComputer[TReference, TMetadata]) ComputeCompatibleToolchainsForTypeValue(ctx context.Context, key model_core.Message[*model_analysis_pb.CompatibleToolchainsForType_Key, TReference], e CompatibleToolchainsForTypeEnvironment[TReference, TMetadata]) (PatchedCompatibleToolchainsForTypeValue, error) {
	registeredToolchains := e.GetRegisteredToolchainsForTypeValue(&model_analysis_pb.RegisteredToolchainsForType_Key{
		ToolchainType: key.Message.ToolchainType,
	})
	if !registeredToolchains.IsSet() {
		return PatchedCompatibleToolchainsForTypeValue{}, evaluation.ErrMissingDependency
	}

	// Obtain the constraints associated with the current platform.
	configuration, err := c.getConfigurationByReference(
		ctx,
		model_core.NewNestedMessage(key, key.Message.ConfigurationReference),
	)
	if err != nil {
		return PatchedCompatibleToolchainsForTypeValue{}, err
	}
	commandLineOptionPlatformsLabelStr := commandLineOptionPlatformsLabel.String()
	platformOverride, err := btree.Find(
		ctx,
		c.configurationBuildSettingOverrideReader,
		model_core.NewNestedMessage(configuration, configuration.Message.BuildSettingOverrides),
		func(entry *model_analysis_pb.Configuration_BuildSettingOverride) (int, *model_core_pb.Reference) {
			switch level := entry.Level.(type) {
			case *model_analysis_pb.Configuration_BuildSettingOverride_Leaf_:
				return strings.Compare(commandLineOptionPlatformsLabelStr, level.Leaf.Label), nil
			case *model_analysis_pb.Configuration_BuildSettingOverride_Parent_:
				return strings.Compare(commandLineOptionPlatformsLabelStr, level.Parent.FirstLabel), level.Parent.Reference
			default:
				return 0, nil
			}
		},
	)
	if err != nil {
		return PatchedCompatibleToolchainsForTypeValue{}, err
	}
	var platformLabel string
	if platformOverride.IsSet() {
		leaf, ok := platformOverride.Message.Level.(*model_analysis_pb.Configuration_BuildSettingOverride_Leaf_)
		if !ok {
			return PatchedCompatibleToolchainsForTypeValue{}, errors.New("build setting override is not a valid leaf")
		}
		labelValue, ok := leaf.Leaf.Value.GetKind().(*model_starlark_pb.Value_Label)
		if !ok {
			return PatchedCompatibleToolchainsForTypeValue{}, fmt.Errorf("build setting override for %#v is not a label", commandLineOptionPlatformsLabelStr)
		}
		platformLabel = labelValue.Label
	} else {
		targetValue := e.GetTargetValue(&model_analysis_pb.Target_Key{
			Label: commandLineOptionPlatformsLabelStr,
		})
		if !targetValue.IsSet() {
			return PatchedCompatibleToolchainsForTypeValue{}, evaluation.ErrMissingDependency
		}
		labelSetting, ok := targetValue.Message.Definition.GetKind().(*model_starlark_pb.Target_Definition_LabelSetting)
		if !ok {
			return PatchedCompatibleToolchainsForTypeValue{}, fmt.Errorf("target %#v is not a label setting", commandLineOptionPlatformsLabelStr)
		}
		platformLabel = labelSetting.LabelSetting.BuildSettingDefault
	}
	// TODO: VisibleTarget?
	platformInfoProvider, err := getProviderFromConfiguredTarget(
		e,
		platformLabel,
		model_core.NewSimplePatchedMessage[model_core.WalkableReferenceMetadata, *model_core_pb.Reference](nil),
		platformInfoProviderIdentifier,
	)
	if err != nil {
		return PatchedCompatibleToolchainsForTypeValue{}, fmt.Errorf("failed to obtain PlatformInfo provider for target %#v: %w", platformLabel, err)
	}
	constraintsValue, err := model_starlark.GetStructFieldValue(ctx, c.valueReaders.List, platformInfoProvider, "constraints")
	if err != nil {
		return PatchedCompatibleToolchainsForTypeValue{}, fmt.Errorf("failed to obtain constraints field of PlatformInfo provider for target %#v: %w", err)
	}
	constraints, err := c.extractFromPlatformInfoConstraints(ctx, constraintsValue)
	if err != nil {
		return PatchedCompatibleToolchainsForTypeValue{}, fmt.Errorf("failed to extract constraints from PlatformInfo provider for target %#v", platformLabel)
	}

	allToolchains := registeredToolchains.Message.Toolchains
	var compatibleToolchains []*model_analysis_pb.RegisteredToolchain
	for _, toolchain := range allToolchains {
		if constraintsAreCompatible(constraints, toolchain.TargetCompatibleWith) {
			compatibleToolchains = append(compatibleToolchains, toolchain)
		}
	}

	return model_core.NewSimplePatchedMessage[dag.ObjectContentsWalker](&model_analysis_pb.CompatibleToolchainsForType_Value{
		Toolchains: compatibleToolchains,
	}), nil
}
