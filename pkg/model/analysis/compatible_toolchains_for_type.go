package analysis

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/buildbarn/bb-playground/pkg/evaluation"
	"github.com/buildbarn/bb-playground/pkg/label"
	model_core "github.com/buildbarn/bb-playground/pkg/model/core"
	"github.com/buildbarn/bb-playground/pkg/model/core/btree"
	model_parser "github.com/buildbarn/bb-playground/pkg/model/parser"
	model_starlark "github.com/buildbarn/bb-playground/pkg/model/starlark"
	model_analysis_pb "github.com/buildbarn/bb-playground/pkg/proto/model/analysis"
	model_core_pb "github.com/buildbarn/bb-playground/pkg/proto/model/core"
	model_starlark_pb "github.com/buildbarn/bb-playground/pkg/proto/model/starlark"
	"github.com/buildbarn/bb-playground/pkg/storage/dag"
	"github.com/buildbarn/bb-playground/pkg/storage/object"
)

func (c *baseComputer) getConfigurationByReference(ctx context.Context, configurationReference model_core.Message[*model_core_pb.Reference]) (model_core.Message[*model_analysis_pb.Configuration], error) {
	if configurationReference.Message == nil {
		// Empty configuration.
		return model_core.Message[*model_analysis_pb.Configuration]{
			Message:            &model_analysis_pb.Configuration{},
			OutgoingReferences: object.OutgoingReferencesList{},
		}, nil
	}

	configurationReferenceIndex, err := model_core.GetIndexFromReferenceMessage(configurationReference.Message, configurationReference.OutgoingReferences.GetDegree())
	if err != nil {
		return model_core.Message[*model_analysis_pb.Configuration]{}, fmt.Errorf("invalid configuration reference: %w", err)
	}
	configurationReader := model_parser.NewStorageBackedParsedObjectReader(
		c.objectDownloader,
		c.getValueObjectEncoder(),
		model_parser.NewMessageObjectParser[object.LocalReference, model_analysis_pb.Configuration](),
	)
	configuration, _, err := configurationReader.ReadParsedObject(ctx, configurationReference.OutgoingReferences.GetOutgoingReference(configurationReferenceIndex))
	if err != nil {
		return model_core.Message[*model_analysis_pb.Configuration]{}, fmt.Errorf("failed to read configuration: %w", err)
	}
	return configuration, nil
}

var commandLineOptionPlatformsLabel = label.MustNewCanonicalLabel("@@bazel_tools+//command_line_option:platforms")

func (c *baseComputer) ComputeCompatibleToolchainsForTypeValue(ctx context.Context, key model_core.Message[*model_analysis_pb.CompatibleToolchainsForType_Key], e CompatibleToolchainsForTypeEnvironment) (PatchedCompatibleToolchainsForTypeValue, error) {
	registeredToolchains := e.GetRegisteredToolchainsForTypeValue(&model_analysis_pb.RegisteredToolchainsForType_Key{
		ToolchainType: key.Message.ToolchainType,
	})
	if !registeredToolchains.IsSet() {
		return PatchedCompatibleToolchainsForTypeValue{}, evaluation.ErrMissingDependency
	}

	// Obtain the constraints associated with the current platform.
	configuration, err := c.getConfigurationByReference(
		ctx,
		model_core.Message[*model_core_pb.Reference]{
			Message:            key.Message.ConfigurationReference,
			OutgoingReferences: key.OutgoingReferences,
		},
	)
	if err != nil {
		return PatchedCompatibleToolchainsForTypeValue{}, err
	}
	commandLineOptionPlatformsLabelStr := commandLineOptionPlatformsLabel.String()
	platformOverride, err := btree.Find(
		ctx,
		model_parser.NewStorageBackedParsedObjectReader(
			c.objectDownloader,
			c.getValueObjectEncoder(),
			model_parser.NewMessageListObjectParser[object.LocalReference, model_analysis_pb.Configuration_BuildSettingOverride](),
		),
		model_core.Message[[]*model_analysis_pb.Configuration_BuildSettingOverride]{
			Message:            configuration.Message.BuildSettingOverrides,
			OutgoingReferences: configuration.OutgoingReferences,
		},
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
		model_core.NewSimplePatchedMessage[dag.ObjectContentsWalker, *model_core_pb.Reference](nil),
		platformInfoProviderIdentifier,
	)
	if err != nil {
		return PatchedCompatibleToolchainsForTypeValue{}, fmt.Errorf("failed to obtain PlatformInfo provider for target %#v: %w", platformLabel, err)
	}
	constraintsValue, err := model_starlark.GetStructFieldValue(
		ctx,
		model_parser.NewStorageBackedParsedObjectReader(
			c.objectDownloader,
			c.getValueObjectEncoder(),
			model_parser.NewMessageListObjectParser[object.LocalReference, model_starlark_pb.List_Element](),
		),
		platformInfoProvider,
		"constraints",
	)
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
