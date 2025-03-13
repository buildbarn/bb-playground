package analysis

import (
	"context"
	"errors"
	"fmt"
	"iter"
	"maps"
	"slices"
	"strconv"
	"strings"

	"github.com/buildbarn/bonanza/pkg/evaluation"
	"github.com/buildbarn/bonanza/pkg/label"
	model_core "github.com/buildbarn/bonanza/pkg/model/core"
	"github.com/buildbarn/bonanza/pkg/model/core/btree"
	model_starlark "github.com/buildbarn/bonanza/pkg/model/starlark"
	model_analysis_pb "github.com/buildbarn/bonanza/pkg/proto/model/analysis"
	model_core_pb "github.com/buildbarn/bonanza/pkg/proto/model/core"
	model_starlark_pb "github.com/buildbarn/bonanza/pkg/proto/model/starlark"
	"github.com/buildbarn/bonanza/pkg/starlark/unpack"
	"github.com/buildbarn/bonanza/pkg/storage/dag"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/emptypb"

	"go.starlark.net/starlark"
)

var commandLineOptionRepoRootPackage = label.MustNewCanonicalPackage("@@bazel_tools+")

type expectedTransitionOutput[TReference any] struct {
	label         string
	key           string
	canonicalizer unpack.Canonicalizer
	defaultValue  model_core.Message[*model_starlark_pb.Value, TReference]
}

func (c *baseComputer[TReference, TMetadata]) applyTransition(
	ctx context.Context,
	e model_core.ObjectCapturer[TReference, TMetadata],
	configuration model_core.Message[*model_analysis_pb.Configuration, TReference],
	expectedOutputs []expectedTransitionOutput[TReference],
	thread *starlark.Thread,
	outputs map[string]starlark.Value,
	valueEncodingOptions *model_starlark.ValueEncodingOptions[TReference, TMetadata],
) (model_core.PatchedMessage[*model_core_pb.Reference, TMetadata], error) {
	if len(outputs) != len(expectedOutputs) {
		return model_core.PatchedMessage[*model_core_pb.Reference, TMetadata]{}, fmt.Errorf("output dictionary contains %d keys, while the transition's definition only has %d outputs", len(outputs), len(expectedOutputs))
	}

	var errIter error
	existingIter, existingIterStop := iter.Pull(btree.AllLeaves(
		ctx,
		c.configurationBuildSettingOverrideReader,
		model_core.NewNestedMessage(configuration, configuration.Message.BuildSettingOverrides),
		func(override model_core.Message[*model_analysis_pb.Configuration_BuildSettingOverride, TReference]) (*model_core_pb.Reference, error) {
			if level, ok := override.Message.Level.(*model_analysis_pb.Configuration_BuildSettingOverride_Parent_); ok {
				return level.Parent.Reference, nil
			}
			return nil, nil
		},
		&errIter,
	))
	defer existingIterStop()

	// TODO: Use a proper encoder!
	treeBuilder := btree.NewSplitProllyBuilder(
		/* minimumSizeBytes = */ 32*1024,
		/* maximumSizeBytes = */ 128*1024,
		btree.NewObjectCreatingNodeMerger(
			c.getValueObjectEncoder(),
			c.getReferenceFormat(),
			/* parentNodeComputer = */ func(createdObject model_core.CreatedObject[TMetadata], childNodes []*model_analysis_pb.Configuration_BuildSettingOverride) (model_core.PatchedMessage[*model_analysis_pb.Configuration_BuildSettingOverride, TMetadata], error) {
				var firstLabel string
				switch firstEntry := childNodes[0].Level.(type) {
				case *model_analysis_pb.Configuration_BuildSettingOverride_Leaf_:
					firstLabel = firstEntry.Leaf.Label
				case *model_analysis_pb.Configuration_BuildSettingOverride_Parent_:
					firstLabel = firstEntry.Parent.FirstLabel
				}
				patcher := model_core.NewReferenceMessagePatcher[TMetadata]()
				return model_core.NewPatchedMessage(
					&model_analysis_pb.Configuration_BuildSettingOverride{
						Level: &model_analysis_pb.Configuration_BuildSettingOverride_Parent_{
							Parent: &model_analysis_pb.Configuration_BuildSettingOverride_Parent{
								Reference: patcher.AddReference(
									createdObject.Contents.GetReference(),
									e.CaptureCreatedObject(createdObject),
								),
								FirstLabel: firstLabel,
							},
						},
					},
					patcher,
				), nil
			},
		),
	)

	existingOverride, existingOverrideOK := existingIter()
	for existingOverrideOK || len(expectedOutputs) > 0 {
		var cmp int
		if !existingOverrideOK {
			cmp = 1
		} else if len(expectedOutputs) == 0 {
			cmp = -1
		} else {
			level, ok := existingOverride.Message.Level.(*model_analysis_pb.Configuration_BuildSettingOverride_Leaf_)
			if !ok {
				return model_core.PatchedMessage[*model_core_pb.Reference, TMetadata]{}, errors.New("build setting override is not a valid leaf")
			}
			cmp = strings.Compare(level.Leaf.Label, expectedOutputs[0].label)
		}
		if cmp < 0 {
			// Preserve existing build setting.
			treeBuilder.PushChild(model_core.NewPatchedMessageFromExistingCaptured(e, existingOverride))
		} else {
			// Either replace or remove an existing build
			// setting override, or inject a new one.
			expectedOutput := expectedOutputs[0]
			expectedOutputs = expectedOutputs[1:]
			literalValue, ok := outputs[expectedOutput.key]
			if !ok {
				return model_core.PatchedMessage[*model_core_pb.Reference, TMetadata]{}, fmt.Errorf("no value for output %#v has been provided", expectedOutput.label)
			}
			canonicalizedValue, err := expectedOutput.canonicalizer.Canonicalize(thread, literalValue)
			if err != nil {
				return model_core.PatchedMessage[*model_core_pb.Reference, TMetadata]{}, fmt.Errorf("failed to canonicalize output %#v: %w", expectedOutput.label, err)
			}
			encodedValue, _, err := model_starlark.EncodeValue(
				canonicalizedValue,
				/* path = */ map[starlark.Value]struct{}{},
				/* identifier = */ nil,
				valueEncodingOptions,
			)
			if err != nil {
				return model_core.PatchedMessage[*model_core_pb.Reference, TMetadata]{}, fmt.Errorf("failed to encode \"build_setting_default\": %w", err)
			}

			// Only store the build setting override if its
			// value differs from the default value. This
			// ensures that the configuration remains
			// canonical.
			if sortedEncodedValue, _ := encodedValue.SortAndSetReferences(); !model_core.MessagesEqual(sortedEncodedValue, expectedOutput.defaultValue) {
				treeBuilder.PushChild(
					model_core.NewPatchedMessage(
						&model_analysis_pb.Configuration_BuildSettingOverride{
							Level: &model_analysis_pb.Configuration_BuildSettingOverride_Leaf_{
								Leaf: &model_analysis_pb.Configuration_BuildSettingOverride_Leaf{
									Label: expectedOutput.label,
									Value: encodedValue.Message,
								},
							},
						},
						encodedValue.Patcher,
					),
				)
			}
		}
		if cmp <= 0 {
			existingOverride, existingOverrideOK = existingIter()
		}
	}
	if errIter != nil {
		return model_core.PatchedMessage[*model_core_pb.Reference, TMetadata]{}, errIter
	}
	buildSettingOverrides, err := treeBuilder.FinalizeList()
	if err != nil {
		return model_core.PatchedMessage[*model_core_pb.Reference, TMetadata]{}, fmt.Errorf("failed to finalize build setting overrides: %w", err)
	}

	newConfiguration := &model_analysis_pb.Configuration{
		BuildSettingOverrides: buildSettingOverrides.Message,
	}
	if proto.Size(newConfiguration) == 0 {
		return model_core.NewSimplePatchedMessage[TMetadata, *model_core_pb.Reference](nil), nil
	}

	createdConfiguration, err := model_core.MarshalAndEncodePatchedMessage(
		model_core.NewPatchedMessage(newConfiguration, buildSettingOverrides.Patcher),
		c.getReferenceFormat(),
		c.getValueObjectEncoder(),
	)
	if err != nil {
		return model_core.PatchedMessage[*model_core_pb.Reference, TMetadata]{}, fmt.Errorf("failed to marshal configuration: %w", err)
	}
	configurationReferencePatcher := model_core.NewReferenceMessagePatcher[TMetadata]()
	return model_core.NewPatchedMessage(
		configurationReferencePatcher.AddReference(
			createdConfiguration.Contents.GetReference(),
			e.CaptureCreatedObject(createdConfiguration),
		),
		configurationReferencePatcher,
	), nil
}

type performUserDefinedTransitionEnvironment[TReference, TMetadata any] interface {
	model_core.ObjectManager[TReference, TMetadata]
	starlarkThreadEnvironment[TReference]

	GetCompiledBzlFileGlobalValue(*model_analysis_pb.CompiledBzlFileGlobal_Key) model_core.Message[*model_analysis_pb.CompiledBzlFileGlobal_Value, TReference]
	GetVisibleTargetValue(model_core.PatchedMessage[*model_analysis_pb.VisibleTarget_Key, dag.ObjectContentsWalker]) model_core.Message[*model_analysis_pb.VisibleTarget_Value, TReference]
	GetTargetValue(*model_analysis_pb.Target_Key) model_core.Message[*model_analysis_pb.Target_Value, TReference]
}

type performUserDefinedTransitionResult[TMetadata model_core.ReferenceMetadata] = model_core.PatchedMessage[*model_analysis_pb.UserDefinedTransition_Value_Success, TMetadata]

func (c *baseComputer[TReference, TMetadata]) performUserDefinedTransition(ctx context.Context, e performUserDefinedTransitionEnvironment[TReference, TMetadata], transitionIdentifierStr string, configurationReference model_core.Message[*model_core_pb.Reference, TReference], attrParameter starlark.Value) (performUserDefinedTransitionResult[TMetadata], error) {
	transitionIdentifier, err := label.NewCanonicalStarlarkIdentifier(transitionIdentifierStr)
	if err != nil {
		return performUserDefinedTransitionResult[TMetadata]{}, fmt.Errorf("invalid transition identifier: %w", err)
	}

	allBuiltinsModulesNames := e.GetBuiltinsModuleNamesValue(&model_analysis_pb.BuiltinsModuleNames_Key{})
	transitionValue := e.GetCompiledBzlFileGlobalValue(&model_analysis_pb.CompiledBzlFileGlobal_Key{
		Identifier: transitionIdentifier.String(),
	})
	if !allBuiltinsModulesNames.IsSet() || !transitionValue.IsSet() {
		return performUserDefinedTransitionResult[TMetadata]{}, evaluation.ErrMissingDependency
	}
	v, ok := transitionValue.Message.Global.GetKind().(*model_starlark_pb.Value_Transition)
	if !ok {
		return performUserDefinedTransitionResult[TMetadata]{}, fmt.Errorf("%#v is not a transition", transitionIdentifier.String())
	}
	d, ok := v.Transition.Kind.(*model_starlark_pb.Transition_Definition_)
	if !ok {
		return performUserDefinedTransitionResult[TMetadata]{}, fmt.Errorf("%#v is not a rule definition", transitionIdentifier.String())
	}
	transitionDefinition := model_core.NewNestedMessage(transitionValue, d.Definition)

	transitionFilename := transitionIdentifier.GetCanonicalLabel()
	transitionPackage := transitionFilename.GetCanonicalPackage()
	transitionRepo := transitionPackage.GetCanonicalRepo()

	configuration, err := c.getConfigurationByReference(ctx, configurationReference)
	if err != nil {
		return performUserDefinedTransitionResult[TMetadata]{}, err
	}

	thread := c.newStarlarkThread(ctx, e, allBuiltinsModulesNames.Message.BuiltinsModuleNames)

	// Collect inputs to provide to the implementation function.
	missingDependencies := false
	inputs := starlark.NewDict(len(transitionDefinition.Message.Inputs))
	for _, input := range transitionDefinition.Message.Inputs {
		// Resolve the actual build setting target corresponding
		// to the string value provided as part of the
		// transition definition.
		pkg := transitionPackage
		if strings.HasPrefix(input, "//command_line_option:") {
			pkg = commandLineOptionRepoRootPackage
		}
		apparentBuildSettingLabel, err := pkg.AppendLabel(input)
		if err != nil {
			return performUserDefinedTransitionResult[TMetadata]{}, fmt.Errorf("invalid build setting label %#v: %w", input, err)
		}
		canonicalBuildSettingLabel, err := resolveApparent(e, transitionRepo, apparentBuildSettingLabel)
		if err != nil {
			if errors.Is(err, evaluation.ErrMissingDependency) {
				missingDependencies = true
				continue
			}
			return performUserDefinedTransitionResult[TMetadata]{}, err
		}
		visibleTargetValue := e.GetVisibleTargetValue(
			model_core.NewSimplePatchedMessage[dag.ObjectContentsWalker](
				&model_analysis_pb.VisibleTarget_Key{
					FromPackage:        canonicalBuildSettingLabel.GetCanonicalPackage().String(),
					ToLabel:            canonicalBuildSettingLabel.String(),
					StopAtLabelSetting: true,
				},
			),
		)
		if !visibleTargetValue.IsSet() {
			missingDependencies = true
			continue
		}
		visibleBuildSettingLabel := visibleTargetValue.Message.Label

		// Determine the current value of the build setting.
		buildSettingOverride, err := btree.Find(
			ctx,
			c.configurationBuildSettingOverrideReader,
			model_core.NewNestedMessage(configuration, configuration.Message.BuildSettingOverrides),
			func(entry *model_analysis_pb.Configuration_BuildSettingOverride) (int, *model_core_pb.Reference) {
				switch level := entry.Level.(type) {
				case *model_analysis_pb.Configuration_BuildSettingOverride_Leaf_:
					return strings.Compare(visibleBuildSettingLabel, level.Leaf.Label), nil
				case *model_analysis_pb.Configuration_BuildSettingOverride_Parent_:
					return strings.Compare(visibleBuildSettingLabel, level.Parent.FirstLabel), level.Parent.Reference
				default:
					return 0, nil
				}
			},
		)
		if err != nil {
			return performUserDefinedTransitionResult[TMetadata]{}, err
		}

		if buildSettingOverride.IsSet() {
			// Configuration contains an override for the
			// build setting. Use the value contained in the
			// configuratoin.
			level, ok := buildSettingOverride.Message.Level.(*model_analysis_pb.Configuration_BuildSettingOverride_Leaf_)
			if !ok {
				return performUserDefinedTransitionResult[TMetadata]{}, fmt.Errorf("build setting override for label setting %#v is not a valid leaf", visibleBuildSettingLabel)
			}
			v, err := model_starlark.DecodeValue[TReference, TMetadata](
				model_core.NewNestedMessage(buildSettingOverride, level.Leaf.Value),
				/* currentIdentifier = */ nil,
				c.getValueDecodingOptions(ctx, func(resolvedLabel label.ResolvedLabel) (starlark.Value, error) {
					return model_starlark.NewLabel[TReference, TMetadata](resolvedLabel), nil
				}),
			)
			if err := inputs.SetKey(thread, starlark.String(input), v); err != nil {
				return performUserDefinedTransitionResult[TMetadata]{}, err
			}
			if err != nil {
				return performUserDefinedTransitionResult[TMetadata]{}, fmt.Errorf("failed to decode build setting override for label setting %#v: %w", visibleBuildSettingLabel, err)
			}
		} else {
			// No override present. Obtain the default value
			// of the build setting.
			targetValue := e.GetTargetValue(&model_analysis_pb.Target_Key{
				Label: visibleBuildSettingLabel,
			})
			if !targetValue.IsSet() {
				missingDependencies = true
				continue
			}
			switch targetKind := targetValue.Message.Definition.GetKind().(type) {
			case *model_starlark_pb.Target_Definition_LabelSetting:
				// Build setting is a label_setting() or
				// label_flag().
				buildSettingDefault, err := label.NewResolvedLabel(targetKind.LabelSetting.BuildSettingDefault)
				if err != nil {
					return performUserDefinedTransitionResult[TMetadata]{}, fmt.Errorf("invalid build setting default for label setting %#v: %w", visibleBuildSettingLabel)
				}
				if err := inputs.SetKey(thread, starlark.String(input), model_starlark.NewLabel[TReference, TMetadata](buildSettingDefault)); err != nil {
					return performUserDefinedTransitionResult[TMetadata]{}, err
				}
			case *model_starlark_pb.Target_Definition_RuleTarget:
				// Build setting is written in Starlark.
				if targetKind.RuleTarget.BuildSettingDefault == nil {
					return performUserDefinedTransitionResult[TMetadata]{}, fmt.Errorf("rule %#v used by build setting %#v does not have \"build_setting\" set", targetKind.RuleTarget.RuleIdentifier, visibleBuildSettingLabel)
				}
				v, err := model_starlark.DecodeValue[TReference, TMetadata](
					model_core.NewNestedMessage(targetValue, targetKind.RuleTarget.BuildSettingDefault),
					/* currentIdentifier = */ nil,
					c.getValueDecodingOptions(ctx, func(resolvedLabel label.ResolvedLabel) (starlark.Value, error) {
						return nil, errors.New("build settings implemented in Starlark cannot be of type Label")
					}),
				)
				if err := inputs.SetKey(thread, starlark.String(input), v); err != nil {
					return performUserDefinedTransitionResult[TMetadata]{}, err
				}
				if err != nil {
					return performUserDefinedTransitionResult[TMetadata]{}, fmt.Errorf("failed to decode build setting default for build setting %#v: %w", visibleBuildSettingLabel, err)
				}
			default:
				return performUserDefinedTransitionResult[TMetadata]{}, fmt.Errorf("target %#v is not a build setting or rule target", visibleBuildSettingLabel)
			}
		}
	}
	inputs.Freeze()

	// Preprocess the outputs that we expect to see.
	expectedOutputs := make([]expectedTransitionOutput[TReference], 0, len(transitionDefinition.Message.Outputs))
	expectedOutputLabels := make(map[string]string, len(transitionDefinition.Message.Outputs))
	for _, output := range transitionDefinition.Message.Outputs {
		// Resolve the actual build setting target corresponding
		// to the string value provided as part of the
		// transition definition.
		pkg := transitionPackage
		if strings.HasPrefix(output, "//command_line_option:") {
			pkg = commandLineOptionRepoRootPackage
		}
		apparentBuildSettingLabel, err := pkg.AppendLabel(output)
		if err != nil {
			return performUserDefinedTransitionResult[TMetadata]{}, fmt.Errorf("invalid build setting label %#v: %w", output, err)
		}
		canonicalBuildSettingLabel, err := resolveApparent(e, transitionRepo, apparentBuildSettingLabel)
		if err != nil {
			if errors.Is(err, evaluation.ErrMissingDependency) {
				missingDependencies = true
				continue
			}
			return performUserDefinedTransitionResult[TMetadata]{}, err
		}
		visibleTargetValue := e.GetVisibleTargetValue(
			model_core.NewSimplePatchedMessage[dag.ObjectContentsWalker](
				&model_analysis_pb.VisibleTarget_Key{
					FromPackage:        canonicalBuildSettingLabel.GetCanonicalPackage().String(),
					ToLabel:            canonicalBuildSettingLabel.String(),
					StopAtLabelSetting: true,
				},
			),
		)
		if !visibleTargetValue.IsSet() {
			missingDependencies = true
			continue
		}
		visibleBuildSettingLabel := visibleTargetValue.Message.Label

		// Determine how values associated with this build
		// setting need to be canonicalized.
		targetValue := e.GetTargetValue(&model_analysis_pb.Target_Key{
			Label: visibleBuildSettingLabel,
		})
		if !targetValue.IsSet() {
			missingDependencies = true
			continue
		}
		var canonicalizer unpack.Canonicalizer
		var defaultValue model_core.Message[*model_starlark_pb.Value, TReference]
		switch targetKind := targetValue.Message.Definition.GetKind().(type) {
		case *model_starlark_pb.Target_Definition_LabelSetting:
			// Build setting is a label_setting() or label_flag().
			canonicalizer = model_starlark.NewLabelOrStringUnpackerInto[TReference, TMetadata](transitionPackage)
			defaultValue = model_core.NewSimpleMessage[TReference](
				&model_starlark_pb.Value{
					Kind: &model_starlark_pb.Value_Label{
						Label: targetKind.LabelSetting.BuildSettingDefault,
					},
				},
			)
		case *model_starlark_pb.Target_Definition_RuleTarget:
			// Build setting is written in Starlark.
			if targetKind.RuleTarget.BuildSettingDefault == nil {
				return performUserDefinedTransitionResult[TMetadata]{}, fmt.Errorf("rule %#v used by label setting %#v does not have \"build_setting\" set", targetKind.RuleTarget.RuleIdentifier, visibleBuildSettingLabel)
			}
			ruleValue := e.GetCompiledBzlFileGlobalValue(&model_analysis_pb.CompiledBzlFileGlobal_Key{
				Identifier: targetKind.RuleTarget.RuleIdentifier,
			})
			if !ruleValue.IsSet() {
				missingDependencies = true
				continue
			}
			rule, ok := ruleValue.Message.Global.Kind.(*model_starlark_pb.Value_Rule)
			if !ok {
				return performUserDefinedTransitionResult[TMetadata]{}, fmt.Errorf("identifier %#v used by build setting %#v is not a rule", targetKind.RuleTarget.RuleIdentifier, visibleBuildSettingLabel)
			}
			ruleDefinition, ok := rule.Rule.Kind.(*model_starlark_pb.Rule_Definition_)
			if !ok {
				return performUserDefinedTransitionResult[TMetadata]{}, fmt.Errorf("rule %#v used by build setting %#v does not have a definition", targetKind.RuleTarget.RuleIdentifier, visibleBuildSettingLabel)
			}
			if ruleDefinition.Definition.BuildSetting == nil {
				return performUserDefinedTransitionResult[TMetadata]{}, fmt.Errorf("rule %#v used by build setting %#v does not have \"build_setting\" set", targetKind.RuleTarget.RuleIdentifier, visibleBuildSettingLabel)
			}
			buildSettingType, err := model_starlark.DecodeBuildSettingType(ruleDefinition.Definition.BuildSetting)
			if err != nil {
				return performUserDefinedTransitionResult[TMetadata]{}, fmt.Errorf("failed to decode build setting type for rule %#v used by build setting %#v: %w", targetKind.RuleTarget.RuleIdentifier, visibleBuildSettingLabel, err)
			}
			canonicalizer = buildSettingType.GetCanonicalizer()
			defaultValue = model_core.NewNestedMessage(targetValue, targetKind.RuleTarget.BuildSettingDefault)
		default:
			return performUserDefinedTransitionResult[TMetadata]{}, fmt.Errorf("target %#v is not a label setting or rule target", visibleBuildSettingLabel)
		}

		if existing, ok := expectedOutputLabels[visibleBuildSettingLabel]; ok {
			return performUserDefinedTransitionResult[TMetadata]{}, fmt.Errorf("outputs %#v and %#v both refer to build setting %#v", existing, output, visibleBuildSettingLabel)
		}
		expectedOutputLabels[visibleBuildSettingLabel] = output
		expectedOutputs = append(expectedOutputs, expectedTransitionOutput[TReference]{
			label:         visibleBuildSettingLabel,
			key:           output,
			canonicalizer: canonicalizer,
			defaultValue:  defaultValue,
		})
	}
	slices.SortFunc(expectedOutputs, func(a, b expectedTransitionOutput[TReference]) int {
		return strings.Compare(a.label, b.label)
	})

	if missingDependencies {
		return performUserDefinedTransitionResult[TMetadata]{}, evaluation.ErrMissingDependency
	}

	// Invoke transition implementation function.
	valueEncodingOptions := c.getValueEncodingOptions(e, transitionFilename)
	outputs, err := starlark.Call(
		thread,
		model_starlark.NewNamedFunction(
			model_starlark.NewProtoNamedFunctionDefinition[TReference, TMetadata](
				model_core.NewNestedMessage(transitionDefinition, transitionDefinition.Message.Implementation),
			),
		),
		/* args = */ starlark.Tuple{
			inputs,
			attrParameter,
		},
		/* kwargs = */ nil,
	)
	if err != nil {
		return performUserDefinedTransitionResult[TMetadata]{}, err
	}

	// Process return value of transition implementation function.
	var outputsDict map[string]map[string]starlark.Value
	switch typedOutputs := outputs.(type) {
	case starlark.Indexable:
		// 1:2+ transition in the form of a list.
		var outputsList []map[string]starlark.Value
		if err := unpack.List(unpack.Dict(unpack.String, unpack.Any)).UnpackInto(thread, typedOutputs, &outputsList); err != nil {
			return performUserDefinedTransitionResult[TMetadata]{}, err
		}
		outputsDict = make(map[string]map[string]starlark.Value, len(outputsList))
		for i, outputs := range outputsList {
			outputsDict[strconv.FormatInt(int64(i), 10)] = outputs
		}
	case starlark.IterableMapping:
		// If the implementation function returns a dict, this
		// can either be a 1:1 transition or a 1:2+ transition
		// in the form of a dictionary of dictionaries. Check
		// whether the return value is a dict of dicts.
		gotEntries := false
		dictOfDicts := true
		for _, value := range starlark.Entries(thread, typedOutputs) {
			gotEntries = true
			if _, ok := value.(starlark.Mapping); !ok {
				dictOfDicts = false
				break
			}
		}
		if gotEntries && dictOfDicts {
			// 1:2+ transition in the form of a dictionary.
			if err := unpack.Dict(unpack.String, unpack.Dict(unpack.String, unpack.Any)).UnpackInto(thread, typedOutputs, &outputsDict); err != nil {
				return performUserDefinedTransitionResult[TMetadata]{}, err
			}
		} else {
			// 1:1 transition. These are implicitly converted to a
			// singleton list.
			var outputs map[string]starlark.Value
			if err := unpack.Dict(unpack.String, unpack.Any).UnpackInto(thread, typedOutputs, &outputs); err != nil {
				return performUserDefinedTransitionResult[TMetadata]{}, err
			}
			outputsDict = map[string]map[string]starlark.Value{
				"0": outputs,
			}
		}
	default:
		return performUserDefinedTransitionResult[TMetadata]{}, errors.New("transition did not yield a list or dict")
	}

	patcher := model_core.NewReferenceMessagePatcher[TMetadata]()
	entries := make([]*model_analysis_pb.UserDefinedTransition_Value_Success_Entry, 0, len(outputsDict))
	for i, key := range slices.Sorted(maps.Keys(outputsDict)) {
		outputConfigurationReference, err := c.applyTransition(ctx, e, configuration, expectedOutputs, thread, outputsDict[key], valueEncodingOptions)
		if err != nil {
			return performUserDefinedTransitionResult[TMetadata]{}, fmt.Errorf("key %#v: %w", i, err)
		}
		entries = append(entries, &model_analysis_pb.UserDefinedTransition_Value_Success_Entry{
			Key:                          key,
			OutputConfigurationReference: outputConfigurationReference.Message,
		})
		patcher.Merge(outputConfigurationReference.Patcher)
	}
	return model_core.NewPatchedMessage(
		&model_analysis_pb.UserDefinedTransition_Value_Success{
			Entries: entries,
		},
		patcher,
	), nil
}

func (c *baseComputer[TReference, TMetadata]) ComputeUserDefinedTransitionValue(ctx context.Context, key model_core.Message[*model_analysis_pb.UserDefinedTransition_Key, TReference], e UserDefinedTransitionEnvironment[TReference, TMetadata]) (PatchedUserDefinedTransitionValue, error) {
	entries, err := c.performUserDefinedTransition(
		ctx,
		e,
		key.Message.TransitionIdentifier,
		model_core.NewNestedMessage(key, key.Message.InputConfigurationReference),
		stubbedTransitionAttr{},
	)
	if err != nil {
		if errors.Is(err, errTransitionDependsOnAttrs) {
			// Can't compute the transition indepently of
			// the rule in which it is referenced. Return
			// this to the caller, so that it can apply the
			// transition directly.
			return model_core.NewSimplePatchedMessage[dag.ObjectContentsWalker](
				&model_analysis_pb.UserDefinedTransition_Value{
					Result: &model_analysis_pb.UserDefinedTransition_Value_TransitionDependsOnAttrs{
						TransitionDependsOnAttrs: &emptypb.Empty{},
					},
				},
			), nil
		}
		return PatchedUserDefinedTransitionValue{}, err
	}

	return model_core.NewPatchedMessage(
		&model_analysis_pb.UserDefinedTransition_Value{
			Result: &model_analysis_pb.UserDefinedTransition_Value_Success_{
				Success: entries.Message,
			},
		},
		model_core.MapReferenceMetadataToWalkers(entries.Patcher),
	), nil
}

type stubbedTransitionAttr struct{}

var _ starlark.HasAttrs = stubbedTransitionAttr{}

func (stubbedTransitionAttr) String() string {
	return "<transition_attr>"
}

func (stubbedTransitionAttr) Type() string {
	return "transition_attr"
}

func (stubbedTransitionAttr) Freeze() {}

func (stubbedTransitionAttr) Truth() starlark.Bool {
	return starlark.True
}

func (stubbedTransitionAttr) Hash(thread *starlark.Thread) (uint32, error) {
	return 0, errors.New("transition_attr cannot be hashed")
}

var errTransitionDependsOnAttrs = errors.New("transition depends on rule attrs, which are not available in this context")

func (stubbedTransitionAttr) Attr(*starlark.Thread, string) (starlark.Value, error) {
	return nil, errTransitionDependsOnAttrs
}

func (stubbedTransitionAttr) AttrNames() []string {
	// TODO: This should also be able to return an error.
	return nil
}
