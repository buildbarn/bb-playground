package analysis

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	"sort"
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
	"github.com/buildbarn/bonanza/pkg/starlark/unpack"
	"github.com/buildbarn/bonanza/pkg/storage/dag"
	"github.com/buildbarn/bonanza/pkg/storage/object"

	"google.golang.org/protobuf/types/known/emptypb"

	"go.starlark.net/starlark"
	"go.starlark.net/syntax"
)

var (
	constraintValueInfoProviderIdentifier      = label.MustNewCanonicalStarlarkIdentifier("@@builtins_core+//:exports.bzl%ConstraintValueInfo")
	defaultInfoProviderIdentifier              = label.MustNewCanonicalStarlarkIdentifier("@@builtins_core+//:exports.bzl%DefaultInfo")
	fragmentInfoProviderIdentifier             = label.MustNewCanonicalStarlarkIdentifier("@@bazel_tools+//fragments:fragments.bzl%FragmentInfo")
	fragmentsPackage                           = label.MustNewCanonicalPackage("@@bazel_tools+//fragments")
	packageSpecificationInfoProviderIdentifier = label.MustNewCanonicalStarlarkIdentifier("@@builtins_core+//:exports.bzl%PackageSpecificationInfo")
	toolchainInfoProviderIdentifier            = label.MustNewCanonicalStarlarkIdentifier("@@builtins_core+//:exports.bzl%ToolchainInfo")
	filesToRunProviderIdentifier               = label.MustNewCanonicalStarlarkIdentifier("@@builtins_core+//:exports.bzl%FilesToRun")
)

type constraintValuesToConstraintsEnvironment interface {
	GetConfiguredTargetValue(model_core.PatchedMessage[*model_analysis_pb.ConfiguredTarget_Key, dag.ObjectContentsWalker]) model_core.Message[*model_analysis_pb.ConfiguredTarget_Value]
	GetVisibleTargetValue(model_core.PatchedMessage[*model_analysis_pb.VisibleTarget_Key, dag.ObjectContentsWalker]) model_core.Message[*model_analysis_pb.VisibleTarget_Value]
}

// constraintValuesToConstraints converts a list of labels of constraint
// values to a list of Constraint messages that include both the
// constraint setting and constraint value labels. These can be used to
// perform matching of constraints.
func (c *baseComputer) constraintValuesToConstraints(ctx context.Context, e constraintValuesToConstraintsEnvironment, fromPackage label.CanonicalPackage, constraintValues []string) ([]*model_analysis_pb.Constraint, error) {
	constraints := make(map[string]string, len(constraintValues))
	missingDependencies := false
	for _, constraintValue := range constraintValues {
		visibleTarget := e.GetVisibleTargetValue(
			model_core.NewSimplePatchedMessage[dag.ObjectContentsWalker](
				&model_analysis_pb.VisibleTarget_Key{
					FromPackage: fromPackage.String(),
					ToLabel:     constraintValue,
					// Don't use any configuration
					// when resolving constraint
					// values, as that only leads to
					// confusion.
				},
			),
		)
		if !visibleTarget.IsSet() {
			missingDependencies = true
			continue
		}

		constrainValueInfoProvider, err := getProviderFromConfiguredTarget(
			e,
			visibleTarget.Message.Label,
			model_core.NewSimplePatchedMessage[dag.ObjectContentsWalker, *model_core_pb.Reference](nil),
			constraintValueInfoProviderIdentifier,
		)
		if err != nil {
			if errors.Is(err, evaluation.ErrMissingDependency) {
				missingDependencies = true
				continue
			}
			return nil, err
		}

		var actualConstraintSetting, actualConstraintValue, defaultConstraintValue *string
		var errIter error
		listReader := model_parser.NewStorageBackedParsedObjectReader(
			c.objectDownloader,
			c.getValueObjectEncoder(),
			model_parser.NewMessageListObjectParser[object.LocalReference, model_starlark_pb.List_Element](),
		)
		for key, value := range model_starlark.AllStructFields(ctx, listReader, constrainValueInfoProvider, &errIter) {
			switch key {
			case "constraint":
				constraintSettingInfoProvider, ok := value.Message.Kind.(*model_starlark_pb.Value_Struct)
				if !ok {
					return nil, fmt.Errorf("field \"constraint\" of ConstraintValueInfo provider of target %#v is not a struct")
				}
				var errIter error
				for key, value := range model_starlark.AllStructFields(
					ctx,
					listReader,
					model_core.NewNestedMessage(value, constraintSettingInfoProvider.Struct.Fields),
					&errIter,
				) {
					switch key {
					case "default_constraint_value":
						switch v := value.Message.Kind.(type) {
						case *model_starlark_pb.Value_Label:
							defaultConstraintValue = &v.Label
						case *model_starlark_pb.Value_None:
						default:
							return nil, fmt.Errorf("field \"constraint.default_constraint_value\" of ConstraintValueInfo provider of target %#v is not a Label or None")
						}
					case "label":
						v, ok := value.Message.Kind.(*model_starlark_pb.Value_Label)
						if !ok {
							return nil, fmt.Errorf("field \"constraint.label\" of ConstraintValueInfo provider of target %#v is not a Label")
						}
						actualConstraintSetting = &v.Label
					}
				}
				if errIter != nil {
					return nil, err
				}
			case "label":
				v, ok := value.Message.Kind.(*model_starlark_pb.Value_Label)
				if !ok {
					return nil, fmt.Errorf("field \"label\" of ConstraintValueInfo provider of target %#v is not a Label")
				}
				actualConstraintValue = &v.Label
			}
		}
		if errIter != nil {
			return nil, errIter
		}
		if actualConstraintSetting == nil {
			return nil, fmt.Errorf("ConstraintValueInfo provider of target %#v does not contain field \"constraint.label\"")
		}
		if actualConstraintValue == nil {
			return nil, fmt.Errorf("ConstraintValueInfo provider of target %#v does not contain field \"label\"")
		}
		effectiveConstraintValue := *actualConstraintValue
		if defaultConstraintValue != nil && effectiveConstraintValue == *defaultConstraintValue {
			effectiveConstraintValue = ""
		}

		if _, ok := constraints[*actualConstraintSetting]; ok {
			return nil, fmt.Errorf("got multiple constraint values for constraint setting %#v", *actualConstraintSetting)
		}
		constraints[*actualConstraintSetting] = effectiveConstraintValue

	}
	if missingDependencies {
		return nil, evaluation.ErrMissingDependency
	}

	sortedConstraints := make([]*model_analysis_pb.Constraint, 0, len(constraints))
	for _, constraintSetting := range slices.Sorted(maps.Keys(constraints)) {
		sortedConstraints = append(
			sortedConstraints,
			&model_analysis_pb.Constraint{
				Setting: constraintSetting,
				Value:   constraints[constraintSetting],
			},
		)
	}
	return sortedConstraints, nil
}

func getDefaultInfoSimpleFilesToRun(executable *model_starlark_pb.Value) *model_starlark_pb.List_Element {
	return &model_starlark_pb.List_Element{
		Level: &model_starlark_pb.List_Element_Leaf{
			Leaf: &model_starlark_pb.Value{
				Kind: &model_starlark_pb.Value_Struct{
					Struct: &model_starlark_pb.Struct{
						ProviderInstanceProperties: &model_starlark_pb.Provider_InstanceProperties{
							ProviderIdentifier: filesToRunProviderIdentifier.String(),
						},
						Fields: &model_starlark_pb.Struct_Fields{
							Keys: []string{
								"executable",
								"repo_mapping_manifest",
								"runfiles_manifest",
							},
							Values: []*model_starlark_pb.List_Element{
								{
									Level: &model_starlark_pb.List_Element_Leaf{
										Leaf: executable,
									},
								},
								{
									Level: &model_starlark_pb.List_Element_Leaf{
										Leaf: &model_starlark_pb.Value{
											Kind: &model_starlark_pb.Value_None{
												None: &emptypb.Empty{},
											},
										},
									},
								},
								{
									Level: &model_starlark_pb.List_Element_Leaf{
										Leaf: &model_starlark_pb.Value{
											Kind: &model_starlark_pb.Value_None{
												None: &emptypb.Empty{},
											},
										},
									},
								},
							},
						},
					},
				},
			},
		},
	}
}

var emptyRunfilesValue = &model_starlark_pb.Value{
	Kind: &model_starlark_pb.Value_Runfiles{
		Runfiles: &model_starlark_pb.Runfiles{
			Files:        &model_starlark_pb.Depset{},
			RootSymlinks: &model_starlark_pb.Depset{},
			Symlinks:     &model_starlark_pb.Depset{},
		},
	},
}

var (
	defaultInfoProviderInstanceProperties = model_starlark.NewProviderInstanceProperties(&defaultInfoProviderIdentifier, false)
	emptyDefaultInfo                      = model_starlark.NewStructFromDict(
		defaultInfoProviderInstanceProperties,
		map[string]any{
			"data_runfiles": model_starlark.NewRunfiles(
				&model_starlark.EmptyDepset,
				&model_starlark.EmptyDepset,
				&model_starlark.EmptyDepset,
			),
			"default_runfiles": model_starlark.NewRunfiles(
				&model_starlark.EmptyDepset,
				&model_starlark.EmptyDepset,
				&model_starlark.EmptyDepset,
			),
			"files": &model_starlark.EmptyDepset,
			"files_to_run": model_starlark.NewStructFromDict(
				model_starlark.NewProviderInstanceProperties(&filesToRunProviderIdentifier, false),
				map[string]any{
					"executable":            starlark.None,
					"repo_mapping_manifest": starlark.None,
					"runfiles_manifest":     starlark.None,
				},
			),
		},
	)
)

func getSingleFileConfiguredTargetValue(file *model_starlark_pb.File) PatchedConfiguredTargetValue {
	fileValue := &model_starlark_pb.Value{
		Kind: &model_starlark_pb.Value_File{
			File: file,
		},
	}
	return model_core.NewSimplePatchedMessage[dag.ObjectContentsWalker](
		&model_analysis_pb.ConfiguredTarget_Value{
			ProviderInstances: []*model_starlark_pb.Struct{{
				ProviderInstanceProperties: &model_starlark_pb.Provider_InstanceProperties{
					ProviderIdentifier: defaultInfoProviderIdentifier.String(),
				},
				Fields: &model_starlark_pb.Struct_Fields{
					Keys: []string{
						"data_runfiles",
						"default_runfiles",
						"files",
						"files_to_run",
					},
					Values: []*model_starlark_pb.List_Element{
						{
							Level: &model_starlark_pb.List_Element_Leaf{
								Leaf: emptyRunfilesValue,
							},
						},
						{
							Level: &model_starlark_pb.List_Element_Leaf{
								Leaf: emptyRunfilesValue,
							},
						},
						{
							Level: &model_starlark_pb.List_Element_Leaf{
								Leaf: &model_starlark_pb.Value{
									Kind: &model_starlark_pb.Value_Depset{
										Depset: &model_starlark_pb.Depset{
											Elements: []*model_starlark_pb.List_Element{{
												Level: &model_starlark_pb.List_Element_Leaf{
													Leaf: fileValue,
												},
											}},
										},
									},
								},
							},
						},
						getDefaultInfoSimpleFilesToRun(fileValue),
					},
				},
			}},
		},
	)
}

func (c *baseComputer) ComputeConfiguredTargetValue(ctx context.Context, key model_core.Message[*model_analysis_pb.ConfiguredTarget_Key], e ConfiguredTargetEnvironment) (PatchedConfiguredTargetValue, error) {
	targetLabel, err := label.NewCanonicalLabel(key.Message.Label)
	if err != nil {
		return PatchedConfiguredTargetValue{}, fmt.Errorf("invalid target label: %w", err)
	}
	targetValue := e.GetTargetValue(&model_analysis_pb.Target_Key{
		Label: targetLabel.String(),
	})
	if !targetValue.IsSet() {
		return PatchedConfiguredTargetValue{}, evaluation.ErrMissingDependency
	}

	switch targetKind := targetValue.Message.Definition.GetKind().(type) {
	case *model_starlark_pb.Target_Definition_PackageGroup:
		return model_core.NewSimplePatchedMessage[dag.ObjectContentsWalker](
			&model_analysis_pb.ConfiguredTarget_Value{
				ProviderInstances: []*model_starlark_pb.Struct{
					{
						ProviderInstanceProperties: &model_starlark_pb.Provider_InstanceProperties{
							ProviderIdentifier: defaultInfoProviderIdentifier.String(),
						},
						Fields: &model_starlark_pb.Struct_Fields{
							Keys: []string{
								"data_runfiles",
								"default_runfiles",
								"files",
								"files_to_run",
							},
							Values: []*model_starlark_pb.List_Element{
								{
									Level: &model_starlark_pb.List_Element_Leaf{
										Leaf: emptyRunfilesValue,
									},
								},
								{
									Level: &model_starlark_pb.List_Element_Leaf{
										Leaf: emptyRunfilesValue,
									},
								},
								{
									Level: &model_starlark_pb.List_Element_Leaf{
										Leaf: &model_starlark_pb.Value{
											Kind: &model_starlark_pb.Value_Depset{
												Depset: &model_starlark_pb.Depset{},
											},
										},
									},
								},
								getDefaultInfoSimpleFilesToRun(&model_starlark_pb.Value{
									Kind: &model_starlark_pb.Value_None{
										None: &emptypb.Empty{},
									},
								}),
							},
						},
					},
					{
						ProviderInstanceProperties: &model_starlark_pb.Provider_InstanceProperties{
							ProviderIdentifier: packageSpecificationInfoProviderIdentifier.String(),
						},
						Fields: &model_starlark_pb.Struct_Fields{},
					},
				},
			},
		), nil
	case *model_starlark_pb.Target_Definition_PredeclaredOutputFileTarget:
		// Handcraft a DefaultInfo provider for this source file.
		return getSingleFileConfiguredTargetValue(&model_starlark_pb.File{
			Owner: &model_starlark_pb.File_Owner{
				Cfg:        []byte{0xbe, 0x8a, 0x60, 0x1c, 0xe3, 0x03, 0x44, 0xf0},
				TargetName: targetKind.PredeclaredOutputFileTarget.OwnerTargetName,
			},
			Package:             targetLabel.GetCanonicalPackage().String(),
			PackageRelativePath: targetLabel.GetTargetName().String(),
			Type:                model_starlark_pb.File_FILE,
		}), nil
	case *model_starlark_pb.Target_Definition_RuleTarget:
		ruleTarget := targetKind.RuleTarget
		ruleIdentifier, err := label.NewCanonicalStarlarkIdentifier(ruleTarget.RuleIdentifier)
		if err != nil {
			return PatchedConfiguredTargetValue{}, evaluation.ErrMissingDependency
		}

		allBuiltinsModulesNames := e.GetBuiltinsModuleNamesValue(&model_analysis_pb.BuiltinsModuleNames_Key{})
		ruleValue := e.GetCompiledBzlFileGlobalValue(&model_analysis_pb.CompiledBzlFileGlobal_Key{
			Identifier: ruleIdentifier.String(),
		})
		if !allBuiltinsModulesNames.IsSet() || !ruleValue.IsSet() {
			return PatchedConfiguredTargetValue{}, evaluation.ErrMissingDependency
		}
		v, ok := ruleValue.Message.Global.GetKind().(*model_starlark_pb.Value_Rule)
		if !ok {
			return PatchedConfiguredTargetValue{}, fmt.Errorf("%#v is not a rule", ruleIdentifier.String())
		}
		d, ok := v.Rule.Kind.(*model_starlark_pb.Rule_Definition_)
		if !ok {
			return PatchedConfiguredTargetValue{}, fmt.Errorf("%#v is not a rule definition", ruleIdentifier.String())
		}
		ruleDefinition := model_core.NewNestedMessage(ruleValue, d.Definition)

		// Determine the configuration to use. If an incoming
		// edge transition is specified, apply it.
		configurationReference := model_core.NewNestedMessage(key, key.Message.ConfigurationReference)
		if cfgTransitionIdentifier := ruleDefinition.Message.CfgTransitionIdentifier; cfgTransitionIdentifier != "" {
			patchedConfigurationReference := model_core.NewPatchedMessageFromExisting(
				configurationReference,
				func(index int) dag.ObjectContentsWalker {
					return dag.ExistingObjectContentsWalker
				},
			)
			incomingEdgeTransitionValue := e.GetUserDefinedTransitionValue(
				model_core.PatchedMessage[*model_analysis_pb.UserDefinedTransition_Key, dag.ObjectContentsWalker]{
					Message: &model_analysis_pb.UserDefinedTransition_Key{
						TransitionIdentifier:        cfgTransitionIdentifier,
						InputConfigurationReference: patchedConfigurationReference.Message,
					},
					Patcher: patchedConfigurationReference.Patcher,
				},
			)
			if !incomingEdgeTransitionValue.IsSet() {
				return PatchedConfiguredTargetValue{}, evaluation.ErrMissingDependency
			}
			switch result := incomingEdgeTransitionValue.Message.Result.(type) {
			case *model_analysis_pb.UserDefinedTransition_Value_TransitionDependsOnAttrs:
				return PatchedConfiguredTargetValue{}, fmt.Errorf("TODO: support incoming edge transitions that depends on attrs")
			case *model_analysis_pb.UserDefinedTransition_Value_Success_:
				if l := len(result.Success.Entries); l != 1 {
					return PatchedConfiguredTargetValue{}, fmt.Errorf("incoming edge transition %#v used by rule %#v is a 1:%d transition, while a 1:1 transition was expected", cfgTransitionIdentifier, ruleIdentifier.String(), l)
				}
				configurationReference = model_core.NewNestedMessage(incomingEdgeTransitionValue, result.Success.Entries[0].OutputConfigurationReference)
			default:
				return PatchedConfiguredTargetValue{}, fmt.Errorf("incoming edge transition %#v used by rule %#v is not a 1:1 transition", cfgTransitionIdentifier, ruleIdentifier.String())
			}
		}

		thread := c.newStarlarkThread(ctx, e, allBuiltinsModulesNames.Message.BuiltinsModuleNames)
		rc := &ruleContext{
			computer:               c,
			context:                ctx,
			environment:            e,
			ruleIdentifier:         ruleIdentifier,
			targetLabel:            targetLabel,
			configurationReference: configurationReference,
			ruleDefinition:         ruleDefinition,
			ruleTarget:             model_core.NewNestedMessage(targetValue, ruleTarget),
			attrs:                  make([]starlark.Value, len(ruleDefinition.Message.Attrs)),
			executables:            make([]starlark.Value, len(ruleDefinition.Message.Attrs)),
			singleFiles:            make([]starlark.Value, len(ruleDefinition.Message.Attrs)),
			multipleFiles:          make([]starlark.Value, len(ruleDefinition.Message.Attrs)),
			outputs:                make([]starlark.Value, len(ruleDefinition.Message.Attrs)),
			execGroups:             make([]*ruleContextExecGroupState, len(ruleDefinition.Message.ExecGroups)),
			fragments:              map[string]*model_starlark.Struct{},
		}
		thread.SetLocal(model_starlark.CurrentCtxKey, rc)

		thread.SetLocal(model_starlark.SubruleInvokerKey, func(subruleIdentifier label.CanonicalStarlarkIdentifier, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
			// TODO: Subrules are allowed to be nested. Keep a stack!
			permittedSubruleIdentifiers := ruleDefinition.Message.SubruleIdentifiers

			subruleIdentifierStr := subruleIdentifier.String()
			if _, ok := sort.Find(
				len(permittedSubruleIdentifiers),
				func(i int) int { return strings.Compare(subruleIdentifierStr, permittedSubruleIdentifiers[i]) },
			); !ok {
				return nil, fmt.Errorf("subrule %#v cannot be invoked from within the current (sub)rule", subruleIdentifierStr)
			}
			subruleValue := e.GetCompiledBzlFileGlobalValue(&model_analysis_pb.CompiledBzlFileGlobal_Key{
				Identifier: subruleIdentifierStr,
			})
			if !subruleValue.IsSet() {
				return nil, evaluation.ErrMissingDependency
			}
			v, ok := subruleValue.Message.Global.GetKind().(*model_starlark_pb.Value_Subrule)
			if !ok {
				return nil, fmt.Errorf("%#v is not a subrule", subruleIdentifierStr)
			}
			d, ok := v.Subrule.Kind.(*model_starlark_pb.Subrule_Definition_)
			if !ok {
				return nil, fmt.Errorf("%#v is not a subrule definition", subruleIdentifierStr)
			}
			subruleDefinition := model_core.NewNestedMessage(subruleValue, d.Definition)

			missingDependencies := false

			implementationArgs := append(
				starlark.Tuple{
					&subruleContext{ruleContext: rc},
				},
				args...,
			)
			implementationKwargs := append(
				make([]starlark.Tuple, 0, len(kwargs)+len(subruleDefinition.Message.Attrs)),
				kwargs...,
			)
			for _, namedAttr := range subruleDefinition.Message.Attrs {
				defaultValue := namedAttr.Attr.GetDefault()
				if defaultValue == nil {
					return nil, fmt.Errorf("missing value for mandatory attr %#v", namedAttr.Name)
				}
				// TODO: Is this using the correct configuration?
				value, err := rc.configureAttr(
					thread,
					namedAttr,
					model_core.NewNestedMessage(rc.ruleDefinition, []*model_starlark_pb.Value{defaultValue}),
					rc.ruleIdentifier.GetCanonicalLabel().GetCanonicalPackage(),
				)
				if err != nil {
					if errors.Is(err, evaluation.ErrMissingDependency) {
						missingDependencies = true
						continue
					}
					return nil, err
				}
				implementationKwargs = append(
					implementationKwargs,
					starlark.Tuple{
						starlark.String(namedAttr.Name),
						value,
					},
				)
			}

			if missingDependencies {
				return nil, evaluation.ErrMissingDependency
			}

			return starlark.Call(
				thread,
				model_starlark.NewNamedFunction(
					model_starlark.NewProtoNamedFunctionDefinition(
						model_core.NewNestedMessage(subruleDefinition, subruleDefinition.Message.Implementation),
					),
				),
				implementationArgs,
				implementationKwargs,
			)
		})

		returnValue, err := starlark.Call(
			thread,
			model_starlark.NewNamedFunction(
				model_starlark.NewProtoNamedFunctionDefinition(
					model_core.NewNestedMessage(ruleDefinition, ruleDefinition.Message.Implementation),
				),
			),
			/* args = */ starlark.Tuple{rc},
			/* kwargs = */ nil,
		)
		if err != nil {
			if !errors.Is(err, evaluation.ErrMissingDependency) {
				var evalErr *starlark.EvalError
				if errors.As(err, &evalErr) {
					return PatchedConfiguredTargetValue{}, errors.New(evalErr.Backtrace())
				}
			}
			return PatchedConfiguredTargetValue{}, err
		}

		// Bazel permits returning either a single provider, or
		// a list of providers.
		var providerInstances []*model_starlark.Struct
		structUnpackerInto := unpack.Type[*model_starlark.Struct]("struct")
		if err := unpack.IfNotNone(
			unpack.Or([]unpack.UnpackerInto[[]*model_starlark.Struct]{
				unpack.Singleton(structUnpackerInto),
				unpack.List(structUnpackerInto),
			}),
		).UnpackInto(thread, returnValue, &providerInstances); err != nil {
			return PatchedConfiguredTargetValue{}, fmt.Errorf("failed to unpack implementation function return value: %w", err)
		}

		// Convert list of providers to a map where the provider
		// identifier is the key.
		providerInstancesByIdentifier := make(map[label.CanonicalStarlarkIdentifier]*model_starlark.Struct, len(providerInstances))
		for _, providerInstance := range providerInstances {
			providerIdentifier, err := providerInstance.GetProviderIdentifier()
			if err != nil {
				return PatchedConfiguredTargetValue{}, err
			}
			if _, ok := providerInstancesByIdentifier[providerIdentifier]; ok {
				return PatchedConfiguredTargetValue{}, fmt.Errorf("implementation function returned multiple structs for provider %#v", providerIdentifier.String())
			}
			providerInstancesByIdentifier[providerIdentifier] = providerInstance
		}

		if defaultInfo, ok := providerInstancesByIdentifier[defaultInfoProviderIdentifier]; ok {
			// Rule returned DefaultInfo. Make sure that
			// "data_runfiles", "default_runfiles" and
			// "files" are not set to None.
			//
			// Ideally we'd do this as part of DefaultInfo's
			// init function, but runfiles objects can only
			// be constructed via ctx.
			attrNames := defaultInfo.AttrNames()
			newAttrs := make(map[string]any, len(attrNames))
			for _, attrName := range attrNames {
				attrValue, err := defaultInfo.Attr(thread, attrName)
				if err != nil {
					return PatchedConfiguredTargetValue{}, err
				}
				switch attrName {
				case "data_runfiles", "default_runfiles":
					if attrValue == starlark.None {
						attrValue = model_starlark.NewRunfiles(
							&model_starlark.EmptyDepset,
							&model_starlark.EmptyDepset,
							&model_starlark.EmptyDepset,
						)
					}
				case "files":
					if attrValue == starlark.None {
						attrValue = &model_starlark.EmptyDepset
					}
				}
				newAttrs[attrName] = attrValue
			}
			providerInstancesByIdentifier[defaultInfoProviderIdentifier] = model_starlark.NewStructFromDict(defaultInfoProviderInstanceProperties, newAttrs)
		} else {
			// Rule did not return DefaultInfo. Return an
			// empty one.
			providerInstancesByIdentifier[defaultInfoProviderIdentifier] = emptyDefaultInfo
		}

		encodedProviderInstances := make([]*model_starlark_pb.Struct, 0, len(providerInstancesByIdentifier))
		patcher := model_core.NewReferenceMessagePatcher[dag.ObjectContentsWalker]()
		for _, providerIdentifier := range slices.SortedFunc(
			maps.Keys(providerInstancesByIdentifier),
			func(a, b label.CanonicalStarlarkIdentifier) int {
				return strings.Compare(a.String(), b.String())
			},
		) {
			v, _, err := providerInstancesByIdentifier[providerIdentifier].
				Encode(map[starlark.Value]struct{}{}, c.getValueEncodingOptions(targetLabel))
			if err != nil {
				return PatchedConfiguredTargetValue{}, err
			}
			encodedProviderInstances = append(encodedProviderInstances, v.Message)
			patcher.Merge(v.Patcher)
		}

		return model_core.NewPatchedMessage(
			&model_analysis_pb.ConfiguredTarget_Value{
				ProviderInstances: encodedProviderInstances,
			},
			patcher,
		), nil
	case *model_starlark_pb.Target_Definition_SourceFileTarget:
		// Handcraft a DefaultInfo provider for this source file.
		return getSingleFileConfiguredTargetValue(&model_starlark_pb.File{
			Package:             targetLabel.GetCanonicalPackage().String(),
			PackageRelativePath: targetLabel.GetTargetName().String(),
			Type:                model_starlark_pb.File_FILE,
		}), nil
	default:
		return PatchedConfiguredTargetValue{}, errors.New("only source file targets and rule targets can be configured")
	}
}

type ruleContext struct {
	computer               *baseComputer
	context                context.Context
	environment            ConfiguredTargetEnvironment
	ruleIdentifier         label.CanonicalStarlarkIdentifier
	targetLabel            label.CanonicalLabel
	configurationReference model_core.Message[*model_core_pb.Reference]
	ruleDefinition         model_core.Message[*model_starlark_pb.Rule_Definition]
	ruleTarget             model_core.Message[*model_starlark_pb.RuleTarget]
	attrs                  []starlark.Value
	buildSettingValue      starlark.Value
	executables            []starlark.Value
	singleFiles            []starlark.Value
	multipleFiles          []starlark.Value
	outputs                []starlark.Value
	execGroups             []*ruleContextExecGroupState
	tags                   *starlark.List
	fragments              map[string]*model_starlark.Struct
}

var _ starlark.HasAttrs = (*ruleContext)(nil)

func (rc *ruleContext) String() string {
	return fmt.Sprintf("<ctx for %s>", rc.targetLabel.String())
}

func (ruleContext) Type() string {
	return "ctx"
}

func (ruleContext) Freeze() {
}

func (ruleContext) Truth() starlark.Bool {
	return starlark.True
}

func (ruleContext) Hash(thread *starlark.Thread) (uint32, error) {
	return 0, errors.New("ctx cannot be hashed")
}

func (rc *ruleContext) Attr(thread *starlark.Thread, name string) (starlark.Value, error) {
	switch name {
	case "actions":
		return &ruleContextActions{
			ruleContext: rc,
		}, nil
	case "attr":
		return &ruleContextAttr{
			ruleContext: rc,
		}, nil
	case "build_setting_value":
		if rc.buildSettingValue == nil {
			buildSettingDefault := rc.ruleTarget.Message.BuildSettingDefault
			if buildSettingDefault == nil {
				return nil, errors.New("rule is not a build setting")
			}

			configuration, err := rc.computer.getConfigurationByReference(rc.context, rc.configurationReference)
			if err != nil {
				return nil, err
			}

			targetLabelStr := rc.targetLabel.String()
			override, err := btree.Find(
				rc.context,
				model_parser.NewStorageBackedParsedObjectReader(
					rc.computer.objectDownloader,
					rc.computer.getValueObjectEncoder(),
					model_parser.NewMessageListObjectParser[object.LocalReference, model_analysis_pb.Configuration_BuildSettingOverride](),
				),
				model_core.NewNestedMessage(configuration, configuration.Message.BuildSettingOverrides),
				func(entry *model_analysis_pb.Configuration_BuildSettingOverride) (int, *model_core_pb.Reference) {
					switch level := entry.Level.(type) {
					case *model_analysis_pb.Configuration_BuildSettingOverride_Leaf_:
						return strings.Compare(targetLabelStr, level.Leaf.Label), nil
					case *model_analysis_pb.Configuration_BuildSettingOverride_Parent_:
						return strings.Compare(targetLabelStr, level.Parent.FirstLabel), level.Parent.Reference
					default:
						return 0, nil
					}
				},
			)
			if err != nil {
				return nil, err
			}

			var encodedValue model_core.Message[*model_starlark_pb.Value]
			if override.IsSet() {
				overrideLeaf, ok := override.Message.Level.(*model_analysis_pb.Configuration_BuildSettingOverride_Leaf_)
				if !ok {
					return nil, errors.New("build setting override is not a valid leaf")
				}
				encodedValue = model_core.NewNestedMessage(override, overrideLeaf.Leaf.Value)
			} else {
				encodedValue = model_core.NewNestedMessage(rc.ruleTarget, rc.ruleTarget.Message.BuildSettingDefault)
			}

			value, err := model_starlark.DecodeValue(
				encodedValue,
				/* currentIdentifier = */ nil,
				rc.computer.getValueDecodingOptions(rc.context, func(resolvedLabel label.ResolvedLabel) (starlark.Value, error) {
					return nil, errors.New("did not expect label values")
				}),
			)
			if err != nil {
				return nil, err
			}
			rc.buildSettingValue = value
		}
		return rc.buildSettingValue, nil
	case "configuration":
		// TODO: Should we move this into a rule like we do for
		// ctx.fragments?
		return model_starlark.NewStructFromDict(nil, map[string]any{
			"coverage_enabled": starlark.False,
			"has_separate_genfiles_directory": starlark.NewBuiltin("ctx.configuration.has_separate_genfiles_directory", func(thread *starlark.Thread, b *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
				return starlark.False, nil
			}),
			// TODO: Use ";" on Windows.
			"host_path_separator": starlark.String(":"),
			"is_sibling_repository_layout": starlark.NewBuiltin("ctx.configuration.is_sibling_repository_layout", func(thread *starlark.Thread, b *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
				return starlark.True, nil
			}),
			"is_tool_configuration": starlark.NewBuiltin("ctx.configuration.is_tool_configuration", func(thread *starlark.Thread, b *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
				// TODO: Check whether "//command_line_option:is exec configuration" is set!
				return starlark.False, nil
			}),
			"stamp_binaries": starlark.NewBuiltin("ctx.configuration.stamp_binaries", func(thread *starlark.Thread, b *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
				// TODO: Check whether --stamp is set!
				return starlark.False, nil
			}),
		}), nil
	case "coverage_instrumented":
		return starlark.NewBuiltin("ctx.coverage_instrumented", rc.doCoverageInstrumented), nil
	case "bin_dir":
		return model_starlark.NewStructFromDict(nil, map[string]any{
			// TODO: Fill in the right configuration in the path.
			"path": starlark.String("bazel-bin/TODO-CONFIGURATION/bin"),
		}), nil
	case "disabled_features":
		return starlark.NewList(nil), nil
	case "exec_groups":
		return &ruleContextExecGroups{
			ruleContext: rc,
		}, nil
	case "executable":
		return &ruleContextExecutable{
			ruleContext: rc,
		}, nil
	case "features":
		// TODO: Do we want to support ctx.features in a meaningful way?
		return starlark.NewList(nil), nil
	case "file":
		return &ruleContextFile{
			ruleContext: rc,
		}, nil
	case "files":
		return &ruleContextFiles{
			ruleContext: rc,
		}, nil
	case "fragments":
		return &ruleContextFragments{
			ruleContext: rc,
		}, nil
	case "info_file":
		// Fill all of this in properly.
		return model_starlark.NewFile(&model_starlark_pb.File{
			Owner: &model_starlark_pb.File_Owner{
				Cfg:        []byte{0xbe, 0x8a, 0x60, 0x1c, 0xe3, 0x03, 0x44, 0xf0},
				TargetName: "stamp",
			},
			Package:             "@@builtins_core+",
			PackageRelativePath: "stable-status.txt",
			Type:                model_starlark_pb.File_FILE,
		}), nil
	case "label":
		return model_starlark.NewLabel(rc.targetLabel.AsResolved()), nil
	case "outputs":
		return &ruleContextOutputs{
			ruleContext: rc,
		}, nil
	case "runfiles":
		return starlark.NewBuiltin("ctx.runfiles", rc.doRunfiles), nil
	case "toolchains":
		execGroups := rc.ruleDefinition.Message.ExecGroups
		execGroupIndex, ok := sort.Find(
			len(execGroups),
			func(i int) int { return strings.Compare("", execGroups[i].Name) },
		)
		if !ok {
			return nil, errors.New("rule does not have a default exec group")
		}
		return &toolchainContext{
			ruleContext:    rc,
			execGroupIndex: execGroupIndex,
		}, nil
	case "var":
		// We shouldn't attempt to support --define, as Bazel
		// only provides it for backward compatibility. Provide
		// an empty dictionary to keep existing users happy.
		//
		// TODO: Should we at least provide support for
		// platform_common.TemplateVariableInfo?
		d := starlark.NewDict(0)
		d.Freeze()
		return d, nil
	case "version_file":
		// Fill all of this in properly.
		return model_starlark.NewFile(&model_starlark_pb.File{
			Owner: &model_starlark_pb.File_Owner{
				Cfg:        []byte{0xbe, 0x8a, 0x60, 0x1c, 0xe3, 0x03, 0x44, 0xf0},
				TargetName: "stamp",
			},
			Package:             "@@builtins_core+",
			PackageRelativePath: "volatile-status.txt",
			Type:                model_starlark_pb.File_FILE,
		}), nil
	case "workspace_name":
		return starlark.String("_main"), nil
	default:
		return nil, nil
	}
}

func (rc *ruleContext) configureAttr(thread *starlark.Thread, namedAttr *model_starlark_pb.NamedAttr, valueParts model_core.Message[[]*model_starlark_pb.Value], visibilityFromPackage label.CanonicalPackage) (starlark.Value, error) {
	// See if any transitions need to be applied.
	var cfg *model_starlark_pb.Transition_Reference
	isScalar := false
	switch attrType := namedAttr.Attr.GetType().(type) {
	case *model_starlark_pb.Attr_Label:
		cfg = attrType.Label.ValueOptions.GetCfg()
		isScalar = true
	case *model_starlark_pb.Attr_LabelKeyedStringDict:
		cfg = attrType.LabelKeyedStringDict.DictKeyOptions.GetCfg()
	case *model_starlark_pb.Attr_LabelList:
		cfg = attrType.LabelList.ListValueOptions.GetCfg()
	}
	var configurationReferences []model_core.Message[*model_core_pb.Reference]
	mayHaveMultipleConfigurations := false
	if cfg != nil {
		switch tr := cfg.Kind.(type) {
		case *model_starlark_pb.Transition_Reference_ExecGroup:
			// TODO: Actually transition to the exec platform!
			configurationReferences = []model_core.Message[*model_core_pb.Reference]{
				rc.configurationReference,
			}
		case *model_starlark_pb.Transition_Reference_None:
			// Use the empty configuration.
			configurationReferences = []model_core.Message[*model_core_pb.Reference]{
				model_core.NewSimpleMessage[*model_core_pb.Reference](nil),
			}
		case *model_starlark_pb.Transition_Reference_Target:
			// Don't transition. Use the current target.
			configurationReferences = []model_core.Message[*model_core_pb.Reference]{
				rc.configurationReference,
			}
		case *model_starlark_pb.Transition_Reference_Unconfigured:
			// Leave targets unconfigured.
		case *model_starlark_pb.Transition_Reference_UserDefined:
			// TODO: Should we cache this in the ruleContext?
			configurationReference := rc.getPatchedConfigurationReference()
			transitionValue := rc.environment.GetUserDefinedTransitionValue(
				model_core.PatchedMessage[*model_analysis_pb.UserDefinedTransition_Key, dag.ObjectContentsWalker]{
					Message: &model_analysis_pb.UserDefinedTransition_Key{
						TransitionIdentifier:        tr.UserDefined,
						InputConfigurationReference: configurationReference.Message,
					},
					Patcher: configurationReference.Patcher,
				},
			)
			if !transitionValue.IsSet() {
				return nil, evaluation.ErrMissingDependency
			}
			switch result := transitionValue.Message.Result.(type) {
			case *model_analysis_pb.UserDefinedTransition_Value_TransitionDependsOnAttrs:
				return nil, fmt.Errorf("TODO: support transitions that depends on attrs")
			case *model_analysis_pb.UserDefinedTransition_Value_Success_:
				configurationReferences = make([]model_core.Message[*model_core_pb.Reference], 0, len(result.Success.Entries))
				for _, entry := range result.Success.Entries {
					configurationReferences = append(configurationReferences, model_core.NewNestedMessage(transitionValue, entry.OutputConfigurationReference))
				}
				mayHaveMultipleConfigurations = true
			default:
				return nil, fmt.Errorf("transition %#v uses an unknown result type", tr.UserDefined)
			}
		default:
			return nil, fmt.Errorf("attr %#v uses an unknown transition type", namedAttr.Name)
		}
	}

	decodedParts := make([]starlark.Value, 0, len(valueParts.Message))
	if len(configurationReferences) == 0 {
		for _, valuePart := range valueParts.Message {
			decodedPart, err := model_starlark.DecodeValue(
				model_core.NewNestedMessage(valueParts, valuePart),
				/* currentIdentifier = */ nil,
				rc.computer.getValueDecodingOptions(rc.context, func(resolvedLabel label.ResolvedLabel) (starlark.Value, error) {
					// We should leave the target
					// unconfigured. Provide a
					// target reference that does
					// not contain any providers.
					return model_starlark.NewTargetReference(
						resolvedLabel,
						model_core.Message[[]*model_starlark_pb.Struct]{},
					), nil
				}),
			)
			if err != nil {
				return nil, err
			}
			decodedParts = append(decodedParts, decodedPart)
		}
	} else {
		missingDependencies := false
		for _, configurationReference := range configurationReferences {
			valueDecodingOptions := rc.computer.getValueDecodingOptions(rc.context, func(resolvedLabel label.ResolvedLabel) (starlark.Value, error) {
				// Resolve the label.
				canonicalLabel, err := resolvedLabel.AsCanonical()
				if err != nil {
					return nil, err
				}
				patchedConfigurationReference1 := model_core.NewPatchedMessageFromExisting(
					configurationReference,
					func(index int) dag.ObjectContentsWalker {
						return dag.ExistingObjectContentsWalker
					},
				)
				resolvedLabelValue := rc.environment.GetVisibleTargetValue(
					model_core.PatchedMessage[*model_analysis_pb.VisibleTarget_Key, dag.ObjectContentsWalker]{
						Message: &model_analysis_pb.VisibleTarget_Key{
							FromPackage:            visibilityFromPackage.String(),
							ToLabel:                canonicalLabel.String(),
							ConfigurationReference: patchedConfigurationReference1.Message,
						},
						Patcher: patchedConfigurationReference1.Patcher,
					},
				)
				if !resolvedLabelValue.IsSet() {
					missingDependencies = true
					return starlark.None, nil
				}
				if resolvedLabelStr := resolvedLabelValue.Message.Label; resolvedLabelStr != "" {
					canonicalLabel, err := label.NewCanonicalLabel(resolvedLabelStr)
					if err != nil {
						return nil, fmt.Errorf("invalid label %#v: %w", resolvedLabelStr, err)
					}

					// Obtain the providers of the target.
					patchedConfigurationReference2 := model_core.NewPatchedMessageFromExisting(
						configurationReference,
						func(index int) dag.ObjectContentsWalker {
							return dag.ExistingObjectContentsWalker
						},
					)
					configuredTarget := rc.environment.GetConfiguredTargetValue(
						model_core.PatchedMessage[*model_analysis_pb.ConfiguredTarget_Key, dag.ObjectContentsWalker]{
							Message: &model_analysis_pb.ConfiguredTarget_Key{
								Label:                  resolvedLabelStr,
								ConfigurationReference: patchedConfigurationReference2.Message,
							},
							Patcher: patchedConfigurationReference2.Patcher,
						},
					)
					if !configuredTarget.IsSet() {
						missingDependencies = true
						return starlark.None, nil
					}

					return model_starlark.NewTargetReference(
						canonicalLabel.AsResolved(),
						model_core.NewNestedMessage(configuredTarget, configuredTarget.Message.ProviderInstances),
					), nil
				} else {
					return starlark.None, nil
				}
			})
			for _, valuePart := range valueParts.Message {
				decodedPart, err := model_starlark.DecodeValue(
					model_core.NewNestedMessage(valueParts, valuePart),
					/* currentIdentifier = */ nil,
					valueDecodingOptions,
				)
				if err != nil {
					return nil, err
				}
				if isScalar && mayHaveMultipleConfigurations {
					decodedPart = starlark.NewList([]starlark.Value{decodedPart})
				}
				decodedParts = append(decodedParts, decodedPart)
			}
		}
		if missingDependencies {
			return nil, evaluation.ErrMissingDependency
		}
	}

	// Combine the values of the parts into a single value.
	if len(decodedParts) == 0 {
		return nil, errors.New("attr value does not have any parts")
	}
	attr := decodedParts[0]
	concatenationOperator := syntax.PLUS
	if _, ok := attr.(*starlark.Dict); ok {
		concatenationOperator = syntax.PIPE
	}
	for _, decodedPart := range decodedParts[1:] {
		var err error
		attr, err = starlark.Binary(thread, concatenationOperator, attr, decodedPart)
		if err != nil {
			return nil, err
		}
	}

	attr.Freeze()
	return attr, nil
}

var ruleContextAttrNames = []string{
	"actions",
	"attr",
	"build_setting_value",
	"exec_groups",
	"executable",
	"features",
	"file",
	"files",
	"fragments",
	"info_file",
	"label",
	"runfiles",
	"toolchains",
	"var",
	"version_file",
	"workspace_name",
}

func (ruleContext) AttrNames() []string {
	return ruleContextAttrNames
}

func (ruleContext) doCoverageInstrumented(thread *starlark.Thread, b *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
	return starlark.False, nil
}

func toSymlinkEntryDepset(v any) *model_starlark.Depset {
	switch typedV := v.(type) {
	case *model_starlark.Depset:
		return typedV
	case map[string]string:
		panic("TODO: convert dict of strings to SymlinkEntry list")
	default:
		panic("unknown type")
	}
}

func (ruleContext) doRunfiles(thread *starlark.Thread, b *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
	var files []starlark.Value
	transitiveFiles := &model_starlark.EmptyDepset
	var symlinks any = &model_starlark.EmptyDepset
	var rootSymlinks any = &model_starlark.EmptyDepset
	symlinksUnpackerInto := unpack.Or([]unpack.UnpackerInto[any]{
		unpack.Decay(unpack.Dict(unpack.String, unpack.String)),
		unpack.Decay(unpack.Type[*model_starlark.Depset]("depset")),
	})
	if err := starlark.UnpackArgs(
		b.Name(), args, kwargs,
		"files?", unpack.Bind(thread, &files, unpack.List(unpack.Canonicalize(unpack.Type[model_starlark.File]("File")))),
		"transitive_files?", unpack.Bind(thread, &transitiveFiles, unpack.IfNotNone(unpack.Type[*model_starlark.Depset]("depset"))),
		"symlinks?", unpack.Bind(thread, &symlinks, symlinksUnpackerInto),
		"root_symlinks?", unpack.Bind(thread, &rootSymlinks, symlinksUnpackerInto),
	); err != nil {
		return nil, err
	}

	filesDepset, err := model_starlark.NewDepset(
		thread,
		files,
		[]*model_starlark.Depset{transitiveFiles},
		model_starlark_pb.Depset_DEFAULT,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create files depset: %w", err)
	}

	return model_starlark.NewRunfiles(
		filesDepset,
		toSymlinkEntryDepset(rootSymlinks),
		toSymlinkEntryDepset(symlinks),
	), nil
}

func (rc *ruleContext) getAttrValueParts(namedAttr *model_starlark_pb.NamedAttr) (valueParts model_core.Message[[]*model_starlark_pb.Value], visibilityFromPackage label.CanonicalPackage, err error) {
	attr := namedAttr.Attr
	var badCanonicalPackage label.CanonicalPackage
	if attr == nil {
		return model_core.Message[[]*model_starlark_pb.Value]{}, badCanonicalPackage, fmt.Errorf("attr %#v misses a definition", namedAttr.Name)
	}

	if !strings.HasPrefix(namedAttr.Name, "_") {
		// Attr is public. Extract the value from the rule target.
		ruleTargetAttrValues := rc.ruleTarget.Message.AttrValues
		ruleTargetAttrValueIndex, ok := sort.Find(
			len(ruleTargetAttrValues),
			func(i int) int { return strings.Compare(namedAttr.Name, ruleTargetAttrValues[i].Name) },
		)
		if !ok {
			return model_core.Message[[]*model_starlark_pb.Value]{}, badCanonicalPackage, fmt.Errorf("missing value for attr %#v", namedAttr.Name)
		}

		selectGroups := ruleTargetAttrValues[ruleTargetAttrValueIndex].ValueParts
		if len(selectGroups) == 0 {
			return model_core.Message[[]*model_starlark_pb.Value]{}, badCanonicalPackage, fmt.Errorf("attr %#v has no select groups", namedAttr.Name)
		}
		valueParts := make([]*model_starlark_pb.Value, 0, len(selectGroups))
		for _, selectGroup := range selectGroups {
			valuePart, err := getValueFromSelectGroup(rc.environment, selectGroup, false)
			if err != nil {
				return model_core.Message[[]*model_starlark_pb.Value]{}, badCanonicalPackage, err
			}
			valueParts = append(valueParts, valuePart)
		}

		// If the value is None, fall back to the default value
		// from the rule definition.
		gotProperValue := true
		if len(valueParts) == 1 {
			if _, ok := valueParts[0].Kind.(*model_starlark_pb.Value_None); ok {
				gotProperValue = false
			}
		}
		if gotProperValue {
			return model_core.NewNestedMessage(rc.ruleTarget, valueParts),
				rc.targetLabel.GetCanonicalPackage(),
				nil
		}
	}

	// No value provided. Use the default value from the rule definition.
	if attr.Default == nil {
		return model_core.Message[[]*model_starlark_pb.Value]{}, badCanonicalPackage, fmt.Errorf("missing value for mandatory attr %#v", namedAttr.Name)
	}
	return model_core.NewNestedMessage(rc.ruleDefinition, []*model_starlark_pb.Value{attr.Default}),
		rc.ruleIdentifier.GetCanonicalLabel().GetCanonicalPackage(),
		nil
}

func (rc *ruleContext) getPatchedConfigurationReference() model_core.PatchedMessage[*model_core_pb.Reference, dag.ObjectContentsWalker] {
	// TODO: This function should likely not exist, as we need to
	// take transitions into account.
	return model_core.NewPatchedMessageFromExisting(
		rc.configurationReference,
		func(index int) dag.ObjectContentsWalker {
			return dag.ExistingObjectContentsWalker
		},
	)
}

type ruleContextActions struct {
	ruleContext *ruleContext
}

var _ starlark.HasAttrs = (*ruleContextActions)(nil)

func (ruleContextActions) String() string {
	return "<ctx.actions>"
}

func (ruleContextActions) Type() string {
	return "ctx.actions"
}

func (ruleContextActions) Freeze() {
}

func (ruleContextActions) Truth() starlark.Bool {
	return starlark.True
}

func (ruleContextActions) Hash(thread *starlark.Thread) (uint32, error) {
	return 0, errors.New("ctx.actions cannot be hashed")
}

func (rca *ruleContextActions) Attr(thread *starlark.Thread, name string) (starlark.Value, error) {
	switch name {
	case "args":
		return starlark.NewBuiltin("ctx.actions.args", rca.doArgs), nil
	case "declare_directory":
		return starlark.NewBuiltin("ctx.actions.declare_directory", rca.doDeclareDirectory), nil
	case "declare_file":
		return starlark.NewBuiltin("ctx.actions.declare_file", rca.doDeclareFile), nil
	case "run":
		return starlark.NewBuiltin("ctx.actions.run", rca.doRun), nil
	case "run_shell":
		return starlark.NewBuiltin("ctx.actions.run_shell", rca.doRunShell), nil
	case "symlink":
		return starlark.NewBuiltin("ctx.actions.symlink", rca.doSymlink), nil
	case "transform_info_file":
		return starlark.NewBuiltin("ctx.actions.transform_info_file", rca.doTransformInfoFile), nil
	case "transform_version_file":
		return starlark.NewBuiltin("ctx.actions.transform_version_file", rca.doTransformVersionFile), nil
	case "write":
		return starlark.NewBuiltin("ctx.actions.write", rca.doWrite), nil
	default:
		return nil, nil
	}
}

var ruleContextActionsAttrNames = []string{
	"args",
	"declare_directory",
	"declare_file",
	"run",
	"run_shell",
	"symlink",
	"transform_info_file",
	"transform_version_file",
	"write",
}

func (rca *ruleContextActions) AttrNames() []string {
	return ruleContextActionsAttrNames
}

func (rca *ruleContextActions) doArgs(thread *starlark.Thread, b *starlark.Builtin, arguments starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
	return &args{}, nil
}

func (rca *ruleContextActions) doDeclareDirectory(thread *starlark.Thread, b *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
	if len(args) > 1 {
		return nil, fmt.Errorf("%s: got %d positional arguments, want at most 1", b.Name(), len(args))
	}
	var filename label.TargetName
	var sibling *model_starlark.File
	if err := starlark.UnpackArgs(
		b.Name(), args, kwargs,
		"filename", unpack.Bind(thread, &filename, unpack.TargetName),
		"sibling?", unpack.Bind(thread, &sibling, unpack.IfNotNone(unpack.Pointer(unpack.Type[model_starlark.File]("File")))),
	); err != nil {
		return nil, err
	}

	rc := rca.ruleContext
	return model_starlark.NewFile(&model_starlark_pb.File{
		Owner: &model_starlark_pb.File_Owner{
			// TODO: Fill in a proper hash.
			Cfg:        []byte{0xbe, 0x8a, 0x60, 0x1c, 0xe3, 0x03, 0x44, 0xf0},
			TargetName: rc.targetLabel.GetTargetName().String(),
		},
		Package:             rc.targetLabel.GetCanonicalPackage().String(),
		PackageRelativePath: filename.String(),
		Type:                model_starlark_pb.File_DIRECTORY,
	}), nil
}

func (rca *ruleContextActions) doDeclareFile(thread *starlark.Thread, b *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
	if len(args) > 1 {
		return nil, fmt.Errorf("%s: got %d positional arguments, want at most 1", b.Name(), len(args))
	}
	var filename label.TargetName
	var sibling *model_starlark.File
	if err := starlark.UnpackArgs(
		b.Name(), args, kwargs,
		"filename", unpack.Bind(thread, &filename, unpack.TargetName),
		"sibling?", unpack.Bind(thread, &sibling, unpack.IfNotNone(unpack.Pointer(unpack.Type[model_starlark.File]("File")))),
	); err != nil {
		return nil, err
	}

	rc := rca.ruleContext
	return model_starlark.NewFile(&model_starlark_pb.File{
		Owner: &model_starlark_pb.File_Owner{
			// TODO: Fill in a proper hash.
			Cfg:        []byte{0xbe, 0x8a, 0x60, 0x1c, 0xe3, 0x03, 0x44, 0xf0},
			TargetName: rc.targetLabel.GetTargetName().String(),
		},
		Package:             rc.targetLabel.GetCanonicalPackage().String(),
		PackageRelativePath: filename.String(),
		Type:                model_starlark_pb.File_FILE,
	}), nil
}

func (rca *ruleContextActions) doRun(thread *starlark.Thread, b *starlark.Builtin, fnArgs starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
	if len(fnArgs) != 0 {
		return nil, fmt.Errorf("%s: got %d positional arguments, want 0", b.Name(), len(fnArgs))
	}
	var executable any
	var outputs []model_starlark.File
	var arguments []any
	var env map[string]string
	execGroup := ""
	var executionRequirements map[string]string
	var inputs any
	mnemonic := ""
	var progressMessage string
	var resourceSet *model_starlark.NamedFunction
	var toolchain *label.ResolvedLabel
	var tools []any
	useDefaultShellEnv := false
	if err := starlark.UnpackArgs(
		b.Name(), fnArgs, kwargs,
		// Required arguments.
		"executable", unpack.Bind(thread, &executable, unpack.Or([]unpack.UnpackerInto[any]{
			unpack.Decay(unpack.String),
			unpack.Decay(unpack.Type[model_starlark.File]("File")),
			unpack.Decay(unpack.Type[*model_starlark.Struct]("struct")),
		})),
		"outputs", unpack.Bind(thread, &outputs, unpack.List(unpack.Type[model_starlark.File]("File"))),
		// Optional arguments.
		"arguments?", unpack.Bind(thread, &arguments, unpack.List(unpack.Or([]unpack.UnpackerInto[any]{
			unpack.Decay(unpack.Type[*args]("Args")),
			unpack.Decay(unpack.String),
		}))),
		"env?", unpack.Bind(thread, &env, unpack.Dict(unpack.String, unpack.String)),
		"exec_group?", unpack.Bind(thread, &execGroup, unpack.IfNotNone(unpack.String)),
		"execution_requirements?", unpack.Bind(thread, &executionRequirements, unpack.Dict(unpack.String, unpack.String)),
		"inputs?", unpack.Bind(thread, &inputs, unpack.Or([]unpack.UnpackerInto[any]{
			unpack.Decay(unpack.Type[*model_starlark.Depset]("depset")),
			unpack.Decay(unpack.List(unpack.Type[model_starlark.File]("File"))),
		})),
		"mnemonic?", unpack.Bind(thread, &mnemonic, unpack.IfNotNone(unpack.String)),
		"progress_message?", unpack.Bind(thread, &progressMessage, unpack.IfNotNone(unpack.String)),
		"resource_set?", unpack.Bind(thread, &resourceSet, unpack.IfNotNone(unpack.Pointer(model_starlark.NamedFunctionUnpackerInto))),
		"toolchain?", unpack.Bind(thread, &toolchain, unpack.IfNotNone(unpack.Pointer(model_starlark.NewLabelOrStringUnpackerInto(model_starlark.CurrentFilePackage(thread, 1))))),
		"tools?", unpack.Bind(thread, &tools, unpack.Or([]unpack.UnpackerInto[[]any]{
			unpack.Singleton(unpack.Decay(unpack.Type[*model_starlark.Depset]("depset"))),
			unpack.List(unpack.Or([]unpack.UnpackerInto[any]{
				unpack.Decay(unpack.Type[*model_starlark.Depset]("depset")),
				unpack.Decay(unpack.Type[model_starlark.File]("File")),
				unpack.Decay(unpack.Type[*model_starlark.Struct]("struct")),
			})),
		})),
		"use_default_shell_env?", unpack.Bind(thread, &useDefaultShellEnv, unpack.Bool),
	); err != nil {
		return nil, err
	}

	return starlark.None, nil
}

func (rca *ruleContextActions) doRunShell(thread *starlark.Thread, b *starlark.Builtin, fnArgs starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
	if len(fnArgs) != 0 {
		return nil, fmt.Errorf("%s: got %d positional arguments, want 0", b.Name(), len(fnArgs))
	}
	var command string
	var outputs []model_starlark.File
	var arguments []any
	var env map[string]string
	execGroup := ""
	var executionRequirements map[string]string
	var inputs any
	mnemonic := ""
	progressMessage := ""
	var resourceSet *model_starlark.NamedFunction
	var toolchain *label.ResolvedLabel
	var tools []any
	useDefaultShellEnv := false
	if err := starlark.UnpackArgs(
		b.Name(), fnArgs, kwargs,
		// Required arguments.
		"outputs", unpack.Bind(thread, &outputs, unpack.List(unpack.Type[model_starlark.File]("File"))),
		"command", unpack.Bind(thread, &command, unpack.String),
		// Optional arguments.
		"arguments?", unpack.Bind(thread, &arguments, unpack.List(unpack.Or([]unpack.UnpackerInto[any]{
			unpack.Decay(unpack.Type[*args]("Args")),
			unpack.Decay(unpack.String),
		}))),
		"env?", unpack.Bind(thread, &env, unpack.Dict(unpack.String, unpack.String)),
		"exec_group?", unpack.Bind(thread, &execGroup, unpack.IfNotNone(unpack.String)),
		"execution_requirements?", unpack.Bind(thread, &executionRequirements, unpack.Dict(unpack.String, unpack.String)),
		"inputs?", unpack.Bind(thread, &inputs, unpack.Or([]unpack.UnpackerInto[any]{
			unpack.Decay(unpack.Type[*model_starlark.Depset]("depset")),
			unpack.Decay(unpack.List(unpack.Type[model_starlark.File]("File"))),
		})),
		"mnemonic?", unpack.Bind(thread, &mnemonic, unpack.IfNotNone(unpack.String)),
		"progress_message?", unpack.Bind(thread, &progressMessage, unpack.IfNotNone(unpack.String)),
		"resource_set?", unpack.Bind(thread, &resourceSet, unpack.IfNotNone(unpack.Pointer(model_starlark.NamedFunctionUnpackerInto))),
		"toolchain?", unpack.Bind(thread, &toolchain, unpack.IfNotNone(unpack.Pointer(model_starlark.NewLabelOrStringUnpackerInto(model_starlark.CurrentFilePackage(thread, 1))))),
		"tools?", unpack.Bind(thread, &tools, unpack.Or([]unpack.UnpackerInto[[]any]{
			unpack.Singleton(unpack.Decay(unpack.Type[*model_starlark.Depset]("depset"))),
			unpack.List(unpack.Or([]unpack.UnpackerInto[any]{
				unpack.Decay(unpack.Type[*model_starlark.Depset]("depset")),
				unpack.Decay(unpack.Type[model_starlark.File]("File")),
				unpack.Decay(unpack.Type[*model_starlark.Struct]("struct")),
			})),
		})),
		"use_default_shell_env?", unpack.Bind(thread, &useDefaultShellEnv, unpack.Bool),
	); err != nil {
		return nil, err
	}

	// TODO: Actually register the action.
	return starlark.None, nil
}

func (rca *ruleContextActions) doSymlink(thread *starlark.Thread, b *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
	var output model_starlark.File
	var targetFile *model_starlark.File
	targetPath := ""
	isExecutable := false
	progressMessage := ""
	if err := starlark.UnpackArgs(
		b.Name(), args, kwargs,
		"output", unpack.Bind(thread, &output, unpack.Type[model_starlark.File]("File")),
		"target_file?", unpack.Bind(thread, &targetFile, unpack.IfNotNone(unpack.Pointer(unpack.Type[model_starlark.File]("File")))),
		"target_path?", unpack.Bind(thread, &targetPath, unpack.IfNotNone(unpack.String)),
		"is_executable?", unpack.Bind(thread, &isExecutable, unpack.Bool),
		"progress_message?", unpack.Bind(thread, &progressMessage, unpack.IfNotNone(unpack.String)),
	); err != nil {
		return nil, err
	}

	// TODO: Actually register the action.
	return starlark.None, nil
}

func (rca *ruleContextActions) doTransformInfoFile(thread *starlark.Thread, b *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
	return starlark.None, nil
}

func (rca *ruleContextActions) doTransformVersionFile(thread *starlark.Thread, b *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
	return starlark.None, nil
}

func (rca *ruleContextActions) doWrite(thread *starlark.Thread, b *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
	var output model_starlark.File
	var content string
	isExecutable := false
	if err := starlark.UnpackArgs(
		b.Name(), args, kwargs,
		"output", unpack.Bind(thread, &output, unpack.Type[model_starlark.File]("File")),
		// TODO: Accept Args.
		"content", unpack.Bind(thread, &content, unpack.String),
		"is_executable?", unpack.Bind(thread, &isExecutable, unpack.Bool),
	); err != nil {
		return nil, err
	}
	return starlark.None, nil
}

type ruleContextAttr struct {
	ruleContext *ruleContext
}

var _ starlark.HasAttrs = (*ruleContextAttr)(nil)

func (ruleContextAttr) String() string {
	return "<ctx.attr>"
}

func (ruleContextAttr) Type() string {
	return "ctx.attr"
}

func (ruleContextAttr) Freeze() {
}

func (ruleContextAttr) Truth() starlark.Bool {
	return starlark.True
}

func (ruleContextAttr) Hash(thread *starlark.Thread) (uint32, error) {
	return 0, errors.New("ctx.attr cannot be hashed")
}

func (rca *ruleContextAttr) Attr(thread *starlark.Thread, name string) (starlark.Value, error) {
	rc := rca.ruleContext
	switch name {
	case "tags":
		if rc.tags == nil {
			tags := rc.ruleTarget.Message.Tags
			tagValues := make([]starlark.Value, 0, len(tags))
			for _, tag := range tags {
				tagValues = append(tagValues, starlark.String(tag))
			}
			rc.tags = starlark.NewList(tagValues)
			rc.tags.Freeze()
		}
		return rc.tags, nil
	default:
		// Starlark rule attr.
		ruleDefinitionAttrs := rc.ruleDefinition.Message.Attrs
		ruleDefinitionAttrIndex, ok := sort.Find(
			len(ruleDefinitionAttrs),
			func(i int) int { return strings.Compare(name, ruleDefinitionAttrs[i].Name) },
		)
		if !ok {
			return nil, starlark.NoSuchAttrError(fmt.Sprintf("rule does not have an attr named %#v", name))
		}
		attr := rc.attrs[ruleDefinitionAttrIndex]
		if attr == nil {
			// Decode the values of the parts of the attribute.
			namedAttr := ruleDefinitionAttrs[ruleDefinitionAttrIndex]
			valueParts, visibilityFromPackage, err := rc.getAttrValueParts(namedAttr)
			if err != nil {
				return nil, err
			}
			attr, err = rc.configureAttr(thread, namedAttr, valueParts, visibilityFromPackage)
			if err != nil {
				return nil, err
			}
			rc.attrs[ruleDefinitionAttrIndex] = attr
		}
		return attr, nil
	}
}

func (rca *ruleContextAttr) AttrNames() []string {
	attrs := rca.ruleContext.ruleDefinition.Message.Attrs
	attrNames := append(
		make([]string, 0, len(attrs)+1),
		"tags",
	)
	for _, attr := range attrs {
		attrNames = append(attrNames, attr.Name)
	}
	return attrNames
}

type ruleContextExecGroups struct {
	ruleContext *ruleContext
}

var _ starlark.Mapping = (*ruleContextExecGroups)(nil)

func (ruleContextExecGroups) String() string {
	return "<ctx.exec_groups>"
}

func (ruleContextExecGroups) Type() string {
	return "ctx.exec_groups"
}

func (ruleContextExecGroups) Freeze() {
}

func (ruleContextExecGroups) Truth() starlark.Bool {
	return starlark.True
}

func (ruleContextExecGroups) Hash(thread *starlark.Thread) (uint32, error) {
	return 0, errors.New("ctx.exec_groups cannot be hashed")
}

func (rca *ruleContextExecGroups) Get(thread *starlark.Thread, key starlark.Value) (starlark.Value, bool, error) {
	var execGroupName string
	if err := unpack.String.UnpackInto(thread, key, &execGroupName); err != nil {
		return nil, false, err
	}

	rc := rca.ruleContext
	execGroups := rc.ruleDefinition.Message.ExecGroups
	execGroupIndex, ok := sort.Find(
		len(execGroups),
		func(i int) int { return strings.Compare(execGroupName, execGroups[i].Name) },
	)
	if !ok {
		return nil, false, fmt.Errorf("rule does not have an exec group with name %#v", execGroupName)
	}

	return nil, true, fmt.Errorf("TODO: use exec group with index %d", execGroupIndex)
}

type ruleContextExecutable struct {
	ruleContext *ruleContext
}

var _ starlark.HasAttrs = (*ruleContextExecutable)(nil)

func (ruleContextExecutable) String() string {
	return "<ctx.executable>"
}

func (ruleContextExecutable) Type() string {
	return "ctx.executable"
}

func (ruleContextExecutable) Freeze() {
}

func (ruleContextExecutable) Truth() starlark.Bool {
	return starlark.True
}

func (ruleContextExecutable) Hash(thread *starlark.Thread) (uint32, error) {
	return 0, errors.New("ctx.executable cannot be hashed")
}

func (rce *ruleContextExecutable) Attr(thread *starlark.Thread, name string) (starlark.Value, error) {
	rc := rce.ruleContext
	ruleDefinitionAttrs := rc.ruleDefinition.Message.Attrs
	ruleDefinitionAttrIndex, ok := sort.Find(
		len(ruleDefinitionAttrs),
		func(i int) int { return strings.Compare(name, ruleDefinitionAttrs[i].Name) },
	)
	if !ok {
		return nil, starlark.NoSuchAttrError(fmt.Sprintf("rule does not have an attr named %#v", name))
	}

	executable := rc.executables[ruleDefinitionAttrIndex]
	if executable == nil {
		labelType, ok := ruleDefinitionAttrs[ruleDefinitionAttrIndex].Attr.GetType().(*model_starlark_pb.Attr_Label)
		if !ok {
			return nil, fmt.Errorf("attr %#v is not of type of label", name)
		}
		if !labelType.Label.Executable {
			return nil, fmt.Errorf("attr %#v does not have executable set", name)
		}

		// Decode the values of the parts of the attribute.
		valueParts, visibilityFromPackage, err := rc.getAttrValueParts(ruleDefinitionAttrs[ruleDefinitionAttrIndex])
		if err != nil {
			return nil, err
		}
		if len(valueParts.Message) != 1 {
			return nil, errors.New("labels cannot consist of multiple value parts")
		}
		switch labelValue := valueParts.Message[0].GetKind().(type) {
		case *model_starlark_pb.Value_Label:
			// Extract the executable from the label's DefaultInfo.
			configurationReference := rc.getPatchedConfigurationReference()
			visibleTarget := rc.environment.GetVisibleTargetValue(
				model_core.NewPatchedMessage(
					&model_analysis_pb.VisibleTarget_Key{
						FromPackage:            visibilityFromPackage.String(),
						ToLabel:                labelValue.Label,
						ConfigurationReference: configurationReference.Message,
					},
					configurationReference.Patcher,
				),
			)
			if !visibleTarget.IsSet() {
				return nil, evaluation.ErrMissingDependency
			}
			defaultInfo, err := getProviderFromConfiguredTarget(
				rc.environment,
				visibleTarget.Message.Label,
				rc.getPatchedConfigurationReference(),
				defaultInfoProviderIdentifier,
			)
			if err != nil {
				return nil, fmt.Errorf("attr %#v with label %#v: %w", name, visibleTarget.Message.Label, err)
			}
			listReader := model_parser.NewStorageBackedParsedObjectReader(
				rc.computer.objectDownloader,
				rc.computer.getValueObjectEncoder(),
				model_parser.NewMessageListObjectParser[object.LocalReference, model_starlark_pb.List_Element](),
			)
			filesToRun, err := model_starlark.GetStructFieldValue(rc.context, listReader, defaultInfo, "files_to_run")
			if err != nil {
				return nil, fmt.Errorf("failed to obtain field \"files_to_run\" of DefaultInfo provider of target with label %#v: %w", visibleTarget.Message.Label, err)
			}
			filesToRunStruct, ok := filesToRun.Message.Kind.(*model_starlark_pb.Value_Struct)
			if !ok {
				return nil, fmt.Errorf("field \"files_to_run\" of DefaultInfo provider of target with label %#v is not a struct", visibleTarget.Message.Label)
			}
			encodedExecutable, err := model_starlark.GetStructFieldValue(
				rc.context,
				listReader,
				model_core.NewNestedMessage(filesToRun, filesToRunStruct.Struct.Fields),
				"executable",
			)
			if err != nil {
				return nil, fmt.Errorf("failed to obtain field \"files_to_run.executable\" of DefaultInfo provider of target with label %#v: %w", visibleTarget.Message.Label, err)
			}

			executable, err = model_starlark.DecodeValue(
				encodedExecutable,
				/* currentIdentifier = */ nil,
				rc.computer.getValueDecodingOptions(rc.context, func(resolvedLabel label.ResolvedLabel) (starlark.Value, error) {
					return model_starlark.NewLabel(resolvedLabel), nil
				}),
			)
			if err != nil {
				return nil, fmt.Errorf("failed to decode executable of target with label %#v: %w", visibleTarget.Message.Label, err)
			}
		case *model_starlark_pb.Value_None:
			executable = starlark.None
		default:
			return nil, fmt.Errorf("value of attr %#v is not of type label", name)

		}

		// Cache attr value for subsequent lookups.
		executable.Freeze()
		rc.executables[ruleDefinitionAttrIndex] = executable
	}
	return executable, nil
}

func (ruleContextExecutable) AttrNames() []string {
	panic("TODO")
}

type ruleContextFile struct {
	ruleContext *ruleContext
}

var _ starlark.HasAttrs = (*ruleContextFile)(nil)

func (ruleContextFile) String() string {
	return "<ctx.file>"
}

func (ruleContextFile) Type() string {
	return "ctx.file"
}

func (ruleContextFile) Freeze() {
}

func (ruleContextFile) Truth() starlark.Bool {
	return starlark.True
}

func (ruleContextFile) Hash(thread *starlark.Thread) (uint32, error) {
	return 0, errors.New("ctx.file cannot be hashed")
}

func (rcf *ruleContextFile) Attr(thread *starlark.Thread, name string) (starlark.Value, error) {
	rc := rcf.ruleContext
	ruleDefinitionAttrs := rc.ruleDefinition.Message.Attrs
	ruleDefinitionAttrIndex, ok := sort.Find(
		len(ruleDefinitionAttrs),
		func(i int) int { return strings.Compare(name, ruleDefinitionAttrs[i].Name) },
	)
	if !ok {
		return nil, starlark.NoSuchAttrError(fmt.Sprintf("rule does not have an attr named %#v", name))
	}

	file := rc.singleFiles[ruleDefinitionAttrIndex]
	if file == nil {
		labelType, ok := ruleDefinitionAttrs[ruleDefinitionAttrIndex].Attr.GetType().(*model_starlark_pb.Attr_Label)
		if !ok {
			return nil, fmt.Errorf("attr %#v is not of type of label", name)
		}
		if !labelType.Label.AllowSingleFile {
			return nil, fmt.Errorf("attr %#v does not have allow_single_file set", name)
		}

		// Decode the values of the parts of the attribute.
		valueParts, visibilityFromPackage, err := rc.getAttrValueParts(ruleDefinitionAttrs[ruleDefinitionAttrIndex])
		if err != nil {
			return nil, err
		}
		if len(valueParts.Message) != 1 {
			return nil, errors.New("labels cannot consist of multiple value parts")
		}
		switch labelValue := valueParts.Message[0].GetKind().(type) {
		case *model_starlark_pb.Value_Label:
			// Extract the file from the label's DefaultInfo.
			configurationReference := rc.getPatchedConfigurationReference()
			visibleTarget := rc.environment.GetVisibleTargetValue(
				model_core.NewPatchedMessage(
					&model_analysis_pb.VisibleTarget_Key{
						FromPackage:            visibilityFromPackage.String(),
						ToLabel:                labelValue.Label,
						ConfigurationReference: configurationReference.Message,
					},
					configurationReference.Patcher,
				),
			)
			if !visibleTarget.IsSet() {
				return nil, evaluation.ErrMissingDependency
			}
			defaultInfo, err := getProviderFromConfiguredTarget(
				rc.environment,
				visibleTarget.Message.Label,
				rc.getPatchedConfigurationReference(),
				defaultInfoProviderIdentifier,
			)
			if err != nil {
				return nil, fmt.Errorf("attr %#v with label %#v: %w", name, visibleTarget.Message.Label, err)
			}
			listReader := model_parser.NewStorageBackedParsedObjectReader(
				rc.computer.objectDownloader,
				rc.computer.getValueObjectEncoder(),
				model_parser.NewMessageListObjectParser[object.LocalReference, model_starlark_pb.List_Element](),
			)
			files, err := model_starlark.GetStructFieldValue(rc.context, listReader, defaultInfo, "files")
			if err != nil {
				return nil, fmt.Errorf("failed to obtain field \"files\" of DefaultInfo provider of target with label %#v: %w", visibleTarget.Message.Label, err)
			}
			valueDepset, ok := files.Message.Kind.(*model_starlark_pb.Value_Depset)
			if !ok {
				return nil, fmt.Errorf("field \"files\" of DefaultInfo provider of target with label %#v is not a depset", visibleTarget.Message.Label)
			}
			filesDepset := valueDepset.Depset
			if len(filesDepset.Elements) != 1 {
				return nil, fmt.Errorf("target with label %#v does not yield exactly one file", visibleTarget.Message.Label)
			}
			element, ok := filesDepset.Elements[0].Level.(*model_starlark_pb.List_Element_Leaf)
			if !ok {
				return nil, fmt.Errorf("target with label %#v does not yield exactly one file", visibleTarget.Message.Label)
			}

			file, err = model_starlark.DecodeValue(
				model_core.NewNestedMessage(files, element.Leaf),
				/* currentIdentifier = */ nil,
				rc.computer.getValueDecodingOptions(rc.context, func(resolvedLabel label.ResolvedLabel) (starlark.Value, error) {
					return model_starlark.NewLabel(resolvedLabel), nil
				}),
			)
			if err != nil {
				return nil, fmt.Errorf("failed to decode file of target with label %#v: %w", visibleTarget.Message.Label, err)
			}
		case *model_starlark_pb.Value_None:
			file = starlark.None
		default:
			return nil, fmt.Errorf("value of attr %#v is not of type label", name)

		}

		// Cache attr value for subsequent lookups.
		file.Freeze()
		rc.singleFiles[ruleDefinitionAttrIndex] = file
	}
	return file, nil
}

func (rcf *ruleContextFile) AttrNames() []string {
	var attrNames []string
	for _, namedAttr := range rcf.ruleContext.ruleDefinition.Message.Attrs {
		if labelType, ok := namedAttr.Attr.GetType().(*model_starlark_pb.Attr_Label); ok && labelType.Label.AllowSingleFile {
			attrNames = append(attrNames, namedAttr.Name)
		}
	}
	return attrNames
}

type ruleContextFiles struct {
	ruleContext *ruleContext
}

var _ starlark.HasAttrs = (*ruleContextFiles)(nil)

func (ruleContextFiles) String() string {
	return "<ctx.files>"
}

func (ruleContextFiles) Type() string {
	return "ctx.files"
}

func (ruleContextFiles) Freeze() {
}

func (ruleContextFiles) Truth() starlark.Bool {
	return starlark.True
}

func (ruleContextFiles) Hash(thread *starlark.Thread) (uint32, error) {
	return 0, errors.New("ctx.files cannot be hashed")
}

func (rcf *ruleContextFiles) Attr(thread *starlark.Thread, name string) (starlark.Value, error) {
	rc := rcf.ruleContext
	ruleDefinitionAttrs := rc.ruleDefinition.Message.Attrs
	ruleDefinitionAttrIndex, ok := sort.Find(
		len(ruleDefinitionAttrs),
		func(i int) int { return strings.Compare(name, ruleDefinitionAttrs[i].Name) },
	)
	if !ok {
		return nil, starlark.NoSuchAttrError(fmt.Sprintf("rule does not have an attr named %#v", name))
	}

	files := rc.multipleFiles[ruleDefinitionAttrIndex]
	if files == nil {
		namedAttr := ruleDefinitionAttrs[ruleDefinitionAttrIndex]
		switch namedAttr.Attr.GetType().(type) {
		case *model_starlark_pb.Attr_Label, *model_starlark_pb.Attr_LabelList:
		default:
			return nil, fmt.Errorf("attr %#v is not of type label or label_list", name)
		}

		// Walk over all labels contained in the value.
		valueParts, visibilityFromPackage, err := rc.getAttrValueParts(namedAttr)
		if err != nil {
			return nil, err
		}
		labeListParentsSeen := map[object.LocalReference]struct{}{}
		listReader := model_parser.NewStorageBackedParsedObjectReader(
			rc.computer.objectDownloader,
			rc.computer.getValueObjectEncoder(),
			model_parser.NewMessageListObjectParser[object.LocalReference, model_starlark_pb.List_Element](),
		)
		missingDependencies := false
		var filesDepsetElements []any
		for _, valuePart := range valueParts.Message {
			var labelList model_core.Message[[]*model_starlark_pb.List_Element]
			if listValue, ok := valuePart.Kind.(*model_starlark_pb.Value_List); ok {
				labelList = model_core.NewNestedMessage(valueParts, listValue.List.Elements)
			} else {
				labelList = model_core.NewNestedMessage(valueParts, []*model_starlark_pb.List_Element{{
					Level: &model_starlark_pb.List_Element_Leaf{
						Leaf: valuePart,
					},
				}})
			}

			var errIter error
			for encodedElement := range model_starlark.AllListLeafElementsSkippingDuplicateParents(
				rc.context,
				listReader,
				labelList,
				labeListParentsSeen,
				&errIter,
			) {
				// For each label contained in the value,
				// obtain the DefaultInfo.
				labelElement, ok := encodedElement.Message.Kind.(*model_starlark_pb.Value_Label)
				if !ok {
					return nil, fmt.Errorf("attr %#v contains non-label values", name)
				}
				configurationReference := rc.getPatchedConfigurationReference()
				visibleTarget := rc.environment.GetVisibleTargetValue(
					model_core.NewPatchedMessage(
						&model_analysis_pb.VisibleTarget_Key{
							FromPackage:            visibilityFromPackage.String(),
							ToLabel:                labelElement.Label,
							ConfigurationReference: configurationReference.Message,
						},
						configurationReference.Patcher,
					),
				)
				if !visibleTarget.IsSet() {
					missingDependencies = true
					continue
				}
				defaultInfo, err := getProviderFromConfiguredTarget(
					rc.environment,
					visibleTarget.Message.Label,
					rc.getPatchedConfigurationReference(),
					defaultInfoProviderIdentifier,
				)
				if err != nil {
					return nil, fmt.Errorf("attr %#v with label %#v: %w", name, visibleTarget.Message.Label, err)
				}

				// Obtain the "files" depset contained within
				// the DefaultInfo provider.
				files, err := model_starlark.GetStructFieldValue(rc.context, listReader, defaultInfo, "files")
				if err != nil {
					return nil, fmt.Errorf("failed to obtain field \"files\" of DefaultInfo provider of target with label %#v: %w", visibleTarget.Message.Label, err)
				}
				valueDepset, ok := files.Message.Kind.(*model_starlark_pb.Value_Depset)
				if !ok {
					return nil, fmt.Errorf("field \"files\" of DefaultInfo provider of target with label %#v is not a depset", visibleTarget.Message.Label)
				}
				for _, element := range valueDepset.Depset.Elements {
					filesDepsetElements = append(filesDepsetElements, model_core.NewNestedMessage(files, element))
				}
			}
			if errIter != nil {
				return nil, fmt.Errorf("failed to iterate value of attr %#v: %w", name, errIter)
			}
		}
		if missingDependencies {
			return nil, evaluation.ErrMissingDependency
		}

		// Place all of the gathered file elements in a single
		// depset and convert it back to a list.
		filesDepset := model_starlark.NewDepsetFromList(filesDepsetElements, model_starlark_pb.Depset_DEFAULT)
		files, err = filesDepset.ToList(thread)
		if err != nil {
			return nil, err
		}

		// Cache attr value for subsequent lookups.
		files.Freeze()
		rc.multipleFiles[ruleDefinitionAttrIndex] = files
	}
	return files, nil
}

func (rcf *ruleContextFiles) AttrNames() []string {
	var attrNames []string
	for _, namedAttr := range rcf.ruleContext.ruleDefinition.Message.Attrs {
		switch namedAttr.Attr.GetType().(type) {
		case *model_starlark_pb.Attr_Label, *model_starlark_pb.Attr_LabelList:
			attrNames = append(attrNames, namedAttr.Name)
		default:
		}
	}
	return attrNames
}

type ruleContextFragments struct {
	ruleContext *ruleContext
}

var _ starlark.HasAttrs = (*ruleContextFragments)(nil)

func (ruleContextFragments) String() string {
	return "<ctx.fragments>"
}

func (ruleContextFragments) Type() string {
	return "ctx.fragments"
}

func (ruleContextFragments) Freeze() {
}

func (ruleContextFragments) Truth() starlark.Bool {
	return starlark.True
}

func (ruleContextFragments) Hash(thread *starlark.Thread) (uint32, error) {
	return 0, errors.New("ctx.fragments cannot be hashed")
}

func (rcf *ruleContextFragments) Attr(thread *starlark.Thread, name string) (starlark.Value, error) {
	rc := rcf.ruleContext
	fragmentInfo, ok := rc.fragments[name]
	if !ok {
		targetName, err := label.NewTargetName(name)
		if err != nil {
			return nil, fmt.Errorf("invalid target name %#v: %w", name, err)
		}
		encodedFragmentInfo, err := getProviderFromConfiguredTarget(
			rc.environment,
			fragmentsPackage.AppendTargetName(targetName).String(),
			rc.getPatchedConfigurationReference(),
			fragmentInfoProviderIdentifier,
		)
		if err != nil {
			return nil, err
		}
		fragmentInfo, err = model_starlark.DecodeStruct(
			model_core.NewNestedMessage(encodedFragmentInfo, &model_starlark_pb.Struct{
				ProviderInstanceProperties: &model_starlark_pb.Provider_InstanceProperties{
					ProviderIdentifier: fragmentInfoProviderIdentifier.String(),
				},
				Fields: encodedFragmentInfo.Message,
			}),
			rc.computer.getValueDecodingOptions(rc.context, func(resolvedLabel label.ResolvedLabel) (starlark.Value, error) {
				return model_starlark.NewLabel(resolvedLabel), nil
			}),
		)
		if err != nil {
			return nil, err
		}
		rc.fragments[name] = fragmentInfo
	}
	return fragmentInfo, nil
}

func (ruleContextFragments) AttrNames() []string {
	// TODO: implement.
	return nil
}

type ruleContextOutputs struct {
	ruleContext *ruleContext
}

var _ starlark.HasAttrs = (*ruleContextOutputs)(nil)

func (ruleContextOutputs) String() string {
	return "<ctx.outputs>"
}

func (ruleContextOutputs) Type() string {
	return "ctx.outputs"
}

func (ruleContextOutputs) Freeze() {
}

func (ruleContextOutputs) Truth() starlark.Bool {
	return starlark.True
}

func (ruleContextOutputs) Hash(thread *starlark.Thread) (uint32, error) {
	return 0, errors.New("ctx.outputs cannot be hashed")
}

func (rco *ruleContextOutputs) Attr(thread *starlark.Thread, name string) (starlark.Value, error) {
	rc := rco.ruleContext
	ruleDefinitionAttrs := rc.ruleDefinition.Message.Attrs
	ruleDefinitionAttrIndex, ok := sort.Find(
		len(ruleDefinitionAttrs),
		func(i int) int { return strings.Compare(name, ruleDefinitionAttrs[i].Name) },
	)
	if !ok {
		return nil, starlark.NoSuchAttrError(fmt.Sprintf("rule does not have an attr named %#v", name))
	}

	outputs := rc.outputs[ruleDefinitionAttrIndex]
	if outputs == nil {
		namedAttr := ruleDefinitionAttrs[ruleDefinitionAttrIndex]
		switch namedAttr.Attr.GetType().(type) {
		case *model_starlark_pb.Attr_Output:
			// Single output file.
			valueParts, _, err := rc.getAttrValueParts(ruleDefinitionAttrs[ruleDefinitionAttrIndex])
			if err != nil {
				return nil, err
			}
			if len(valueParts.Message) != 1 {
				return nil, errors.New("labels cannot consist of multiple value parts")
			}
			switch valueParts.Message[0].GetKind().(type) {
			case *model_starlark_pb.Value_Label:
				labelValue, ok := valueParts.Message[0].GetKind().(*model_starlark_pb.Value_Label)
				if !ok {
					return nil, fmt.Errorf("value of attr %#v is not of type label", name)
				}
				outputLabel, err := label.NewResolvedLabel(labelValue.Label)
				if err != nil {
					return nil, fmt.Errorf("invalid resolved label %#v: %w", labelValue.Label, err)
				}
				outputCanonicalLabel, err := outputLabel.AsCanonical()
				if err != nil {
					return nil, err
				}
				outputPackage := outputCanonicalLabel.GetCanonicalPackage()
				if outputPackage != rc.targetLabel.GetCanonicalPackage() {
					return nil, fmt.Errorf("output attr %#v is set to label %#v, which refers to a different package", name, labelValue.Label)
				}
				outputs = model_starlark.NewFile(&model_starlark_pb.File{
					Owner: &model_starlark_pb.File_Owner{
						// TODO: Fill in a proper hash.
						Cfg:        []byte{0xbe, 0x8a, 0x60, 0x1c, 0xe3, 0x03, 0x44, 0xf0},
						TargetName: rc.targetLabel.GetTargetName().String(),
					},
					Package:             outputPackage.String(),
					PackageRelativePath: outputLabel.GetTargetName().String(),
					Type:                model_starlark_pb.File_FILE,
				})
			case *model_starlark_pb.Value_None:
				outputs = starlark.None
			default:
				return nil, fmt.Errorf("attr %#v has a non-label value", name)
			}
		case *model_starlark_pb.Attr_OutputList:
			return nil, errors.New("TODO: Implement!")
		default:
			return nil, fmt.Errorf("attr %#v is not of type output or output_list", name)
		}

		// Cache attr value for subsequent lookups.
		outputs.Freeze()
		rc.outputs[ruleDefinitionAttrIndex] = outputs
	}
	return outputs, nil
}

func (ruleContextOutputs) AttrNames() []string {
	return nil
}

type toolchainContext struct {
	ruleContext    *ruleContext
	execGroupIndex int
}

var _ starlark.Mapping = (*toolchainContext)(nil)

func (toolchainContext) String() string {
	return "<toolchain context>"
}

func (toolchainContext) Type() string {
	return "ToolchainContext"
}

func (toolchainContext) Freeze() {
}

func (toolchainContext) Truth() starlark.Bool {
	return starlark.True
}

func (toolchainContext) Hash(thread *starlark.Thread) (uint32, error) {
	return 0, errors.New("ToolchainContext cannot be hashed")
}

func (tc *toolchainContext) Get(thread *starlark.Thread, v starlark.Value) (starlark.Value, bool, error) {
	rc := tc.ruleContext
	labelUnpackerInto := unpack.Stringer(model_starlark.NewLabelOrStringUnpackerInto(model_starlark.CurrentFilePackage(thread, 0)))
	var toolchainType string
	if err := labelUnpackerInto.UnpackInto(thread, v, &toolchainType); err != nil {
		return nil, false, err
	}

	namedExecGroup := rc.ruleDefinition.Message.ExecGroups[tc.execGroupIndex]
	execGroupDefinition := namedExecGroup.ExecGroup
	if execGroupDefinition == nil {
		return nil, false, errors.New("rule definition lacks exec group definition")
	}

	toolchains := execGroupDefinition.Toolchains
	toolchainIndex, ok := sort.Find(
		len(toolchains),
		func(i int) int { return strings.Compare(toolchainType, toolchains[i].ToolchainType) },
	)
	if !ok {
		return nil, false, fmt.Errorf("exec group %#v does not depend on toolchain type %#v", namedExecGroup.Name, toolchainType)
	}

	execGroup := rc.execGroups[tc.execGroupIndex]
	if execGroup == nil {
		execCompatibleWith, err := rc.computer.constraintValuesToConstraints(
			rc.context,
			rc.environment,
			rc.targetLabel.GetCanonicalPackage(),
			execGroupDefinition.ExecCompatibleWith,
		)
		if err != nil {
			return nil, true, evaluation.ErrMissingDependency
		}
		configurationReference := rc.getPatchedConfigurationReference()
		resolvedToolchains := rc.environment.GetResolvedToolchainsValue(
			model_core.PatchedMessage[*model_analysis_pb.ResolvedToolchains_Key, dag.ObjectContentsWalker]{
				Message: &model_analysis_pb.ResolvedToolchains_Key{
					ExecCompatibleWith:     execCompatibleWith,
					ConfigurationReference: configurationReference.Message,
					Toolchains:             execGroupDefinition.Toolchains,
				},
				Patcher: configurationReference.Patcher,
			},
		)
		if !resolvedToolchains.IsSet() {
			return nil, true, evaluation.ErrMissingDependency
		}
		toolchainIdentifiers := resolvedToolchains.Message.ToolchainIdentifiers
		if actual, expected := len(toolchainIdentifiers), len(toolchains); actual != expected {
			return nil, true, fmt.Errorf("obtained %d resolved toolchains, while %d were expected", actual, expected)
		}

		execGroup = &ruleContextExecGroupState{
			toolchainIdentifiers: toolchainIdentifiers,
			toolchainInfos:       make([]starlark.Value, len(toolchains)),
		}
		rc.execGroups[tc.execGroupIndex] = execGroup
	}

	toolchainInfo := execGroup.toolchainInfos[toolchainIndex]
	if toolchainInfo == nil {
		toolchainIdentifier := execGroup.toolchainIdentifiers[toolchainIndex]
		if toolchainIdentifier == "" {
			// Toolchain was optional, and no matching
			// toolchain was found.
			toolchainInfo = starlark.None
		} else {
			encodedToolchainInfo, err := getProviderFromConfiguredTarget(
				rc.environment,
				toolchainIdentifier,
				rc.getPatchedConfigurationReference(),
				toolchainInfoProviderIdentifier,
			)
			if err != nil {
				return nil, true, err
			}

			toolchainInfo, err = model_starlark.DecodeStruct(
				model_core.NewNestedMessage(encodedToolchainInfo, &model_starlark_pb.Struct{
					ProviderInstanceProperties: &model_starlark_pb.Provider_InstanceProperties{
						ProviderIdentifier: toolchainInfoProviderIdentifier.String(),
					},
					Fields: encodedToolchainInfo.Message,
				}),
				rc.computer.getValueDecodingOptions(rc.context, func(resolvedLabel label.ResolvedLabel) (starlark.Value, error) {
					return model_starlark.NewLabel(resolvedLabel), nil
				}),
			)
			if err != nil {
				return nil, true, err
			}
		}
		execGroup.toolchainInfos[toolchainIndex] = toolchainInfo
	}
	return toolchainInfo, true, nil
}

type ruleContextExecGroupState struct {
	toolchainIdentifiers []string
	toolchainInfos       []starlark.Value
}

type getProviderFromConfiguredTargetEnvironment interface {
	GetConfiguredTargetValue(key model_core.PatchedMessage[*model_analysis_pb.ConfiguredTarget_Key, dag.ObjectContentsWalker]) model_core.Message[*model_analysis_pb.ConfiguredTarget_Value]
}

// getProviderFromConfiguredTarget looks up a single provider that is
// provided by a configured target
func getProviderFromConfiguredTarget(e getProviderFromConfiguredTargetEnvironment, targetLabel string, configurationReference model_core.PatchedMessage[*model_core_pb.Reference, dag.ObjectContentsWalker], providerIdentifier label.CanonicalStarlarkIdentifier) (model_core.Message[*model_starlark_pb.Struct_Fields], error) {
	configuredTargetValue := e.GetConfiguredTargetValue(
		model_core.PatchedMessage[*model_analysis_pb.ConfiguredTarget_Key, dag.ObjectContentsWalker]{
			Message: &model_analysis_pb.ConfiguredTarget_Key{
				Label:                  targetLabel,
				ConfigurationReference: configurationReference.Message,
			},
			Patcher: configurationReference.Patcher,
		},
	)
	if !configuredTargetValue.IsSet() {
		return model_core.Message[*model_starlark_pb.Struct_Fields]{}, evaluation.ErrMissingDependency
	}

	providerIdentifierStr := providerIdentifier.String()
	providerInstances := configuredTargetValue.Message.ProviderInstances
	if providerIndex, ok := sort.Find(
		len(providerInstances),
		func(i int) int {
			return strings.Compare(providerIdentifierStr, providerInstances[i].ProviderInstanceProperties.GetProviderIdentifier())
		},
	); ok {
		return model_core.NewNestedMessage(configuredTargetValue, providerInstances[providerIndex].Fields), nil
	}
	return model_core.Message[*model_starlark_pb.Struct_Fields]{}, fmt.Errorf("target did not yield provider %#v", providerIdentifierStr)
}

type args struct{}

var _ starlark.HasAttrs = (*args)(nil)

func (args) String() string {
	return "<Args>"
}

func (args) Type() string {
	return "Args"
}

func (args) Freeze() {}

func (args) Truth() starlark.Bool {
	return starlark.True
}

func (args) Hash(thread *starlark.Thread) (uint32, error) {
	return 0, errors.New("Args cannot be hashed")
}

func (a *args) Attr(thread *starlark.Thread, name string) (starlark.Value, error) {
	switch name {
	case "add":
		return starlark.NewBuiltin("Args.add", a.doAdd), nil
	case "add_all":
		return starlark.NewBuiltin("Args.add_all", a.doAddAll), nil
	case "add_joined":
		return starlark.NewBuiltin("Args.add_joined", a.doAddJoined), nil
	case "set_param_file_format":
		return starlark.NewBuiltin("Args.set_param_file_format", a.doSetParamFileFormat), nil
	case "use_param_file":
		return starlark.NewBuiltin("Args.use_param_file", a.doUseParamFile), nil
	default:
		return nil, nil
	}
}

var argsAttrNames = []string{
	"add",
	"add_all",
	"add_joined",
	"set_param_file_format",
	"use_param_file",
}

func (args) AttrNames() []string {
	return argsAttrNames
}

func (a *args) doAdd(thread *starlark.Thread, b *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
	return a, nil
}

func (a *args) doAddAll(thread *starlark.Thread, b *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
	return a, nil
}

func (a *args) doAddJoined(thread *starlark.Thread, b *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
	return a, nil
}

func (a *args) doSetParamFileFormat(thread *starlark.Thread, b *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
	return a, nil
}

func (a *args) doUseParamFile(thread *starlark.Thread, b *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
	return a, nil
}

type subruleContext struct {
	ruleContext *ruleContext
}

var _ starlark.HasAttrs = (*subruleContext)(nil)

func (sc *subruleContext) String() string {
	rc := sc.ruleContext
	return fmt.Sprintf("<subrule_ctx for %s>", rc.targetLabel.String())
}

func (subruleContext) Type() string {
	return "subrule_ctx"
}

func (subruleContext) Freeze() {
}

func (subruleContext) Truth() starlark.Bool {
	return starlark.True
}

func (subruleContext) Hash(thread *starlark.Thread) (uint32, error) {
	return 0, errors.New("subrule_ctx cannot be hashed")
}

func (sc *subruleContext) Attr(thread *starlark.Thread, name string) (starlark.Value, error) {
	rc := sc.ruleContext
	switch name {
	case "fragments":
		return &ruleContextFragments{
			ruleContext: rc,
		}, nil
	default:
		return nil, nil
	}
}

var subruleContextAttrNames = []string{
	"fragments",
}

func (subruleContext) AttrNames() []string {
	return subruleContextAttrNames
}
