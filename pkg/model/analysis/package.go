package analysis

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"path"
	"slices"
	"sort"

	"github.com/buildbarn/bb-playground/pkg/evaluation"
	"github.com/buildbarn/bb-playground/pkg/label"
	model_core "github.com/buildbarn/bb-playground/pkg/model/core"
	"github.com/buildbarn/bb-playground/pkg/model/core/btree"
	model_encoding "github.com/buildbarn/bb-playground/pkg/model/encoding"
	model_filesystem "github.com/buildbarn/bb-playground/pkg/model/filesystem"
	model_starlark "github.com/buildbarn/bb-playground/pkg/model/starlark"
	model_analysis_pb "github.com/buildbarn/bb-playground/pkg/proto/model/analysis"
	model_filesystem_pb "github.com/buildbarn/bb-playground/pkg/proto/model/filesystem"
	model_starlark_pb "github.com/buildbarn/bb-playground/pkg/proto/model/starlark"
	"github.com/buildbarn/bb-playground/pkg/storage/dag"
	"github.com/buildbarn/bb-playground/pkg/storage/object"

	"go.starlark.net/starlark"
	"go.starlark.net/syntax"
)

var buildDotBazelTargetNames = []label.TargetName{
	label.MustNewTargetName("BUILD.bazel"),
	label.MustNewTargetName("BUILD"),
}

func (c *baseComputer) ComputePackageValue(ctx context.Context, key *model_analysis_pb.Package_Key, e PackageEnvironment) (PatchedPackageValue, error) {
	canonicalPackage, err := label.NewCanonicalPackage(key.Label)
	if err != nil {
		return model_core.NewSimplePatchedMessage[dag.ObjectContentsWalker](&model_analysis_pb.Package_Value{
			Result: &model_analysis_pb.Package_Value_Failure{
				Failure: fmt.Sprintf("invalid package label %#v: %s", key.Label, err),
			},
		}), nil
	}
	canonicalRepo := canonicalPackage.GetCanonicalRepo()

	for _, buildFileName := range buildDotBazelTargetNames {
		buildFileProperties := e.GetFilePropertiesValue(&model_analysis_pb.FileProperties_Key{
			CanonicalRepo: canonicalRepo.String(),
			Path:          path.Join(canonicalPackage.GetPackagePath(), buildFileName.String()),
		})
		fileReader, fileReaderErr := e.GetFileReaderValue(&model_analysis_pb.FileReader_Key{})
		if fileReaderErr != nil && !errors.Is(fileReaderErr, evaluation.ErrMissingDependency) {
			return model_core.NewSimplePatchedMessage[dag.ObjectContentsWalker](&model_analysis_pb.Package_Value{
				Result: &model_analysis_pb.Package_Value_Failure{
					Failure: fileReaderErr.Error(),
				},
			}), nil
		}
		allBuiltinsModulesNames := e.GetBuiltinsModuleNamesValue(&model_analysis_pb.BuiltinsModuleNames_Key{})

		repoDefaultAttrsValue := e.GetRepoDefaultAttrsValue(&model_analysis_pb.RepoDefaultAttrs_Key{
			CanonicalRepo: canonicalRepo.String(),
		})

		if !buildFileProperties.IsSet() {
			return PatchedPackageValue{}, evaluation.ErrMissingDependency
		}
		switch buildFilePropertiesResult := buildFileProperties.Message.Result.(type) {
		case *model_analysis_pb.FileProperties_Value_Exists:
			if fileReaderErr != nil {
				return PatchedPackageValue{}, fileReaderErr
			}
			if !allBuiltinsModulesNames.IsSet() || !repoDefaultAttrsValue.IsSet() {
				return PatchedPackageValue{}, evaluation.ErrMissingDependency
			}

			builtinsModuleNames := allBuiltinsModulesNames.Message.BuiltinsModuleNames
			buildFileBuiltins, err := c.getBzlFileBuiltins(e, builtinsModuleNames, model_starlark.BuildFileBuiltins, "exported_rules")
			if err != nil {
				if !errors.Is(err, evaluation.ErrMissingDependency) {
					return model_core.NewSimplePatchedMessage[dag.ObjectContentsWalker](&model_analysis_pb.Package_Value{
						Result: &model_analysis_pb.Package_Value_Failure{
							Failure: err.Error(),
						},
					}), nil
				}
				return PatchedPackageValue{}, err
			}

			buildFileContentsEntry, err := model_filesystem.NewFileContentsEntryFromProto(
				model_core.Message[*model_filesystem_pb.FileContents]{
					Message:            buildFilePropertiesResult.Exists.GetContents(),
					OutgoingReferences: buildFileProperties.OutgoingReferences,
				},
				c.buildSpecificationReference.GetReferenceFormat(),
			)
			if err != nil {
				return model_core.NewSimplePatchedMessage[dag.ObjectContentsWalker](&model_analysis_pb.Package_Value{
					Result: &model_analysis_pb.Package_Value_Failure{
						Failure: fmt.Sprintf("Invalid file contents: %s", err),
					},
				}), nil
			}
			buildFileData, err := fileReader.FileReadAll(ctx, buildFileContentsEntry, 1<<20)
			if err != nil {
				return PatchedPackageValue{}, err
			}

			buildFileLabel := canonicalPackage.AppendTargetName(buildFileName)
			_, program, err := starlark.SourceProgramOptions(
				&syntax.FileOptions{},
				buildFileLabel.String(),
				buildFileData,
				buildFileBuiltins.Has,
			)
			if err != nil {
				return model_core.NewSimplePatchedMessage[dag.ObjectContentsWalker](&model_analysis_pb.Package_Value{
					Result: &model_analysis_pb.Package_Value_Failure{
						Failure: fmt.Sprintf("failed to load %#v: %s", buildFileLabel.String(), err),
					},
				}), nil
			}

			if err := c.preloadBzlGlobals(e, canonicalPackage, program, builtinsModuleNames); err != nil {
				if !errors.Is(err, evaluation.ErrMissingDependency) {
					return model_core.NewSimplePatchedMessage[dag.ObjectContentsWalker](&model_analysis_pb.Package_Value{
						Result: &model_analysis_pb.Package_Value_Failure{
							Failure: err.Error(),
						},
					}), nil
				}
				return PatchedPackageValue{}, err
			}

			thread := c.newStarlarkThread(e, builtinsModuleNames)
			thread.SetLocal(model_starlark.CanonicalPackageKey, canonicalPackage)

			var repoDefaultAttrs model_core.Message[*model_starlark_pb.InheritableAttrs]
			switch repoDefaultAttrsResult := repoDefaultAttrsValue.Message.Result.(type) {
			case *model_analysis_pb.RepoDefaultAttrs_Value_Success:
				repoDefaultAttrs = model_core.Message[*model_starlark_pb.InheritableAttrs]{
					Message:            repoDefaultAttrsResult.Success,
					OutgoingReferences: repoDefaultAttrsValue.OutgoingReferences,
				}
			case *model_analysis_pb.RepoDefaultAttrs_Value_Failure:
				return model_core.NewSimplePatchedMessage[dag.ObjectContentsWalker](&model_analysis_pb.Package_Value{
					Result: &model_analysis_pb.Package_Value_Failure{
						Failure: repoDefaultAttrsResult.Failure,
					},
				}), nil
			default:
				return model_core.NewSimplePatchedMessage[dag.ObjectContentsWalker](&model_analysis_pb.Package_Value{
					Result: &model_analysis_pb.Package_Value_Failure{
						Failure: "Repo default attrs value has an unknown result type",
					},
				}), nil
			}

			targetRegistrar := model_starlark.NewTargetRegistrar(c.getInlinedTreeOptions(), repoDefaultAttrs)
			thread.SetLocal(model_starlark.TargetRegistrarKey, targetRegistrar)

			thread.SetLocal(model_starlark.GlobalResolverKey, func(identifier label.CanonicalStarlarkIdentifier) (model_core.Message[*model_starlark_pb.Value], error) {
				canonicalLabel := identifier.GetCanonicalLabel()
				compiledBzlFile := e.GetCompiledBzlFileValue(&model_analysis_pb.CompiledBzlFile_Key{
					Label:               canonicalLabel.String(),
					BuiltinsModuleNames: trimBuiltinModuleNames(builtinsModuleNames, canonicalLabel.GetCanonicalRepo().GetModuleInstance().GetModule()),
				})
				if !compiledBzlFile.IsSet() {
					return model_core.Message[*model_starlark_pb.Value]{}, evaluation.ErrMissingDependency
				}
				switch resultType := compiledBzlFile.Message.Result.(type) {
				case *model_analysis_pb.CompiledBzlFile_Value_CompiledProgram:
					identifierStr := identifier.GetStarlarkIdentifier().String()
					globals := resultType.CompiledProgram.Globals
					if i := sort.Search(len(globals), func(i int) bool {
						return globals[i].Name >= identifierStr
					}); i < len(globals) && globals[i].Name == identifierStr {
						global := globals[i]
						if global.Value == nil {
							return model_core.Message[*model_starlark_pb.Value]{}, fmt.Errorf("global %#v has no value", identifier.String())
						}
						return model_core.Message[*model_starlark_pb.Value]{
							Message:            global.Value,
							OutgoingReferences: compiledBzlFile.OutgoingReferences,
						}, nil
					}
					return model_core.Message[*model_starlark_pb.Value]{}, fmt.Errorf("global %#v does not exist", identifier.String())
				case *model_analysis_pb.CompiledBzlFile_Value_Failure:
					return model_core.Message[*model_starlark_pb.Value]{}, errors.New(resultType.Failure)
				default:
					return model_core.Message[*model_starlark_pb.Value]{}, errors.New("compiled .bzl file value has an unknown result type")
				}
			})

			thread.SetLocal(model_starlark.ValueEncodingOptionsKey, c.getValueEncodingOptions(buildFileLabel))

			// Execute the BUILD.bazel file, so that all targets
			// contained within are instantiated.
			if _, err := program.Init(thread, buildFileBuiltins); err != nil {
				if !errors.Is(err, evaluation.ErrMissingDependency) {
					var failureMessage string
					var evalErr *starlark.EvalError
					if errors.As(err, &evalErr) {
						failureMessage = fmt.Sprintf("%s: %s", evalErr.Backtrace(), evalErr.Msg)
					} else {
						failureMessage = err.Error()
					}
					return model_core.NewSimplePatchedMessage[dag.ObjectContentsWalker](&model_analysis_pb.Package_Value{
						Result: &model_analysis_pb.Package_Value_Failure{
							Failure: failureMessage,
						},
					}), nil
				}
				return PatchedPackageValue{}, err
			}

			// Store all targets in a B-tree.
			// TODO: Use a proper encoder!
			treeBuilder := btree.NewSplitProllyBuilder(
				/* minimumSizeBytes = */ 32*1024,
				/* maximumSizeBytes = */ 128*1024,
				btree.NewObjectCreatingNodeMerger(
					model_encoding.NewChainedBinaryEncoder(nil),
					c.buildSpecificationReference.GetReferenceFormat(),
					/* parentNodeComputer = */ func(contents *object.Contents, childNodes []*model_analysis_pb.Package_Value_TargetList_Element, outgoingReferences object.OutgoingReferences, metadata []dag.ObjectContentsWalker) (model_core.PatchedMessage[*model_analysis_pb.Package_Value_TargetList_Element, dag.ObjectContentsWalker], error) {
						var firstName string
						switch firstElement := childNodes[0].Level.(type) {
						case *model_analysis_pb.Package_Value_TargetList_Element_Leaf:
							firstName = firstElement.Leaf.Name
						case *model_analysis_pb.Package_Value_TargetList_Element_Parent_:
							firstName = firstElement.Parent.FirstName
						}
						patcher := model_core.NewReferenceMessagePatcher[dag.ObjectContentsWalker]()
						return model_core.PatchedMessage[*model_analysis_pb.Package_Value_TargetList_Element, dag.ObjectContentsWalker]{
							Message: &model_analysis_pb.Package_Value_TargetList_Element{
								Level: &model_analysis_pb.Package_Value_TargetList_Element_Parent_{
									Parent: &model_analysis_pb.Package_Value_TargetList_Element_Parent{
										Reference: patcher.AddReference(contents.GetReference(), dag.NewSimpleObjectContentsWalker(contents, metadata)),
										FirstName: firstName,
									},
								},
							},
							Patcher: patcher,
						}, nil
					},
				),
			)

			targets := targetRegistrar.GetTargets()
			for _, name := range slices.Sorted(maps.Keys(targets)) {
				target := targets[name]
				if err := treeBuilder.PushChild(model_core.PatchedMessage[*model_analysis_pb.Package_Value_TargetList_Element, dag.ObjectContentsWalker]{
					Message: &model_analysis_pb.Package_Value_TargetList_Element{
						Level: &model_analysis_pb.Package_Value_TargetList_Element_Leaf{
							Leaf: target.Message,
						},
					},
					Patcher: target.Patcher,
				}); err != nil {
					return PatchedPackageValue{}, err
				}
			}

			targetsList, err := treeBuilder.FinalizeList()
			if err != nil {
				return PatchedPackageValue{}, err
			}

			return PatchedPackageValue{
				Message: &model_analysis_pb.Package_Value{
					Result: &model_analysis_pb.Package_Value_Success{
						Success: &model_analysis_pb.Package_Value_TargetList{
							Elements: targetsList.Message,
						},
					},
				},
				Patcher: targetsList.Patcher,
			}, nil
		case *model_analysis_pb.FileProperties_Value_DoesNotExist:
			// Try the next filename.
		case *model_analysis_pb.FileProperties_Value_Failure:
			return model_core.NewSimplePatchedMessage[dag.ObjectContentsWalker](&model_analysis_pb.Package_Value{
				Result: &model_analysis_pb.Package_Value_Failure{
					Failure: buildFilePropertiesResult.Failure,
				},
			}), nil
		default:
			return model_core.NewSimplePatchedMessage[dag.ObjectContentsWalker](&model_analysis_pb.Package_Value{
				Result: &model_analysis_pb.Package_Value_Failure{
					Failure: "File properties value has an unknown result type",
				},
			}), nil
		}
	}

	return model_core.NewSimplePatchedMessage[dag.ObjectContentsWalker](&model_analysis_pb.Package_Value{
		Result: &model_analysis_pb.Package_Value_Failure{
			Failure: "BUILD.bazel does not exist",
		},
	}), nil
}
