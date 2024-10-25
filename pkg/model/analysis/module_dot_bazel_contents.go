package analysis

import (
	"context"
	"errors"
	"fmt"
	"sort"

	"github.com/buildbarn/bb-playground/pkg/evaluation"
	"github.com/buildbarn/bb-playground/pkg/label"
	model_core "github.com/buildbarn/bb-playground/pkg/model/core"
	model_filesystem "github.com/buildbarn/bb-playground/pkg/model/filesystem"
	model_analysis_pb "github.com/buildbarn/bb-playground/pkg/proto/model/analysis"
	model_filesystem_pb "github.com/buildbarn/bb-playground/pkg/proto/model/filesystem"
	pg_starlark "github.com/buildbarn/bb-playground/pkg/starlark"
	"github.com/buildbarn/bb-playground/pkg/storage/dag"
)

type parseActiveModuleDotBazelEnvironment interface {
	GetModuleDotBazelContentsValue(key *model_analysis_pb.ModuleDotBazelContents_Key) model_core.Message[*model_analysis_pb.ModuleDotBazelContents_Value]
}

func (c *baseComputer) parseModuleInstanceModuleDotBazel(ctx context.Context, moduleInstance label.ModuleInstance, e parseActiveModuleDotBazelEnvironment, fileReader *model_filesystem.FileReader, handler pg_starlark.ChildModuleDotBazelHandler) error {
	moduleFileLabel := moduleInstance.GetBareCanonicalRepo().
		GetRootPackage().
		AppendTargetName(moduleDotBazelTargetName)
	moduleFileContentsValue := e.GetModuleDotBazelContentsValue(&model_analysis_pb.ModuleDotBazelContents_Key{
		ModuleInstance: moduleInstance.String(),
	})
	if !moduleFileContentsValue.IsSet() {
		return evaluation.ErrMissingDependency
	}
	var moduleFileContents model_core.Message[*model_filesystem_pb.FileContents]
	switch result := moduleFileContentsValue.Message.Result.(type) {
	case *model_analysis_pb.ModuleDotBazelContents_Value_Success_:
		moduleFileContents = model_core.Message[*model_filesystem_pb.FileContents]{
			Message:            result.Success.Contents,
			OutgoingReferences: moduleFileContentsValue.OutgoingReferences,
		}
	case *model_analysis_pb.ModuleDotBazelContents_Value_Failure:
		return fmt.Errorf("failed to obtain properties of %#v: %s", moduleFileLabel.String(), result.Failure)
	default:
		return errors.New("MODULE.bazel contents value has an unknown result type")
	}

	moduleFileContentsEntry, err := model_filesystem.NewFileContentsEntryFromProto(
		moduleFileContents,
		c.buildSpecificationReference.GetReferenceFormat(),
	)
	if err != nil {
		return fmt.Errorf("invalid file contents entry for file %#v: %w", moduleFileLabel.String(), err)
	}

	moduleFileData, err := fileReader.FileReadAll(ctx, moduleFileContentsEntry, 1<<20)
	if err != nil {
		return err
	}

	return pg_starlark.ParseModuleDotBazel(
		string(moduleFileData),
		moduleFileLabel,
		nil,
		pg_starlark.NewOverrideIgnoringRootModuleDotBazelHandler(handler),
	)
}

func (c *baseComputer) ComputeModuleDotBazelContentsValue(ctx context.Context, key *model_analysis_pb.ModuleDotBazelContents_Key, e ModuleDotBazelContentsEnvironment) (PatchedModuleDotBazelContentsValue, error) {
	moduleInstance, err := label.NewModuleInstance(key.ModuleInstance)
	if err != nil {
		return model_core.NewSimplePatchedMessage[dag.ObjectContentsWalker](&model_analysis_pb.ModuleDotBazelContents_Value{
			Result: &model_analysis_pb.ModuleDotBazelContents_Value_Failure{
				Failure: fmt.Sprintf("Invalid module instance: %s", err),
			},
		}), nil
	}

	finalBuildListValue := e.GetModuleFinalBuildListValue(&model_analysis_pb.ModuleFinalBuildList_Key{})
	if !finalBuildListValue.IsSet() {
		return PatchedModuleDotBazelContentsValue{}, evaluation.ErrMissingDependency
	}

	var buildList []*model_analysis_pb.BuildList_Module
	switch result := finalBuildListValue.Message.Result.(type) {
	case *model_analysis_pb.ModuleFinalBuildList_Value_Success:
		buildList = result.Success.Modules
	case *model_analysis_pb.ModuleFinalBuildList_Value_Failure:
		return model_core.NewSimplePatchedMessage[dag.ObjectContentsWalker](&model_analysis_pb.ModuleDotBazelContents_Value{
			Result: &model_analysis_pb.ModuleDotBazelContents_Value_Failure{
				Failure: result.Failure,
			},
		}), nil
	default:
		return model_core.NewSimplePatchedMessage[dag.ObjectContentsWalker](&model_analysis_pb.ModuleDotBazelContents_Value{
			Result: &model_analysis_pb.ModuleDotBazelContents_Value_Failure{
				Failure: "Final build list value has an unknown result type",
			},
		}), nil
	}

	// See if the module instance is one of the resolved modules
	// that was downloaded from Bazel Central Registry. If so, we
	// prefer using the MODULE.bazel file that was downloaded
	// separately instead of the one contained in the module's
	// source archive. This prevents us from downloading and
	// extracting modules that are otherwise unused by the build.
	expectedName := moduleInstance.GetModule()
	expectedNameStr := expectedName.String()
	expectedVersion, hasVersion := moduleInstance.GetModuleVersion()
	if i := sort.Search(
		len(buildList),
		func(i int) bool {
			module := buildList[i]
			if expectedNameStr < module.Name {
				return true
			} else if expectedNameStr > module.Name {
				return false
			}
			if hasVersion {
				if version, err := label.NewModuleVersion(module.Version); err == nil {
					if cmp := expectedVersion.Compare(version); cmp < 0 {
						return true
					} else if cmp > 0 {
						return false
					}
				}
			}
			return true
		},
	); i < len(buildList) && buildList[i].Name == expectedNameStr {
		foundModule := buildList[i]
		foundVersion, err := label.NewModuleVersion(foundModule.Version)
		if err != nil {
			return model_core.NewSimplePatchedMessage[dag.ObjectContentsWalker](&model_analysis_pb.ModuleDotBazelContents_Value{
				Result: &model_analysis_pb.ModuleDotBazelContents_Value_Failure{
					Failure: fmt.Sprintf("Invalid version %#v for module %#v: %s", foundModule.Version, foundModule.Name, err),
				},
			}), nil
		}
		if !hasVersion || expectedVersion.Compare(foundVersion) == 0 {
			moduleFileURL, err := getModuleDotBazelURL(foundModule.RegistryUrl, expectedName, foundVersion)
			if err != nil {
				return model_core.NewSimplePatchedMessage[dag.ObjectContentsWalker](&model_analysis_pb.ModuleDotBazelContents_Value{
					Result: &model_analysis_pb.ModuleDotBazelContents_Value_Failure{
						Failure: fmt.Sprintf("Failed to construct URL for module %s with version %s in registry %#v: %s", foundModule.Name, foundModule.Version, foundModule.RegistryUrl),
					},
				}), nil
			}

			fileContentsValue := e.GetHttpFileContentsValue(&model_analysis_pb.HttpFileContents_Key{Url: moduleFileURL})
			if !fileContentsValue.IsSet() {
				return PatchedModuleDotBazelContentsValue{}, evaluation.ErrMissingDependency
			}
			switch httpFileContentsResult := fileContentsValue.Message.Result.(type) {
			case *model_analysis_pb.HttpFileContents_Value_Exists_:
				fileContents := model_core.NewPatchedMessageFromExisting(
					model_core.Message[*model_filesystem_pb.FileContents]{
						Message:            httpFileContentsResult.Exists.Contents,
						OutgoingReferences: fileContentsValue.OutgoingReferences,
					},
					func(index int) dag.ObjectContentsWalker {
						return dag.ExistingObjectContentsWalker
					},
				)
				return PatchedModuleDotBazelContentsValue{
					Message: &model_analysis_pb.ModuleDotBazelContents_Value{
						Result: &model_analysis_pb.ModuleDotBazelContents_Value_Success_{
							Success: &model_analysis_pb.ModuleDotBazelContents_Value_Success{
								Contents: fileContents.Message,
							},
						},
					},
					Patcher: fileContents.Patcher,
				}, nil
			case *model_analysis_pb.HttpFileContents_Value_DoesNotExist:
				return model_core.NewSimplePatchedMessage[dag.ObjectContentsWalker](&model_analysis_pb.ModuleDotBazelContents_Value{
					Result: &model_analysis_pb.ModuleDotBazelContents_Value_Failure{
						Failure: fmt.Sprintf("Failed to fetch %#v, as the file does not exist", moduleFileURL),
					},
				}), nil
			case *model_analysis_pb.HttpFileContents_Value_Failure:
				return model_core.NewSimplePatchedMessage[dag.ObjectContentsWalker](&model_analysis_pb.ModuleDotBazelContents_Value{
					Result: &model_analysis_pb.ModuleDotBazelContents_Value_Failure{
						Failure: fmt.Sprintf("Failed to fetch %#v: %s", moduleFileURL, httpFileContentsResult.Failure),
					},
				}), nil
			default:
				return model_core.NewSimplePatchedMessage[dag.ObjectContentsWalker](&model_analysis_pb.ModuleDotBazelContents_Value{
					Result: &model_analysis_pb.ModuleDotBazelContents_Value_Failure{
						Failure: "HTTP file contents value has an unknown result type",
					},
				}), nil
			}
		}
	}

	// Access the MODULE.bazel file that is part of the module's sources.
	filePropertiesValue := e.GetFilePropertiesValue(&model_analysis_pb.FileProperties_Key{
		CanonicalRepo: moduleInstance.String(),
		Path:          moduleDotBazelFilename,
	})
	if !filePropertiesValue.IsSet() {
		return PatchedModuleDotBazelContentsValue{}, evaluation.ErrMissingDependency
	}
	switch filePropertiesResult := filePropertiesValue.Message.Result.(type) {
	case *model_analysis_pb.FileProperties_Value_Exists:
		fileContents := model_core.NewPatchedMessageFromExisting(
			model_core.Message[*model_filesystem_pb.FileContents]{
				Message:            filePropertiesResult.Exists.Contents,
				OutgoingReferences: filePropertiesValue.OutgoingReferences,
			},
			func(index int) dag.ObjectContentsWalker {
				return dag.ExistingObjectContentsWalker
			},
		)
		return PatchedModuleDotBazelContentsValue{
			Message: &model_analysis_pb.ModuleDotBazelContents_Value{
				Result: &model_analysis_pb.ModuleDotBazelContents_Value_Success_{
					Success: &model_analysis_pb.ModuleDotBazelContents_Value_Success{
						Contents: fileContents.Message,
					},
				},
			},
			Patcher: fileContents.Patcher,
		}, nil
	case *model_analysis_pb.FileProperties_Value_DoesNotExist:
		return model_core.NewSimplePatchedMessage[dag.ObjectContentsWalker](&model_analysis_pb.ModuleDotBazelContents_Value{
			Result: &model_analysis_pb.ModuleDotBazelContents_Value_Failure{
				Failure: "File does not exist",
			},
		}), nil
	case *model_analysis_pb.FileProperties_Value_Failure:
		return model_core.NewSimplePatchedMessage[dag.ObjectContentsWalker](&model_analysis_pb.ModuleDotBazelContents_Value{
			Result: &model_analysis_pb.ModuleDotBazelContents_Value_Failure{
				Failure: filePropertiesResult.Failure,
			},
		}), nil
	default:
		return model_core.NewSimplePatchedMessage[dag.ObjectContentsWalker](&model_analysis_pb.ModuleDotBazelContents_Value{
			Result: &model_analysis_pb.ModuleDotBazelContents_Value_Failure{
				Failure: "File properties value has an unknown result type",
			},
		}), nil
	}
}
