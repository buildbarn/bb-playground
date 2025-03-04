package analysis

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/buildbarn/bonanza/pkg/evaluation"
	"github.com/buildbarn/bonanza/pkg/label"
	model_core "github.com/buildbarn/bonanza/pkg/model/core"
	model_filesystem "github.com/buildbarn/bonanza/pkg/model/filesystem"
	model_analysis_pb "github.com/buildbarn/bonanza/pkg/proto/model/analysis"
	model_filesystem_pb "github.com/buildbarn/bonanza/pkg/proto/model/filesystem"
	pg_starlark "github.com/buildbarn/bonanza/pkg/starlark"
	"github.com/buildbarn/bonanza/pkg/storage/dag"
	"github.com/buildbarn/bonanza/pkg/storage/object"
)

type parseLocalModuleDotBazelEnvironment interface {
	parseModuleDotBazelFileEnvironment
	GetFilePropertiesValue(key *model_analysis_pb.FileProperties_Key) model_core.Message[*model_analysis_pb.FileProperties_Value, object.OutgoingReferences]
}

func (c *baseComputer) parseLocalModuleInstanceModuleDotBazel(ctx context.Context, moduleInstance label.ModuleInstance, e parseLocalModuleDotBazelEnvironment, handler pg_starlark.RootModuleDotBazelHandler) error {
	// Load a file that we know exists in storage already.
	moduleFileProperties := e.GetFilePropertiesValue(&model_analysis_pb.FileProperties_Key{
		CanonicalRepo: moduleInstance.String(),
		Path:          moduleDotBazelFilename,
	})
	if !moduleFileProperties.IsSet() {
		return evaluation.ErrMissingDependency
	}
	if moduleFileProperties.Message.Exists == nil {
		return fmt.Errorf("file %#v does not exist", moduleDotBazelTargetName.String())
	}

	return c.parseModuleDotBazel(
		ctx,
		model_core.NewNestedMessage(moduleFileProperties, moduleFileProperties.Message.Exists.Contents),
		moduleInstance,
		e,
		handler,
	)
}

type parseActiveModuleDotBazelEnvironment interface {
	parseModuleDotBazelFileEnvironment
	GetModuleDotBazelContentsValue(key *model_analysis_pb.ModuleDotBazelContents_Key) model_core.Message[*model_analysis_pb.ModuleDotBazelContents_Value, object.OutgoingReferences]
}

func (c *baseComputer) parseActiveModuleInstanceModuleDotBazel(ctx context.Context, moduleInstance label.ModuleInstance, e parseActiveModuleDotBazelEnvironment, handler pg_starlark.RootModuleDotBazelHandler) error {
	// This module file might have to be loaded.
	moduleFileContentsValue := e.GetModuleDotBazelContentsValue(&model_analysis_pb.ModuleDotBazelContents_Key{
		ModuleInstance: moduleInstance.String(),
	})
	if !moduleFileContentsValue.IsSet() {
		return evaluation.ErrMissingDependency
	}
	return c.parseModuleDotBazel(
		ctx,
		model_core.NewNestedMessage(moduleFileContentsValue, moduleFileContentsValue.Message.Contents),
		moduleInstance,
		e,
		handler,
	)
}

type parseModuleDotBazelFileEnvironment interface {
	GetFileReaderValue(key *model_analysis_pb.FileReader_Key) (*model_filesystem.FileReader, bool)
}

func (c *baseComputer) parseModuleDotBazel(ctx context.Context, moduleContentsMsg model_core.Message[*model_filesystem_pb.FileContents, object.OutgoingReferences], moduleInstance label.ModuleInstance, e parseModuleDotBazelFileEnvironment, handler pg_starlark.RootModuleDotBazelHandler) error {
	fileReader, gotFileReader := e.GetFileReaderValue(&model_analysis_pb.FileReader_Key{})
	if !gotFileReader {
		return evaluation.ErrMissingDependency
	}

	moduleTarget := moduleInstance.GetBareCanonicalRepo().GetRootPackage().AppendTargetName(moduleDotBazelTargetName)
	moduleFileContentsEntry, err := model_filesystem.NewFileContentsEntryFromProto(
		moduleContentsMsg,
		c.getReferenceFormat(),
	)
	if err != nil {
		return fmt.Errorf("invalid file contents entry for file %#v: %w", moduleTarget.String(), err)
	}
	moduleFileContents, err := fileReader.FileReadAll(ctx, moduleFileContentsEntry, 1<<20)
	if err != nil {
		return err
	}

	return pg_starlark.ParseModuleDotBazel(
		string(moduleFileContents),
		moduleTarget,
		nil,
		handler,
	)
}

type visitModuleDotBazelFilesBreadthFirstEnvironment interface {
	parseActiveModuleDotBazelEnvironment

	GetFileReaderValue(*model_analysis_pb.FileReader_Key) (*model_filesystem.FileReader, bool)
	GetModulesWithMultipleVersionsObjectValue(*model_analysis_pb.ModulesWithMultipleVersionsObject_Key) (map[label.Module]OverrideVersions, bool)
	GetRootModuleValue(*model_analysis_pb.RootModule_Key) model_core.Message[*model_analysis_pb.RootModule_Value, object.OutgoingReferences]
}

type dependencQueueingModuleDotBazelHandler struct {
	pg_starlark.ChildModuleDotBazelHandler

	modulesWithMultipleVersions map[label.Module]OverrideVersions
	moduleInstancesToCheck      *[]label.ModuleInstance
	moduleInstancesSeen         map[label.ModuleInstance]struct{}
	ignoreDevDependencies       bool
}

func (h *dependencQueueingModuleDotBazelHandler) BazelDep(name label.Module, version *label.ModuleVersion, maxCompatibilityLevel int, repoName label.ApparentRepo, devDependency bool) error {
	if devDependency && h.ignoreDevDependencies {
		return nil
	}

	var moduleInstance label.ModuleInstance
	overrideVersions, ok := h.modulesWithMultipleVersions[name]
	if ok {
		v, err := overrideVersions.LookupNearestVersion(version)
		if err != nil {
			return fmt.Errorf("invalid dependency of module %#v: %w", name.String(), err)
		}
		moduleInstance = name.ToModuleInstance(&v)
	} else {
		moduleInstance = name.ToModuleInstance(nil)
	}

	if _, ok := h.moduleInstancesSeen[moduleInstance]; !ok {
		*h.moduleInstancesToCheck = append(*h.moduleInstancesToCheck, moduleInstance)
		h.moduleInstancesSeen[moduleInstance] = struct{}{}
	}

	return h.ChildModuleDotBazelHandler.BazelDep(name, version, maxCompatibilityLevel, repoName, devDependency)
}

func (c *baseComputer) visitModuleDotBazelFilesBreadthFirst(
	ctx context.Context,
	e visitModuleDotBazelFilesBreadthFirstEnvironment,
	createHandler func(moduleInstance label.ModuleInstance, ignoreDevDependencies bool) pg_starlark.ChildModuleDotBazelHandler,
) error {
	rootModuleValue := e.GetRootModuleValue(&model_analysis_pb.RootModule_Key{})
	modulesWithMultipleVersions, gotModulesWithMultipleVersions := e.GetModulesWithMultipleVersionsObjectValue(&model_analysis_pb.ModulesWithMultipleVersionsObject_Key{})
	if !rootModuleValue.IsSet() || !gotModulesWithMultipleVersions {
		return evaluation.ErrMissingDependency
	}

	// The root module is the starting point of our traversal.
	rootModuleName, err := label.NewModule(rootModuleValue.Message.RootModuleName)
	if err != nil {
		return err
	}
	rootModuleInstance := rootModuleName.ToModuleInstance(nil)
	moduleInstancesToCheck := []label.ModuleInstance{rootModuleInstance}
	moduleInstancesSeen := map[label.ModuleInstance]struct{}{
		rootModuleInstance: {},
	}
	ignoreDevDependencies := rootModuleValue.Message.IgnoreRootModuleDevDependencies

	var finalErr error
	for len(moduleInstancesToCheck) > 0 {
		moduleInstance := moduleInstancesToCheck[0]
		moduleInstancesToCheck = moduleInstancesToCheck[1:]

		if err := c.parseActiveModuleInstanceModuleDotBazel(
			ctx,
			moduleInstance,
			e,
			pg_starlark.NewOverrideIgnoringRootModuleDotBazelHandler(&dependencQueueingModuleDotBazelHandler{
				ChildModuleDotBazelHandler:  createHandler(moduleInstance, ignoreDevDependencies),
				modulesWithMultipleVersions: modulesWithMultipleVersions,
				moduleInstancesToCheck:      &moduleInstancesToCheck,
				moduleInstancesSeen:         moduleInstancesSeen,
				ignoreDevDependencies:       ignoreDevDependencies,
			}),
		); err != nil {
			// Continue iteration if we have missing
			// dependency errors, so that we compute these
			// aggressively.
			if !errors.Is(err, evaluation.ErrMissingDependency) {
				return err
			}
			finalErr = err
		}

		ignoreDevDependencies = true
	}
	return finalErr
}

func (c *baseComputer) ComputeModuleDotBazelContentsValue(ctx context.Context, key *model_analysis_pb.ModuleDotBazelContents_Key, e ModuleDotBazelContentsEnvironment) (PatchedModuleDotBazelContentsValue, error) {
	moduleInstance, err := label.NewModuleInstance(key.ModuleInstance)
	if err != nil {
		return PatchedModuleDotBazelContentsValue{}, fmt.Errorf("invalid module instance: %w", err)
	}

	canonicalRepo := moduleInstance.GetBareCanonicalRepo()
	expectedName := moduleInstance.GetModule()
	expectedNameStr := expectedName.String()
	expectedVersion, hasVersion := moduleInstance.GetModuleVersion()

	// Check to see if there is an override for this module, and if it has been loaded.
	moduleOverrides := e.GetModulesWithOverridesValue(&model_analysis_pb.ModulesWithOverrides_Key{})
	if !moduleOverrides.IsSet() {
		return PatchedModuleDotBazelContentsValue{}, evaluation.ErrMissingDependency
	}

	overrideList := moduleOverrides.Message.OverridesList
	if _, found := sort.Find(
		len(overrideList),
		func(i int) int {
			return strings.Compare(expectedNameStr, overrideList[i].Name)
		},
	); found { // Override found.
		// Access the MODULE.bazel file that is part of the module's sources.
		filePropertiesValue := e.GetFilePropertiesValue(&model_analysis_pb.FileProperties_Key{
			CanonicalRepo: canonicalRepo.String(),
			Path:          moduleDotBazelFilename,
		})
		if !filePropertiesValue.IsSet() {
			return PatchedModuleDotBazelContentsValue{}, evaluation.ErrMissingDependency
		}
		fileContents := model_core.NewPatchedMessageFromExisting(
			model_core.NewNestedMessage(filePropertiesValue, filePropertiesValue.Message.Exists.Contents),
			func(index int) dag.ObjectContentsWalker {
				return dag.ExistingObjectContentsWalker
			},
		)
		return PatchedModuleDotBazelContentsValue{
			Message: &model_analysis_pb.ModuleDotBazelContents_Value{
				Contents: fileContents.Message,
			},
			Patcher: fileContents.Patcher,
		}, nil
	}

	// See if the module instance is one of the resolved modules
	// that was downloaded from Bazel Central Registry. If so, we
	// prefer using the MODULE.bazel file that was downloaded
	// separately instead of the one contained in the module's
	// source archive. This prevents us from downloading and
	// extracting modules that are otherwise unused by the build.
	finalBuildListValue := e.GetModuleFinalBuildListValue(&model_analysis_pb.ModuleFinalBuildList_Key{})
	if !finalBuildListValue.IsSet() {
		return PatchedModuleDotBazelContentsValue{}, evaluation.ErrMissingDependency
	}

	buildList := finalBuildListValue.Message.BuildList
	if i, ok := sort.Find(
		len(buildList),
		func(i int) int {
			module := buildList[i]
			if cmp := strings.Compare(expectedNameStr, module.Name); cmp != 0 {
				return cmp
			}
			if hasVersion {
				if version, err := label.NewModuleVersion(module.Version); err == nil {
					if cmp := expectedVersion.Compare(version); cmp != 0 {
						return cmp
					}
				}
			}
			return 0
		},
	); ok {
		foundModule := buildList[i]
		foundVersion, err := label.NewModuleVersion(foundModule.Version)
		if err != nil {
			return PatchedModuleDotBazelContentsValue{}, fmt.Errorf("invalid version %#v for module %#v: %w", foundModule.Version, foundModule.Name, err)
		}
		if !hasVersion || expectedVersion.Compare(foundVersion) == 0 {
			moduleFileURL, err := getModuleDotBazelURL(foundModule.RegistryUrl, expectedName, foundVersion)
			if err != nil {
				return PatchedModuleDotBazelContentsValue{}, fmt.Errorf("failed to construct URL for module %s with version %s in registry %#v: %s", foundModule.Name, foundModule.Version, foundModule.RegistryUrl)
			}

			fileContentsValue := e.GetHttpFileContentsValue(&model_analysis_pb.HttpFileContents_Key{Urls: []string{moduleFileURL}})
			if !fileContentsValue.IsSet() {
				return PatchedModuleDotBazelContentsValue{}, evaluation.ErrMissingDependency
			}
			if fileContentsValue.Message.Exists == nil {
				return PatchedModuleDotBazelContentsValue{}, fmt.Errorf("file at URL %#v does not exist", moduleFileURL)
			}
			fileContents := model_core.NewPatchedMessageFromExisting(
				model_core.NewNestedMessage(fileContentsValue, fileContentsValue.Message.Exists.Contents),
				func(index int) dag.ObjectContentsWalker {
					return dag.ExistingObjectContentsWalker
				},
			)
			return PatchedModuleDotBazelContentsValue{
				Message: &model_analysis_pb.ModuleDotBazelContents_Value{
					Contents: fileContents.Message,
				},
				Patcher: fileContents.Patcher,
			}, nil
		}
	}

	return PatchedModuleDotBazelContentsValue{}, errors.New("unknown module")
}
