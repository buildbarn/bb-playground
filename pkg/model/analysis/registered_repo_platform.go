package analysis

import (
	"context"
	"errors"
	"fmt"

	"github.com/buildbarn/bb-playground/pkg/evaluation"
	"github.com/buildbarn/bb-playground/pkg/label"
	model_core "github.com/buildbarn/bb-playground/pkg/model/core"
	model_analysis_pb "github.com/buildbarn/bb-playground/pkg/proto/model/analysis"
	model_starlark_pb "github.com/buildbarn/bb-playground/pkg/proto/model/starlark"
	"github.com/buildbarn/bb-playground/pkg/storage/dag"
)

func (c *baseComputer) ComputeRegisteredRepoPlatformValue(ctx context.Context, key *model_analysis_pb.RegisteredRepoPlatform_Key, e RegisteredRepoPlatformEnvironment) (PatchedRegisteredRepoPlatformValue, error) {
	// Obtain the label of the repo platform that was provided by
	// the client through the --repo_platform command line flag.
	buildSpecificationValue := e.GetBuildSpecificationValue(&model_analysis_pb.BuildSpecification_Key{})
	if !buildSpecificationValue.IsSet() {
		return PatchedRegisteredRepoPlatformValue{}, evaluation.ErrMissingDependency
	}
	buildSpecification := buildSpecificationValue.Message.BuildSpecification

	rootModuleName := buildSpecification.GetRootModuleName()
	rootModule, err := label.NewModule(rootModuleName)
	if err != nil {
		return PatchedRegisteredRepoPlatformValue{}, fmt.Errorf("invalid root module name %#v: %w", rootModuleName, err)
	}
	rootRepo := rootModule.ToModuleInstance(nil).GetBareCanonicalRepo()

	repoPlatformStr := buildSpecification.GetRepoPlatform()
	if repoPlatformStr == "" {
		return PatchedRegisteredRepoPlatformValue{}, errors.New("no repo platform specified, meaning module extensions and repository rules cannot be evaluated")
	}
	apparentRepoPlatformLabel, err := rootRepo.GetRootPackage().AppendLabel(repoPlatformStr)
	if err != nil {
		return PatchedRegisteredRepoPlatformValue{}, fmt.Errorf("invalid repo platform label %#v: %w", repoPlatformStr, err)
	}
	repoPlatformLabel, err := resolveApparentLabel(e, rootRepo, apparentRepoPlatformLabel)
	if err != nil {
		return PatchedRegisteredRepoPlatformValue{}, fmt.Errorf("failed to resolve repo platform label %#v: %w", apparentRepoPlatformLabel.String(), err)
	}

	// Obtain the PlatformInfo provider of the repo platform.
	platformInfoProvider, err := getPlatformInfoProvider(e, repoPlatformLabel)
	if err != nil {
		return PatchedRegisteredRepoPlatformValue{}, fmt.Errorf("failed to obtain PlatformInfo of repo platform %#v: %w", repoPlatformLabel.String(), err)
	}
	platformInfoFields := make(map[string]*model_starlark_pb.Value, len(platformInfoProvider.Message.Fields))
	for _, field := range platformInfoProvider.Message.Fields {
		platformInfoFields[field.Name] = field.Value
	}

	// Extract fields from the PlatformInfo provider that are needed
	// when evaluating module extensions and repository rules. We
	// don't need to extract the constraints, as those are only used
	// by the toolchain resolution process of regular build rules.
	//
	// TODO: Extract exec_properties and repository_os_environ!
	repositoryOSArch, ok := platformInfoFields["repository_os_arch"].GetKind().(*model_starlark_pb.Value_Str)
	if !ok {
		return PatchedRegisteredRepoPlatformValue{}, fmt.Errorf("PlatformInfo of repo platform %#v lacks mandatory field repository_os_arch", repoPlatformLabel.String())
	}
	if repositoryOSArch.Str == "" {
		return PatchedRegisteredRepoPlatformValue{}, fmt.Errorf("repository_os_arch field of PlatformInfo of repo platform %#v is not set to a non-empty string", repoPlatformLabel.String())
	}
	repositoryOSName, ok := platformInfoFields["repository_os_name"].GetKind().(*model_starlark_pb.Value_Str)
	if !ok {
		return PatchedRegisteredRepoPlatformValue{}, fmt.Errorf("PlatformInfo of repo platform %#v lacks mandatory field repository_os_name", repoPlatformLabel.String())
	}
	if repositoryOSName.Str == "" {
		return PatchedRegisteredRepoPlatformValue{}, fmt.Errorf("repository_os_name field of PlatformInfo of repo platform %#v is not set to a non-empty string", repoPlatformLabel.String())
	}

	return model_core.NewSimplePatchedMessage[dag.ObjectContentsWalker](
		&model_analysis_pb.RegisteredRepoPlatform_Value{
			RepositoryOsArch: repositoryOSArch.Str,
			RepositoryOsName: repositoryOSName.Str,
		},
	), nil
}
