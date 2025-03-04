package analysis

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"maps"
	"slices"

	"github.com/buildbarn/bonanza/pkg/evaluation"
	"github.com/buildbarn/bonanza/pkg/label"
	model_core "github.com/buildbarn/bonanza/pkg/model/core"
	model_starlark "github.com/buildbarn/bonanza/pkg/model/starlark"
	model_analysis_pb "github.com/buildbarn/bonanza/pkg/proto/model/analysis"
	model_core_pb "github.com/buildbarn/bonanza/pkg/proto/model/core"
	model_starlark_pb "github.com/buildbarn/bonanza/pkg/proto/model/starlark"
	"github.com/buildbarn/bonanza/pkg/storage/dag"
	"github.com/buildbarn/bonanza/pkg/storage/object"
)

func (c *baseComputer) decodeStringDict(ctx context.Context, d model_core.Message[*model_starlark_pb.Value, object.OutgoingReferences]) (map[string]string, error) {
	dict, ok := d.Message.GetKind().(*model_starlark_pb.Value_Dict)
	if !ok {
		return nil, errors.New("not a dict")
	}
	var iterErr error
	o := map[string]string{}
	for entry := range model_starlark.AllDictLeafEntries(
		ctx,
		c.valueDereferencers.Dict,
		model_core.NewNestedMessage(d, dict.Dict),
		&iterErr,
	) {
		key, ok := entry.Message.Key.GetKind().(*model_starlark_pb.Value_Str)
		if !ok {
			return nil, errors.New("key is not a string")
		}
		value, ok := entry.Message.Value.GetKind().(*model_starlark_pb.Value_Str)
		if !ok {
			return nil, errors.New("value is not a string")
		}
		o[key.Str] = value.Str
	}
	if iterErr != nil {
		return nil, fmt.Errorf("failed to iterate dict: %w", iterErr)
	}
	return o, nil
}

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
	repoPlatformLabel, err := resolveApparent(e, rootRepo, apparentRepoPlatformLabel)
	if err != nil {
		return PatchedRegisteredRepoPlatformValue{}, fmt.Errorf("failed to resolve repo platform label %#v: %w", apparentRepoPlatformLabel.String(), err)
	}
	repoPlatformLabelStr := repoPlatformLabel.String()

	// Obtain the PlatformInfo provider of the repo platform.
	platformInfoProvider, err := getProviderFromConfiguredTarget(
		e,
		repoPlatformLabelStr,
		model_core.NewSimplePatchedMessage[dag.ObjectContentsWalker, *model_core_pb.Reference](nil),
		platformInfoProviderIdentifier,
	)
	if err != nil {
		return PatchedRegisteredRepoPlatformValue{}, fmt.Errorf("failed to obtain PlatformInfo of repo platform %#v: %w", repoPlatformLabelStr, err)
	}

	// Extract fields from the PlatformInfo provider that are needed
	// when evaluating module extensions and repository rules. We
	// don't need to extract the constraints, as those are only used
	// by the toolchain resolution process of regular build rules.
	var execPKIXPublicKey []byte
	var repositoryOSArch, repositoryOSName string
	var repositoryOSEnviron []*model_analysis_pb.RegisteredRepoPlatform_Value_EnvironmentVariable
	var errIter error
	for key, value := range model_starlark.AllStructFields(
		ctx,
		c.valueDereferencers.List,
		platformInfoProvider,
		&errIter,
	) {
		switch key {
		case "exec_pkix_public_key":
			str, ok := value.Message.Kind.(*model_starlark_pb.Value_Str)
			if !ok {
				return PatchedRegisteredRepoPlatformValue{}, fmt.Errorf("exec_pkix_public_key field of PlatformInfo of repo platform %#v is not a string", repoPlatformLabelStr)
			}
			execPKIXPublicKey, err = base64.StdEncoding.DecodeString(str.Str)
			if err != nil {
				return PatchedRegisteredRepoPlatformValue{}, fmt.Errorf("exec_pkix_public_key field of PlatformInfo of repo platform %#v: %w", repoPlatformLabelStr, err)
			}
		case "repository_os_arch":
			str, ok := value.Message.Kind.(*model_starlark_pb.Value_Str)
			if !ok {
				return PatchedRegisteredRepoPlatformValue{}, fmt.Errorf("repository_os_arch field of PlatformInfo of repo platform %#v is not a string", repoPlatformLabelStr)
			}
			repositoryOSArch = str.Str
		case "repository_os_environ":
			p, err := c.decodeStringDict(ctx, value)
			if err != nil {
				return PatchedRegisteredRepoPlatformValue{}, fmt.Errorf("repository_os_environ field of PlatformInfo of repo platform %#v: %w", repoPlatformLabelStr, err)
			}
			repositoryOSEnviron = make([]*model_analysis_pb.RegisteredRepoPlatform_Value_EnvironmentVariable, 0, len(p))
			for _, name := range slices.Sorted(maps.Keys(p)) {
				repositoryOSEnviron = append(repositoryOSEnviron, &model_analysis_pb.RegisteredRepoPlatform_Value_EnvironmentVariable{
					Name:  name,
					Value: p[name],
				})
			}
		case "repository_os_name":
			str, ok := value.Message.Kind.(*model_starlark_pb.Value_Str)
			if !ok {
				return PatchedRegisteredRepoPlatformValue{}, fmt.Errorf("repository_os_name field of PlatformInfo of repo platform %#v is not a string", repoPlatformLabelStr)
			}
			repositoryOSName = str.Str
		}
	}
	if errIter != nil {
		return PatchedRegisteredRepoPlatformValue{}, errIter
	}

	if len(execPKIXPublicKey) == 0 {
		return PatchedRegisteredRepoPlatformValue{}, fmt.Errorf("exec_pkix_public_key field of PlatformInfo of repo platform %#v is not set to a non-empty string", repoPlatformLabelStr)
	}
	if repositoryOSArch == "" {
		return PatchedRegisteredRepoPlatformValue{}, fmt.Errorf("repository_os_arch field of PlatformInfo of repo platform %#v is not set to a non-empty string", repoPlatformLabelStr)
	}
	if repositoryOSEnviron == nil {
		return PatchedRegisteredRepoPlatformValue{}, fmt.Errorf("PlatformInfo of repo platform %#v does not contain field repository_os_environ", repoPlatformLabelStr)
	}
	if repositoryOSName == "" {
		return PatchedRegisteredRepoPlatformValue{}, fmt.Errorf("repository_os_name field of PlatformInfo of repo platform %#v is not set to a non-empty string", repoPlatformLabelStr)
	}

	return model_core.NewSimplePatchedMessage[dag.ObjectContentsWalker](
		&model_analysis_pb.RegisteredRepoPlatform_Value{
			ExecPkixPublicKey:   execPKIXPublicKey,
			RepositoryOsArch:    repositoryOSArch,
			RepositoryOsEnviron: repositoryOSEnviron,
			RepositoryOsName:    repositoryOSName,
		},
	), nil
}
