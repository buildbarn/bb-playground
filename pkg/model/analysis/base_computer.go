package analysis

import (
	"context"
	"fmt"
	"path"

	"github.com/buildbarn/bb-playground/pkg/label"
	model_core "github.com/buildbarn/bb-playground/pkg/model/core"
	model_analysis_pb "github.com/buildbarn/bb-playground/pkg/proto/model/analysis"
	model_build_pb "github.com/buildbarn/bb-playground/pkg/proto/model/build"
	"github.com/buildbarn/bb-playground/pkg/storage/dag"
	"github.com/buildbarn/bb-playground/pkg/storage/object"
	"github.com/buildbarn/bb-storage/pkg/util"
)

type baseComputer struct {
	buildSpecification model_core.Message[*model_build_pb.BuildSpecification]
}

func NewBaseComputer(buildSpecification model_core.Message[*model_build_pb.BuildSpecification]) Computer {
	return &baseComputer{
		buildSpecification: buildSpecification,
	}
}

func (c *baseComputer) ComputeBuildResultValue(ctx context.Context, key *model_analysis_pb.BuildResult_Key, e BuildResultEnvironment) (model_core.PatchedMessage[*model_analysis_pb.BuildResult_Value, dag.ObjectContentsWalker], error) {
	// TODO: Do something proper here.
	targetCompletion := e.GetTargetCompletionValue(&model_analysis_pb.TargetCompletion_Key{
		Label: "@@com_github_buildbarn_bb_storage+//cmd/bb_storage",
	})
	if !targetCompletion.IsSet() {
		return model_core.PatchedMessage[*model_analysis_pb.BuildResult_Value, dag.ObjectContentsWalker]{}, nil
	}
	return model_core.PatchedMessage[*model_analysis_pb.BuildResult_Value, dag.ObjectContentsWalker]{
		Message: &model_analysis_pb.BuildResult_Value{},
		Patcher: model_core.NewReferenceMessagePatcher[dag.ObjectContentsWalker](),
	}, nil
}

func (c *baseComputer) ComputeBuildSpecificationValue(ctx context.Context, key *model_analysis_pb.BuildSpecification_Key, e BuildSpecificationEnvironment) (model_core.PatchedMessage[*model_analysis_pb.BuildSpecification_Value, dag.ObjectContentsWalker], error) {
	buildSpecification, err := model_core.NewPatchedMessageFromExisting(
		c.buildSpecification,
		func(reference object.LocalReference) dag.ObjectContentsWalker {
			return dag.ExistingObjectContentsWalker
		},
	)
	if err != nil {
		return model_core.PatchedMessage[*model_analysis_pb.BuildSpecification_Value, dag.ObjectContentsWalker]{}, nil
	}
	return model_core.PatchedMessage[*model_analysis_pb.BuildSpecification_Value, dag.ObjectContentsWalker]{
		Message: &model_analysis_pb.BuildSpecification_Value{
			BuildSpecification: buildSpecification.Message,
		},
		Patcher: buildSpecification.Patcher,
	}, nil
}

func (c *baseComputer) ComputeConfiguredTargetValue(ctx context.Context, key *model_analysis_pb.ConfiguredTarget_Key, e ConfiguredTargetEnvironment) (model_core.PatchedMessage[*model_analysis_pb.ConfiguredTarget_Value, dag.ObjectContentsWalker], error) {
	targetLabel, err := label.NewLabel(key.Label)
	if err != nil {
		return model_core.PatchedMessage[*model_analysis_pb.ConfiguredTarget_Value, dag.ObjectContentsWalker]{
			Message: &model_analysis_pb.ConfiguredTarget_Value{
				Result: &model_analysis_pb.ConfiguredTarget_Value_Failure{
					Failure: util.StatusWrapf(err, "Invalid target label %#v", key.Label).Error(),
				},
			},
			Patcher: model_core.NewReferenceMessagePatcher[dag.ObjectContentsWalker](),
		}, nil
	}
	packageData := e.GetPackageValue(&model_analysis_pb.Package_Key{
		Label: targetLabel.GetPackageLabel().String(),
	})
	if !packageData.IsSet() {
		return model_core.PatchedMessage[*model_analysis_pb.ConfiguredTarget_Value, dag.ObjectContentsWalker]{}, nil
	}
	return model_core.PatchedMessage[*model_analysis_pb.ConfiguredTarget_Value, dag.ObjectContentsWalker]{
		Message: &model_analysis_pb.ConfiguredTarget_Value{},
		Patcher: model_core.NewReferenceMessagePatcher[dag.ObjectContentsWalker](),
	}, nil
}

func (c *baseComputer) ComputeFilePropertiesValue(ctx context.Context, key *model_analysis_pb.FileProperties_Key, e FilePropertiesEnvironment) (model_core.PatchedMessage[*model_analysis_pb.FileProperties_Value, dag.ObjectContentsWalker], error) {
	repo := e.GetRepoValue(&model_analysis_pb.Repo_Key{
		CanonicalRepo: key.CanonicalRepo,
	})
	if !repo.IsSet() {
		return model_core.PatchedMessage[*model_analysis_pb.FileProperties_Value, dag.ObjectContentsWalker]{}, nil
	}
	panic("TODO")
}

func (c *baseComputer) ComputePackageValue(ctx context.Context, key *model_analysis_pb.Package_Key, e PackageEnvironment) (model_core.PatchedMessage[*model_analysis_pb.Package_Value, dag.ObjectContentsWalker], error) {
	packageLabel, err := label.NewLabel(key.Label)
	if err != nil {
		return model_core.PatchedMessage[*model_analysis_pb.Package_Value, dag.ObjectContentsWalker]{
			Message: &model_analysis_pb.Package_Value{
				Result: &model_analysis_pb.Package_Value_Failure{
					Failure: util.StatusWrapf(err, "Invalid package label %#v", key.Label).Error(),
				},
			},
			Patcher: model_core.NewReferenceMessagePatcher[dag.ObjectContentsWalker](),
		}, nil
	}

	canonicalRepo, ok := packageLabel.GetCanonicalRepo()
	if !ok {
		return model_core.PatchedMessage[*model_analysis_pb.Package_Value, dag.ObjectContentsWalker]{
			Message: &model_analysis_pb.Package_Value{
				Result: &model_analysis_pb.Package_Value_Failure{
					Failure: fmt.Sprintf("Package label %#v does not have a canonical repo name", packageLabel.String()),
				},
			},
			Patcher: model_core.NewReferenceMessagePatcher[dag.ObjectContentsWalker](),
		}, nil
	}
	packagePath, ok := packageLabel.GetPackagePath()
	if !ok {
		return model_core.PatchedMessage[*model_analysis_pb.Package_Value, dag.ObjectContentsWalker]{
			Message: &model_analysis_pb.Package_Value{
				Result: &model_analysis_pb.Package_Value_Failure{
					Failure: fmt.Sprintf("Package label %#v does not have a package path", packageLabel.String()),
				},
			},
			Patcher: model_core.NewReferenceMessagePatcher[dag.ObjectContentsWalker](),
		}, nil
	}
	buildFileProperties := e.GetFilePropertiesValue(&model_analysis_pb.FileProperties_Key{
		CanonicalRepo: canonicalRepo.String(),
		Path:          path.Join(packagePath, "BUILD.bazel"),
	})
	if !buildFileProperties.IsSet() {
		return model_core.PatchedMessage[*model_analysis_pb.Package_Value, dag.ObjectContentsWalker]{}, nil
	}

	panic(packageLabel.String())
}

func (c *baseComputer) ComputeRepoValue(ctx context.Context, key *model_analysis_pb.Repo_Key, e RepoEnvironment) (model_core.PatchedMessage[*model_analysis_pb.Repo_Value, dag.ObjectContentsWalker], error) {
	canonicalRepo, err := label.NewCanonicalRepo(key.CanonicalRepo)
	if err != nil {
		return model_core.PatchedMessage[*model_analysis_pb.Repo_Value, dag.ObjectContentsWalker]{
			Message: &model_analysis_pb.Repo_Value{
				Result: &model_analysis_pb.Repo_Value_Failure{
					Failure: util.StatusWrapf(err, "Invalid canonical repo %#v", key.CanonicalRepo).Error(),
				},
			},
			Patcher: model_core.NewReferenceMessagePatcher[dag.ObjectContentsWalker](),
		}, nil
	}

	buildSpecification := e.GetBuildSpecificationValue(&model_analysis_pb.BuildSpecification_Key{})
	if !buildSpecification.IsSet() {
		return model_core.PatchedMessage[*model_analysis_pb.Repo_Value, dag.ObjectContentsWalker]{}, nil
	}

	// TODO: Actually implement proper handling of MODULE.bazel.

	moduleName := canonicalRepo.GetModule().String()
	buildSpecificationDegree := buildSpecification.OutgoingReferences.GetDegree()
	for _, module := range buildSpecification.Message.BuildSpecification.GetModules() {
		if module.Name == moduleName {
			index, err := model_core.GetIndexFromReferenceMessage(module.RootDirectoryReference, buildSpecificationDegree)
			if err != nil {
				return model_core.PatchedMessage[*model_analysis_pb.Repo_Value, dag.ObjectContentsWalker]{
					Message: &model_analysis_pb.Repo_Value{
						Result: &model_analysis_pb.Repo_Value_Failure{
							Failure: util.StatusWrapf(err, "Invalid root directory reference for module %#v", moduleName).Error(),
						},
					},
					Patcher: model_core.NewReferenceMessagePatcher[dag.ObjectContentsWalker](),
				}, nil
			}

			patcher := model_core.NewReferenceMessagePatcher[dag.ObjectContentsWalker]()
			return model_core.PatchedMessage[*model_analysis_pb.Repo_Value, dag.ObjectContentsWalker]{
				Message: &model_analysis_pb.Repo_Value{
					Result: &model_analysis_pb.Repo_Value_RootDirectoryReference{
						RootDirectoryReference: patcher.AddReference(
							buildSpecification.OutgoingReferences.GetOutgoingReference(index),
							dag.ExistingObjectContentsWalker,
						),
					},
				},
				Patcher: model_core.NewReferenceMessagePatcher[dag.ObjectContentsWalker](),
			}, nil
		}
	}

	return model_core.PatchedMessage[*model_analysis_pb.Repo_Value, dag.ObjectContentsWalker]{
		Message: &model_analysis_pb.Repo_Value{
			Result: &model_analysis_pb.Repo_Value_Failure{
				Failure: fmt.Sprintf("Module %#v not found", moduleName),
			},
		},
		Patcher: model_core.NewReferenceMessagePatcher[dag.ObjectContentsWalker](),
	}, nil
}

func (c *baseComputer) ComputeTargetCompletionValue(ctx context.Context, key *model_analysis_pb.TargetCompletion_Key, e TargetCompletionEnvironment) (model_core.PatchedMessage[*model_analysis_pb.TargetCompletion_Value, dag.ObjectContentsWalker], error) {
	configuredTarget := e.GetConfiguredTargetValue(&model_analysis_pb.ConfiguredTarget_Key{
		Label: key.Label,
	})
	if !configuredTarget.IsSet() {
		return model_core.PatchedMessage[*model_analysis_pb.TargetCompletion_Value, dag.ObjectContentsWalker]{}, nil
	}
	// TODO: Actually do something here.
	return model_core.PatchedMessage[*model_analysis_pb.TargetCompletion_Value, dag.ObjectContentsWalker]{
		Message: &model_analysis_pb.TargetCompletion_Value{},
		Patcher: model_core.NewReferenceMessagePatcher[dag.ObjectContentsWalker](),
	}, nil
}
