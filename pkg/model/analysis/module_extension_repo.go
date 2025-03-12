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
	model_analysis_pb "github.com/buildbarn/bonanza/pkg/proto/model/analysis"
	model_core_pb "github.com/buildbarn/bonanza/pkg/proto/model/core"
)

func (c *baseComputer[TReference, TMetadata]) ComputeModuleExtensionRepoValue(ctx context.Context, key *model_analysis_pb.ModuleExtensionRepo_Key, e ModuleExtensionRepoEnvironment[TReference]) (PatchedModuleExtensionRepoValue, error) {
	canonicalRepo, err := label.NewCanonicalRepo(key.CanonicalRepo)
	if err != nil {
		return PatchedModuleExtensionRepoValue{}, fmt.Errorf("invalid repo: %w", err)
	}
	moduleExtension, apparentRepo, ok := canonicalRepo.GetModuleExtension()
	if !ok {
		return PatchedModuleExtensionRepoValue{}, errors.New("repo does not include a module extension")
	}
	moduleExtensionReposValue := e.GetModuleExtensionReposValue(&model_analysis_pb.ModuleExtensionRepos_Key{
		ModuleExtension: moduleExtension.String(),
	})
	if !moduleExtensionReposValue.IsSet() {
		return PatchedModuleExtensionRepoValue{}, evaluation.ErrMissingDependency
	}

	repoName := apparentRepo.String()
	repo, err := btree.Find(
		ctx,
		c.moduleExtensionReposValueRepoReader,
		model_core.NewNestedMessage(moduleExtensionReposValue, moduleExtensionReposValue.Message.Repos),
		func(entry *model_analysis_pb.ModuleExtensionRepos_Value_Repo) (int, *model_core_pb.Reference) {
			switch level := entry.Level.(type) {
			case *model_analysis_pb.ModuleExtensionRepos_Value_Repo_Leaf:
				return strings.Compare(repoName, level.Leaf.Name), nil
			case *model_analysis_pb.ModuleExtensionRepos_Value_Repo_Parent_:
				return strings.Compare(repoName, level.Parent.FirstName), level.Parent.Reference
			default:
				return 0, nil
			}
		},
	)
	if err != nil {
		return PatchedModuleExtensionRepoValue{}, err
	}
	if !repo.IsSet() {
		return PatchedModuleExtensionRepoValue{}, errors.New("repo does not exist")
	}

	level, ok := repo.Message.Level.(*model_analysis_pb.ModuleExtensionRepos_Value_Repo_Leaf)
	if !ok {
		return PatchedModuleExtensionRepoValue{}, errors.New("repo has an unknown level type")
	}
	definition := level.Leaf.Definition
	if definition == nil {
		return PatchedModuleExtensionRepoValue{}, errors.New("repo does not have a definition")
	}
	patchedDefinition := model_core.NewPatchedMessageFromExistingCaptured(
		c.objectCapturer,
		model_core.NewNestedMessage(repo, definition),
	)
	return model_core.NewPatchedMessage(
		&model_analysis_pb.ModuleExtensionRepo_Value{
			Definition: patchedDefinition.Message,
		},
		model_core.MapReferenceMetadataToWalkers(patchedDefinition.Patcher),
	), nil
}
