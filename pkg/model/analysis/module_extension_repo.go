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
	model_analysis_pb "github.com/buildbarn/bb-playground/pkg/proto/model/analysis"
	model_core_pb "github.com/buildbarn/bb-playground/pkg/proto/model/core"
	model_starlark_pb "github.com/buildbarn/bb-playground/pkg/proto/model/starlark"
	"github.com/buildbarn/bb-playground/pkg/storage/dag"
	"github.com/buildbarn/bb-playground/pkg/storage/object"
)

func (c *baseComputer) ComputeModuleExtensionRepoValue(ctx context.Context, key *model_analysis_pb.ModuleExtensionRepo_Key, e ModuleExtensionRepoEnvironment) (PatchedModuleExtensionRepoValue, error) {
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
		model_parser.NewStorageBackedParsedObjectReader(
			c.objectDownloader,
			c.getValueObjectEncoder(),
			model_parser.NewMessageListObjectParser[object.LocalReference, model_analysis_pb.ModuleExtensionRepos_Value_Repo](),
		),
		model_core.Message[[]*model_analysis_pb.ModuleExtensionRepos_Value_Repo]{
			Message:            moduleExtensionReposValue.Message.Repos,
			OutgoingReferences: moduleExtensionReposValue.OutgoingReferences,
		},
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
	patchedDefinition := model_core.NewPatchedMessageFromExisting(
		model_core.Message[*model_starlark_pb.Repo_Definition]{
			Message:            definition,
			OutgoingReferences: repo.OutgoingReferences,
		},
		func(index int) dag.ObjectContentsWalker {
			return dag.ExistingObjectContentsWalker
		},
	)
	return model_core.NewPatchedMessage(
		&model_analysis_pb.ModuleExtensionRepo_Value{
			Definition: patchedDefinition.Message,
		},
		patchedDefinition.Patcher,
	), nil
}
