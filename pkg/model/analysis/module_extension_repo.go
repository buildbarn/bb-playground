package analysis

import (
	"context"
	"errors"
	"fmt"
	"sort"

	"github.com/buildbarn/bb-playground/pkg/evaluation"
	"github.com/buildbarn/bb-playground/pkg/label"
	model_core "github.com/buildbarn/bb-playground/pkg/model/core"
	model_parser "github.com/buildbarn/bb-playground/pkg/model/parser"
	model_analysis_pb "github.com/buildbarn/bb-playground/pkg/proto/model/analysis"
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

	reader := model_parser.NewStorageBackedParsedObjectReader(
		c.objectDownloader,
		c.getValueObjectEncoder(),
		model_parser.NewMessageObjectParser[object.LocalReference, model_analysis_pb.ModuleExtensionRepos_Value_RepoList](),
	)

	repoName := apparentRepo.String()
	repoList := model_core.Message[[]*model_analysis_pb.ModuleExtensionRepos_Value_RepoList_Element]{
		Message:            moduleExtensionReposValue.Message.Repos,
		OutgoingReferences: moduleExtensionReposValue.OutgoingReferences,
	}
	for {
		index := uint(sort.Search(
			len(repoList.Message),
			func(i int) bool {
				switch level := repoList.Message[i].Level.(type) {
				case *model_analysis_pb.ModuleExtensionRepos_Value_RepoList_Element_Leaf:
					return repoName < level.Leaf.Name
				case *model_analysis_pb.ModuleExtensionRepos_Value_RepoList_Element_Parent_:
					return repoName < level.Parent.FirstName
				default:
					return false
				}
			},
		) - 1)
		if index >= uint(len(repoList.Message)) {
			return PatchedModuleExtensionRepoValue{}, errors.New("repo does not exist")
		}
		switch level := repoList.Message[index].Level.(type) {
		case *model_analysis_pb.ModuleExtensionRepos_Value_RepoList_Element_Leaf:
			if level.Leaf.Name != repoName {
				return PatchedModuleExtensionRepoValue{}, errors.New("repo does not exist")
			}
			definition := level.Leaf.Definition
			if definition == nil {
				return PatchedModuleExtensionRepoValue{}, errors.New("repo does not have a definition")
			}
			patchedDefinition := model_core.NewPatchedMessageFromExisting(
				model_core.Message[*model_starlark_pb.Repo_Definition]{
					Message:            definition,
					OutgoingReferences: repoList.OutgoingReferences,
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
		case *model_analysis_pb.ModuleExtensionRepos_Value_RepoList_Element_Parent_:
			index, err := model_core.GetIndexFromReferenceMessage(level.Parent.Reference, repoList.OutgoingReferences.GetDegree())
			if err != nil {
				return PatchedModuleExtensionRepoValue{}, err
			}
			listMessage, _, err := reader.ReadParsedObject(
				ctx,
				repoList.OutgoingReferences.GetOutgoingReference(index),
			)
			if err != nil {
				return PatchedModuleExtensionRepoValue{}, err
			}
			repoList = model_core.Message[[]*model_analysis_pb.ModuleExtensionRepos_Value_RepoList_Element]{
				Message:            listMessage.Message.Elements,
				OutgoingReferences: listMessage.OutgoingReferences,
			}
		default:
			return PatchedModuleExtensionRepoValue{}, errors.New("repo list has an unknown level type")
		}
	}
}
