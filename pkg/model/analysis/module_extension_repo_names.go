package analysis

import (
	"context"
	"errors"

	"github.com/buildbarn/bb-playground/pkg/evaluation"
	model_core "github.com/buildbarn/bb-playground/pkg/model/core"
	"github.com/buildbarn/bb-playground/pkg/model/core/btree"
	model_parser "github.com/buildbarn/bb-playground/pkg/model/parser"
	model_analysis_pb "github.com/buildbarn/bb-playground/pkg/proto/model/analysis"
	model_core_pb "github.com/buildbarn/bb-playground/pkg/proto/model/core"
	"github.com/buildbarn/bb-playground/pkg/storage/dag"
	"github.com/buildbarn/bb-playground/pkg/storage/object"
)

func (c *baseComputer) ComputeModuleExtensionRepoNamesValue(ctx context.Context, key *model_analysis_pb.ModuleExtensionRepoNames_Key, e ModuleExtensionRepoNamesEnvironment) (PatchedModuleExtensionRepoNamesValue, error) {
	moduleExtensionReposValue := e.GetModuleExtensionReposValue(&model_analysis_pb.ModuleExtensionRepos_Key{
		ModuleExtension: key.ModuleExtension,
	})
	if !moduleExtensionReposValue.IsSet() {
		return PatchedModuleExtensionRepoNamesValue{}, evaluation.ErrMissingDependency
	}

	var repoNames []string
	var errIter error
	for entry := range btree.AllLeaves(
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
		func(entry *model_analysis_pb.ModuleExtensionRepos_Value_Repo) *model_core_pb.Reference {
			if parent, ok := entry.Level.(*model_analysis_pb.ModuleExtensionRepos_Value_Repo_Parent_); ok {
				return parent.Parent.Reference
			}
			return nil
		},
		&errIter,
	) {
		leaf, ok := entry.Message.Level.(*model_analysis_pb.ModuleExtensionRepos_Value_Repo_Leaf)
		if !ok {
			return PatchedModuleExtensionRepoNamesValue{}, errors.New("not a valid leaf entry")
		}
		repoNames = append(repoNames, leaf.Leaf.Name)
	}
	if errIter != nil {
		return PatchedModuleExtensionRepoNamesValue{}, errIter
	}

	return model_core.NewSimplePatchedMessage[dag.ObjectContentsWalker](&model_analysis_pb.ModuleExtensionRepoNames_Value{
		RepoNames: repoNames,
	}), nil
}
