package analysis

import (
	"context"
	"errors"

	"github.com/buildbarn/bonanza/pkg/evaluation"
	model_core "github.com/buildbarn/bonanza/pkg/model/core"
	"github.com/buildbarn/bonanza/pkg/model/core/btree"
	model_analysis_pb "github.com/buildbarn/bonanza/pkg/proto/model/analysis"
	model_core_pb "github.com/buildbarn/bonanza/pkg/proto/model/core"
	"github.com/buildbarn/bonanza/pkg/storage/dag"
)

func (c *baseComputer[TReference, TMetadata]) ComputeModuleExtensionRepoNamesValue(ctx context.Context, key *model_analysis_pb.ModuleExtensionRepoNames_Key, e ModuleExtensionRepoNamesEnvironment[TReference, TMetadata]) (PatchedModuleExtensionRepoNamesValue, error) {
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
		c.moduleExtensionReposValueRepoReader,
		model_core.NewNestedMessage(moduleExtensionReposValue, moduleExtensionReposValue.Message.Repos),
		func(entry model_core.Message[*model_analysis_pb.ModuleExtensionRepos_Value_Repo, TReference]) (*model_core_pb.Reference, error) {
			if parent, ok := entry.Message.Level.(*model_analysis_pb.ModuleExtensionRepos_Value_Repo_Parent_); ok {
				return parent.Parent.Reference, nil
			}
			return nil, nil
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
