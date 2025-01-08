package analysis

import (
	"context"
	"errors"

	"github.com/buildbarn/bb-playground/pkg/evaluation"
	model_core "github.com/buildbarn/bb-playground/pkg/model/core"
	model_parser "github.com/buildbarn/bb-playground/pkg/model/parser"
	model_analysis_pb "github.com/buildbarn/bb-playground/pkg/proto/model/analysis"
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

	reader := model_parser.NewStorageBackedParsedObjectReader(
		c.objectDownloader,
		c.getValueObjectEncoder(),
		model_parser.NewMessageObjectParser[object.LocalReference, model_analysis_pb.ModuleExtensionRepos_Value_RepoList](),
	)

	var repoNames []string
	repoLists := []model_core.Message[[]*model_analysis_pb.ModuleExtensionRepos_Value_RepoList_Element]{{
		Message:            moduleExtensionReposValue.Message.Repos,
		OutgoingReferences: moduleExtensionReposValue.OutgoingReferences,
	}}
	for len(repoLists) > 0 {
		lastDict := &repoLists[len(repoLists)-1]
		if len(lastDict.Message) == 0 {
			repoLists = repoLists[:len(repoLists)-1]
		} else {
			entry := lastDict.Message[0]
			lastDict.Message = lastDict.Message[1:]
			switch level := entry.Level.(type) {
			case *model_analysis_pb.ModuleExtensionRepos_Value_RepoList_Element_Leaf:
				repoNames = append(repoNames, level.Leaf.Name)
			case *model_analysis_pb.ModuleExtensionRepos_Value_RepoList_Element_Parent_:
				index, err := model_core.GetIndexFromReferenceMessage(level.Parent.Reference, lastDict.OutgoingReferences.GetDegree())
				if err != nil {
					return PatchedModuleExtensionRepoNamesValue{}, err
				}
				child, _, err := reader.ReadParsedObject(
					ctx,
					lastDict.OutgoingReferences.GetOutgoingReference(index),
				)
				if err != nil {
					return PatchedModuleExtensionRepoNamesValue{}, err
				}
				repoLists = append(repoLists, model_core.Message[[]*model_analysis_pb.ModuleExtensionRepos_Value_RepoList_Element]{
					Message:            child.Message.Elements,
					OutgoingReferences: child.OutgoingReferences,
				})
			default:
				return PatchedModuleExtensionRepoNamesValue{}, errors.New("list element is of an unknown type")
			}
		}
	}

	return model_core.NewSimplePatchedMessage[dag.ObjectContentsWalker](&model_analysis_pb.ModuleExtensionRepoNames_Value{
		RepoNames: repoNames,
	}), nil
}
