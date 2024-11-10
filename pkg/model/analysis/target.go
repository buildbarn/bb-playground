package analysis

import (
	"context"
	"errors"
	"fmt"
	"sort"

	"github.com/buildbarn/bb-playground/pkg/evaluation"
	"github.com/buildbarn/bb-playground/pkg/label"
	model_core "github.com/buildbarn/bb-playground/pkg/model/core"
	model_analysis_pb "github.com/buildbarn/bb-playground/pkg/proto/model/analysis"
	model_starlark_pb "github.com/buildbarn/bb-playground/pkg/proto/model/starlark"
	"github.com/buildbarn/bb-playground/pkg/storage/dag"
)

func (c *baseComputer) ComputeTargetValue(ctx context.Context, key *model_analysis_pb.Target_Key, e TargetEnvironment) (PatchedTargetValue, error) {
	targetLabel, err := label.NewCanonicalLabel(key.Label)
	if err != nil {
		return PatchedTargetValue{}, fmt.Errorf("invalid target label: %w", err)
	}
	packageValue := e.GetPackageValue(&model_analysis_pb.Package_Key{
		Label: targetLabel.GetCanonicalPackage().String(),
	})
	if !packageValue.IsSet() {
		return PatchedTargetValue{}, evaluation.ErrMissingDependency
	}

	targetName := targetLabel.GetTargetName().String()
	targetList := model_core.Message[[]*model_analysis_pb.Package_Value_TargetList_Element]{
		Message:            packageValue.Message.Targets,
		OutgoingReferences: packageValue.OutgoingReferences,
	}
	for {
		index := uint(sort.Search(
			len(targetList.Message),
			func(i int) bool {
				switch level := targetList.Message[i].Level.(type) {
				case *model_analysis_pb.Package_Value_TargetList_Element_Leaf:
					return targetName < level.Leaf.Name
				case *model_analysis_pb.Package_Value_TargetList_Element_Parent_:
					return targetName < level.Parent.FirstName
				default:
					return false
				}
			},
		) - 1)
		if index >= uint(len(targetList.Message)) {
			return PatchedTargetValue{}, errors.New("target does not exist")
		}
		switch level := targetList.Message[index].Level.(type) {
		case *model_analysis_pb.Package_Value_TargetList_Element_Leaf:
			if level.Leaf.Name != targetName {
				return PatchedTargetValue{}, errors.New("target does not exist")
			}
			definition := level.Leaf.Definition
			if definition == nil {
				return PatchedTargetValue{}, errors.New("target does not have a definition")
			}
			patchedDefinition := model_core.NewPatchedMessageFromExisting(
				model_core.Message[*model_starlark_pb.Target_Definition]{
					Message:            definition,
					OutgoingReferences: targetList.OutgoingReferences,
				},
				func(index int) dag.ObjectContentsWalker {
					return dag.ExistingObjectContentsWalker
				},
			)
			return model_core.PatchedMessage[*model_analysis_pb.Target_Value, dag.ObjectContentsWalker]{
				Message: &model_analysis_pb.Target_Value{
					Definition: patchedDefinition.Message,
				},
				Patcher: patchedDefinition.Patcher,
			}, nil
		case *model_analysis_pb.Package_Value_TargetList_Element_Parent_:
			panic("TODO: Load target list from storage!")
		default:
			return PatchedTargetValue{}, errors.New("target list has an unknown level type")
		}
	}
}
