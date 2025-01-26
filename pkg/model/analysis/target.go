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

func (c *baseComputer) lookupTargetDefinitionInTargetList(ctx context.Context, targetList model_core.Message[[]*model_analysis_pb.Package_Value_TargetList_Element], targetName label.TargetName) (model_core.Message[*model_starlark_pb.Target_Definition], error) {
	reader := model_parser.NewStorageBackedParsedObjectReader(
		c.objectDownloader,
		c.getValueObjectEncoder(),
		model_parser.NewMessageObjectParser[object.LocalReference, model_analysis_pb.Package_Value_TargetList](),
	)

	targetNameStr := targetName.String()
	for {
		index := uint(sort.Search(
			len(targetList.Message),
			func(i int) bool {
				switch level := targetList.Message[i].Level.(type) {
				case *model_analysis_pb.Package_Value_TargetList_Element_Leaf:
					return targetNameStr < level.Leaf.Name
				case *model_analysis_pb.Package_Value_TargetList_Element_Parent_:
					return targetNameStr < level.Parent.FirstName
				default:
					return false
				}
			},
		) - 1)
		if index >= uint(len(targetList.Message)) {
			return model_core.Message[*model_starlark_pb.Target_Definition]{}, nil
		}
		switch level := targetList.Message[index].Level.(type) {
		case *model_analysis_pb.Package_Value_TargetList_Element_Leaf:
			if level.Leaf.Name != targetNameStr {
				return model_core.Message[*model_starlark_pb.Target_Definition]{}, nil
			}
			definition := level.Leaf.Definition
			if definition == nil {
				return model_core.Message[*model_starlark_pb.Target_Definition]{}, errors.New("target does not have a definition")
			}
			return model_core.Message[*model_starlark_pb.Target_Definition]{
				Message:            definition,
				OutgoingReferences: targetList.OutgoingReferences,
			}, nil
		case *model_analysis_pb.Package_Value_TargetList_Element_Parent_:
			index, err := model_core.GetIndexFromReferenceMessage(level.Parent.Reference, targetList.OutgoingReferences.GetDegree())
			if err != nil {
				return model_core.Message[*model_starlark_pb.Target_Definition]{}, err
			}
			listMessage, _, err := reader.ReadParsedObject(
				ctx,
				targetList.OutgoingReferences.GetOutgoingReference(index),
			)
			if err != nil {
				return model_core.Message[*model_starlark_pb.Target_Definition]{}, err
			}
			targetList = model_core.Message[[]*model_analysis_pb.Package_Value_TargetList_Element]{
				Message:            listMessage.Message.Elements,
				OutgoingReferences: listMessage.OutgoingReferences,
			}
		default:
			return model_core.Message[*model_starlark_pb.Target_Definition]{}, errors.New("target list has an unknown level type")
		}
	}
}

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

	definition, err := c.lookupTargetDefinitionInTargetList(
		ctx,
		model_core.Message[[]*model_analysis_pb.Package_Value_TargetList_Element]{
			Message:            packageValue.Message.Targets,
			OutgoingReferences: packageValue.OutgoingReferences,
		},
		targetLabel.GetTargetName(),
	)
	if err != nil {
		return PatchedTargetValue{}, err
	}
	if !definition.IsSet() {
		return PatchedTargetValue{}, errors.New("target does not exist")
	}

	patchedDefinition := model_core.NewPatchedMessageFromExisting(
		definition,
		func(index int) dag.ObjectContentsWalker {
			return dag.ExistingObjectContentsWalker
		},
	)
	return model_core.NewPatchedMessage(
		&model_analysis_pb.Target_Value{
			Definition: patchedDefinition.Message,
		},
		patchedDefinition.Patcher,
	), nil
}
