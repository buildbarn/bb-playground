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
	model_starlark_pb "github.com/buildbarn/bonanza/pkg/proto/model/starlark"
	"github.com/buildbarn/bonanza/pkg/storage/dag"
	"github.com/buildbarn/bonanza/pkg/storage/object"
)

func (c *baseComputer) lookupTargetDefinitionInTargetList(ctx context.Context, targetList model_core.Message[[]*model_analysis_pb.Package_Value_Target, object.OutgoingReferences], targetName label.TargetName) (model_core.Message[*model_starlark_pb.Target_Definition, object.OutgoingReferences], error) {
	targetNameStr := targetName.String()
	target, err := btree.Find(
		ctx,
		c.packageValueTargetDereferencer,
		targetList,
		func(entry *model_analysis_pb.Package_Value_Target) (int, *model_core_pb.Reference) {
			switch level := entry.Level.(type) {
			case *model_analysis_pb.Package_Value_Target_Leaf:
				return strings.Compare(targetNameStr, level.Leaf.Name), nil
			case *model_analysis_pb.Package_Value_Target_Parent_:
				return strings.Compare(targetNameStr, level.Parent.FirstName), level.Parent.Reference
			default:
				return 0, nil
			}
		},
	)
	if err != nil {
		return model_core.Message[*model_starlark_pb.Target_Definition, object.OutgoingReferences]{}, err
	}
	if !target.IsSet() {
		return model_core.Message[*model_starlark_pb.Target_Definition, object.OutgoingReferences]{}, nil
	}

	level, ok := target.Message.Level.(*model_analysis_pb.Package_Value_Target_Leaf)
	if !ok {
		return model_core.Message[*model_starlark_pb.Target_Definition, object.OutgoingReferences]{}, errors.New("target list has an unknown level type")
	}
	definition := level.Leaf.Definition
	if definition == nil {
		return model_core.Message[*model_starlark_pb.Target_Definition, object.OutgoingReferences]{}, errors.New("target does not have a definition")
	}
	return model_core.NewNestedMessage(targetList, definition), nil
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
		model_core.NewNestedMessage(packageValue, packageValue.Message.Targets),
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
