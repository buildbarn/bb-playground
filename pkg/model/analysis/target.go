package analysis

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sort"

	"github.com/buildbarn/bb-playground/pkg/evaluation"
	"github.com/buildbarn/bb-playground/pkg/label"
	model_core "github.com/buildbarn/bb-playground/pkg/model/core"
	model_analysis_pb "github.com/buildbarn/bb-playground/pkg/proto/model/analysis"
	model_starlark_pb "github.com/buildbarn/bb-playground/pkg/proto/model/starlark"
	"github.com/buildbarn/bb-playground/pkg/storage/dag"

	"google.golang.org/protobuf/encoding/protojson"
)

func (c *baseComputer) ComputeTargetValue(ctx context.Context, key *model_analysis_pb.Target_Key, e TargetEnvironment) (PatchedTargetValue, error) {
	targetLabel, err := label.NewCanonicalLabel(key.Label)
	if err != nil {
		return model_core.NewSimplePatchedMessage[dag.ObjectContentsWalker](&model_analysis_pb.Target_Value{
			Result: &model_analysis_pb.Target_Value_Failure{
				Failure: fmt.Sprintf("invalid target label %#v: %s", key.Label, err),
			},
		}), nil
	}
	packageValue := e.GetPackageValue(&model_analysis_pb.Package_Key{
		Label: targetLabel.GetCanonicalPackage().String(),
	})
	if !packageValue.IsSet() {
		return PatchedTargetValue{}, evaluation.ErrMissingDependency
	}

	switch packageResult := packageValue.Message.Result.(type) {
	case *model_analysis_pb.Package_Value_Success:
		targetName := targetLabel.GetTargetName().String()
		targetList := model_core.Message[*model_analysis_pb.Package_Value_TargetList]{
			Message:            packageResult.Success,
			OutgoingReferences: packageValue.OutgoingReferences,
		}
		for {
			elements := targetList.Message.Elements
			index := uint(sort.Search(
				len(elements),
				func(i int) bool {
					switch level := elements[i].Level.(type) {
					case *model_analysis_pb.Package_Value_TargetList_Element_Leaf:
						return targetName < level.Leaf.Name
					case *model_analysis_pb.Package_Value_TargetList_Element_Parent_:
						return targetName < level.Parent.FirstName
					default:
						return false
					}
				},
			) - 1)
			if index >= uint(len(elements)) {
				return model_core.NewSimplePatchedMessage[dag.ObjectContentsWalker](&model_analysis_pb.Target_Value{
					Result: &model_analysis_pb.Target_Value_Failure{
						Failure: "Target does not exist",
					},
				}), nil
			}
			switch level := elements[index].Level.(type) {
			case *model_analysis_pb.Package_Value_TargetList_Element_Leaf:
				if level.Leaf.Name != targetName {
					return model_core.NewSimplePatchedMessage[dag.ObjectContentsWalker](&model_analysis_pb.Target_Value{
						Result: &model_analysis_pb.Target_Value_Failure{
							Failure: "Target does not exist",
						},
					}), nil
				}

				target := model_core.NewPatchedMessageFromExisting(
					model_core.Message[*model_starlark_pb.Target]{
						Message:            level.Leaf,
						OutgoingReferences: targetList.OutgoingReferences,
					},
					func(index int) dag.ObjectContentsWalker {
						return dag.ExistingObjectContentsWalker
					},
				)
				definition := target.Message.Definition
				if definition == nil {
					return model_core.NewSimplePatchedMessage[dag.ObjectContentsWalker](&model_analysis_pb.Target_Value{
						Result: &model_analysis_pb.Target_Value_Failure{
							Failure: "Target does not have a definition",
						},
					}), nil
				}

				log.Printf("TARGET: %s", targetLabel.String())
				log.Print(protojson.Format(definition))

				return model_core.PatchedMessage[*model_analysis_pb.Target_Value, dag.ObjectContentsWalker]{
					Message: &model_analysis_pb.Target_Value{
						Result: &model_analysis_pb.Target_Value_Success{
							Success: definition,
						},
					},
					Patcher: target.Patcher,
				}, nil
			case *model_analysis_pb.Package_Value_TargetList_Element_Parent_:
				panic("TODO: Load target list from storage!")
			default:
				return model_core.NewSimplePatchedMessage[dag.ObjectContentsWalker](&model_analysis_pb.Target_Value{
					Result: &model_analysis_pb.Target_Value_Failure{
						Failure: "Target list has unknown level type",
					},
				}), nil
			}
		}
	case *model_analysis_pb.Package_Value_Failure:
		return model_core.NewSimplePatchedMessage[dag.ObjectContentsWalker](&model_analysis_pb.Target_Value{
			Result: &model_analysis_pb.Target_Value_Failure{
				Failure: packageResult.Failure,
			},
		}), nil
	default:
		return PatchedTargetValue{}, errors.New("Package value has an unknown result type")
	}
}
