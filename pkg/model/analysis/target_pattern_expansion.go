package analysis

import (
	"context"
	"errors"
	"fmt"
	"iter"

	"github.com/buildbarn/bb-playground/pkg/evaluation"
	"github.com/buildbarn/bb-playground/pkg/label"
	model_core "github.com/buildbarn/bb-playground/pkg/model/core"
	"github.com/buildbarn/bb-playground/pkg/model/core/btree"
	model_encoding "github.com/buildbarn/bb-playground/pkg/model/encoding"
	model_parser "github.com/buildbarn/bb-playground/pkg/model/parser"
	model_analysis_pb "github.com/buildbarn/bb-playground/pkg/proto/model/analysis"
	model_starlark_pb "github.com/buildbarn/bb-playground/pkg/proto/model/starlark"
	"github.com/buildbarn/bb-playground/pkg/storage/dag"
	"github.com/buildbarn/bb-playground/pkg/storage/object"
)

type expandCanonicalTargetPatternEnvironment interface {
	GetTargetPatternExpansionValue(*model_analysis_pb.TargetPatternExpansion_Key) model_core.Message[*model_analysis_pb.TargetPatternExpansion_Value]
}

// expandCanonicalTargetPattern returns canonical labels for each target
// matched by a canonical target pattern.
func (c *baseComputer) expandCanonicalTargetPattern(
	ctx context.Context,
	e expandCanonicalTargetPatternEnvironment,
	targetPattern label.CanonicalTargetPattern,
	errOut *error,
) iter.Seq[label.CanonicalLabel] {
	return func(yield func(canonicalLabel label.CanonicalLabel) bool) {
		if l, ok := targetPattern.AsCanonicalLabel(); ok {
			// Target pattern refers to a single label. No
			// need to perform actual expansion.
			yield(l)
			*errOut = nil
			return
		}

		targetPatternExpansion := e.GetTargetPatternExpansionValue(&model_analysis_pb.TargetPatternExpansion_Key{
			TargetPattern: targetPattern.String(),
		})
		if !targetPatternExpansion.IsSet() {
			*errOut = evaluation.ErrMissingDependency
			return
		}

		reader := model_parser.NewStorageBackedParsedObjectReader(
			c.objectDownloader,
			c.getValueObjectEncoder(),
			model_parser.NewMessageObjectParser[object.LocalReference, model_analysis_pb.TargetPatternExpansion_Value_TargetLabelList](),
		)

		lists := []model_core.Message[[]*model_analysis_pb.TargetPatternExpansion_Value_TargetLabelList_Element]{{
			Message:            targetPatternExpansion.Message.TargetLabels,
			OutgoingReferences: targetPatternExpansion.OutgoingReferences,
		}}
		for len(lists) > 0 {
			lastList := &lists[len(lists)-1]
			if len(lastList.Message) == 0 {
				lists = lists[:len(lists)-1]
			} else {
				entry := lastList.Message[0]
				lastList.Message = lastList.Message[1:]
				switch level := entry.Level.(type) {
				case *model_analysis_pb.TargetPatternExpansion_Value_TargetLabelList_Element_Leaf:
					targetLabel, err := label.NewCanonicalLabel(level.Leaf)
					if err != nil {
						*errOut = fmt.Errorf("invalid target label %#v: %w", level.Leaf, err)
						return
					}
					if !yield(targetLabel) {
						*errOut = nil
						return
					}
				case *model_analysis_pb.TargetPatternExpansion_Value_TargetLabelList_Element_Parent_:
					index, err := model_core.GetIndexFromReferenceMessage(level.Parent.Reference, lastList.OutgoingReferences.GetDegree())
					if err != nil {
						*errOut = err
						return
					}
					child, _, err := reader.ReadParsedObject(
						ctx,
						lastList.OutgoingReferences.GetOutgoingReference(index),
					)
					if err != nil {
						*errOut = err
						return
					}
					lists = append(lists, model_core.Message[[]*model_analysis_pb.TargetPatternExpansion_Value_TargetLabelList_Element]{
						Message:            child.Message.Elements,
						OutgoingReferences: child.OutgoingReferences,
					})
				default:
					*errOut = errors.New("list entry is of an unknown type")
					return
				}
			}
		}
	}
}

func (c *baseComputer) ComputeTargetPatternExpansionValue(ctx context.Context, key *model_analysis_pb.TargetPatternExpansion_Key, e TargetPatternExpansionEnvironment) (PatchedTargetPatternExpansionValue, error) {
	canonicalTargetPattern, err := label.NewCanonicalTargetPattern(key.TargetPattern)
	if err != nil {
		return PatchedTargetPatternExpansionValue{}, fmt.Errorf("invalid target pattern: %w", err)
	}

	if initialTarget, includeFileTargets, ok := canonicalTargetPattern.AsSinglePackageTargetPattern(); ok {
		// Target pattern of shape "@@a+//b:all",
		// "@@a+//b:all-targets" or "@@a+//b:*".
		canonicalPackage := initialTarget.GetCanonicalPackage()
		packageValue := e.GetPackageValue(&model_analysis_pb.Package_Key{
			Label: canonicalPackage.String(),
		})
		if !packageValue.IsSet() {
			return PatchedTargetPatternExpansionValue{}, evaluation.ErrMissingDependency
		}

		definition, err := c.lookupTargetDefinitionInTargetList(
			ctx,
			model_core.Message[[]*model_analysis_pb.Package_Value_TargetList_Element]{
				Message:            packageValue.Message.Targets,
				OutgoingReferences: packageValue.OutgoingReferences,
			},
			initialTarget.GetTargetName(),
		)
		if err != nil {
			return PatchedTargetPatternExpansionValue{}, err
		}
		if definition.IsSet() {
			// Package contains an actual target that is
			// named "all", "all-targets" or "*". Prefer
			// matching just that target, as opposed to
			// performing actual wildcard expansion.
			return model_core.NewSimplePatchedMessage[dag.ObjectContentsWalker](&model_analysis_pb.TargetPatternExpansion_Value{
				TargetLabels: []*model_analysis_pb.TargetPatternExpansion_Value_TargetLabelList_Element{{
					Level: &model_analysis_pb.TargetPatternExpansion_Value_TargetLabelList_Element_Leaf{
						Leaf: initialTarget.String(),
					},
				}},
			}), nil
		}

		treeBuilder := btree.NewSplitProllyBuilder(
			/* minimumSizeBytes = */ 32*1024,
			/* maximumSizeBytes = */ 128*1024,
			btree.NewObjectCreatingNodeMerger(
				model_encoding.NewChainedBinaryEncoder(nil),
				c.buildSpecificationReference.GetReferenceFormat(),
				/* parentNodeComputer = */ func(contents *object.Contents, childNodes []*model_analysis_pb.TargetPatternExpansion_Value_TargetLabelList_Element, outgoingReferences object.OutgoingReferences, metadata []dag.ObjectContentsWalker) (model_core.PatchedMessage[*model_analysis_pb.TargetPatternExpansion_Value_TargetLabelList_Element, dag.ObjectContentsWalker], error) {
					patcher := model_core.NewReferenceMessagePatcher[dag.ObjectContentsWalker]()
					return model_core.NewPatchedMessage(
						&model_analysis_pb.TargetPatternExpansion_Value_TargetLabelList_Element{
							Level: &model_analysis_pb.TargetPatternExpansion_Value_TargetLabelList_Element_Parent_{
								Parent: &model_analysis_pb.TargetPatternExpansion_Value_TargetLabelList_Element_Parent{
									Reference: patcher.AddReference(contents.GetReference(), dag.NewSimpleObjectContentsWalker(contents, metadata)),
								},
							},
						},
						patcher,
					), nil
				},
			),
		)

		reader := model_parser.NewStorageBackedParsedObjectReader(
			c.objectDownloader,
			c.getValueObjectEncoder(),
			model_parser.NewMessageObjectParser[object.LocalReference, model_analysis_pb.Package_Value_TargetList](),
		)

		targetLists := []model_core.Message[[]*model_analysis_pb.Package_Value_TargetList_Element]{{
			Message:            packageValue.Message.Targets,
			OutgoingReferences: packageValue.OutgoingReferences,
		}}
		for len(targetLists) > 0 {
			lastList := &targetLists[len(targetLists)-1]
			if len(lastList.Message) == 0 {
				targetLists = targetLists[:len(targetLists)-1]
			} else {
				entry := lastList.Message[0]
				lastList.Message = lastList.Message[1:]
				switch level := entry.Level.(type) {
				case *model_analysis_pb.Package_Value_TargetList_Element_Leaf:
					reportTarget := false
					switch level.Leaf.Definition.GetKind().(type) {
					case *model_starlark_pb.Target_Definition_Alias:
						reportTarget = true
					case *model_starlark_pb.Target_Definition_RuleTarget:
						reportTarget = true
					case *model_starlark_pb.Target_Definition_SourceFileTarget:
						if includeFileTargets {
							reportTarget = true
						}
					}
					if reportTarget {
						targetName, err := label.NewTargetName(level.Leaf.Name)
						if err != nil {
							return PatchedTargetPatternExpansionValue{}, fmt.Errorf("invalid target name %#v: %w", level.Leaf.Name, err)
						}
						if err := treeBuilder.PushChild(model_core.NewSimplePatchedMessage[dag.ObjectContentsWalker](
							&model_analysis_pb.TargetPatternExpansion_Value_TargetLabelList_Element{
								Level: &model_analysis_pb.TargetPatternExpansion_Value_TargetLabelList_Element_Leaf{
									Leaf: canonicalPackage.AppendTargetName(targetName).String(),
								},
							},
						)); err != nil {
							return PatchedTargetPatternExpansionValue{}, err
						}
					}
				case *model_analysis_pb.Package_Value_TargetList_Element_Parent_:
					index, err := model_core.GetIndexFromReferenceMessage(level.Parent.Reference, lastList.OutgoingReferences.GetDegree())
					if err != nil {
						return PatchedTargetPatternExpansionValue{}, err
					}
					child, _, err := reader.ReadParsedObject(
						ctx,
						lastList.OutgoingReferences.GetOutgoingReference(index),
					)
					if err != nil {
						return PatchedTargetPatternExpansionValue{}, err
					}
					targetLists = append(targetLists, model_core.Message[[]*model_analysis_pb.Package_Value_TargetList_Element]{
						Message:            child.Message.Elements,
						OutgoingReferences: child.OutgoingReferences,
					})
				default:
					return PatchedTargetPatternExpansionValue{}, errors.New("list entry is of an unknown type")
				}
			}
		}

		targetLabelsList, err := treeBuilder.FinalizeList()
		if err != nil {
			return PatchedTargetPatternExpansionValue{}, err
		}

		return PatchedTargetPatternExpansionValue{
			Message: &model_analysis_pb.TargetPatternExpansion_Value{
				TargetLabels: targetLabelsList.Message,
			},
			Patcher: targetLabelsList.Patcher,
		}, nil
	}

	if basePackage, includeFileTargets, ok := canonicalTargetPattern.AsRecursiveTargetPattern(); ok {
		// Target pattern of shape "@@a+//b/..." or "@@a+//b/...:*".
		return PatchedTargetPatternExpansionValue{}, fmt.Errorf("TODO: recursive target pattern %#v %#v", basePackage.String(), includeFileTargets)
	}

	return PatchedTargetPatternExpansionValue{}, errors.New("target pattern does not require any expansion")
}
