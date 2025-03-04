package analysis

import (
	"context"
	"errors"
	"fmt"
	"iter"

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

type expandCanonicalTargetPatternEnvironment interface {
	GetTargetPatternExpansionValue(*model_analysis_pb.TargetPatternExpansion_Key) model_core.Message[*model_analysis_pb.TargetPatternExpansion_Value, object.OutgoingReferences]
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

		for entry := range btree.AllLeaves(
			ctx,
			c.targetPatternExpansionValueTargetLabelDereferencer,
			model_core.NewNestedMessage(targetPatternExpansion, targetPatternExpansion.Message.TargetLabels),
			func(entry model_core.Message[*model_analysis_pb.TargetPatternExpansion_Value_TargetLabel, object.OutgoingReferences]) (*model_core_pb.Reference, error) {
				if level, ok := entry.Message.Level.(*model_analysis_pb.TargetPatternExpansion_Value_TargetLabel_Parent_); ok {
					return level.Parent.Reference, nil
				}
				return nil, nil
			},
			errOut,
		) {
			level, ok := entry.Message.Level.(*model_analysis_pb.TargetPatternExpansion_Value_TargetLabel_Leaf)
			if !ok {
				*errOut = errors.New("not a valid leaf entry")
				return
			}
			targetLabel, err := label.NewCanonicalLabel(level.Leaf)
			if err != nil {
				*errOut = fmt.Errorf("invalid target label %#v: %w", level.Leaf, err)
				return
			}
			if !yield(targetLabel) {
				*errOut = nil
				return
			}
		}
	}
}

func (c *baseComputer) ComputeTargetPatternExpansionValue(ctx context.Context, key *model_analysis_pb.TargetPatternExpansion_Key, e TargetPatternExpansionEnvironment) (PatchedTargetPatternExpansionValue, error) {
	treeBuilder := btree.NewSplitProllyBuilder(
		/* minimumSizeBytes = */ 32*1024,
		/* maximumSizeBytes = */ 128*1024,
		btree.NewObjectCreatingNodeMerger(
			c.getValueObjectEncoder(),
			c.getReferenceFormat(),
			/* parentNodeComputer = */ func(createdObject model_core.CreatedObject[dag.ObjectContentsWalker], childNodes []*model_analysis_pb.TargetPatternExpansion_Value_TargetLabel) (model_core.PatchedMessage[*model_analysis_pb.TargetPatternExpansion_Value_TargetLabel, dag.ObjectContentsWalker], error) {
				patcher := model_core.NewReferenceMessagePatcher[dag.ObjectContentsWalker]()
				return model_core.NewPatchedMessage(
					&model_analysis_pb.TargetPatternExpansion_Value_TargetLabel{
						Level: &model_analysis_pb.TargetPatternExpansion_Value_TargetLabel_Parent_{
							Parent: &model_analysis_pb.TargetPatternExpansion_Value_TargetLabel_Parent{
								Reference: patcher.AddReference(
									createdObject.Contents.GetReference(),
									dag.NewSimpleObjectContentsWalker(createdObject.Contents, createdObject.Metadata)),
							},
						},
					},
					patcher,
				), nil
			},
		),
	)

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
			model_core.NewNestedMessage(packageValue, packageValue.Message.Targets),
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
			if err := treeBuilder.PushChild(model_core.NewSimplePatchedMessage[dag.ObjectContentsWalker](
				&model_analysis_pb.TargetPatternExpansion_Value_TargetLabel{
					Level: &model_analysis_pb.TargetPatternExpansion_Value_TargetLabel_Leaf{
						Leaf: initialTarget.String(),
					},
				},
			)); err != nil {
				return PatchedTargetPatternExpansionValue{}, err
			}
		} else if err := c.addPackageToTargetPatternExpansion(ctx, canonicalPackage, packageValue, includeFileTargets, treeBuilder); err != nil {
			return PatchedTargetPatternExpansionValue{}, err
		}
	} else if basePackage, includeFileTargets, ok := canonicalTargetPattern.AsRecursiveTargetPattern(); ok {
		// Target pattern of shape "@@a+//b/..." or "@@a+//b/...:*".
		packagesAtAndBelow := e.GetPackagesAtAndBelowValue(&model_analysis_pb.PackagesAtAndBelow_Key{
			BasePackage: basePackage.String(),
		})
		if !packagesAtAndBelow.IsSet() {
			return PatchedTargetPatternExpansionValue{}, evaluation.ErrMissingDependency
		}

		// Add targets belonging to the current package.
		missingDependencies := false
		if packagesAtAndBelow.Message.PackageAtBasePackage {
			if packageValue := e.GetPackageValue(&model_analysis_pb.Package_Key{
				Label: basePackage.String(),
			}); packageValue.IsSet() {
				if err := c.addPackageToTargetPatternExpansion(ctx, basePackage, packageValue, includeFileTargets, treeBuilder); err != nil {
					return PatchedTargetPatternExpansionValue{}, err
				}
			} else {
				missingDependencies = true
			}
		}

		// Merge results from child packages. We don't recurse
		// into the results, meaning that the resulting B-tree
		// doesn't have a uniform height. This is likely
		// acceptable, as it prevents duplication of data.
		for _, packagePath := range packagesAtAndBelow.Message.PackagesBelowBasePackage {
			childTargetPattern, err := basePackage.ToRecursiveTargetPatternBelow(packagePath, includeFileTargets)
			if err != nil {
				return PatchedTargetPatternExpansionValue{}, fmt.Errorf("invalid package path %#v: %w", packagePath)
			}
			childTargetPatternExpansion := e.GetTargetPatternExpansionValue(&model_analysis_pb.TargetPatternExpansion_Key{
				TargetPattern: childTargetPattern.String(),
			})
			if missingDependencies || !childTargetPatternExpansion.IsSet() {
				missingDependencies = true
				continue
			}

			for _, targetLabel := range childTargetPatternExpansion.Message.TargetLabels {
				if err := treeBuilder.PushChild(
					model_core.NewPatchedMessageFromExisting(
						model_core.NewNestedMessage(childTargetPatternExpansion, targetLabel),
						func(index int) dag.ObjectContentsWalker {
							return dag.ExistingObjectContentsWalker
						},
					),
				); err != nil {
					return PatchedTargetPatternExpansionValue{}, err
				}
			}
		}

		if missingDependencies {
			return PatchedTargetPatternExpansionValue{}, evaluation.ErrMissingDependency
		}
	} else {
		return PatchedTargetPatternExpansionValue{}, errors.New("target pattern does not require any expansion")
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

func (c *baseComputer) addPackageToTargetPatternExpansion(
	ctx context.Context,
	canonicalPackage label.CanonicalPackage,
	packageValue model_core.Message[*model_analysis_pb.Package_Value, object.OutgoingReferences],
	includeFileTargets bool,
	treeBuilder btree.Builder[*model_analysis_pb.TargetPatternExpansion_Value_TargetLabel, dag.ObjectContentsWalker],
) error {
	var errIter error
	for entry := range btree.AllLeaves(
		ctx,
		c.packageValueTargetDereferencer,
		model_core.NewNestedMessage(packageValue, packageValue.Message.Targets),
		func(entry model_core.Message[*model_analysis_pb.Package_Value_Target, object.OutgoingReferences]) (*model_core_pb.Reference, error) {
			if level, ok := entry.Message.Level.(*model_analysis_pb.Package_Value_Target_Parent_); ok {
				return level.Parent.Reference, nil
			}
			return nil, nil
		},
		&errIter,
	) {
		level, ok := entry.Message.Level.(*model_analysis_pb.Package_Value_Target_Leaf)
		if !ok {
			return errors.New("not a valid leaf entry")
		}
		reportTarget := false
		switch level.Leaf.Definition.GetKind().(type) {
		case *model_starlark_pb.Target_Definition_Alias:
			reportTarget = true
		case *model_starlark_pb.Target_Definition_LabelSetting:
			reportTarget = true
		case *model_starlark_pb.Target_Definition_PredeclaredOutputFileTarget:
			if includeFileTargets {
				reportTarget = true
			}
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
				return fmt.Errorf("invalid target name %#v: %w", level.Leaf.Name, err)
			}
			if err := treeBuilder.PushChild(model_core.NewSimplePatchedMessage[dag.ObjectContentsWalker](
				&model_analysis_pb.TargetPatternExpansion_Value_TargetLabel{
					Level: &model_analysis_pb.TargetPatternExpansion_Value_TargetLabel_Leaf{
						Leaf: canonicalPackage.AppendTargetName(targetName).String(),
					},
				},
			)); err != nil {
				return err
			}
		}
	}
	return errIter
}
