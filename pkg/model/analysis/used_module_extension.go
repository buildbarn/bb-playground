package analysis

import (
	"context"
	"errors"
	"fmt"
	"sort"

	"github.com/buildbarn/bonanza/pkg/evaluation"
	"github.com/buildbarn/bonanza/pkg/label"
	model_core "github.com/buildbarn/bonanza/pkg/model/core"
	model_analysis_pb "github.com/buildbarn/bonanza/pkg/proto/model/analysis"
	"github.com/buildbarn/bonanza/pkg/storage/dag"
)

func (c *baseComputer[TReference, TMetadata]) ComputeUsedModuleExtensionValue(ctx context.Context, key *model_analysis_pb.UsedModuleExtension_Key, e UsedModuleExtensionEnvironment[TReference]) (PatchedUsedModuleExtensionValue, error) {
	usedModuleExtensions := e.GetUsedModuleExtensionsValue(&model_analysis_pb.UsedModuleExtensions_Key{})
	if !usedModuleExtensions.IsSet() {
		return PatchedUsedModuleExtensionValue{}, evaluation.ErrMissingDependency
	}
	extensions := usedModuleExtensions.Message.ModuleExtensions
	if i := sort.Search(
		len(extensions),
		func(i int) bool {
			identifier, err := label.NewCanonicalStarlarkIdentifier(extensions[i].Identifier)
			return err == nil && identifier.ToModuleExtension().String() >= key.ModuleExtension
		},
	); i < len(extensions) {
		extension := extensions[i]
		identifier, err := label.NewCanonicalStarlarkIdentifier(extension.Identifier)
		if err != nil {
			return PatchedUsedModuleExtensionValue{}, fmt.Errorf("invalid module extensions Starlark identifier %#v: %w", extension.Identifier, err)
		}
		if identifier.ToModuleExtension().String() == key.ModuleExtension {
			patchedExtension := model_core.NewPatchedMessageFromExisting(
				model_core.NewNestedMessage(usedModuleExtensions, extension),
				func(index int) dag.ObjectContentsWalker {
					return dag.ExistingObjectContentsWalker
				},
			)
			return PatchedUsedModuleExtensionValue{
				Message: &model_analysis_pb.UsedModuleExtension_Value{
					ModuleExtension: patchedExtension.Message,
				},
				Patcher: patchedExtension.Patcher,
			}, nil
		}
	}
	return PatchedUsedModuleExtensionValue{}, errors.New("module extension not found")
}
