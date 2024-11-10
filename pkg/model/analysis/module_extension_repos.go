package analysis

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"

	"github.com/buildbarn/bb-playground/pkg/evaluation"
	"github.com/buildbarn/bb-playground/pkg/label"
	model_core "github.com/buildbarn/bb-playground/pkg/model/core"
	"github.com/buildbarn/bb-playground/pkg/model/core/btree"
	model_encoding "github.com/buildbarn/bb-playground/pkg/model/encoding"
	model_starlark "github.com/buildbarn/bb-playground/pkg/model/starlark"
	model_analysis_pb "github.com/buildbarn/bb-playground/pkg/proto/model/analysis"
	model_starlark_pb "github.com/buildbarn/bb-playground/pkg/proto/model/starlark"
	"github.com/buildbarn/bb-playground/pkg/storage/dag"
	"github.com/buildbarn/bb-playground/pkg/storage/object"

	"go.starlark.net/starlark"
)

func (c *baseComputer) ComputeModuleExtensionReposValue(ctx context.Context, key *model_analysis_pb.ModuleExtensionRepos_Key, e ModuleExtensionReposEnvironment) (PatchedModuleExtensionReposValue, error) {
	// Resolve the module extension object that was declared within
	// Starlark code.
	moduleExtensionName, err := label.NewModuleExtension(key.ModuleExtension)
	if err != nil {
		return PatchedModuleExtensionReposValue{}, fmt.Errorf("invalid module extension: %w", err)
	}

	usedModuleExtension := e.GetUsedModuleExtensionValue(&model_analysis_pb.UsedModuleExtension_Key{
		ModuleExtension: moduleExtensionName.String(),
	})
	allBuiltinsModulesNames := e.GetBuiltinsModuleNamesValue(&model_analysis_pb.BuiltinsModuleNames_Key{})
	if !usedModuleExtension.IsSet() || !allBuiltinsModulesNames.IsSet() {
		return PatchedModuleExtensionReposValue{}, evaluation.ErrMissingDependency
	}

	moduleExtensionIdentifierStr := usedModuleExtension.Message.ModuleExtension.GetIdentifier()
	moduleExtensionIdentifier, err := label.NewCanonicalStarlarkIdentifier(moduleExtensionIdentifierStr)
	if err != nil {
		return PatchedModuleExtensionReposValue{}, fmt.Errorf("invalid module extension identifier %#v: %w", moduleExtensionIdentifierStr, err)
	}
	moduleExtensionDefinitionValue := e.GetCompiledBzlFileGlobalValue(&model_analysis_pb.CompiledBzlFileGlobal_Key{
		Identifier: moduleExtensionIdentifier.String(),
	})
	if !moduleExtensionDefinitionValue.IsSet() {
		return PatchedModuleExtensionReposValue{}, evaluation.ErrMissingDependency
	}
	v, ok := moduleExtensionDefinitionValue.Message.Global.GetKind().(*model_starlark_pb.Value_ModuleExtension)
	if !ok {
		return PatchedModuleExtensionReposValue{}, fmt.Errorf("%#v is not a module extension", moduleExtensionIdentifier.String())
	}
	moduleExtensionDefinition := v.ModuleExtension

	// Call into the implementation function to obtain a set of
	// repos declared by this module extension.
	thread := c.newStarlarkThread(ctx, e, allBuiltinsModulesNames.Message.BuiltinsModuleNames)
	thread.SetLocal(model_starlark.CanonicalPackageKey, moduleExtensionName.GetModuleInstance().GetBareCanonicalRepo().GetRootPackage())
	thread.SetLocal(model_starlark.ValueEncodingOptionsKey, c.getValueEncodingOptions(moduleExtensionIdentifier.GetCanonicalLabel()))

	repoRegistrar := model_starlark.NewRepoRegistrar()
	thread.SetLocal(model_starlark.RepoRegistrarKey, repoRegistrar)

	// TODO: Capture extension_metadata.
	_, err = starlark.Call(
		thread,
		model_starlark.NewNamedFunction(model_starlark.NewProtoNamedFunctionDefinition(
			model_core.Message[*model_starlark_pb.Function]{
				Message:            moduleExtensionDefinition.Implementation,
				OutgoingReferences: moduleExtensionDefinitionValue.OutgoingReferences,
			},
		)),
		/* args = */ starlark.Tuple{
			starlark.None,
		},
		/* kwargs = */ nil,
	)
	if err != nil {
		var evalErr *starlark.EvalError
		if !errors.Is(err, evaluation.ErrMissingDependency) && errors.As(err, &evalErr) {
			return PatchedModuleExtensionReposValue{}, errors.New(evalErr.Backtrace())
		}
		return PatchedModuleExtensionReposValue{}, err
	}

	// Store all repos in a B-tree.
	// TODO: Use a proper encoder!
	treeBuilder := btree.NewSplitProllyBuilder(
		/* minimumSizeBytes = */ 32*1024,
		/* maximumSizeBytes = */ 128*1024,
		btree.NewObjectCreatingNodeMerger(
			model_encoding.NewChainedBinaryEncoder(nil),
			c.buildSpecificationReference.GetReferenceFormat(),
			/* parentNodeComputer = */ func(contents *object.Contents, childNodes []*model_analysis_pb.ModuleExtensionRepos_Value_RepoList_Element, outgoingReferences object.OutgoingReferences, metadata []dag.ObjectContentsWalker) (model_core.PatchedMessage[*model_analysis_pb.ModuleExtensionRepos_Value_RepoList_Element, dag.ObjectContentsWalker], error) {
				var firstName string
				switch firstElement := childNodes[0].Level.(type) {
				case *model_analysis_pb.ModuleExtensionRepos_Value_RepoList_Element_Leaf:
					firstName = firstElement.Leaf.Name
				case *model_analysis_pb.ModuleExtensionRepos_Value_RepoList_Element_Parent_:
					firstName = firstElement.Parent.FirstName
				}
				patcher := model_core.NewReferenceMessagePatcher[dag.ObjectContentsWalker]()
				return model_core.PatchedMessage[*model_analysis_pb.ModuleExtensionRepos_Value_RepoList_Element, dag.ObjectContentsWalker]{
					Message: &model_analysis_pb.ModuleExtensionRepos_Value_RepoList_Element{
						Level: &model_analysis_pb.ModuleExtensionRepos_Value_RepoList_Element_Parent_{
							Parent: &model_analysis_pb.ModuleExtensionRepos_Value_RepoList_Element_Parent{
								Reference: patcher.AddReference(contents.GetReference(), dag.NewSimpleObjectContentsWalker(contents, metadata)),
								FirstName: firstName,
							},
						},
					},
					Patcher: patcher,
				}, nil
			},
		),
	)

	repos := repoRegistrar.GetRepos()
	for _, name := range slices.Sorted(maps.Keys(repos)) {
		repo := repos[name]
		if err := treeBuilder.PushChild(model_core.PatchedMessage[*model_analysis_pb.ModuleExtensionRepos_Value_RepoList_Element, dag.ObjectContentsWalker]{
			Message: &model_analysis_pb.ModuleExtensionRepos_Value_RepoList_Element{
				Level: &model_analysis_pb.ModuleExtensionRepos_Value_RepoList_Element_Leaf{
					Leaf: repo.Message,
				},
			},
			Patcher: repo.Patcher,
		}); err != nil {
			return PatchedModuleExtensionReposValue{}, err
		}
	}

	reposList, err := treeBuilder.FinalizeList()
	if err != nil {
		return PatchedModuleExtensionReposValue{}, err
	}

	return PatchedModuleExtensionReposValue{
		Message: &model_analysis_pb.ModuleExtensionRepos_Value{
			Repos: reposList.Message,
		},
		Patcher: reposList.Patcher,
	}, nil
}
