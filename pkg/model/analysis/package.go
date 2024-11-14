package analysis

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	"sort"

	"github.com/buildbarn/bb-playground/pkg/evaluation"
	"github.com/buildbarn/bb-playground/pkg/label"
	model_core "github.com/buildbarn/bb-playground/pkg/model/core"
	"github.com/buildbarn/bb-playground/pkg/model/core/btree"
	model_encoding "github.com/buildbarn/bb-playground/pkg/model/encoding"
	model_filesystem "github.com/buildbarn/bb-playground/pkg/model/filesystem"
	model_starlark "github.com/buildbarn/bb-playground/pkg/model/starlark"
	model_analysis_pb "github.com/buildbarn/bb-playground/pkg/proto/model/analysis"
	model_filesystem_pb "github.com/buildbarn/bb-playground/pkg/proto/model/filesystem"
	model_starlark_pb "github.com/buildbarn/bb-playground/pkg/proto/model/starlark"
	"github.com/buildbarn/bb-playground/pkg/storage/dag"
	"github.com/buildbarn/bb-playground/pkg/storage/object"

	"go.starlark.net/starlark"
	"go.starlark.net/syntax"
)

var buildDotBazelTargetNames = []label.TargetName{
	label.MustNewTargetName("BUILD.bazel"),
	label.MustNewTargetName("BUILD"),
}

func (c *baseComputer) ComputePackageValue(ctx context.Context, key *model_analysis_pb.Package_Key, e PackageEnvironment) (PatchedPackageValue, error) {
	canonicalPackage, err := label.NewCanonicalPackage(key.Label)
	if err != nil {
		return PatchedPackageValue{}, fmt.Errorf("invalid package label: %w", err)
	}
	canonicalRepo := canonicalPackage.GetCanonicalRepo()

	allBuiltinsModulesNames := e.GetBuiltinsModuleNamesValue(&model_analysis_pb.BuiltinsModuleNames_Key{})
	repoDefaultAttrsValue := e.GetRepoDefaultAttrsValue(&model_analysis_pb.RepoDefaultAttrs_Key{
		CanonicalRepo: canonicalRepo.String(),
	})
	fileReader, gotFileReader := e.GetFileReaderValue(&model_analysis_pb.FileReader_Key{})
	if !allBuiltinsModulesNames.IsSet() || !repoDefaultAttrsValue.IsSet() || !gotFileReader {
		return PatchedPackageValue{}, evaluation.ErrMissingDependency
	}

	builtinsModuleNames := allBuiltinsModulesNames.Message.BuiltinsModuleNames
	buildFileBuiltins, err := c.getBzlFileBuiltins(e, builtinsModuleNames, model_starlark.BuildFileBuiltins, "exported_rules")
	if err != nil {
		return PatchedPackageValue{}, err
	}

	for _, buildFileName := range buildDotBazelTargetNames {
		buildFileProperties := e.GetFilePropertiesValue(&model_analysis_pb.FileProperties_Key{
			CanonicalRepo: canonicalRepo.String(),
			Path:          canonicalPackage.AppendTargetName(buildFileName).GetRepoRelativePath(),
		})
		if !buildFileProperties.IsSet() {
			return PatchedPackageValue{}, evaluation.ErrMissingDependency
		}
		if buildFileProperties.Message.Exists == nil {
			continue
		}

		buildFileLabel := canonicalPackage.AppendTargetName(buildFileName)
		buildFileContentsEntry, err := model_filesystem.NewFileContentsEntryFromProto(
			model_core.Message[*model_filesystem_pb.FileContents]{
				Message:            buildFileProperties.Message.Exists.Contents,
				OutgoingReferences: buildFileProperties.OutgoingReferences,
			},
			c.buildSpecificationReference.GetReferenceFormat(),
		)
		if err != nil {
			return PatchedPackageValue{}, fmt.Errorf("invalid contents for file %#v: %w", buildFileLabel.String(), err)
		}
		buildFileData, err := fileReader.FileReadAll(ctx, buildFileContentsEntry, 1<<20)
		if err != nil {
			return PatchedPackageValue{}, err
		}

		_, program, err := starlark.SourceProgramOptions(
			&syntax.FileOptions{},
			buildFileLabel.String(),
			buildFileData,
			buildFileBuiltins.Has,
		)
		if err != nil {
			return PatchedPackageValue{}, fmt.Errorf("failed to load %#v: %w", buildFileLabel.String(), err)
		}

		if err := c.preloadBzlGlobals(e, canonicalPackage, program, builtinsModuleNames); err != nil {
			return PatchedPackageValue{}, err
		}

		thread := c.newStarlarkThread(ctx, e, builtinsModuleNames)
		thread.SetLocal(model_starlark.CanonicalPackageKey, canonicalPackage)
		thread.SetLocal(model_starlark.ValueEncodingOptionsKey, c.getValueEncodingOptions(buildFileLabel))

		repoDefaultAttrs := model_core.Message[*model_starlark_pb.InheritableAttrs]{
			Message:            repoDefaultAttrsValue.Message.InheritableAttrs,
			OutgoingReferences: repoDefaultAttrsValue.OutgoingReferences,
		}
		targetRegistrar := model_starlark.NewTargetRegistrar(c.getInlinedTreeOptions(), repoDefaultAttrs)
		thread.SetLocal(model_starlark.TargetRegistrarKey, targetRegistrar)

		thread.SetLocal(model_starlark.GlobalResolverKey, func(identifier label.CanonicalStarlarkIdentifier) (model_core.Message[*model_starlark_pb.Value], error) {
			canonicalLabel := identifier.GetCanonicalLabel()
			compiledBzlFile := e.GetCompiledBzlFileValue(&model_analysis_pb.CompiledBzlFile_Key{
				Label:               canonicalLabel.String(),
				BuiltinsModuleNames: trimBuiltinModuleNames(builtinsModuleNames, canonicalLabel.GetCanonicalRepo().GetModuleInstance().GetModule()),
			})
			if !compiledBzlFile.IsSet() {
				return model_core.Message[*model_starlark_pb.Value]{}, evaluation.ErrMissingDependency
			}
			identifierStr := identifier.GetStarlarkIdentifier().String()
			globals := compiledBzlFile.Message.CompiledProgram.GetGlobals()
			if i := sort.Search(len(globals), func(i int) bool {
				return globals[i].Name >= identifierStr
			}); i < len(globals) && globals[i].Name == identifierStr {
				global := globals[i]
				if global.Value == nil {
					return model_core.Message[*model_starlark_pb.Value]{}, fmt.Errorf("global %#v has no value", identifier.String())
				}
				return model_core.Message[*model_starlark_pb.Value]{
					Message:            global.Value,
					OutgoingReferences: compiledBzlFile.OutgoingReferences,
				}, nil
			}
			return model_core.Message[*model_starlark_pb.Value]{}, fmt.Errorf("global %#v does not exist", identifier.String())
		})

		// Execute the BUILD.bazel file, so that all targets
		// contained within are instantiated.
		if _, err := program.Init(thread, buildFileBuiltins); err != nil {
			var evalErr *starlark.EvalError
			if !errors.Is(err, evaluation.ErrMissingDependency) && errors.As(err, &evalErr) {
				return PatchedPackageValue{}, fmt.Errorf("%s: %s", evalErr.Backtrace(), evalErr.Msg)
			}
			return PatchedPackageValue{}, err
		}

		// Store all targets in a B-tree.
		// TODO: Use a proper encoder!
		treeBuilder := btree.NewSplitProllyBuilder(
			/* minimumSizeBytes = */ 32*1024,
			/* maximumSizeBytes = */ 128*1024,
			btree.NewObjectCreatingNodeMerger(
				model_encoding.NewChainedBinaryEncoder(nil),
				c.buildSpecificationReference.GetReferenceFormat(),
				/* parentNodeComputer = */ func(contents *object.Contents, childNodes []*model_analysis_pb.Package_Value_TargetList_Element, outgoingReferences object.OutgoingReferences, metadata []dag.ObjectContentsWalker) (model_core.PatchedMessage[*model_analysis_pb.Package_Value_TargetList_Element, dag.ObjectContentsWalker], error) {
					var firstName string
					switch firstElement := childNodes[0].Level.(type) {
					case *model_analysis_pb.Package_Value_TargetList_Element_Leaf:
						firstName = firstElement.Leaf.Name
					case *model_analysis_pb.Package_Value_TargetList_Element_Parent_:
						firstName = firstElement.Parent.FirstName
					}
					patcher := model_core.NewReferenceMessagePatcher[dag.ObjectContentsWalker]()
					return model_core.PatchedMessage[*model_analysis_pb.Package_Value_TargetList_Element, dag.ObjectContentsWalker]{
						Message: &model_analysis_pb.Package_Value_TargetList_Element{
							Level: &model_analysis_pb.Package_Value_TargetList_Element_Parent_{
								Parent: &model_analysis_pb.Package_Value_TargetList_Element_Parent{
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

		targets := targetRegistrar.GetTargets()
		for _, name := range slices.Sorted(maps.Keys(targets)) {
			target := targets[name]
			if err := treeBuilder.PushChild(model_core.PatchedMessage[*model_analysis_pb.Package_Value_TargetList_Element, dag.ObjectContentsWalker]{
				Message: &model_analysis_pb.Package_Value_TargetList_Element{
					Level: &model_analysis_pb.Package_Value_TargetList_Element_Leaf{
						Leaf: target.Message,
					},
				},
				Patcher: target.Patcher,
			}); err != nil {
				return PatchedPackageValue{}, err
			}
		}

		targetsList, err := treeBuilder.FinalizeList()
		if err != nil {
			return PatchedPackageValue{}, err
		}

		return PatchedPackageValue{
			Message: &model_analysis_pb.Package_Value{
				Targets: targetsList.Message,
			},
			Patcher: targetsList.Patcher,
		}, nil
	}

	return PatchedPackageValue{}, errors.New("BUILD.bazel does not exist")
}
