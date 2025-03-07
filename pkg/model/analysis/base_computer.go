package analysis

import (
	"context"
	"errors"
	"fmt"
	"net/http"

	re_filesystem "github.com/buildbarn/bb-remote-execution/pkg/filesystem"
	"github.com/buildbarn/bb-storage/pkg/filesystem"
	"github.com/buildbarn/bonanza/pkg/evaluation"
	"github.com/buildbarn/bonanza/pkg/label"
	model_core "github.com/buildbarn/bonanza/pkg/model/core"
	"github.com/buildbarn/bonanza/pkg/model/core/inlinedtree"
	model_encoding "github.com/buildbarn/bonanza/pkg/model/encoding"
	model_filesystem "github.com/buildbarn/bonanza/pkg/model/filesystem"
	model_parser "github.com/buildbarn/bonanza/pkg/model/parser"
	model_starlark "github.com/buildbarn/bonanza/pkg/model/starlark"
	model_analysis_pb "github.com/buildbarn/bonanza/pkg/proto/model/analysis"
	model_build_pb "github.com/buildbarn/bonanza/pkg/proto/model/build"
	model_command_pb "github.com/buildbarn/bonanza/pkg/proto/model/command"
	model_starlark_pb "github.com/buildbarn/bonanza/pkg/proto/model/starlark"
	object_pb "github.com/buildbarn/bonanza/pkg/proto/storage/object"
	remoteexecution "github.com/buildbarn/bonanza/pkg/remoteexecution"
	"github.com/buildbarn/bonanza/pkg/storage/dag"
	"github.com/buildbarn/bonanza/pkg/storage/object"

	"google.golang.org/protobuf/types/known/emptypb"

	"go.starlark.net/starlark"
)

type BaseComputerReferenceMetadata interface {
	model_core.CloneableReferenceMetadata
	model_core.WalkableReferenceMetadata
}

type baseComputer[TReference object.BasicReference, TMetadata BaseComputerReferenceMetadata] struct {
	parsedObjectPoolIngester    *model_parser.ParsedObjectPoolIngester[TReference]
	buildSpecificationReference TReference
	objectCapturer              model_core.CreatedOrExistingObjectCapturer[TReference, TMetadata]
	httpClient                  *http.Client
	filePool                    re_filesystem.FilePool
	cacheDirectory              filesystem.Directory
	executionClient             *remoteexecution.Client[*model_command_pb.Action, emptypb.Empty, *emptypb.Empty, *model_command_pb.Result]
	executionNamespace          *object_pb.Namespace
	bzlFileBuiltins             starlark.StringDict
	buildFileBuiltins           starlark.StringDict

	// Readers for various message types.
	// TODO: These should likely be removed and instantiated later
	// on, so that we can encrypt all data in storage.
	valueReaders                                 model_starlark.ValueReaders[TReference]
	buildSpecificationReader                     model_parser.ParsedObjectReader[TReference, model_core.Message[*model_build_pb.BuildSpecification, TReference]]
	commandOutputsReader                         model_parser.ParsedObjectReader[TReference, model_core.Message[*model_command_pb.Outputs, TReference]]
	configurationBuildSettingOverrideReader      model_parser.ParsedObjectReader[TReference, model_core.Message[[]*model_analysis_pb.Configuration_BuildSettingOverride, TReference]]
	configurationReader                          model_parser.ParsedObjectReader[TReference, model_core.Message[*model_analysis_pb.Configuration, TReference]]
	moduleExtensionReposValueRepoReader          model_parser.ParsedObjectReader[TReference, model_core.Message[[]*model_analysis_pb.ModuleExtensionRepos_Value_Repo, TReference]]
	packageValueTargetReader                     model_parser.ParsedObjectReader[TReference, model_core.Message[[]*model_analysis_pb.Package_Value_Target, TReference]]
	targetPatternExpansionValueTargetLabelReader model_parser.ParsedObjectReader[TReference, model_core.Message[[]*model_analysis_pb.TargetPatternExpansion_Value_TargetLabel, TReference]]
}

func NewBaseComputer[TReference object.BasicReference, TMetadata BaseComputerReferenceMetadata](
	parsedObjectPoolIngester *model_parser.ParsedObjectPoolIngester[TReference],
	buildSpecificationReference TReference,
	buildSpecificationEncoder model_encoding.BinaryEncoder,
	objectCapturer model_core.CreatedOrExistingObjectCapturer[TReference, TMetadata],
	httpClient *http.Client,
	filePool re_filesystem.FilePool,
	cacheDirectory filesystem.Directory,
	executionClient *remoteexecution.Client[*model_command_pb.Action, emptypb.Empty, *emptypb.Empty, *model_command_pb.Result],
	executionInstanceName object.InstanceName,
	bzlFileBuiltins starlark.StringDict,
	buildFileBuiltins starlark.StringDict,
) Computer[TReference] {
	return &baseComputer[TReference, TMetadata]{
		parsedObjectPoolIngester:    parsedObjectPoolIngester,
		buildSpecificationReference: buildSpecificationReference,
		objectCapturer:              objectCapturer,
		httpClient:                  httpClient,
		filePool:                    filePool,
		cacheDirectory:              cacheDirectory,
		executionClient:             executionClient,
		executionNamespace: object.Namespace{
			InstanceName:    executionInstanceName,
			ReferenceFormat: buildSpecificationReference.GetReferenceFormat(),
		}.ToProto(),
		bzlFileBuiltins:   bzlFileBuiltins,
		buildFileBuiltins: buildFileBuiltins,

		// TODO: Set up encoding!
		valueReaders: model_starlark.ValueReaders[TReference]{
			Dict: model_parser.LookupParsedObjectReader(
				parsedObjectPoolIngester,
				model_parser.NewMessageListObjectParser[TReference, model_starlark_pb.Dict_Entry](),
			),
			List: model_parser.LookupParsedObjectReader(
				parsedObjectPoolIngester,
				model_parser.NewMessageListObjectParser[TReference, model_starlark_pb.List_Element](),
			),
		},
		buildSpecificationReader: model_parser.LookupParsedObjectReader(
			parsedObjectPoolIngester,
			model_parser.NewChainedObjectParser(
				model_parser.NewEncodedObjectParser[TReference](buildSpecificationEncoder),
				model_parser.NewMessageObjectParser[TReference, model_build_pb.BuildSpecification](),
			),
		),
		commandOutputsReader: model_parser.LookupParsedObjectReader(
			parsedObjectPoolIngester,
			model_parser.NewMessageObjectParser[TReference, model_command_pb.Outputs](),
		),
		configurationBuildSettingOverrideReader: model_parser.LookupParsedObjectReader(
			parsedObjectPoolIngester,
			model_parser.NewMessageListObjectParser[TReference, model_analysis_pb.Configuration_BuildSettingOverride](),
		),
		configurationReader: model_parser.LookupParsedObjectReader(
			parsedObjectPoolIngester,
			model_parser.NewMessageObjectParser[TReference, model_analysis_pb.Configuration](),
		),
		moduleExtensionReposValueRepoReader: model_parser.LookupParsedObjectReader(
			parsedObjectPoolIngester,
			model_parser.NewMessageListObjectParser[TReference, model_analysis_pb.ModuleExtensionRepos_Value_Repo](),
		),
		packageValueTargetReader: model_parser.LookupParsedObjectReader(
			parsedObjectPoolIngester,
			model_parser.NewMessageListObjectParser[TReference, model_analysis_pb.Package_Value_Target](),
		),
		targetPatternExpansionValueTargetLabelReader: model_parser.LookupParsedObjectReader(
			parsedObjectPoolIngester,
			model_parser.NewMessageListObjectParser[TReference, model_analysis_pb.TargetPatternExpansion_Value_TargetLabel](),
		),
	}
}

func (c *baseComputer[TReference, TMetadata]) getReferenceFormat() object.ReferenceFormat {
	return c.buildSpecificationReference.GetReferenceFormat()
}

func (c *baseComputer[TReference, TMetadata]) getValueObjectEncoder() model_encoding.BinaryEncoder {
	// TODO: Use a proper encoder!
	return model_encoding.NewChainedBinaryEncoder(nil)
}

func (c *baseComputer[TReference, TMetadata]) getValueEncodingOptions(currentFilename label.CanonicalLabel) *model_starlark.ValueEncodingOptions[TReference, TMetadata] {
	return &model_starlark.ValueEncodingOptions[TReference, TMetadata]{
		CurrentFilename:        currentFilename,
		ObjectEncoder:          c.getValueObjectEncoder(),
		ObjectReferenceFormat:  c.getReferenceFormat(),
		ObjectCapturer:         c.objectCapturer,
		ObjectMinimumSizeBytes: 32 * 1024,
		ObjectMaximumSizeBytes: 128 * 1024,
	}
}

func (c *baseComputer[TReference, TMetadata]) getValueDecodingOptions(ctx context.Context, labelCreator func(label.ResolvedLabel) (starlark.Value, error)) *model_starlark.ValueDecodingOptions[TReference] {
	return &model_starlark.ValueDecodingOptions[TReference]{
		Context:         ctx,
		Readers:         &c.valueReaders,
		LabelCreator:    labelCreator,
		BzlFileBuiltins: c.bzlFileBuiltins,
	}
}

func (c *baseComputer[TReference, TMetadata]) getInlinedTreeOptions() *inlinedtree.Options {
	return &inlinedtree.Options{
		ReferenceFormat:  c.getReferenceFormat(),
		Encoder:          c.getValueObjectEncoder(),
		MaximumSizeBytes: 32 * 1024,
	}
}

type resolveApparentEnvironment[TReference any] interface {
	GetCanonicalRepoNameValue(*model_analysis_pb.CanonicalRepoName_Key) model_core.Message[*model_analysis_pb.CanonicalRepoName_Value, TReference]
	GetRootModuleValue(*model_analysis_pb.RootModule_Key) model_core.Message[*model_analysis_pb.RootModule_Value, TReference]
}

type canonicalizable[T any] interface {
	AsCanonical() (T, bool)
	GetApparentRepo() (label.ApparentRepo, bool)
	WithCanonicalRepo(canonicalRepo label.CanonicalRepo) T
}

func resolveApparent[TCanonical any, TApparent canonicalizable[TCanonical], TReference any](e resolveApparentEnvironment[TReference], fromRepo label.CanonicalRepo, toApparent TApparent) (TCanonical, error) {
	if toCanonical, ok := toApparent.AsCanonical(); ok {
		// Label was already canonical. Nothing to do.
		return toCanonical, nil
	}

	if toApparentRepo, ok := toApparent.GetApparentRepo(); ok {
		// Label is prefixed with an apparent repo. Resolve the repo.
		v := e.GetCanonicalRepoNameValue(&model_analysis_pb.CanonicalRepoName_Key{
			FromCanonicalRepo: fromRepo.String(),
			ToApparentRepo:    toApparentRepo.String(),
		})
		var bad TCanonical
		if !v.IsSet() {
			return bad, evaluation.ErrMissingDependency
		}
		toCanonicalRepo, err := label.NewCanonicalRepo(v.Message.ToCanonicalRepo)
		if err != nil {
			return bad, fmt.Errorf("invalid canonical repo name %#v: %w", v.Message.ToCanonicalRepo, err)
		}
		return toApparent.WithCanonicalRepo(toCanonicalRepo), nil
	}

	// Label is prefixed with "@@". Resolve to the root module.
	v := e.GetRootModuleValue(&model_analysis_pb.RootModule_Key{})
	var bad TCanonical
	if !v.IsSet() {
		return bad, evaluation.ErrMissingDependency
	}
	rootModule, err := label.NewModule(v.Message.RootModuleName)
	if err != nil {
		return bad, fmt.Errorf("invalid root module name %#v: %w", v.Message.RootModuleName, err)
	}
	return toApparent.WithCanonicalRepo(rootModule.ToModuleInstance(nil).GetBareCanonicalRepo()), nil
}

type loadBzlGlobalsEnvironment[TReference any] interface {
	resolveApparentEnvironment[TReference]
	GetBuiltinsModuleNamesValue(key *model_analysis_pb.BuiltinsModuleNames_Key) model_core.Message[*model_analysis_pb.BuiltinsModuleNames_Value, TReference]
	GetCompiledBzlFileDecodedGlobalsValue(key *model_analysis_pb.CompiledBzlFileDecodedGlobals_Key) (starlark.StringDict, bool)
}

func (c *baseComputer[TReference, TMetadata]) loadBzlGlobals(e loadBzlGlobalsEnvironment[TReference], canonicalPackage label.CanonicalPackage, loadLabelStr string, builtinsModuleNames []string) (starlark.StringDict, error) {
	allBuiltinsModulesNames := e.GetBuiltinsModuleNamesValue(&model_analysis_pb.BuiltinsModuleNames_Key{})
	if !allBuiltinsModulesNames.IsSet() {
		return nil, evaluation.ErrMissingDependency
	}
	apparentLoadLabel, err := canonicalPackage.AppendLabel(loadLabelStr)
	if err != nil {
		return nil, fmt.Errorf("invalid label %#v in load() statement: %w", loadLabelStr, err)
	}
	canonicalRepo := canonicalPackage.GetCanonicalRepo()
	canonicalLoadLabel, err := resolveApparent(e, canonicalRepo, apparentLoadLabel)
	if err != nil {
		return nil, fmt.Errorf("failed to resolve label %#v in load() statement: %w", apparentLoadLabel.String(), err)
	}
	decodedGlobals, ok := e.GetCompiledBzlFileDecodedGlobalsValue(&model_analysis_pb.CompiledBzlFileDecodedGlobals_Key{
		Label:               canonicalLoadLabel.String(),
		BuiltinsModuleNames: builtinsModuleNames,
	})
	if !ok {
		return nil, evaluation.ErrMissingDependency
	}
	return decodedGlobals, nil
}

func (c *baseComputer[TReference, TMetadata]) loadBzlGlobalsInStarlarkThread(e loadBzlGlobalsEnvironment[TReference], thread *starlark.Thread, loadLabelStr string, builtinsModuleNames []string) (starlark.StringDict, error) {
	return c.loadBzlGlobals(e, label.MustNewCanonicalLabel(thread.CallFrame(0).Pos.Filename()).GetCanonicalPackage(), loadLabelStr, builtinsModuleNames)
}

func (c *baseComputer[TReference, TMetadata]) preloadBzlGlobals(e loadBzlGlobalsEnvironment[TReference], canonicalPackage label.CanonicalPackage, program *starlark.Program, builtinsModuleNames []string) (aggregateErr error) {
	numLoads := program.NumLoads()
	for i := 0; i < numLoads; i++ {
		loadLabelStr, _ := program.Load(i)
		if _, err := c.loadBzlGlobals(e, canonicalPackage, loadLabelStr, builtinsModuleNames); err != nil {
			if !errors.Is(err, evaluation.ErrMissingDependency) {
				return err
			}
			aggregateErr = err
		}
	}
	return
}

type starlarkThreadEnvironment[TReference any] interface {
	loadBzlGlobalsEnvironment[TReference]
	GetCompiledBzlFileFunctionFactoryValue(*model_analysis_pb.CompiledBzlFileFunctionFactory_Key) (*starlark.FunctionFactory, bool)
}

// trimBuiltinModuleNames truncates the list of built-in module names up
// to a provided module name. This needs to be called when attempting to
// load() files belonging to a built-in module, so that evaluating code
// belonging to the built-in module does not result into cycles.
func trimBuiltinModuleNames(builtinsModuleNames []string, module label.Module) []string {
	moduleStr := module.String()
	i := 0
	for i < len(builtinsModuleNames) && builtinsModuleNames[i] != moduleStr {
		i++
	}
	return builtinsModuleNames[:i]
}

func (c *baseComputer[TReference, TMetadata]) newStarlarkThread(ctx context.Context, e starlarkThreadEnvironment[TReference], builtinsModuleNames []string) *starlark.Thread {
	thread := &starlark.Thread{
		// TODO: Provide print method.
		Print: nil,
		Load: func(thread *starlark.Thread, loadLabelStr string) (starlark.StringDict, error) {
			return c.loadBzlGlobalsInStarlarkThread(e, thread, loadLabelStr, builtinsModuleNames)
		},
		Steps: 1000,
	}

	thread.SetLocal(model_starlark.CanonicalRepoResolverKey, func(fromCanonicalRepo label.CanonicalRepo, toApparentRepo label.ApparentRepo) (*label.CanonicalRepo, error) {
		v := e.GetCanonicalRepoNameValue(&model_analysis_pb.CanonicalRepoName_Key{
			FromCanonicalRepo: fromCanonicalRepo.String(),
			ToApparentRepo:    toApparentRepo.String(),
		})
		if !v.IsSet() {
			return nil, evaluation.ErrMissingDependency
		}
		if v.Message.ToCanonicalRepo == "" {
			return nil, nil
		}
		canonicalRepo, err := label.NewCanonicalRepo(v.Message.ToCanonicalRepo)
		if err != nil {
			return nil, err
		}
		return &canonicalRepo, nil
	})

	thread.SetLocal(model_starlark.RootModuleResolverKey, func() (label.Module, error) {
		v := e.GetRootModuleValue(&model_analysis_pb.RootModule_Key{})
		var badModule label.Module
		if !v.IsSet() {
			return badModule, evaluation.ErrMissingDependency
		}
		return label.NewModule(v.Message.RootModuleName)
	})
	thread.SetLocal(model_starlark.FunctionFactoryResolverKey, func(filename label.CanonicalLabel) (*starlark.FunctionFactory, error) {
		// Prevent modules containing builtin Starlark code from
		// depending on itself.
		functionFactory, gotFunctionFactory := e.GetCompiledBzlFileFunctionFactoryValue(&model_analysis_pb.CompiledBzlFileFunctionFactory_Key{
			Label:               filename.String(),
			BuiltinsModuleNames: trimBuiltinModuleNames(builtinsModuleNames, filename.GetCanonicalRepo().GetModuleInstance().GetModule()),
		})
		if !gotFunctionFactory {
			return nil, evaluation.ErrMissingDependency
		}
		return functionFactory, nil
	})
	thread.SetLocal(
		model_starlark.ValueDecodingOptionsKey,
		c.getValueDecodingOptions(ctx, func(resolvedLabel label.ResolvedLabel) (starlark.Value, error) {
			return model_starlark.NewLabel[TReference, TMetadata](resolvedLabel), nil
		}),
	)
	return thread
}

func (c *baseComputer[TReference, TMetadata]) ComputeBuildResultValue(ctx context.Context, key *model_analysis_pb.BuildResult_Key, e BuildResultEnvironment[TReference]) (PatchedBuildResultValue, error) {
	buildSpecification := e.GetBuildSpecificationValue(&model_analysis_pb.BuildSpecification_Key{})
	if !buildSpecification.IsSet() {
		return PatchedBuildResultValue{}, evaluation.ErrMissingDependency
	}
	rootModuleName := buildSpecification.Message.BuildSpecification.RootModuleName
	rootModule, err := label.NewModule(rootModuleName)
	if err != nil {
		return PatchedBuildResultValue{}, fmt.Errorf("invalid root module name %#v: %w", rootModuleName, err)
	}
	rootRepo := rootModule.ToModuleInstance(nil).GetBareCanonicalRepo()
	rootPackage := rootRepo.GetRootPackage()

	// TODO: Obtain platform constraints from --platforms.
	missingDependencies := false
	for _, targetPattern := range buildSpecification.Message.BuildSpecification.TargetPatterns {
		apparentTargetPattern, err := rootPackage.AppendTargetPattern(targetPattern)
		if err != nil {
			return PatchedBuildResultValue{}, fmt.Errorf("invalid target pattern %#v: %w", targetPattern, err)
		}
		canonicalTargetPattern, err := resolveApparent(e, rootRepo, apparentTargetPattern)
		if err != nil {
			return PatchedBuildResultValue{}, err
		}

		var iterErr error
		for canonicalTargetLabel := range c.expandCanonicalTargetPattern(ctx, e, canonicalTargetPattern, &iterErr) {
			visibleTargetPatcher := model_core.NewReferenceMessagePatcher[dag.ObjectContentsWalker]()
			visibleTargetValue := e.GetVisibleTargetValue(
				model_core.PatchedMessage[*model_analysis_pb.VisibleTarget_Key, dag.ObjectContentsWalker]{
					Message: &model_analysis_pb.VisibleTarget_Key{
						FromPackage: canonicalTargetLabel.GetCanonicalPackage().String(),
						ToLabel:     canonicalTargetLabel.String(),
					},
					Patcher: visibleTargetPatcher,
				},
			)
			if !visibleTargetValue.IsSet() {
				missingDependencies = true
				continue
			}

			targetCompletionPatcher := model_core.NewReferenceMessagePatcher[dag.ObjectContentsWalker]()
			targetCompletionValue := e.GetTargetCompletionValue(
				model_core.PatchedMessage[*model_analysis_pb.TargetCompletion_Key, dag.ObjectContentsWalker]{
					Message: &model_analysis_pb.TargetCompletion_Key{
						Label: visibleTargetValue.Message.Label,
					},
					Patcher: targetCompletionPatcher,
				},
			)
			if !targetCompletionValue.IsSet() {
				missingDependencies = true
			}
		}
		if iterErr != nil {
			if !errors.Is(iterErr, evaluation.ErrMissingDependency) {
				return PatchedBuildResultValue{}, fmt.Errorf("failed to iterate target pattern %#v: %w", targetPattern, iterErr)
			}
			missingDependencies = true
		}
	}
	if missingDependencies {
		return PatchedBuildResultValue{}, evaluation.ErrMissingDependency
	}
	return model_core.NewSimplePatchedMessage[dag.ObjectContentsWalker](&model_analysis_pb.BuildResult_Value{}), nil
}

func (c *baseComputer[TReference, TMetadata]) ComputeBuildSpecificationValue(ctx context.Context, key *model_analysis_pb.BuildSpecification_Key, e BuildSpecificationEnvironment[TReference]) (PatchedBuildSpecificationValue, error) {
	buildSpecification, err := c.buildSpecificationReader.ReadParsedObject(ctx, c.buildSpecificationReference)
	if err != nil {
		return PatchedBuildSpecificationValue{}, err
	}

	patchedBuildSpecification := model_core.NewPatchedMessageFromExisting(
		buildSpecification,
		func(index int) dag.ObjectContentsWalker {
			return dag.ExistingObjectContentsWalker
		},
	)
	return PatchedBuildSpecificationValue{
		Message: &model_analysis_pb.BuildSpecification_Value{
			BuildSpecification: patchedBuildSpecification.Message,
		},
		Patcher: patchedBuildSpecification.Patcher,
	}, nil
}

func (c *baseComputer[TReference, TMetadata]) ComputeBuiltinsModuleNamesValue(ctx context.Context, key *model_analysis_pb.BuiltinsModuleNames_Key, e BuiltinsModuleNamesEnvironment[TReference]) (PatchedBuiltinsModuleNamesValue, error) {
	buildSpecification := e.GetBuildSpecificationValue(&model_analysis_pb.BuildSpecification_Key{})
	if !buildSpecification.IsSet() {
		return PatchedBuiltinsModuleNamesValue{}, evaluation.ErrMissingDependency
	}
	return model_core.NewSimplePatchedMessage[dag.ObjectContentsWalker](&model_analysis_pb.BuiltinsModuleNames_Value{
		BuiltinsModuleNames: buildSpecification.Message.BuildSpecification.GetBuiltinsModuleNames(),
	}), nil
}

func (c *baseComputer[TReference, TMetadata]) ComputeDirectoryAccessParametersValue(ctx context.Context, key *model_analysis_pb.DirectoryAccessParameters_Key, e DirectoryAccessParametersEnvironment[TReference]) (PatchedDirectoryAccessParametersValue, error) {
	buildSpecification := e.GetBuildSpecificationValue(&model_analysis_pb.BuildSpecification_Key{})
	if !buildSpecification.IsSet() {
		return PatchedDirectoryAccessParametersValue{}, evaluation.ErrMissingDependency
	}
	return model_core.NewSimplePatchedMessage[dag.ObjectContentsWalker](&model_analysis_pb.DirectoryAccessParameters_Value{
		DirectoryAccessParameters: buildSpecification.Message.BuildSpecification.GetDirectoryCreationParameters().GetAccess(),
	}), nil
}

func (c *baseComputer[TReference, TMetadata]) ComputeRepoDefaultAttrsValue(ctx context.Context, key *model_analysis_pb.RepoDefaultAttrs_Key, e RepoDefaultAttrsEnvironment[TReference]) (PatchedRepoDefaultAttrsValue, error) {
	canonicalRepo, err := label.NewCanonicalRepo(key.CanonicalRepo)
	if err != nil {
		return PatchedRepoDefaultAttrsValue{}, fmt.Errorf("invalid canonical repo: %w", err)
	}

	repoFileName := label.MustNewTargetName("REPO.bazel")
	repoFileProperties := e.GetFilePropertiesValue(&model_analysis_pb.FileProperties_Key{
		CanonicalRepo: canonicalRepo.String(),
		Path:          repoFileName.String(),
	})

	fileReader, gotFileReader := e.GetFileReaderValue(&model_analysis_pb.FileReader_Key{})
	if !repoFileProperties.IsSet() || !gotFileReader {
		return PatchedRepoDefaultAttrsValue{}, evaluation.ErrMissingDependency
	}

	// Read the contents of REPO.bazel.
	repoFileLabel := canonicalRepo.GetRootPackage().AppendTargetName(repoFileName)
	if repoFileProperties.Message.Exists == nil {
		return model_core.NewSimplePatchedMessage[dag.ObjectContentsWalker](&model_analysis_pb.RepoDefaultAttrs_Value{
			InheritableAttrs: &model_starlark.DefaultInheritableAttrs,
		}), nil
	}
	repoFileContentsEntry, err := model_filesystem.NewFileContentsEntryFromProto(
		model_core.NewNestedMessage(repoFileProperties, repoFileProperties.Message.Exists.GetContents()),
	)
	if err != nil {
		return PatchedRepoDefaultAttrsValue{}, fmt.Errorf("invalid contents for file %#v: %w", repoFileLabel.String(), err)
	}
	repoFileData, err := fileReader.FileReadAll(ctx, repoFileContentsEntry, 1<<20)
	if err != nil {
		return PatchedRepoDefaultAttrsValue{}, err
	}

	// Extract the default inheritable attrs from REPO.bazel.
	defaultAttrs, err := model_starlark.ParseRepoDotBazel[TReference](
		string(repoFileData),
		canonicalRepo.GetRootPackage().AppendTargetName(repoFileName),
		c.getInlinedTreeOptions(),
		c.objectCapturer,
	)
	if err != nil {
		return PatchedRepoDefaultAttrsValue{}, fmt.Errorf("failed to parse %#v: %w", repoFileLabel.String(), err)
	}

	return model_core.NewPatchedMessage(
		&model_analysis_pb.RepoDefaultAttrs_Value{
			InheritableAttrs: defaultAttrs.Message,
		},
		model_core.MapReferenceMetadataToWalkers(defaultAttrs.Patcher),
	), nil
}

func (c *baseComputer[TReference, TMetadata]) ComputeTargetCompletionValue(ctx context.Context, key model_core.Message[*model_analysis_pb.TargetCompletion_Key, TReference], e TargetCompletionEnvironment[TReference]) (PatchedTargetCompletionValue, error) {
	configurationReference := model_core.NewPatchedMessageFromExisting(
		model_core.NewNestedMessage(key, key.Message.ConfigurationReference),
		func(index int) dag.ObjectContentsWalker {
			return dag.ExistingObjectContentsWalker
		},
	)
	configuredTarget := e.GetConfiguredTargetValue(
		model_core.PatchedMessage[*model_analysis_pb.ConfiguredTarget_Key, dag.ObjectContentsWalker]{
			Message: &model_analysis_pb.ConfiguredTarget_Key{
				Label:                  key.Message.Label,
				ConfigurationReference: configurationReference.Message,
			},
			Patcher: configurationReference.Patcher,
		},
	)
	if !configuredTarget.IsSet() {
		return PatchedTargetCompletionValue{}, evaluation.ErrMissingDependency
	}
	return model_core.NewSimplePatchedMessage[dag.ObjectContentsWalker](&model_analysis_pb.TargetCompletion_Value{}), nil
}
