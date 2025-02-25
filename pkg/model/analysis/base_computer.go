package analysis

import (
	"context"
	"crypto/ecdh"
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
	model_core_pb "github.com/buildbarn/bonanza/pkg/proto/model/core"
	model_filesystem_pb "github.com/buildbarn/bonanza/pkg/proto/model/filesystem"
	remoteexecution_pb "github.com/buildbarn/bonanza/pkg/proto/remoteexecution"
	"github.com/buildbarn/bonanza/pkg/storage/dag"
	"github.com/buildbarn/bonanza/pkg/storage/object"

	"go.starlark.net/starlark"
)

type baseComputer struct {
	objectDownloader                object.Downloader[object.LocalReference]
	buildSpecificationReference     object.GlobalReference
	buildSpecificationEncoder       model_encoding.BinaryEncoder
	httpClient                      *http.Client
	filePool                        re_filesystem.FilePool
	cacheDirectory                  filesystem.Directory
	executionClient                 remoteexecution_pb.ExecutionClient
	executionClientPrivateKey       *ecdh.PrivateKey
	executionClientCertificateChain [][]byte
}

func NewBaseComputer(
	objectDownloader object.Downloader[object.LocalReference],
	buildSpecificationReference object.GlobalReference,
	buildSpecificationEncoder model_encoding.BinaryEncoder,
	httpClient *http.Client,
	filePool re_filesystem.FilePool,
	cacheDirectory filesystem.Directory,
	executionClient remoteexecution_pb.ExecutionClient,
	executionClientPrivateKey *ecdh.PrivateKey,
	executionClientCertificateChain [][]byte,
) Computer {
	return &baseComputer{
		objectDownloader:                objectDownloader,
		buildSpecificationReference:     buildSpecificationReference,
		buildSpecificationEncoder:       buildSpecificationEncoder,
		httpClient:                      httpClient,
		filePool:                        filePool,
		cacheDirectory:                  cacheDirectory,
		executionClient:                 executionClient,
		executionClientPrivateKey:       executionClientPrivateKey,
		executionClientCertificateChain: executionClientCertificateChain,
	}
}

func (c *baseComputer) getValueObjectEncoder() model_encoding.BinaryEncoder {
	// TODO: Use a proper encoder!
	return model_encoding.NewChainedBinaryEncoder(nil)
}

func (c *baseComputer) getValueEncodingOptions(currentFilename label.CanonicalLabel) *model_starlark.ValueEncodingOptions {
	return &model_starlark.ValueEncodingOptions{
		CurrentFilename:        currentFilename,
		ObjectEncoder:          c.getValueObjectEncoder(),
		ObjectReferenceFormat:  c.buildSpecificationReference.GetReferenceFormat(),
		ObjectMinimumSizeBytes: 32 * 1024,
		ObjectMaximumSizeBytes: 128 * 1024,
	}
}

func (c *baseComputer) getValueDecodingOptions(ctx context.Context, labelCreator func(label.ResolvedLabel) (starlark.Value, error)) *model_starlark.ValueDecodingOptions {
	return &model_starlark.ValueDecodingOptions{
		Context:          ctx,
		ObjectDownloader: c.objectDownloader,
		ObjectEncoder:    c.getValueObjectEncoder(),
		LabelCreator:     labelCreator,
	}
}

func (c *baseComputer) getInlinedTreeOptions() *inlinedtree.Options {
	return &inlinedtree.Options{
		ReferenceFormat:  c.buildSpecificationReference.GetReferenceFormat(),
		Encoder:          model_encoding.NewChainedBinaryEncoder(nil),
		MaximumSizeBytes: 32 * 1024,
	}
}

type resolveApparentEnvironment interface {
	GetCanonicalRepoNameValue(*model_analysis_pb.CanonicalRepoName_Key) model_core.Message[*model_analysis_pb.CanonicalRepoName_Value]
	GetRootModuleValue(*model_analysis_pb.RootModule_Key) model_core.Message[*model_analysis_pb.RootModule_Value]
}

type canonicalizable[T any] interface {
	AsCanonical() (T, bool)
	GetApparentRepo() (label.ApparentRepo, bool)
	WithCanonicalRepo(canonicalRepo label.CanonicalRepo) T
}

func resolveApparent[TCanonical any, TApparent canonicalizable[TCanonical]](e resolveApparentEnvironment, fromRepo label.CanonicalRepo, toApparent TApparent) (TCanonical, error) {
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

type loadBzlGlobalsEnvironment interface {
	resolveApparentEnvironment
	GetBuiltinsModuleNamesValue(key *model_analysis_pb.BuiltinsModuleNames_Key) model_core.Message[*model_analysis_pb.BuiltinsModuleNames_Value]
	GetCompiledBzlFileDecodedGlobalsValue(key *model_analysis_pb.CompiledBzlFileDecodedGlobals_Key) (starlark.StringDict, bool)
}

func (c *baseComputer) loadBzlGlobals(e loadBzlGlobalsEnvironment, canonicalPackage label.CanonicalPackage, loadLabelStr string, builtinsModuleNames []string) (starlark.StringDict, error) {
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

func (c *baseComputer) loadBzlGlobalsInStarlarkThread(e loadBzlGlobalsEnvironment, thread *starlark.Thread, loadLabelStr string, builtinsModuleNames []string) (starlark.StringDict, error) {
	return c.loadBzlGlobals(e, label.MustNewCanonicalLabel(thread.CallFrame(0).Pos.Filename()).GetCanonicalPackage(), loadLabelStr, builtinsModuleNames)
}

func (c *baseComputer) preloadBzlGlobals(e loadBzlGlobalsEnvironment, canonicalPackage label.CanonicalPackage, program *starlark.Program, builtinsModuleNames []string) (aggregateErr error) {
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

type starlarkThreadEnvironment interface {
	loadBzlGlobalsEnvironment
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

func (c *baseComputer) newStarlarkThread(ctx context.Context, e starlarkThreadEnvironment, builtinsModuleNames []string) *starlark.Thread {
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
			return model_starlark.NewLabel(resolvedLabel), nil
		}),
	)
	return thread
}

func (c *baseComputer) ComputeBuildResultValue(ctx context.Context, key *model_analysis_pb.BuildResult_Key, e BuildResultEnvironment) (PatchedBuildResultValue, error) {
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

func (c *baseComputer) ComputeBuildSpecificationValue(ctx context.Context, key *model_analysis_pb.BuildSpecification_Key, e BuildSpecificationEnvironment) (PatchedBuildSpecificationValue, error) {
	reader := model_parser.NewStorageBackedParsedObjectReader(
		c.objectDownloader,
		c.buildSpecificationEncoder,
		model_parser.NewMessageObjectParser[object.LocalReference, model_build_pb.BuildSpecification](),
	)
	buildSpecification, _, err := reader.ReadParsedObject(ctx, c.buildSpecificationReference.LocalReference)
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

func (c *baseComputer) ComputeBuiltinsModuleNamesValue(ctx context.Context, key *model_analysis_pb.BuiltinsModuleNames_Key, e BuiltinsModuleNamesEnvironment) (PatchedBuiltinsModuleNamesValue, error) {
	buildSpecification := e.GetBuildSpecificationValue(&model_analysis_pb.BuildSpecification_Key{})
	if !buildSpecification.IsSet() {
		return PatchedBuiltinsModuleNamesValue{}, evaluation.ErrMissingDependency
	}
	return model_core.NewSimplePatchedMessage[dag.ObjectContentsWalker](&model_analysis_pb.BuiltinsModuleNames_Value{
		BuiltinsModuleNames: buildSpecification.Message.BuildSpecification.GetBuiltinsModuleNames(),
	}), nil
}

func (c *baseComputer) ComputeDirectoryAccessParametersValue(ctx context.Context, key *model_analysis_pb.DirectoryAccessParameters_Key, e DirectoryAccessParametersEnvironment) (PatchedDirectoryAccessParametersValue, error) {
	buildSpecification := e.GetBuildSpecificationValue(&model_analysis_pb.BuildSpecification_Key{})
	if !buildSpecification.IsSet() {
		return PatchedDirectoryAccessParametersValue{}, evaluation.ErrMissingDependency
	}
	return model_core.NewSimplePatchedMessage[dag.ObjectContentsWalker](&model_analysis_pb.DirectoryAccessParameters_Value{
		DirectoryAccessParameters: buildSpecification.Message.BuildSpecification.GetDirectoryCreationParameters().GetAccess(),
	}), nil
}

func (c *baseComputer) ComputeFileReaderValue(ctx context.Context, key *model_analysis_pb.FileReader_Key, e FileReaderEnvironment) (*model_filesystem.FileReader, error) {
	fileAccessParametersValue := e.GetFileAccessParametersValue(&model_analysis_pb.FileAccessParameters_Key{})
	if !fileAccessParametersValue.IsSet() {
		return nil, evaluation.ErrMissingDependency
	}
	fileAccessParameters, err := model_filesystem.NewFileAccessParametersFromProto(
		fileAccessParametersValue.Message.FileAccessParameters,
		c.buildSpecificationReference.GetReferenceFormat(),
	)
	if err != nil {
		return nil, fmt.Errorf("invalid directory access parameters: %w", err)
	}
	fileContentsListReader := model_parser.NewStorageBackedParsedObjectReader(
		c.objectDownloader,
		fileAccessParameters.GetFileContentsListEncoder(),
		model_filesystem.NewFileContentsListObjectParser[object.LocalReference](),
	)
	fileChunkReader := model_parser.NewStorageBackedParsedObjectReader(
		c.objectDownloader,
		fileAccessParameters.GetChunkEncoder(),
		model_parser.NewRawObjectParser[object.LocalReference](),
	)
	return model_filesystem.NewFileReader(fileContentsListReader, fileChunkReader), nil
}

func (c *baseComputer) ComputeRepoDefaultAttrsValue(ctx context.Context, key *model_analysis_pb.RepoDefaultAttrs_Key, e RepoDefaultAttrsEnvironment) (PatchedRepoDefaultAttrsValue, error) {
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
	referenceFormat := c.buildSpecificationReference.GetReferenceFormat()
	repoFileContentsEntry, err := model_filesystem.NewFileContentsEntryFromProto(
		model_core.Message[*model_filesystem_pb.FileContents]{
			Message:            repoFileProperties.Message.Exists.GetContents(),
			OutgoingReferences: repoFileProperties.OutgoingReferences,
		},
		referenceFormat,
	)
	if err != nil {
		return PatchedRepoDefaultAttrsValue{}, fmt.Errorf("invalid contents for file %#v: %w", repoFileLabel.String(), err)
	}
	repoFileData, err := fileReader.FileReadAll(ctx, repoFileContentsEntry, 1<<20)
	if err != nil {
		return PatchedRepoDefaultAttrsValue{}, err
	}

	// Extract the default inheritable attrs from REPO.bazel.
	defaultAttrs, err := model_starlark.ParseRepoDotBazel(
		string(repoFileData),
		canonicalRepo.GetRootPackage().AppendTargetName(repoFileName),
		c.getInlinedTreeOptions(),
	)
	if err != nil {
		return PatchedRepoDefaultAttrsValue{}, fmt.Errorf("failed to parse %#v: %w", repoFileLabel.String(), err)
	}

	return model_core.NewPatchedMessage(
		&model_analysis_pb.RepoDefaultAttrs_Value{
			InheritableAttrs: defaultAttrs.Message,
		},
		defaultAttrs.Patcher,
	), nil
}

func (c *baseComputer) ComputeTargetCompletionValue(ctx context.Context, key model_core.Message[*model_analysis_pb.TargetCompletion_Key], e TargetCompletionEnvironment) (PatchedTargetCompletionValue, error) {
	configurationReference := model_core.NewPatchedMessageFromExisting(
		model_core.Message[*model_core_pb.Reference]{
			Message:            key.Message.ConfigurationReference,
			OutgoingReferences: key.OutgoingReferences,
		},
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
