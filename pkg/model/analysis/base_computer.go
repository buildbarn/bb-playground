package analysis

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"net/http"
	"path"
	"slices"
	"sort"

	"github.com/buildbarn/bb-playground/pkg/evaluation"
	"github.com/buildbarn/bb-playground/pkg/label"
	model_core "github.com/buildbarn/bb-playground/pkg/model/core"
	"github.com/buildbarn/bb-playground/pkg/model/core/btree"
	"github.com/buildbarn/bb-playground/pkg/model/core/inlinedtree"
	model_encoding "github.com/buildbarn/bb-playground/pkg/model/encoding"
	model_filesystem "github.com/buildbarn/bb-playground/pkg/model/filesystem"
	model_parser "github.com/buildbarn/bb-playground/pkg/model/parser"
	model_starlark "github.com/buildbarn/bb-playground/pkg/model/starlark"
	model_analysis_pb "github.com/buildbarn/bb-playground/pkg/proto/model/analysis"
	model_build_pb "github.com/buildbarn/bb-playground/pkg/proto/model/build"
	model_filesystem_pb "github.com/buildbarn/bb-playground/pkg/proto/model/filesystem"
	model_starlark_pb "github.com/buildbarn/bb-playground/pkg/proto/model/starlark"
	"github.com/buildbarn/bb-playground/pkg/storage/dag"
	"github.com/buildbarn/bb-playground/pkg/storage/object"
	re_filesystem "github.com/buildbarn/bb-remote-execution/pkg/filesystem"
	bb_path "github.com/buildbarn/bb-storage/pkg/filesystem/path"
	"github.com/buildbarn/bb-storage/pkg/util"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"

	"go.starlark.net/starlark"
	"go.starlark.net/syntax"
)

type baseComputer struct {
	objectDownloader            object.Downloader[object.LocalReference]
	buildSpecificationReference object.LocalReference
	buildSpecificationEncoder   model_encoding.BinaryEncoder
	httpClient                  *http.Client
	filePool                    re_filesystem.FilePool
}

func NewBaseComputer(
	objectDownloader object.Downloader[object.LocalReference],
	buildSpecificationReference object.LocalReference,
	buildSpecificationEncoder model_encoding.BinaryEncoder,
	httpClient *http.Client,
	filePool re_filesystem.FilePool,
) Computer {
	return &baseComputer{
		objectDownloader:            objectDownloader,
		buildSpecificationReference: buildSpecificationReference,
		buildSpecificationEncoder:   buildSpecificationEncoder,
		httpClient:                  httpClient,
		filePool:                    filePool,
	}
}

func (c *baseComputer) getValueEncodingOptions(currentFilename label.CanonicalLabel) *model_starlark.ValueEncodingOptions {
	return &model_starlark.ValueEncodingOptions{
		CurrentFilename:        currentFilename,
		ObjectEncoder:          model_encoding.NewChainedBinaryEncoder(nil),
		ObjectReferenceFormat:  c.buildSpecificationReference.GetReferenceFormat(),
		ObjectMinimumSizeBytes: 32 * 1024,
		ObjectMaximumSizeBytes: 128 * 1024,
	}
}

func (c *baseComputer) getInlinedTreeOptions() *inlinedtree.Options {
	return &inlinedtree.Options{
		ReferenceFormat:  c.buildSpecificationReference.GetReferenceFormat(),
		Encoder:          model_encoding.NewChainedBinaryEncoder(nil),
		MaximumSizeBytes: 32 * 1024,
	}
}

type resolveApparentLabelEnvironment interface {
	GetCanonicalRepoNameValue(*model_analysis_pb.CanonicalRepoName_Key) model_core.Message[*model_analysis_pb.CanonicalRepoName_Value]
}

func resolveApparentLabel(e resolveApparentLabelEnvironment, fromRepo label.CanonicalRepo, toApparentLabel label.ApparentLabel) (label.CanonicalLabel, error) {
	toCanonicalLabel, ok := toApparentLabel.AsCanonicalLabel()
	if ok {
		// Label was already canonical. Nothing to do.
		return toCanonicalLabel, nil
	}

	// Perform resolution.
	toApparentRepo, ok := toApparentLabel.GetApparentRepo()
	if !ok {
		panic("if AsCanonicalLabel() fails, GetApparentRepo() must succeed")
	}
	v := e.GetCanonicalRepoNameValue(&model_analysis_pb.CanonicalRepoName_Key{
		FromCanonicalRepo: fromRepo.String(),
		ToApparentRepo:    toApparentRepo.String(),
	})
	var badLabel label.CanonicalLabel
	if !v.IsSet() {
		return badLabel, evaluation.ErrMissingDependency
	}
	switch resultType := v.Message.Result.(type) {
	case *model_analysis_pb.CanonicalRepoName_Value_ToCanonicalRepo:
		toCanonicalRepo, err := label.NewCanonicalRepo(resultType.ToCanonicalRepo)
		if err != nil {
			return badLabel, fmt.Errorf("invalid canonical repo name %#v: %w", resultType.ToCanonicalRepo, err)
		}
		toCanonicalLabel := toApparentLabel.WithCanonicalRepo(toCanonicalRepo)
		return toCanonicalLabel, nil
	case *model_analysis_pb.CanonicalRepoName_Value_Failure:
		return badLabel, errors.New(resultType.Failure)
	default:
		return badLabel, errors.New("invalid result type for canonical repo name resolution")
	}
}

type loadBzlGlobalsEnvironment interface {
	resolveApparentLabelEnvironment
	GetBuiltinsModuleNamesValue(key *model_analysis_pb.BuiltinsModuleNames_Key) model_core.Message[*model_analysis_pb.BuiltinsModuleNames_Value]
	GetCompiledBzlFileDecodedGlobalsValue(key *model_analysis_pb.CompiledBzlFileDecodedGlobals_Key) (starlark.StringDict, error)
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
	canonicalLoadLabel, err := resolveApparentLabel(e, canonicalRepo, apparentLoadLabel)
	if err != nil {
		return nil, fmt.Errorf("failed to resolve label %#v in load() statement: %w", apparentLoadLabel.String(), err)
	}
	return e.GetCompiledBzlFileDecodedGlobalsValue(&model_analysis_pb.CompiledBzlFileDecodedGlobals_Key{
		Label:               canonicalLoadLabel.String(),
		BuiltinsModuleNames: builtinsModuleNames,
	})
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

type getBzlFileBuiltinsEnvironment interface {
	GetCompiledBzlFileDecodedGlobalsValue(key *model_analysis_pb.CompiledBzlFileDecodedGlobals_Key) (starlark.StringDict, error)
}

func (c *baseComputer) getBzlFileBuiltins(e getBzlFileBuiltinsEnvironment, builtinsModuleNames []string, baseBuiltins starlark.StringDict, dictName string) (starlark.StringDict, error) {
	allBuiltins := starlark.StringDict{}
	for name, value := range baseBuiltins {
		allBuiltins[name] = value
	}
	for i, builtinsModuleName := range builtinsModuleNames {
		exportsFile := fmt.Sprintf("@@%s+//:exports.bzl", builtinsModuleName)
		globals, err := e.GetCompiledBzlFileDecodedGlobalsValue(&model_analysis_pb.CompiledBzlFileDecodedGlobals_Key{
			Label:               exportsFile,
			BuiltinsModuleNames: builtinsModuleNames[:i],
		})
		if err != nil {
			return nil, err
		}
		exportedToplevels, ok := globals[dictName].(starlark.IterableMapping)
		if !ok {
			return nil, fmt.Errorf("file %#v does not declare exported_toplevels", exportsFile)
		}
		for name, value := range starlark.Entries(exportedToplevels) {
			nameStr, ok := starlark.AsString(name)
			if !ok {
				return nil, fmt.Errorf("file %#v exports builtins with non-string names", exportsFile)
			}
			allBuiltins[nameStr] = value
		}
	}
	return allBuiltins, nil
}

type starlarkThreadEnvironment interface {
	loadBzlGlobalsEnvironment
	GetCompiledBzlFileFunctionsValue(*model_analysis_pb.CompiledBzlFileFunctions_Key) (map[label.StarlarkIdentifier]model_starlark.CallableWithPosition, error)
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

func (c *baseComputer) newStarlarkThread(e starlarkThreadEnvironment, builtinsModuleNames []string) *starlark.Thread {
	thread := &starlark.Thread{
		// TODO: Provide print method.
		Print: nil,
		Load: func(thread *starlark.Thread, loadLabelStr string) (starlark.StringDict, error) {
			return c.loadBzlGlobalsInStarlarkThread(e, thread, loadLabelStr, builtinsModuleNames)
		},
		Steps: 1000,
	}
	thread.SetLocal(model_starlark.CanonicalRepoResolverKey, func(fromCanonicalRepo label.CanonicalRepo, toApparentRepo label.ApparentRepo) (label.CanonicalRepo, error) {
		v := e.GetCanonicalRepoNameValue(&model_analysis_pb.CanonicalRepoName_Key{
			FromCanonicalRepo: fromCanonicalRepo.String(),
			ToApparentRepo:    toApparentRepo.String(),
		})
		var badRepo label.CanonicalRepo
		if !v.IsSet() {
			return badRepo, evaluation.ErrMissingDependency
		}

		switch resultType := v.Message.Result.(type) {
		case *model_analysis_pb.CanonicalRepoName_Value_ToCanonicalRepo:
			return label.NewCanonicalRepo(resultType.ToCanonicalRepo)
		case *model_analysis_pb.CanonicalRepoName_Value_Failure:
			return badRepo, errors.New(resultType.Failure)
		default:
			return badRepo, errors.New("invalid result type for canonical repo name resolution")
		}
	})
	thread.SetLocal(model_starlark.FunctionResolverKey, func(identifier label.CanonicalStarlarkIdentifier) (model_starlark.CallableWithPosition, error) {
		// Prevent modules containing builtin Starlark code from
		// depending on itself.
		identifierLabel := identifier.GetCanonicalLabel()
		callables, err := e.GetCompiledBzlFileFunctionsValue(&model_analysis_pb.CompiledBzlFileFunctions_Key{
			Label:               identifierLabel.String(),
			BuiltinsModuleNames: trimBuiltinModuleNames(builtinsModuleNames, identifierLabel.GetCanonicalRepo().GetModuleInstance().GetModule()),
		})
		if err != nil {
			return nil, err
		}
		callable, ok := callables[identifier.GetStarlarkIdentifier()]
		if !ok {
			return nil, fmt.Errorf("function %#v does not exist", identifier.String())
		}
		return callable, nil
	})
	return thread
}

func (c *baseComputer) ComputeBuildResultValue(ctx context.Context, key *model_analysis_pb.BuildResult_Key, e BuildResultEnvironment) (PatchedBuildResultValue, error) {
	// TODO: Do something proper here.
	missing := false
	for _, pkg := range []string{
		"//cmd/bb_copy",
		"//cmd/bb_replicator",
		"//cmd/bb_storage",
		"//internal/mock",
		"//internal/mock/aliases",
		"//pkg/auth",
		"//pkg/blobstore",
		"//pkg/blobstore/buffer",
		"//pkg/blobstore/completenesschecking",
		"//pkg/blobstore/configuration",
		"//pkg/blobstore/grpcclients",
		"//pkg/blobstore/grpcservers",
		"//pkg/blobstore/local",
		"//pkg/blobstore/mirrored",
		"//pkg/blobstore/readcaching",
		"//pkg/blobstore/readfallback",
		"//pkg/blobstore/replication",
		"//pkg/blobstore/sharding",
		"//pkg/blobstore/slicing",
		"//pkg/blockdevice",
		"//pkg/builder",
		"//pkg/capabilities",
		"//pkg/clock",
		"//pkg/cloud/aws",
		"//pkg/cloud/gcp",
		"//pkg/digest",
		"//pkg/digest/sha256tree",
		"//pkg/eviction",
		"//pkg/filesystem",
		"//pkg/filesystem/path",
		"//pkg/filesystem/windowsext",
		"//pkg/global",
		"//pkg/grpc",
		"//pkg/http",
		"//pkg/jwt",
		"//pkg/otel",
		"//pkg/program",
		"//pkg/prometheus",
		"//pkg/proto/auth",
		"//pkg/proto/blobstore/local",
		"//pkg/proto/configuration/auth",
		"//pkg/proto/configuration/bb_copy",
		"//pkg/proto/configuration/bb_replicator",
		"//pkg/proto/configuration/bb_storage",
		"//pkg/proto/configuration/blobstore",
		"//pkg/proto/configuration/blockdevice",
		"//pkg/proto/configuration/builder",
		"//pkg/proto/configuration/cloud/aws",
		"//pkg/proto/configuration/cloud/gcp",
		"//pkg/proto/configuration/digest",
		"//pkg/proto/configuration/eviction",
		"//pkg/proto/configuration/global",
		"//pkg/proto/configuration/grpc",
		"//pkg/proto/configuration/http",
		"//pkg/proto/configuration/jwt",
		"//pkg/proto/configuration/tls",
		"//pkg/proto/fsac",
		"//pkg/proto/http/oidc",
		"//pkg/proto/icas",
		"//pkg/proto/iscc",
		"//pkg/proto/replicator",
		"//pkg/random",
		"//pkg/testutil",
		"//pkg/util",
	} {
		targetCompletion := e.GetTargetCompletionValue(&model_analysis_pb.TargetCompletion_Key{
			Label: "@@com_github_buildbarn_bb_storage+" + pkg,
		})
		if targetCompletion.IsSet() {
			switch resultType := targetCompletion.Message.Result.(type) {
			case *model_analysis_pb.TargetCompletion_Value_Success:
			case *model_analysis_pb.TargetCompletion_Value_Failure:
				return model_core.NewSimplePatchedMessage[dag.ObjectContentsWalker](&model_analysis_pb.BuildResult_Value{
					Result: &model_analysis_pb.BuildResult_Value_Failure{
						Failure: fmt.Sprintf("Target %#v: %s", pkg, resultType.Failure),
					},
				}), nil
			default:
				return PatchedBuildResultValue{}, errors.New("Target completion value has an unknown result type")
			}
		} else {
			missing = true
		}
	}
	if missing {
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
	buildSpecification, _, err := reader.ReadParsedObject(ctx, c.buildSpecificationReference)
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

func (c *baseComputer) ComputeFilePropertiesValue(ctx context.Context, key *model_analysis_pb.FileProperties_Key, e FilePropertiesEnvironment) (PatchedFilePropertiesValue, error) {
	repoValue := e.GetRepoValue(&model_analysis_pb.Repo_Key{
		CanonicalRepo: key.CanonicalRepo,
	})
	directoryAccessParametersValue := e.GetDirectoryAccessParametersValue(&model_analysis_pb.DirectoryAccessParameters_Key{})
	if !repoValue.IsSet() {
		return PatchedFilePropertiesValue{}, evaluation.ErrMissingDependency
	}

	switch repoResult := repoValue.Message.Result.(type) {
	case *model_analysis_pb.Repo_Value_RootDirectoryReference:
		if !directoryAccessParametersValue.IsSet() {
			return PatchedFilePropertiesValue{}, evaluation.ErrMissingDependency
		}
		directoryAccessParameters, err := model_filesystem.NewDirectoryAccessParametersFromProto(
			directoryAccessParametersValue.Message.DirectoryAccessParameters,
			c.buildSpecificationReference.GetReferenceFormat(),
		)
		if err != nil {
			return model_core.NewSimplePatchedMessage[dag.ObjectContentsWalker](&model_analysis_pb.FileProperties_Value{
				Result: &model_analysis_pb.FileProperties_Value_Failure{
					Failure: fmt.Sprintf("invalid directory access parameters: %s", err),
				},
			}), nil
		}
		directoryReader := model_parser.NewStorageBackedParsedObjectReader(
			c.objectDownloader,
			directoryAccessParameters.GetEncoder(),
			model_parser.NewMessageObjectParser[object.LocalReference, model_filesystem_pb.Directory](),
		)
		leavesReader := model_parser.NewStorageBackedParsedObjectReader(
			c.objectDownloader,
			directoryAccessParameters.GetEncoder(),
			model_parser.NewMessageObjectParser[object.LocalReference, model_filesystem_pb.Leaves](),
		)

		rootDirectoryReferenceIndex, err := model_core.GetIndexFromReferenceMessage(repoResult.RootDirectoryReference, repoValue.OutgoingReferences.GetDegree())
		if err != nil {
			return model_core.NewSimplePatchedMessage[dag.ObjectContentsWalker](&model_analysis_pb.FileProperties_Value{
				Result: &model_analysis_pb.FileProperties_Value_Failure{
					Failure: fmt.Sprintf("invalid root directory reference: %s", err),
				},
			}), nil
		}

		resolver := model_filesystem.NewDirectoryMerkleTreeFileResolver(
			ctx,
			directoryReader,
			leavesReader,
			repoValue.OutgoingReferences.GetOutgoingReference(rootDirectoryReferenceIndex),
		)
		if err := bb_path.Resolve(
			bb_path.UNIXFormat.NewParser(key.Path),
			bb_path.NewLoopDetectingScopeWalker(
				bb_path.NewRelativeScopeWalker(resolver),
			),
		); err != nil {
			if status.Code(err) == codes.NotFound {
				return model_core.NewSimplePatchedMessage[dag.ObjectContentsWalker](&model_analysis_pb.FileProperties_Value{
					Result: &model_analysis_pb.FileProperties_Value_DoesNotExist{
						DoesNotExist: &emptypb.Empty{},
					},
				}), nil
			}
			return PatchedFilePropertiesValue{}, fmt.Errorf("failed to resolve %#v: %w", key.Path, err)
		}

		fileProperties := resolver.GetFileProperties()
		if !fileProperties.IsSet() {
			return model_core.NewSimplePatchedMessage[dag.ObjectContentsWalker](&model_analysis_pb.FileProperties_Value{
				Result: &model_analysis_pb.FileProperties_Value_Failure{
					Failure: "Path resolves to a directory",
				},
			}), nil
		}
		patchedFileProperties := model_core.NewPatchedMessageFromExisting(
			fileProperties,
			func(index int) dag.ObjectContentsWalker {
				return dag.ExistingObjectContentsWalker
			},
		)
		return PatchedFilePropertiesValue{
			Message: &model_analysis_pb.FileProperties_Value{
				Result: &model_analysis_pb.FileProperties_Value_Exists{
					Exists: patchedFileProperties.Message,
				},
			},
			Patcher: patchedFileProperties.Patcher,
		}, nil
	case *model_analysis_pb.Repo_Value_Failure:
		return model_core.NewSimplePatchedMessage[dag.ObjectContentsWalker](&model_analysis_pb.FileProperties_Value{
			Result: &model_analysis_pb.FileProperties_Value_Failure{
				Failure: repoResult.Failure,
			},
		}), nil
	default:
		return model_core.NewSimplePatchedMessage[dag.ObjectContentsWalker](&model_analysis_pb.FileProperties_Value{
			Result: &model_analysis_pb.FileProperties_Value_Failure{
				Failure: "Repo value has an unknown result type",
			},
		}), nil
	}
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

func (c *baseComputer) ComputePackageValue(ctx context.Context, key *model_analysis_pb.Package_Key, e PackageEnvironment) (PatchedPackageValue, error) {
	canonicalPackage, err := label.NewCanonicalPackage(key.Label)
	if err != nil {
		return model_core.NewSimplePatchedMessage[dag.ObjectContentsWalker](&model_analysis_pb.Package_Value{
			Result: &model_analysis_pb.Package_Value_Failure{
				Failure: fmt.Sprintf("invalid package label %#v: %s", key.Label, err),
			},
		}), nil
	}
	canonicalRepo := canonicalPackage.GetCanonicalRepo()

	buildFileName := label.MustNewTargetName("BUILD.bazel")
	buildFileProperties := e.GetFilePropertiesValue(&model_analysis_pb.FileProperties_Key{
		CanonicalRepo: canonicalRepo.String(),
		Path:          path.Join(canonicalPackage.GetPackagePath(), buildFileName.String()),
	})
	fileReader, fileReaderErr := e.GetFileReaderValue(&model_analysis_pb.FileReader_Key{})
	if fileReaderErr != nil && !errors.Is(fileReaderErr, evaluation.ErrMissingDependency) {
		return model_core.NewSimplePatchedMessage[dag.ObjectContentsWalker](&model_analysis_pb.Package_Value{
			Result: &model_analysis_pb.Package_Value_Failure{
				Failure: fileReaderErr.Error(),
			},
		}), nil
	}
	allBuiltinsModulesNames := e.GetBuiltinsModuleNamesValue(&model_analysis_pb.BuiltinsModuleNames_Key{})

	repoDefaultAttrsValue := e.GetRepoDefaultAttrsValue(&model_analysis_pb.RepoDefaultAttrs_Key{
		CanonicalRepo: canonicalRepo.String(),
	})

	if !buildFileProperties.IsSet() {
		return PatchedPackageValue{}, evaluation.ErrMissingDependency
	}
	switch buildFilePropertiesResult := buildFileProperties.Message.Result.(type) {
	case *model_analysis_pb.FileProperties_Value_Exists:
		if fileReaderErr != nil {
			return PatchedPackageValue{}, fileReaderErr
		}
		if !allBuiltinsModulesNames.IsSet() || !repoDefaultAttrsValue.IsSet() {
			return PatchedPackageValue{}, evaluation.ErrMissingDependency
		}

		builtinsModuleNames := allBuiltinsModulesNames.Message.BuiltinsModuleNames
		buildFileBuiltins, err := c.getBzlFileBuiltins(e, builtinsModuleNames, model_starlark.BuildFileBuiltins, "exported_rules")
		if err != nil {
			if !errors.Is(err, evaluation.ErrMissingDependency) {
				return model_core.NewSimplePatchedMessage[dag.ObjectContentsWalker](&model_analysis_pb.Package_Value{
					Result: &model_analysis_pb.Package_Value_Failure{
						Failure: err.Error(),
					},
				}), nil
			}
			return PatchedPackageValue{}, err
		}

		buildFileContentsEntry, err := model_filesystem.NewFileContentsEntryFromProto(
			model_core.Message[*model_filesystem_pb.FileContents]{
				Message:            buildFilePropertiesResult.Exists.GetContents(),
				OutgoingReferences: buildFileProperties.OutgoingReferences,
			},
			c.buildSpecificationReference.GetReferenceFormat(),
		)
		if err != nil {
			return model_core.NewSimplePatchedMessage[dag.ObjectContentsWalker](&model_analysis_pb.Package_Value{
				Result: &model_analysis_pb.Package_Value_Failure{
					Failure: util.StatusWrap(err, "Invalid file contents").Error(),
				},
			}), nil
		}
		buildFileData, err := fileReader.FileReadAll(ctx, buildFileContentsEntry, 1<<20)
		if err != nil {
			return PatchedPackageValue{}, err
		}

		buildFileLabel := canonicalPackage.AppendTargetName(buildFileName)
		_, program, err := starlark.SourceProgramOptions(
			&syntax.FileOptions{},
			buildFileLabel.String(),
			buildFileData,
			buildFileBuiltins.Has,
		)
		if err != nil {
			return model_core.NewSimplePatchedMessage[dag.ObjectContentsWalker](&model_analysis_pb.Package_Value{
				Result: &model_analysis_pb.Package_Value_Failure{
					Failure: fmt.Sprintf("failed to load %#v: %s", buildFileName.String(), err),
				},
			}), nil
		}

		if err := c.preloadBzlGlobals(e, canonicalPackage, program, builtinsModuleNames); err != nil {
			if !errors.Is(err, evaluation.ErrMissingDependency) {
				return model_core.NewSimplePatchedMessage[dag.ObjectContentsWalker](&model_analysis_pb.Package_Value{
					Result: &model_analysis_pb.Package_Value_Failure{
						Failure: err.Error(),
					},
				}), nil
			}
			return PatchedPackageValue{}, err
		}

		thread := c.newStarlarkThread(e, builtinsModuleNames)
		thread.SetLocal(model_starlark.CanonicalPackageKey, canonicalPackage)

		var repoDefaultAttrs model_core.Message[*model_starlark_pb.InheritableAttrs]
		switch repoDefaultAttrsResult := repoDefaultAttrsValue.Message.Result.(type) {
		case *model_analysis_pb.RepoDefaultAttrs_Value_Success:
			repoDefaultAttrs = model_core.Message[*model_starlark_pb.InheritableAttrs]{
				Message:            repoDefaultAttrsResult.Success,
				OutgoingReferences: repoDefaultAttrsValue.OutgoingReferences,
			}
		case *model_analysis_pb.RepoDefaultAttrs_Value_Failure:
			return model_core.NewSimplePatchedMessage[dag.ObjectContentsWalker](&model_analysis_pb.Package_Value{
				Result: &model_analysis_pb.Package_Value_Failure{
					Failure: repoDefaultAttrsResult.Failure,
				},
			}), nil
		default:
			return model_core.NewSimplePatchedMessage[dag.ObjectContentsWalker](&model_analysis_pb.Package_Value{
				Result: &model_analysis_pb.Package_Value_Failure{
					Failure: "Repo default attrs value has an unknown result type",
				},
			}), nil
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
			switch resultType := compiledBzlFile.Message.Result.(type) {
			case *model_analysis_pb.CompiledBzlFile_Value_CompiledProgram:
				identifierStr := identifier.GetStarlarkIdentifier().String()
				globals := resultType.CompiledProgram.Globals
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
			case *model_analysis_pb.CompiledBzlFile_Value_Failure:
				return model_core.Message[*model_starlark_pb.Value]{}, errors.New(resultType.Failure)
			default:
				return model_core.Message[*model_starlark_pb.Value]{}, errors.New("compiled .bzl file value has an unknown result type")
			}
		})

		thread.SetLocal(model_starlark.ValueEncodingOptionsKey, c.getValueEncodingOptions(buildFileLabel))

		// Execute the BUILD.bazel file, so that all targets
		// contained within are instantiated.
		if _, err := program.Init(thread, buildFileBuiltins); err != nil {
			if !errors.Is(err, evaluation.ErrMissingDependency) {
				var failureMessage string
				var evalErr *starlark.EvalError
				if errors.As(err, &evalErr) {
					failureMessage = fmt.Sprintf("%s: %s", evalErr.Backtrace(), evalErr.Msg)
				} else {
					failureMessage = err.Error()
				}
				return model_core.NewSimplePatchedMessage[dag.ObjectContentsWalker](&model_analysis_pb.Package_Value{
					Result: &model_analysis_pb.Package_Value_Failure{
						Failure: failureMessage,
					},
				}), nil
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
				Result: &model_analysis_pb.Package_Value_Success{
					Success: &model_analysis_pb.Package_Value_TargetList{
						Elements: targetsList.Message,
					},
				},
			},
			Patcher: targetsList.Patcher,
		}, nil
	case *model_analysis_pb.FileProperties_Value_DoesNotExist:
		return model_core.NewSimplePatchedMessage[dag.ObjectContentsWalker](&model_analysis_pb.Package_Value{
			Result: &model_analysis_pb.Package_Value_Failure{
				Failure: fmt.Sprintf("%#v does not exist", buildFileName.String()),
			},
		}), nil
	case *model_analysis_pb.FileProperties_Value_Failure:
		return model_core.NewSimplePatchedMessage[dag.ObjectContentsWalker](&model_analysis_pb.Package_Value{
			Result: &model_analysis_pb.Package_Value_Failure{
				Failure: buildFilePropertiesResult.Failure,
			},
		}), nil
	default:
		return model_core.NewSimplePatchedMessage[dag.ObjectContentsWalker](&model_analysis_pb.Package_Value{
			Result: &model_analysis_pb.Package_Value_Failure{
				Failure: "File properties value has an unknown result type",
			},
		}), nil
	}
}

func (c *baseComputer) ComputeRepoDefaultAttrsValue(ctx context.Context, key *model_analysis_pb.RepoDefaultAttrs_Key, e RepoDefaultAttrsEnvironment) (PatchedRepoDefaultAttrsValue, error) {
	canonicalRepo, err := label.NewCanonicalRepo(key.CanonicalRepo)
	if err != nil {
		return model_core.NewSimplePatchedMessage[dag.ObjectContentsWalker](&model_analysis_pb.RepoDefaultAttrs_Value{
			Result: &model_analysis_pb.RepoDefaultAttrs_Value_Failure{
				Failure: fmt.Sprintf("invalid canonical repo %#v: %s", key.CanonicalRepo, err),
			},
		}), nil
	}

	repoFileName := label.MustNewTargetName("REPO.bazel")
	repoFileProperties := e.GetFilePropertiesValue(&model_analysis_pb.FileProperties_Key{
		CanonicalRepo: canonicalRepo.String(),
		Path:          repoFileName.String(),
	})

	fileReader, fileReaderErr := e.GetFileReaderValue(&model_analysis_pb.FileReader_Key{})
	if fileReaderErr != nil && !errors.Is(fileReaderErr, evaluation.ErrMissingDependency) {
		return model_core.NewSimplePatchedMessage[dag.ObjectContentsWalker](&model_analysis_pb.RepoDefaultAttrs_Value{
			Result: &model_analysis_pb.RepoDefaultAttrs_Value_Failure{
				Failure: fileReaderErr.Error(),
			},
		}), nil
	}

	if !repoFileProperties.IsSet() {
		return PatchedRepoDefaultAttrsValue{}, evaluation.ErrMissingDependency
	}

	switch repoFilePropertiesResult := repoFileProperties.Message.Result.(type) {
	case *model_analysis_pb.FileProperties_Value_Exists:
		if fileReaderErr != nil {
			return PatchedRepoDefaultAttrsValue{}, fileReaderErr
		}

		// Read the contents of REPO.bazel.
		referenceFormat := c.buildSpecificationReference.GetReferenceFormat()
		repoFileContentsEntry, err := model_filesystem.NewFileContentsEntryFromProto(
			model_core.Message[*model_filesystem_pb.FileContents]{
				Message:            repoFilePropertiesResult.Exists.GetContents(),
				OutgoingReferences: repoFileProperties.OutgoingReferences,
			},
			referenceFormat,
		)
		if err != nil {
			return model_core.NewSimplePatchedMessage[dag.ObjectContentsWalker](&model_analysis_pb.RepoDefaultAttrs_Value{
				Result: &model_analysis_pb.RepoDefaultAttrs_Value_Failure{
					Failure: util.StatusWrap(err, "Invalid file contents").Error(),
				},
			}), nil
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
			return model_core.NewSimplePatchedMessage[dag.ObjectContentsWalker](&model_analysis_pb.RepoDefaultAttrs_Value{
				Result: &model_analysis_pb.RepoDefaultAttrs_Value_Failure{
					Failure: err.Error(),
				},
			}), nil
		}

		return model_core.PatchedMessage[*model_analysis_pb.RepoDefaultAttrs_Value, dag.ObjectContentsWalker]{
			Message: &model_analysis_pb.RepoDefaultAttrs_Value{
				Result: &model_analysis_pb.RepoDefaultAttrs_Value_Success{
					Success: defaultAttrs.Message,
				},
			},
			Patcher: defaultAttrs.Patcher,
		}, nil
	case *model_analysis_pb.FileProperties_Value_DoesNotExist:
		// No REPO.bazel file is present. Return the default.
		return model_core.NewSimplePatchedMessage[dag.ObjectContentsWalker](&model_analysis_pb.RepoDefaultAttrs_Value{
			Result: &model_analysis_pb.RepoDefaultAttrs_Value_Success{
				Success: &model_starlark.DefaultInheritableAttrs,
			},
		}), nil
	case *model_analysis_pb.FileProperties_Value_Failure:
		return model_core.NewSimplePatchedMessage[dag.ObjectContentsWalker](&model_analysis_pb.RepoDefaultAttrs_Value{
			Result: &model_analysis_pb.RepoDefaultAttrs_Value_Failure{
				Failure: repoFilePropertiesResult.Failure,
			},
		}), nil
	default:
		return model_core.NewSimplePatchedMessage[dag.ObjectContentsWalker](&model_analysis_pb.RepoDefaultAttrs_Value{
			Result: &model_analysis_pb.RepoDefaultAttrs_Value_Failure{
				Failure: "File properties value has an unknown result type",
			},
		}), nil
	}
}

func (c *baseComputer) ComputeTargetCompletionValue(ctx context.Context, key *model_analysis_pb.TargetCompletion_Key, e TargetCompletionEnvironment) (PatchedTargetCompletionValue, error) {
	configuredTarget := e.GetConfiguredTargetValue(&model_analysis_pb.ConfiguredTarget_Key{
		Label: key.Label,
	})
	if !configuredTarget.IsSet() {
		return PatchedTargetCompletionValue{}, evaluation.ErrMissingDependency
	}

	switch resultType := configuredTarget.Message.Result.(type) {
	case *model_analysis_pb.ConfiguredTarget_Value_Success:
		return model_core.NewSimplePatchedMessage[dag.ObjectContentsWalker](&model_analysis_pb.TargetCompletion_Value{
			Result: &model_analysis_pb.TargetCompletion_Value_Success{
				Success: &emptypb.Empty{},
			},
		}), nil
	case *model_analysis_pb.ConfiguredTarget_Value_Failure:
		return model_core.NewSimplePatchedMessage[dag.ObjectContentsWalker](&model_analysis_pb.TargetCompletion_Value{
			Result: &model_analysis_pb.TargetCompletion_Value_Failure{
				Failure: resultType.Failure,
			},
		}), nil
	default:
		return PatchedTargetCompletionValue{}, errors.New("Configured target value has an unknown result type")
	}
}
