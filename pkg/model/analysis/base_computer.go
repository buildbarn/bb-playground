package analysis

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log"
	"path"
	"sort"
	"strings"

	"github.com/buildbarn/bb-playground/pkg/evaluation"
	"github.com/buildbarn/bb-playground/pkg/label"
	model_core "github.com/buildbarn/bb-playground/pkg/model/core"
	model_encoding "github.com/buildbarn/bb-playground/pkg/model/encoding"
	model_filesystem "github.com/buildbarn/bb-playground/pkg/model/filesystem"
	model_parser "github.com/buildbarn/bb-playground/pkg/model/parser"
	model_starlark "github.com/buildbarn/bb-playground/pkg/model/starlark"
	model_analysis_pb "github.com/buildbarn/bb-playground/pkg/proto/model/analysis"
	model_build_pb "github.com/buildbarn/bb-playground/pkg/proto/model/build"
	model_core_pb "github.com/buildbarn/bb-playground/pkg/proto/model/core"
	model_filesystem_pb "github.com/buildbarn/bb-playground/pkg/proto/model/filesystem"
	model_starlark_pb "github.com/buildbarn/bb-playground/pkg/proto/model/starlark"
	"github.com/buildbarn/bb-playground/pkg/storage/dag"
	"github.com/buildbarn/bb-playground/pkg/storage/object"
	bb_path "github.com/buildbarn/bb-storage/pkg/filesystem/path"
	"github.com/buildbarn/bb-storage/pkg/util"

	"google.golang.org/protobuf/encoding/protojson"

	"go.starlark.net/starlark"
	"go.starlark.net/syntax"
)

type baseComputer struct {
	objectDownloader            object.Downloader[object.LocalReference]
	buildSpecificationReference object.LocalReference
	buildSpecificationEncoder   model_encoding.BinaryEncoder
}

func NewBaseComputer(
	objectDownloader object.Downloader[object.LocalReference],
	buildSpecificationReference object.LocalReference,
	buildSpecificationEncoder model_encoding.BinaryEncoder,
) Computer {
	return &baseComputer{
		objectDownloader:            objectDownloader,
		buildSpecificationReference: buildSpecificationReference,
		buildSpecificationEncoder:   buildSpecificationEncoder,
	}
}

type resolveApparentLabelEnvironment interface {
	GetCanonicalRepoNameValue(*model_analysis_pb.CanonicalRepoName_Key) model_core.Message[*model_analysis_pb.CanonicalRepoName_Value]
}

func (c *baseComputer) resolveApparentLabel(e resolveApparentLabelEnvironment, fromRepo label.CanonicalRepo, toApparentLabel label.ApparentLabel) (*label.CanonicalLabel, error) {
	toCanonicalLabel, ok := toApparentLabel.AsCanonicalLabel()
	if ok {
		// Label was already canonical. Nothing to do.
		return &toCanonicalLabel, nil
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
	if !v.IsSet() {
		return nil, evaluation.ErrMissingDependency
	}
	switch resultType := v.Message.Result.(type) {
	case *model_analysis_pb.CanonicalRepoName_Value_ToCanonicalRepo:
		toCanonicalRepo, err := label.NewCanonicalRepo(resultType.ToCanonicalRepo)
		if err != nil {
			return nil, fmt.Errorf("invalid canonical repo name %#v: %w", resultType.ToCanonicalRepo, err)
		}
		toCanonicalLabel := toApparentLabel.WithCanonicalRepo(toCanonicalRepo)
		return &toCanonicalLabel, nil
	case *model_analysis_pb.CanonicalRepoName_Value_Failure:
		return nil, errors.New(resultType.Failure)
	default:
		return nil, errors.New("invalid result type for canonical repo name resolution")
	}
}

type loadBzlGlobalsEnvironment interface {
	resolveApparentLabelEnvironment
	GetCompiledBzlFileGlobalsValue(key *model_analysis_pb.CompiledBzlFileGlobals_Key) (starlark.StringDict, error)
}

func (c *baseComputer) loadBzlGlobals(e loadBzlGlobalsEnvironment, canonicalPackage label.CanonicalPackage, loadLabelStr string, builtin bool) (starlark.StringDict, error) {
	if builtin {
		// Skip the repo name on any load()s performed by
		// builtin code. They must refer to other .bzl files in
		// the same repository.
		if offset := strings.Index(loadLabelStr, "//"); offset > 0 {
			loadLabelStr = loadLabelStr[offset:]
		}
	}
	apparentLoadLabel, err := canonicalPackage.AppendLabel(loadLabelStr)
	if err != nil {
		return nil, fmt.Errorf("invalid label %#v in load() statement: %w", loadLabelStr, err)
	}
	canonicalRepo := canonicalPackage.GetCanonicalRepo()
	canonicalLoadLabel, err := c.resolveApparentLabel(e, canonicalRepo, apparentLoadLabel)
	if err != nil {
		return nil, fmt.Errorf("failed to resolve label %#v in load() statement: %w", apparentLoadLabel.String(), err)
	}
	return e.GetCompiledBzlFileGlobalsValue(&model_analysis_pb.CompiledBzlFileGlobals_Key{
		Label:   canonicalLoadLabel.String(),
		Builtin: builtin,
	})
}

func (c *baseComputer) loadBzlGlobalsInStarlarkThread(e loadBzlGlobalsEnvironment, thread *starlark.Thread, loadLabelStr string, builtin bool) (starlark.StringDict, error) {
	return c.loadBzlGlobals(e, label.MustNewCanonicalLabel(thread.CallFrame(0).Pos.Filename()).GetCanonicalPackage(), loadLabelStr, builtin)
}

func (c *baseComputer) preloadBzlGlobals(e loadBzlGlobalsEnvironment, canonicalPackage label.CanonicalPackage, program *starlark.Program, builtin bool) (aggregateErr error) {
	numLoads := program.NumLoads()
	for i := 0; i < numLoads; i++ {
		loadLabelStr, _ := program.Load(i)
		if _, err := c.loadBzlGlobals(e, canonicalPackage, loadLabelStr, builtin); err != nil {
			if !errors.Is(err, evaluation.ErrMissingDependency) {
				return err
			}
			aggregateErr = err
		}
	}
	return
}

type getBzlFileBuiltinsEnvironment interface {
	GetCompiledBzlFileGlobalsValue(key *model_analysis_pb.CompiledBzlFileGlobals_Key) (starlark.StringDict, error)
}

func (c *baseComputer) getBzlFileBuiltins(e getBzlFileBuiltinsEnvironment, builtin bool) (starlark.StringDict, error) {
	if builtin {
		return model_starlark.BzlFileBuiltins, nil
	}
	return e.GetCompiledBzlFileGlobalsValue(&model_analysis_pb.CompiledBzlFileGlobals_Key{
		Label:   "@@builtins+//:exports.bzl",
		Builtin: true,
	})
}

func (c *baseComputer) ComputeCanonicalRepoNameValue(ctx context.Context, key *model_analysis_pb.CanonicalRepoName_Key, e CanonicalRepoNameEnvironment) (model_core.PatchedMessage[*model_analysis_pb.CanonicalRepoName_Value, dag.ObjectContentsWalker], error) {
	// TODO: Provide an actual implementation!
	var toCanonicalrepo string
	switch key.ToApparentRepo {
	case "io_bazel_rules_go_bazel_features":
		toCanonicalrepo = "bazel_features+"
	default:
		toCanonicalrepo = key.ToApparentRepo + "+"
	}
	return model_core.PatchedMessage[*model_analysis_pb.CanonicalRepoName_Value, dag.ObjectContentsWalker]{
		Message: &model_analysis_pb.CanonicalRepoName_Value{
			Result: &model_analysis_pb.CanonicalRepoName_Value_ToCanonicalRepo{
				ToCanonicalRepo: toCanonicalrepo,
			},
		},
		Patcher: model_core.NewReferenceMessagePatcher[dag.ObjectContentsWalker](),
	}, nil
}

func (c *baseComputer) ComputeBuildResultValue(ctx context.Context, key *model_analysis_pb.BuildResult_Key, e BuildResultEnvironment) (model_core.PatchedMessage[*model_analysis_pb.BuildResult_Value, dag.ObjectContentsWalker], error) {
	// TODO: Do something proper here.
	targetCompletion := e.GetTargetCompletionValue(&model_analysis_pb.TargetCompletion_Key{
		Label: "@@com_github_buildbarn_bb_storage+//cmd/bb_storage",
	})
	if !targetCompletion.IsSet() {
		return model_core.PatchedMessage[*model_analysis_pb.BuildResult_Value, dag.ObjectContentsWalker]{}, evaluation.ErrMissingDependency
	}
	return model_core.PatchedMessage[*model_analysis_pb.BuildResult_Value, dag.ObjectContentsWalker]{
		Message: &model_analysis_pb.BuildResult_Value{},
		Patcher: model_core.NewReferenceMessagePatcher[dag.ObjectContentsWalker](),
	}, nil
}

func (c *baseComputer) ComputeBuildSpecificationValue(ctx context.Context, key *model_analysis_pb.BuildSpecification_Key, e BuildSpecificationEnvironment) (model_core.PatchedMessage[*model_analysis_pb.BuildSpecification_Value, dag.ObjectContentsWalker], error) {
	reader := model_parser.NewStorageBackedParsedObjectReader(
		c.objectDownloader,
		c.buildSpecificationEncoder,
		model_parser.NewMessageObjectParser[object.LocalReference, model_build_pb.BuildSpecification](),
	)
	buildSpecification, _, err := reader.ReadParsedObject(ctx, c.buildSpecificationReference)
	if err != nil {
		return model_core.PatchedMessage[*model_analysis_pb.BuildSpecification_Value, dag.ObjectContentsWalker]{}, err
	}

	patchedBuildSpecification := model_core.NewPatchedMessageFromExisting(
		buildSpecification,
		func(reference object.LocalReference) dag.ObjectContentsWalker {
			return dag.ExistingObjectContentsWalker
		},
	)
	return model_core.PatchedMessage[*model_analysis_pb.BuildSpecification_Value, dag.ObjectContentsWalker]{
		Message: &model_analysis_pb.BuildSpecification_Value{
			BuildSpecification: patchedBuildSpecification.Message,
		},
		Patcher: patchedBuildSpecification.Patcher,
	}, nil
}

func (c *baseComputer) ComputeCompiledBzlFileValue(ctx context.Context, key *model_analysis_pb.CompiledBzlFile_Key, e CompiledBzlFileEnvironment) (model_core.PatchedMessage[*model_analysis_pb.CompiledBzlFile_Value, dag.ObjectContentsWalker], error) {
	canonicalLabel, err := label.NewCanonicalLabel(key.Label)
	if err != nil {
		return model_core.PatchedMessage[*model_analysis_pb.CompiledBzlFile_Value, dag.ObjectContentsWalker]{
			Message: &model_analysis_pb.CompiledBzlFile_Value{
				Result: &model_analysis_pb.CompiledBzlFile_Value_Failure{
					Failure: fmt.Errorf("invalid label %#v: %w", key.Label, err).Error(),
				},
			},
			Patcher: model_core.NewReferenceMessagePatcher[dag.ObjectContentsWalker](),
		}, nil
	}
	canonicalPackage := canonicalLabel.GetCanonicalPackage()
	canonicalRepoStr := canonicalPackage.GetCanonicalRepo().String()

	bzlFileProperties := e.GetFilePropertiesValue(&model_analysis_pb.FileProperties_Key{
		CanonicalRepo: canonicalRepoStr,
		Path:          path.Join(canonicalPackage.GetPackagePath(), canonicalLabel.GetTargetName()),
	})
	fileReader, fileReaderErr := e.GetFileReaderValue(&model_analysis_pb.FileReader_Key{})
	if fileReaderErr != nil && !errors.Is(fileReaderErr, evaluation.ErrMissingDependency) {
		return model_core.PatchedMessage[*model_analysis_pb.CompiledBzlFile_Value, dag.ObjectContentsWalker]{
			Message: &model_analysis_pb.CompiledBzlFile_Value{
				Result: &model_analysis_pb.CompiledBzlFile_Value_Failure{
					Failure: fileReaderErr.Error(),
				},
			},
			Patcher: model_core.NewReferenceMessagePatcher[dag.ObjectContentsWalker](),
		}, nil
	}
	bzlFileBuiltins, bzlFileBuiltinsErr := c.getBzlFileBuiltins(e, key.Builtin)
	if bzlFileBuiltinsErr != nil && !errors.Is(bzlFileBuiltinsErr, evaluation.ErrMissingDependency) {
		return model_core.PatchedMessage[*model_analysis_pb.CompiledBzlFile_Value, dag.ObjectContentsWalker]{
			Message: &model_analysis_pb.CompiledBzlFile_Value{
				Result: &model_analysis_pb.CompiledBzlFile_Value_Failure{
					Failure: bzlFileBuiltinsErr.Error(),
				},
			},
			Patcher: model_core.NewReferenceMessagePatcher[dag.ObjectContentsWalker](),
		}, nil
	}

	if !bzlFileProperties.IsSet() {
		return model_core.PatchedMessage[*model_analysis_pb.CompiledBzlFile_Value, dag.ObjectContentsWalker]{}, evaluation.ErrMissingDependency
	}
	switch bzlFilePropertiesResult := bzlFileProperties.Message.Result.(type) {
	case *model_analysis_pb.FileProperties_Value_Exists:
		if fileReaderErr != nil {
			return model_core.PatchedMessage[*model_analysis_pb.CompiledBzlFile_Value, dag.ObjectContentsWalker]{}, fileReaderErr
		}
		if bzlFileBuiltinsErr != nil {
			return model_core.PatchedMessage[*model_analysis_pb.CompiledBzlFile_Value, dag.ObjectContentsWalker]{}, bzlFileBuiltinsErr
		}

		buildFileContentsEntry, err := model_filesystem.NewFileContentsEntryFromProto(
			model_core.Message[*model_filesystem_pb.FileContents]{
				Message:            bzlFilePropertiesResult.Exists.GetContents(),
				OutgoingReferences: bzlFileProperties.OutgoingReferences,
			},
			c.buildSpecificationReference.GetReferenceFormat(),
		)
		if err != nil {
			return model_core.PatchedMessage[*model_analysis_pb.CompiledBzlFile_Value, dag.ObjectContentsWalker]{
				Message: &model_analysis_pb.CompiledBzlFile_Value{
					Result: &model_analysis_pb.CompiledBzlFile_Value_Failure{
						Failure: fmt.Errorf("invalid file contents: %w", err).Error(),
					},
				},
				Patcher: model_core.NewReferenceMessagePatcher[dag.ObjectContentsWalker](),
			}, nil
		}
		bzlFileData, err := fileReader.FileReadAll(ctx, buildFileContentsEntry, 1<<20)
		if err != nil {
			return model_core.PatchedMessage[*model_analysis_pb.CompiledBzlFile_Value, dag.ObjectContentsWalker]{}, err
		}

		_, program, err := starlark.SourceProgramOptions(
			&syntax.FileOptions{},
			canonicalLabel.String(),
			bzlFileData,
			bzlFileBuiltins.Has,
		)
		if err != nil {
			return model_core.PatchedMessage[*model_analysis_pb.CompiledBzlFile_Value, dag.ObjectContentsWalker]{
				Message: &model_analysis_pb.CompiledBzlFile_Value{
					Result: &model_analysis_pb.CompiledBzlFile_Value_Failure{
						Failure: err.Error(),
					},
				},
				Patcher: model_core.NewReferenceMessagePatcher[dag.ObjectContentsWalker](),
			}, nil
		}

		if err := c.preloadBzlGlobals(e, canonicalPackage, program, key.Builtin); err != nil {
			if !errors.Is(err, evaluation.ErrMissingDependency) {
				return model_core.PatchedMessage[*model_analysis_pb.CompiledBzlFile_Value, dag.ObjectContentsWalker]{
					Message: &model_analysis_pb.CompiledBzlFile_Value{
						Result: &model_analysis_pb.CompiledBzlFile_Value_Failure{
							Failure: err.Error(),
						},
					},
					Patcher: model_core.NewReferenceMessagePatcher[dag.ObjectContentsWalker](),
				}, nil
			}
			return model_core.PatchedMessage[*model_analysis_pb.CompiledBzlFile_Value, dag.ObjectContentsWalker]{}, err
		}

		thread := &starlark.Thread{
			// TODO: Provide print method.
			Print: nil,
			Load: func(thread *starlark.Thread, loadLabelStr string) (starlark.StringDict, error) {
				return c.loadBzlGlobalsInStarlarkThread(e, thread, loadLabelStr, key.Builtin)
			},
		}
		thread.SetLocal(model_starlark.CanonicalRepoResolverKey, func(apparentRepo label.ApparentRepo) (label.CanonicalRepo, error) {
			v := e.GetCanonicalRepoNameValue(&model_analysis_pb.CanonicalRepoName_Key{
				FromCanonicalRepo: canonicalRepoStr,
				ToApparentRepo:    apparentRepo.String(),
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
		thread.SetLocal(model_starlark.FunctionResolverKey, func(filename label.CanonicalLabel, name string) (starlark.Callable, error) {
			callables, err := e.GetCompiledBzlFileFunctionsValue(&model_analysis_pb.CompiledBzlFileFunctions_Key{
				Label:   filename.String(),
				Builtin: key.Builtin,
			})
			if err != nil {
				return nil, err
			}
			callable, ok := callables[name]
			if !ok {
				return nil, fmt.Errorf("evaluation of %#v not yield a function with name %#v", filename.String(), name)
			}
			return callable, nil
		})

		globals, err := program.Init(thread, bzlFileBuiltins)
		if err != nil {
			if !errors.Is(err, evaluation.ErrMissingDependency) {
				log.Printf("FAILURE: %s %s", canonicalLabel.String(), err)
			}
			return model_core.PatchedMessage[*model_analysis_pb.CompiledBzlFile_Value, dag.ObjectContentsWalker]{}, err
		}
		log.Printf("SUCCESS: %s", canonicalLabel.String())

		// TODO! Use proper encoding options!
		compiledProgram, err := model_starlark.EncodeCompiledProgram(program, globals, &model_starlark.ValueEncodingOptions{
			CurrentFilename:        program.Filename(),
			ObjectEncoder:          model_encoding.NewChainedBinaryEncoder(nil),
			ObjectReferenceFormat:  c.buildSpecificationReference.GetReferenceFormat(),
			ObjectMinimumSizeBytes: 32 * 1024,
			ObjectMaximumSizeBytes: 128 * 1024,
		})
		if err != nil {
			return model_core.PatchedMessage[*model_analysis_pb.CompiledBzlFile_Value, dag.ObjectContentsWalker]{
				Message: &model_analysis_pb.CompiledBzlFile_Value{
					Result: &model_analysis_pb.CompiledBzlFile_Value_Failure{
						Failure: err.Error(),
					},
				},
				Patcher: model_core.NewReferenceMessagePatcher[dag.ObjectContentsWalker](),
			}, nil
		}
		return model_core.PatchedMessage[*model_analysis_pb.CompiledBzlFile_Value, dag.ObjectContentsWalker]{
			Message: &model_analysis_pb.CompiledBzlFile_Value{
				Result: &model_analysis_pb.CompiledBzlFile_Value_CompiledProgram{
					CompiledProgram: compiledProgram.Message,
				},
			},
			Patcher: compiledProgram.Patcher,
		}, nil
	case *model_analysis_pb.FileProperties_Value_Failure:
		return model_core.PatchedMessage[*model_analysis_pb.CompiledBzlFile_Value, dag.ObjectContentsWalker]{
			Message: &model_analysis_pb.CompiledBzlFile_Value{
				Result: &model_analysis_pb.CompiledBzlFile_Value_Failure{
					Failure: bzlFilePropertiesResult.Failure,
				},
			},
			Patcher: model_core.NewReferenceMessagePatcher[dag.ObjectContentsWalker](),
		}, nil
	default:
		return model_core.PatchedMessage[*model_analysis_pb.CompiledBzlFile_Value, dag.ObjectContentsWalker]{
			Message: &model_analysis_pb.CompiledBzlFile_Value{
				Result: &model_analysis_pb.CompiledBzlFile_Value_Failure{
					Failure: "File properties value has an unknown result type",
				},
			},
			Patcher: model_core.NewReferenceMessagePatcher[dag.ObjectContentsWalker](),
		}, nil
	}
}

func (c *baseComputer) ComputeCompiledBzlFileFunctionsValue(ctx context.Context, key *model_analysis_pb.CompiledBzlFileFunctions_Key, e CompiledBzlFileFunctionsEnvironment) (map[string]starlark.Callable, error) {
	compiledBzlFile := e.GetCompiledBzlFileValue(&model_analysis_pb.CompiledBzlFile_Key{
		Label:   key.Label,
		Builtin: key.Builtin,
	})
	bzlFileBuiltins, bzlFileBuiltinsErr := c.getBzlFileBuiltins(e, key.Builtin)
	if !compiledBzlFile.IsSet() {
		return nil, evaluation.ErrMissingDependency
	}
	if bzlFileBuiltinsErr != nil {
		return nil, bzlFileBuiltinsErr
	}

	switch resultType := compiledBzlFile.Message.Result.(type) {
	case *model_analysis_pb.CompiledBzlFile_Value_CompiledProgram:
		program, err := starlark.CompiledProgram(bytes.NewBuffer(resultType.CompiledProgram.Code))
		if err != nil {
			return nil, err
		}

		thread := &starlark.Thread{
			// TODO: Provide print method.
			Print: nil,
			Load: func(thread *starlark.Thread, loadLabelStr string) (starlark.StringDict, error) {
				return c.loadBzlGlobalsInStarlarkThread(e, thread, loadLabelStr, key.Builtin)
			},
		}
		// TODO: Set handlers!

		globals, err := program.Init(thread, bzlFileBuiltins)
		if err != nil {
			return nil, err
		}
		globals.Freeze()

		callables := map[string]starlark.Callable{}
		for name, value := range globals {
			if callable, ok := value.(starlark.Callable); ok {
				callables[name] = callable
			}
		}
		return callables, nil
	case *model_analysis_pb.CompiledBzlFile_Value_Failure:
		return nil, errors.New(resultType.Failure)
	default:
		return nil, errors.New("compiled .bzl file value has an unknown result type")
	}
}

func (c *baseComputer) ComputeCompiledBzlFileGlobalsValue(ctx context.Context, key *model_analysis_pb.CompiledBzlFileGlobals_Key, e CompiledBzlFileGlobalsEnvironment) (starlark.StringDict, error) {
	compiledBzlFile := e.GetCompiledBzlFileValue(&model_analysis_pb.CompiledBzlFile_Key{
		Label:   key.Label,
		Builtin: key.Builtin,
	})
	if !compiledBzlFile.IsSet() {
		return nil, evaluation.ErrMissingDependency
	}

	switch resultType := compiledBzlFile.Message.Result.(type) {
	case *model_analysis_pb.CompiledBzlFile_Value_CompiledProgram:
		return model_starlark.DecodeGlobals(
			model_core.Message[[]*model_starlark_pb.Global]{
				Message:            resultType.CompiledProgram.Globals,
				OutgoingReferences: compiledBzlFile.OutgoingReferences,
			},
			&model_starlark.ValueDecodingOptions{
				Context:          ctx,
				ObjectDownloader: c.objectDownloader,
				ObjectEncoder:    model_encoding.NewChainedBinaryEncoder(nil),
			},
		)
	case *model_analysis_pb.CompiledBzlFile_Value_Failure:
		return nil, errors.New(resultType.Failure)
	default:
		return nil, errors.New("File properties value has an unknown result type")
	}
}

func (c *baseComputer) ComputeConfiguredTargetValue(ctx context.Context, key *model_analysis_pb.ConfiguredTarget_Key, e ConfiguredTargetEnvironment) (model_core.PatchedMessage[*model_analysis_pb.ConfiguredTarget_Value, dag.ObjectContentsWalker], error) {
	targetLabel, err := label.NewCanonicalLabel(key.Label)
	if err != nil {
		return model_core.PatchedMessage[*model_analysis_pb.ConfiguredTarget_Value, dag.ObjectContentsWalker]{
			Message: &model_analysis_pb.ConfiguredTarget_Value{
				Result: &model_analysis_pb.ConfiguredTarget_Value_Failure{
					Failure: fmt.Errorf("invalid target label %#v: %w", key.Label, err).Error(),
				},
			},
			Patcher: model_core.NewReferenceMessagePatcher[dag.ObjectContentsWalker](),
		}, nil
	}
	packageValue := e.GetPackageValue(&model_analysis_pb.Package_Key{
		Label: targetLabel.GetCanonicalPackage().String(),
	})
	if !packageValue.IsSet() {
		return model_core.PatchedMessage[*model_analysis_pb.ConfiguredTarget_Value, dag.ObjectContentsWalker]{}, evaluation.ErrMissingDependency
	}

	return model_core.PatchedMessage[*model_analysis_pb.ConfiguredTarget_Value, dag.ObjectContentsWalker]{
		Message: &model_analysis_pb.ConfiguredTarget_Value{
			Result: &model_analysis_pb.ConfiguredTarget_Value_Failure{
				Failure: packageValue.Message.Result.(*model_analysis_pb.Package_Value_Failure).Failure,
			},
		},
		Patcher: model_core.NewReferenceMessagePatcher[dag.ObjectContentsWalker](),
	}, nil
}

func (c *baseComputer) ComputeDirectoryAccessParametersValue(ctx context.Context, key *model_analysis_pb.DirectoryAccessParameters_Key, e DirectoryAccessParametersEnvironment) (model_core.PatchedMessage[*model_analysis_pb.DirectoryAccessParameters_Value, dag.ObjectContentsWalker], error) {
	buildSpecification := e.GetBuildSpecificationValue(&model_analysis_pb.BuildSpecification_Key{})
	if !buildSpecification.IsSet() {
		return model_core.PatchedMessage[*model_analysis_pb.DirectoryAccessParameters_Value, dag.ObjectContentsWalker]{}, evaluation.ErrMissingDependency
	}
	return model_core.PatchedMessage[*model_analysis_pb.DirectoryAccessParameters_Value, dag.ObjectContentsWalker]{
		Message: &model_analysis_pb.DirectoryAccessParameters_Value{
			DirectoryAccessParameters: buildSpecification.Message.BuildSpecification.GetDirectoryCreationParameters().GetAccess(),
		},
		Patcher: model_core.NewReferenceMessagePatcher[dag.ObjectContentsWalker](),
	}, nil
}

func (c *baseComputer) ComputeFileAccessParametersValue(ctx context.Context, key *model_analysis_pb.FileAccessParameters_Key, e FileAccessParametersEnvironment) (model_core.PatchedMessage[*model_analysis_pb.FileAccessParameters_Value, dag.ObjectContentsWalker], error) {
	buildSpecification := e.GetBuildSpecificationValue(&model_analysis_pb.BuildSpecification_Key{})
	if !buildSpecification.IsSet() {
		return model_core.PatchedMessage[*model_analysis_pb.FileAccessParameters_Value, dag.ObjectContentsWalker]{}, evaluation.ErrMissingDependency
	}
	return model_core.PatchedMessage[*model_analysis_pb.FileAccessParameters_Value, dag.ObjectContentsWalker]{
		Message: &model_analysis_pb.FileAccessParameters_Value{
			FileAccessParameters: buildSpecification.Message.BuildSpecification.GetFileCreationParameters().GetAccess(),
		},
		Patcher: model_core.NewReferenceMessagePatcher[dag.ObjectContentsWalker](),
	}, nil
}

func (c *baseComputer) ComputeFilePropertiesValue(ctx context.Context, key *model_analysis_pb.FileProperties_Key, e FilePropertiesEnvironment) (model_core.PatchedMessage[*model_analysis_pb.FileProperties_Value, dag.ObjectContentsWalker], error) {
	repoValue := e.GetRepoValue(&model_analysis_pb.Repo_Key{
		CanonicalRepo: key.CanonicalRepo,
	})
	directoryAccessParametersValue := e.GetDirectoryAccessParametersValue(&model_analysis_pb.DirectoryAccessParameters_Key{})
	if !repoValue.IsSet() {
		return model_core.PatchedMessage[*model_analysis_pb.FileProperties_Value, dag.ObjectContentsWalker]{}, evaluation.ErrMissingDependency
	}

	switch repoResult := repoValue.Message.Result.(type) {
	case *model_analysis_pb.Repo_Value_RootDirectoryReference:
		if !directoryAccessParametersValue.IsSet() {
			return model_core.PatchedMessage[*model_analysis_pb.FileProperties_Value, dag.ObjectContentsWalker]{}, evaluation.ErrMissingDependency
		}
		directoryAccessParameters, err := model_filesystem.NewDirectoryAccessParametersFromProto(
			directoryAccessParametersValue.Message.DirectoryAccessParameters,
			c.buildSpecificationReference.GetReferenceFormat(),
		)
		if err != nil {
			return model_core.PatchedMessage[*model_analysis_pb.FileProperties_Value, dag.ObjectContentsWalker]{
				Message: &model_analysis_pb.FileProperties_Value{
					Result: &model_analysis_pb.FileProperties_Value_Failure{
						Failure: fmt.Errorf("invalid directory access parameters: %w", err).Error(),
					},
				},
				Patcher: model_core.NewReferenceMessagePatcher[dag.ObjectContentsWalker](),
			}, nil
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
			return model_core.PatchedMessage[*model_analysis_pb.FileProperties_Value, dag.ObjectContentsWalker]{
				Message: &model_analysis_pb.FileProperties_Value{
					Result: &model_analysis_pb.FileProperties_Value_Failure{
						Failure: fmt.Errorf("invalid root directory reference: %w", err).Error(),
					},
				},
				Patcher: model_core.NewReferenceMessagePatcher[dag.ObjectContentsWalker](),
			}, nil
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
			// TODO: This should return a message in case of
			// non-infrastructure errors, so that we get caching.
			return model_core.PatchedMessage[*model_analysis_pb.FileProperties_Value, dag.ObjectContentsWalker]{}, fmt.Errorf("failed to resolve %#v: %w", key.Path, err)
		}

		fileProperties := resolver.GetFileProperties()
		if !fileProperties.IsSet() {
			return model_core.PatchedMessage[*model_analysis_pb.FileProperties_Value, dag.ObjectContentsWalker]{
				Message: &model_analysis_pb.FileProperties_Value{
					Result: &model_analysis_pb.FileProperties_Value_Failure{
						Failure: "Path resolves to a directory",
					},
				},
				Patcher: model_core.NewReferenceMessagePatcher[dag.ObjectContentsWalker](),
			}, nil
		}
		patchedFileProperties := model_core.NewPatchedMessageFromExisting(
			fileProperties,
			func(reference object.LocalReference) dag.ObjectContentsWalker {
				return dag.ExistingObjectContentsWalker
			},
		)
		return model_core.PatchedMessage[*model_analysis_pb.FileProperties_Value, dag.ObjectContentsWalker]{
			Message: &model_analysis_pb.FileProperties_Value{
				Result: &model_analysis_pb.FileProperties_Value_Exists{
					Exists: patchedFileProperties.Message,
				},
			},
			Patcher: patchedFileProperties.Patcher,
		}, nil
	case *model_analysis_pb.Repo_Value_Failure:
		return model_core.PatchedMessage[*model_analysis_pb.FileProperties_Value, dag.ObjectContentsWalker]{
			Message: &model_analysis_pb.FileProperties_Value{
				Result: &model_analysis_pb.FileProperties_Value_Failure{
					Failure: repoResult.Failure,
				},
			},
			Patcher: model_core.NewReferenceMessagePatcher[dag.ObjectContentsWalker](),
		}, nil
	default:
		return model_core.PatchedMessage[*model_analysis_pb.FileProperties_Value, dag.ObjectContentsWalker]{
			Message: &model_analysis_pb.FileProperties_Value{
				Result: &model_analysis_pb.FileProperties_Value_Failure{
					Failure: "Repo value has an unknown result type",
				},
			},
			Patcher: model_core.NewReferenceMessagePatcher[dag.ObjectContentsWalker](),
		}, nil
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

func (c *baseComputer) ComputePackageValue(ctx context.Context, key *model_analysis_pb.Package_Key, e PackageEnvironment) (model_core.PatchedMessage[*model_analysis_pb.Package_Value, dag.ObjectContentsWalker], error) {
	canonicalPackage, err := label.NewCanonicalPackage(key.Label)
	if err != nil {
		return model_core.PatchedMessage[*model_analysis_pb.Package_Value, dag.ObjectContentsWalker]{
			Message: &model_analysis_pb.Package_Value{
				Result: &model_analysis_pb.Package_Value_Failure{
					Failure: fmt.Errorf("invalid package label %#v: %w", key.Label, err).Error(),
				},
			},
			Patcher: model_core.NewReferenceMessagePatcher[dag.ObjectContentsWalker](),
		}, nil
	}
	canonicalRepo := canonicalPackage.GetCanonicalRepo()

	buildFileProperties := e.GetFilePropertiesValue(&model_analysis_pb.FileProperties_Key{
		CanonicalRepo: canonicalRepo.String(),
		Path:          path.Join(canonicalPackage.GetPackagePath(), "BUILD.bazel"),
	})
	fileReader, fileReaderErr := e.GetFileReaderValue(&model_analysis_pb.FileReader_Key{})
	if fileReaderErr != nil {
		if !errors.Is(fileReaderErr, evaluation.ErrMissingDependency) {
			return model_core.PatchedMessage[*model_analysis_pb.Package_Value, dag.ObjectContentsWalker]{
				Message: &model_analysis_pb.Package_Value{
					Result: &model_analysis_pb.Package_Value_Failure{
						Failure: fileReaderErr.Error(),
					},
				},
				Patcher: model_core.NewReferenceMessagePatcher[dag.ObjectContentsWalker](),
			}, nil
		}
	}

	if !buildFileProperties.IsSet() {
		return model_core.PatchedMessage[*model_analysis_pb.Package_Value, dag.ObjectContentsWalker]{}, evaluation.ErrMissingDependency
	}
	switch buildFilePropertiesResult := buildFileProperties.Message.Result.(type) {
	case *model_analysis_pb.FileProperties_Value_Exists:
		if fileReaderErr != nil {
			return model_core.PatchedMessage[*model_analysis_pb.Package_Value, dag.ObjectContentsWalker]{}, fileReaderErr
		}

		buildFileContentsEntry, err := model_filesystem.NewFileContentsEntryFromProto(
			model_core.Message[*model_filesystem_pb.FileContents]{
				Message:            buildFilePropertiesResult.Exists.GetContents(),
				OutgoingReferences: buildFileProperties.OutgoingReferences,
			},
			c.buildSpecificationReference.GetReferenceFormat(),
		)
		if err != nil {
			return model_core.PatchedMessage[*model_analysis_pb.Package_Value, dag.ObjectContentsWalker]{
				Message: &model_analysis_pb.Package_Value{
					Result: &model_analysis_pb.Package_Value_Failure{
						Failure: util.StatusWrap(err, "Invalid file contents").Error(),
					},
				},
				Patcher: model_core.NewReferenceMessagePatcher[dag.ObjectContentsWalker](),
			}, nil
		}
		buildFileData, err := fileReader.FileReadAll(ctx, buildFileContentsEntry, 1<<20)
		if err != nil {
			return model_core.PatchedMessage[*model_analysis_pb.Package_Value, dag.ObjectContentsWalker]{}, err
		}

		_, program, err := starlark.SourceProgramOptions(
			&syntax.FileOptions{},
			"TODO FILENAME GOES HERE",
			buildFileData,
			/* isPredeclared = */ func(string) bool { return false },
		)
		if err != nil {
			return model_core.PatchedMessage[*model_analysis_pb.Package_Value, dag.ObjectContentsWalker]{
				Message: &model_analysis_pb.Package_Value{
					Result: &model_analysis_pb.Package_Value_Failure{
						Failure: util.StatusWrap(err, "Failed to load BUILD.bazel").Error(),
					},
				},
				Patcher: model_core.NewReferenceMessagePatcher[dag.ObjectContentsWalker](),
			}, nil
		}

		if err := c.preloadBzlGlobals(e, canonicalPackage, program, false); err != nil {
			if !errors.Is(err, evaluation.ErrMissingDependency) {
				return model_core.PatchedMessage[*model_analysis_pb.Package_Value, dag.ObjectContentsWalker]{
					Message: &model_analysis_pb.Package_Value{
						Result: &model_analysis_pb.Package_Value_Failure{
							Failure: err.Error(),
						},
					},
					Patcher: model_core.NewReferenceMessagePatcher[dag.ObjectContentsWalker](),
				}, nil
			}
			return model_core.PatchedMessage[*model_analysis_pb.Package_Value, dag.ObjectContentsWalker]{}, err
		}

		panic("TODO")
	case *model_analysis_pb.FileProperties_Value_Failure:
		return model_core.PatchedMessage[*model_analysis_pb.Package_Value, dag.ObjectContentsWalker]{
			Message: &model_analysis_pb.Package_Value{
				Result: &model_analysis_pb.Package_Value_Failure{
					Failure: buildFilePropertiesResult.Failure,
				},
			},
			Patcher: model_core.NewReferenceMessagePatcher[dag.ObjectContentsWalker](),
		}, nil
	default:
		return model_core.PatchedMessage[*model_analysis_pb.Package_Value, dag.ObjectContentsWalker]{
			Message: &model_analysis_pb.Package_Value{
				Result: &model_analysis_pb.Package_Value_Failure{
					Failure: "File properties value has an unknown result type",
				},
			},
			Patcher: model_core.NewReferenceMessagePatcher[dag.ObjectContentsWalker](),
		}, nil
	}
}

func (c *baseComputer) ComputeRepoValue(ctx context.Context, key *model_analysis_pb.Repo_Key, e RepoEnvironment) (model_core.PatchedMessage[*model_analysis_pb.Repo_Value, dag.ObjectContentsWalker], error) {
	canonicalRepo, err := label.NewCanonicalRepo(key.CanonicalRepo)
	if err != nil {
		return model_core.PatchedMessage[*model_analysis_pb.Repo_Value, dag.ObjectContentsWalker]{
			Message: &model_analysis_pb.Repo_Value{
				Result: &model_analysis_pb.Repo_Value_Failure{
					Failure: util.StatusWrapf(err, "Invalid canonical repo %#v", key.CanonicalRepo).Error(),
				},
			},
			Patcher: model_core.NewReferenceMessagePatcher[dag.ObjectContentsWalker](),
		}, nil
	}

	buildSpecification := e.GetBuildSpecificationValue(&model_analysis_pb.BuildSpecification_Key{})
	if !buildSpecification.IsSet() {
		return model_core.PatchedMessage[*model_analysis_pb.Repo_Value, dag.ObjectContentsWalker]{}, evaluation.ErrMissingDependency
	}

	// TODO: Actually implement proper handling of MODULE.bazel.

	moduleName := canonicalRepo.GetModule().String()
	modules := buildSpecification.Message.BuildSpecification.GetModules()
	if i := sort.Search(
		len(modules),
		func(i int) bool { return modules[i].Name >= moduleName },
	); i < len(modules) && modules[i].Name == moduleName {
		module := modules[i]
		rootDirectoryReference := model_core.NewPatchedMessageFromExisting(
			model_core.Message[*model_core_pb.Reference]{
				Message:            module.RootDirectoryReference,
				OutgoingReferences: buildSpecification.OutgoingReferences,
			},
			func(reference object.LocalReference) dag.ObjectContentsWalker {
				return dag.ExistingObjectContentsWalker
			},
		)

		return model_core.PatchedMessage[*model_analysis_pb.Repo_Value, dag.ObjectContentsWalker]{
			Message: &model_analysis_pb.Repo_Value{
				Result: &model_analysis_pb.Repo_Value_RootDirectoryReference{
					RootDirectoryReference: rootDirectoryReference.Message,
				},
			},
			Patcher: rootDirectoryReference.Patcher,
		}, nil
	}

	return model_core.PatchedMessage[*model_analysis_pb.Repo_Value, dag.ObjectContentsWalker]{
		Message: &model_analysis_pb.Repo_Value{
			Result: &model_analysis_pb.Repo_Value_Failure{
				Failure: fmt.Sprintf("Module %#v not found", moduleName),
			},
		},
		Patcher: model_core.NewReferenceMessagePatcher[dag.ObjectContentsWalker](),
	}, nil
}

func (c *baseComputer) ComputeTargetCompletionValue(ctx context.Context, key *model_analysis_pb.TargetCompletion_Key, e TargetCompletionEnvironment) (model_core.PatchedMessage[*model_analysis_pb.TargetCompletion_Value, dag.ObjectContentsWalker], error) {
	configuredTarget := e.GetConfiguredTargetValue(&model_analysis_pb.ConfiguredTarget_Key{
		Label: key.Label,
	})
	if !configuredTarget.IsSet() {
		return model_core.PatchedMessage[*model_analysis_pb.TargetCompletion_Value, dag.ObjectContentsWalker]{}, evaluation.ErrMissingDependency
	}
	panic(protojson.Format(configuredTarget.Message))
	return model_core.PatchedMessage[*model_analysis_pb.TargetCompletion_Value, dag.ObjectContentsWalker]{
		Message: &model_analysis_pb.TargetCompletion_Value{},
		Patcher: model_core.NewReferenceMessagePatcher[dag.ObjectContentsWalker](),
	}, nil
}
