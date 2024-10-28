package analysis

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"path"
	"sort"

	"github.com/buildbarn/bb-playground/pkg/evaluation"
	"github.com/buildbarn/bb-playground/pkg/label"
	model_core "github.com/buildbarn/bb-playground/pkg/model/core"
	model_filesystem "github.com/buildbarn/bb-playground/pkg/model/filesystem"
	model_starlark "github.com/buildbarn/bb-playground/pkg/model/starlark"
	model_analysis_pb "github.com/buildbarn/bb-playground/pkg/proto/model/analysis"
	model_filesystem_pb "github.com/buildbarn/bb-playground/pkg/proto/model/filesystem"
	model_starlark_pb "github.com/buildbarn/bb-playground/pkg/proto/model/starlark"
	"github.com/buildbarn/bb-playground/pkg/storage/dag"

	"go.starlark.net/starlark"
	"go.starlark.net/syntax"
)

func (c *baseComputer) ComputeCompiledBzlFileValue(ctx context.Context, key *model_analysis_pb.CompiledBzlFile_Key, e CompiledBzlFileEnvironment) (PatchedCompiledBzlFileValue, error) {
	canonicalLabel, err := label.NewCanonicalLabel(key.Label)
	if err != nil {
		return model_core.NewSimplePatchedMessage[dag.ObjectContentsWalker](&model_analysis_pb.CompiledBzlFile_Value{
			Result: &model_analysis_pb.CompiledBzlFile_Value_Failure{
				Failure: fmt.Sprintf("invalid label %#v: %s", key.Label, err),
			},
		}), nil
	}
	canonicalPackage := canonicalLabel.GetCanonicalPackage()
	canonicalRepo := canonicalPackage.GetCanonicalRepo()

	bzlFileProperties := e.GetFilePropertiesValue(&model_analysis_pb.FileProperties_Key{
		CanonicalRepo: canonicalRepo.String(),
		Path:          path.Join(canonicalPackage.GetPackagePath(), canonicalLabel.GetTargetName().String()),
	})
	fileReader, fileReaderErr := e.GetFileReaderValue(&model_analysis_pb.FileReader_Key{})
	if fileReaderErr != nil && !errors.Is(fileReaderErr, evaluation.ErrMissingDependency) {
		return model_core.NewSimplePatchedMessage[dag.ObjectContentsWalker](&model_analysis_pb.CompiledBzlFile_Value{
			Result: &model_analysis_pb.CompiledBzlFile_Value_Failure{
				Failure: fileReaderErr.Error(),
			},
		}), nil
	}
	bzlFileBuiltins, bzlFileBuiltinsErr := c.getBzlFileBuiltins(e, key.BuiltinsModuleNames, model_starlark.BzlFileBuiltins, "exported_toplevels")
	if bzlFileBuiltinsErr != nil && !errors.Is(bzlFileBuiltinsErr, evaluation.ErrMissingDependency) {
		return model_core.NewSimplePatchedMessage[dag.ObjectContentsWalker](&model_analysis_pb.CompiledBzlFile_Value{
			Result: &model_analysis_pb.CompiledBzlFile_Value_Failure{
				Failure: bzlFileBuiltinsErr.Error(),
			},
		}), nil
	}

	if !bzlFileProperties.IsSet() {
		return PatchedCompiledBzlFileValue{}, evaluation.ErrMissingDependency
	}
	switch bzlFilePropertiesResult := bzlFileProperties.Message.Result.(type) {
	case *model_analysis_pb.FileProperties_Value_Exists:
		if fileReaderErr != nil {
			return PatchedCompiledBzlFileValue{}, fileReaderErr
		}
		if bzlFileBuiltinsErr != nil {
			return PatchedCompiledBzlFileValue{}, bzlFileBuiltinsErr
		}

		buildFileContentsEntry, err := model_filesystem.NewFileContentsEntryFromProto(
			model_core.Message[*model_filesystem_pb.FileContents]{
				Message:            bzlFilePropertiesResult.Exists.GetContents(),
				OutgoingReferences: bzlFileProperties.OutgoingReferences,
			},
			c.buildSpecificationReference.GetReferenceFormat(),
		)
		if err != nil {
			return model_core.NewSimplePatchedMessage[dag.ObjectContentsWalker](&model_analysis_pb.CompiledBzlFile_Value{
				Result: &model_analysis_pb.CompiledBzlFile_Value_Failure{
					Failure: fmt.Sprintf("invalid file contents: %s", err),
				},
			}), nil
		}
		bzlFileData, err := fileReader.FileReadAll(ctx, buildFileContentsEntry, 1<<20)
		if err != nil {
			return PatchedCompiledBzlFileValue{}, err
		}

		_, program, err := starlark.SourceProgramOptions(
			&syntax.FileOptions{},
			canonicalLabel.String(),
			bzlFileData,
			bzlFileBuiltins.Has,
		)
		if err != nil {
			return model_core.NewSimplePatchedMessage[dag.ObjectContentsWalker](&model_analysis_pb.CompiledBzlFile_Value{
				Result: &model_analysis_pb.CompiledBzlFile_Value_Failure{
					Failure: err.Error(),
				},
			}), nil
		}

		if err := c.preloadBzlGlobals(e, canonicalPackage, program, key.BuiltinsModuleNames); err != nil {
			if !errors.Is(err, evaluation.ErrMissingDependency) {
				return model_core.NewSimplePatchedMessage[dag.ObjectContentsWalker](&model_analysis_pb.CompiledBzlFile_Value{
					Result: &model_analysis_pb.CompiledBzlFile_Value_Failure{
						Failure: err.Error(),
					},
				}), nil
			}
			return PatchedCompiledBzlFileValue{}, err
		}

		thread := c.newStarlarkThread(e, key.BuiltinsModuleNames)
		globals, err := program.Init(thread, bzlFileBuiltins)
		if err != nil {
			if !errors.Is(err, evaluation.ErrMissingDependency) {
				var evalErr *starlark.EvalError
				if errors.As(err, &evalErr) {
					return PatchedCompiledBzlFileValue{}, errors.New(evalErr.Backtrace())
				}
			}
			return PatchedCompiledBzlFileValue{}, err
		}
		model_starlark.NameAndExtractGlobals(globals, canonicalLabel)

		// TODO! Use proper encoding options!
		compiledProgram, err := model_starlark.EncodeCompiledProgram(program, globals, c.getValueEncodingOptions(canonicalLabel))
		if err != nil {
			return model_core.NewSimplePatchedMessage[dag.ObjectContentsWalker](&model_analysis_pb.CompiledBzlFile_Value{
				Result: &model_analysis_pb.CompiledBzlFile_Value_Failure{
					Failure: err.Error(),
				},
			}), nil
		}
		return PatchedCompiledBzlFileValue{
			Message: &model_analysis_pb.CompiledBzlFile_Value{
				Result: &model_analysis_pb.CompiledBzlFile_Value_CompiledProgram{
					CompiledProgram: compiledProgram.Message,
				},
			},
			Patcher: compiledProgram.Patcher,
		}, nil
	case *model_analysis_pb.FileProperties_Value_DoesNotExist:
		return model_core.NewSimplePatchedMessage[dag.ObjectContentsWalker](&model_analysis_pb.CompiledBzlFile_Value{
			Result: &model_analysis_pb.CompiledBzlFile_Value_Failure{
				Failure: fmt.Sprintf("%#v does not exist", canonicalLabel.String()),
			},
		}), nil
	case *model_analysis_pb.FileProperties_Value_Failure:
		return model_core.NewSimplePatchedMessage[dag.ObjectContentsWalker](&model_analysis_pb.CompiledBzlFile_Value{
			Result: &model_analysis_pb.CompiledBzlFile_Value_Failure{
				Failure: bzlFilePropertiesResult.Failure,
			},
		}), nil
	default:
		return model_core.NewSimplePatchedMessage[dag.ObjectContentsWalker](&model_analysis_pb.CompiledBzlFile_Value{
			Result: &model_analysis_pb.CompiledBzlFile_Value_Failure{
				Failure: "File properties value has an unknown result type",
			},
		}), nil
	}
}

func (c *baseComputer) ComputeCompiledBzlFileDecodedGlobalsValue(ctx context.Context, key *model_analysis_pb.CompiledBzlFileDecodedGlobals_Key, e CompiledBzlFileDecodedGlobalsEnvironment) (starlark.StringDict, error) {
	compiledBzlFile := e.GetCompiledBzlFileValue(&model_analysis_pb.CompiledBzlFile_Key{
		Label:               key.Label,
		BuiltinsModuleNames: key.BuiltinsModuleNames,
	})
	if !compiledBzlFile.IsSet() {
		return nil, evaluation.ErrMissingDependency
	}

	switch resultType := compiledBzlFile.Message.Result.(type) {
	case *model_analysis_pb.CompiledBzlFile_Value_CompiledProgram:
		currentFilename, err := label.NewCanonicalLabel(key.Label)
		if err != nil {
			return nil, fmt.Errorf("invalid label %#v: %w", key.Label, err)
		}
		return model_starlark.DecodeGlobals(
			model_core.Message[[]*model_starlark_pb.Global]{
				Message:            resultType.CompiledProgram.Globals,
				OutgoingReferences: compiledBzlFile.OutgoingReferences,
			},
			currentFilename,
			c.getValueDecodingOptions(ctx, func(canonicalLabel label.CanonicalLabel) (starlark.Value, error) {
				return model_starlark.NewLabel(canonicalLabel), nil
			}),
		)
	case *model_analysis_pb.CompiledBzlFile_Value_Failure:
		return nil, errors.New(resultType.Failure)
	default:
		return nil, errors.New("File properties value has an unknown result type")
	}
}

func (c *baseComputer) ComputeCompiledBzlFileFunctionsValue(ctx context.Context, key *model_analysis_pb.CompiledBzlFileFunctions_Key, e CompiledBzlFileFunctionsEnvironment) (map[label.StarlarkIdentifier]model_starlark.CallableWithPosition, error) {
	compiledBzlFile := e.GetCompiledBzlFileValue(&model_analysis_pb.CompiledBzlFile_Key{
		Label:               key.Label,
		BuiltinsModuleNames: key.BuiltinsModuleNames,
	})
	bzlFileBuiltins, bzlFileBuiltinsErr := c.getBzlFileBuiltins(e, key.BuiltinsModuleNames, model_starlark.BzlFileBuiltins, "exported_toplevels")
	if !compiledBzlFile.IsSet() {
		return nil, evaluation.ErrMissingDependency
	}
	if bzlFileBuiltinsErr != nil {
		return nil, bzlFileBuiltinsErr
	}

	switch resultType := compiledBzlFile.Message.Result.(type) {
	case *model_analysis_pb.CompiledBzlFile_Value_CompiledProgram:
		canonicalLabel, err := label.NewCanonicalLabel(key.Label)
		if err != nil {
			return nil, err
		}
		program, err := starlark.CompiledProgram(bytes.NewBuffer(resultType.CompiledProgram.Code))
		if err != nil {
			return nil, fmt.Errorf("failed to load previously compiled file %#v: %w", key.Label, err)
		}
		if err := c.preloadBzlGlobals(e, canonicalLabel.GetCanonicalPackage(), program, key.BuiltinsModuleNames); err != nil {
			return nil, err
		}

		thread := c.newStarlarkThread(e, key.BuiltinsModuleNames)
		globals, err := program.Init(thread, bzlFileBuiltins)
		if err != nil {
			return nil, err
		}
		model_starlark.NameAndExtractGlobals(globals, canonicalLabel)
		globals.Freeze()

		callables := map[label.StarlarkIdentifier]model_starlark.CallableWithPosition{}
		for name, value := range globals {
			if callable, ok := value.(model_starlark.CallableWithPosition); ok {
				callables[label.MustNewStarlarkIdentifier(name)] = callable
			}
		}
		return callables, nil
	case *model_analysis_pb.CompiledBzlFile_Value_Failure:
		return nil, errors.New(resultType.Failure)
	default:
		return nil, errors.New("compiled .bzl file value has an unknown result type")
	}
}

func (c *baseComputer) ComputeCompiledBzlFileGlobalValue(ctx context.Context, key *model_analysis_pb.CompiledBzlFileGlobal_Key, e CompiledBzlFileGlobalEnvironment) (PatchedCompiledBzlFileGlobalValue, error) {
	identifier, err := label.NewCanonicalStarlarkIdentifier(key.Identifier)
	if err != nil {
		return model_core.NewSimplePatchedMessage[dag.ObjectContentsWalker](&model_analysis_pb.CompiledBzlFileGlobal_Value{
			Result: &model_analysis_pb.CompiledBzlFileGlobal_Value_Failure{
				Failure: fmt.Sprintf("invalid canonical Starlark identifier %#v: %s", key.Identifier, err),
			},
		}), nil
	}

	allBuiltinsModulesNames := e.GetBuiltinsModuleNamesValue(&model_analysis_pb.BuiltinsModuleNames_Key{})
	if !allBuiltinsModulesNames.IsSet() {
		return PatchedCompiledBzlFileGlobalValue{}, evaluation.ErrMissingDependency
	}

	compiledBzlFile := e.GetCompiledBzlFileValue(&model_analysis_pb.CompiledBzlFile_Key{
		Label:               identifier.GetCanonicalLabel().String(),
		BuiltinsModuleNames: allBuiltinsModulesNames.Message.BuiltinsModuleNames,
	})
	if !compiledBzlFile.IsSet() {
		return PatchedCompiledBzlFileGlobalValue{}, evaluation.ErrMissingDependency
	}
	switch resultType := compiledBzlFile.Message.Result.(type) {
	case *model_analysis_pb.CompiledBzlFile_Value_CompiledProgram:
		globals := resultType.CompiledProgram.Globals
		name := identifier.GetStarlarkIdentifier().String()
		if i := sort.Search(
			len(globals),
			func(i int) bool { return globals[i].Name >= name },
		); i < len(globals) && globals[i].Name == name {
			global := model_core.NewPatchedMessageFromExisting(
				model_core.Message[*model_starlark_pb.Value]{
					Message:            globals[i].Value,
					OutgoingReferences: compiledBzlFile.OutgoingReferences,
				},
				func(index int) dag.ObjectContentsWalker {
					return dag.ExistingObjectContentsWalker
				},
			)
			return PatchedCompiledBzlFileGlobalValue{
				Message: &model_analysis_pb.CompiledBzlFileGlobal_Value{
					Result: &model_analysis_pb.CompiledBzlFileGlobal_Value_Success{
						Success: global.Message,
					},
				},
				Patcher: global.Patcher,
			}, nil
		}
		return model_core.NewSimplePatchedMessage[dag.ObjectContentsWalker](&model_analysis_pb.CompiledBzlFileGlobal_Value{
			Result: &model_analysis_pb.CompiledBzlFileGlobal_Value_Failure{
				Failure: "global does not exist",
			},
		}), nil
	case *model_analysis_pb.CompiledBzlFile_Value_Failure:
		return model_core.NewSimplePatchedMessage[dag.ObjectContentsWalker](&model_analysis_pb.CompiledBzlFileGlobal_Value{
			Result: &model_analysis_pb.CompiledBzlFileGlobal_Value_Failure{
				Failure: resultType.Failure,
			},
		}), nil
	default:
		return model_core.NewSimplePatchedMessage[dag.ObjectContentsWalker](&model_analysis_pb.CompiledBzlFileGlobal_Value{
			Result: &model_analysis_pb.CompiledBzlFileGlobal_Value_Failure{
				Failure: "Compiled .bzl file value has an unknown result type",
			},
		}), nil
	}
}
