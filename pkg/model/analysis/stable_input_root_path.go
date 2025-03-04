package analysis

import (
	"context"
	"fmt"
	"strings"

	"github.com/buildbarn/bb-storage/pkg/filesystem/path"
	"github.com/buildbarn/bonanza/pkg/evaluation"
	model_core "github.com/buildbarn/bonanza/pkg/model/core"
	model_filesystem "github.com/buildbarn/bonanza/pkg/model/filesystem"
	model_starlark "github.com/buildbarn/bonanza/pkg/model/starlark"
	model_analysis_pb "github.com/buildbarn/bonanza/pkg/proto/model/analysis"
	model_command_pb "github.com/buildbarn/bonanza/pkg/proto/model/command"
	model_filesystem_pb "github.com/buildbarn/bonanza/pkg/proto/model/filesystem"
	"github.com/buildbarn/bonanza/pkg/storage/dag"

	"google.golang.org/protobuf/types/known/durationpb"
)

func (c *baseComputer) ComputeStableInputRootPathValue(ctx context.Context, key *model_analysis_pb.StableInputRootPath_Key, e StableInputRootPathEnvironment) (PatchedStableInputRootPathValue, error) {
	commandEncoder, gotCommandEncoder := e.GetCommandEncoderObjectValue(&model_analysis_pb.CommandEncoderObject_Key{})
	directoryCreationParameters, gotDirectoryCreationParameters := e.GetDirectoryCreationParametersObjectValue(&model_analysis_pb.DirectoryCreationParametersObject_Key{})
	directoryCreationParametersValue := e.GetDirectoryCreationParametersValue(&model_analysis_pb.DirectoryCreationParameters_Key{})
	directoryDereferencers, gotDirectoryDereferencers := e.GetDirectoryDereferencersValue(&model_analysis_pb.DirectoryDereferencers_Key{})
	fileCreationParametersValue := e.GetFileCreationParametersValue(&model_analysis_pb.FileCreationParameters_Key{})
	fileReader, gotFileReader := e.GetFileReaderValue(&model_analysis_pb.FileReader_Key{})
	repoPlatform := e.GetRegisteredRepoPlatformValue(&model_analysis_pb.RegisteredRepoPlatform_Key{})
	if !gotCommandEncoder ||
		!gotDirectoryCreationParameters ||
		!directoryCreationParametersValue.IsSet() ||
		!gotDirectoryDereferencers ||
		!fileCreationParametersValue.IsSet() ||
		!gotFileReader ||
		!repoPlatform.IsSet() {
		return PatchedStableInputRootPathValue{}, evaluation.ErrMissingDependency
	}

	// Construct a command that simply invokes "pwd" inside of the
	// stable input root path.
	environment := map[string]string{}
	for _, environmentVariable := range repoPlatform.Message.RepositoryOsEnviron {
		environment[environmentVariable.Name] = environmentVariable.Value
	}
	environmentVariableList, err := c.convertDictToEnvironmentVariableList(environment, commandEncoder)
	if err != nil {
		return PatchedStableInputRootPathValue{}, err
	}

	referenceFormat := c.buildSpecificationReference.GetReferenceFormat()
	createdCommand, err := model_core.MarshalAndEncodePatchedMessage(
		model_core.NewPatchedMessage(
			&model_command_pb.Command{
				Arguments: []*model_command_pb.ArgumentList_Element{{
					Level: &model_command_pb.ArgumentList_Element_Leaf{
						Leaf: "pwd",
					},
				}},
				EnvironmentVariables:        environmentVariableList.Message,
				DirectoryCreationParameters: directoryCreationParametersValue.Message.DirectoryCreationParameters,
				FileCreationParameters:      fileCreationParametersValue.Message.FileCreationParameters,
				WorkingDirectory:            path.EmptyBuilder.GetUNIXString(),
				NeedsStableInputRootPath:    true,
			},
			environmentVariableList.Patcher,
		),
		referenceFormat,
		commandEncoder,
	)
	if err != nil {
		return PatchedStableInputRootPathValue{}, fmt.Errorf("failed to create command: %w", err)
	}

	createdInputRoot, err := model_core.MarshalAndEncodePatchedMessage(
		model_core.NewSimplePatchedMessage[dag.ObjectContentsWalker](
			&model_filesystem_pb.Directory{
				Leaves: &model_filesystem_pb.Directory_LeavesInline{
					LeavesInline: &model_filesystem_pb.Leaves{},
				},
			},
		),
		referenceFormat,
		directoryCreationParameters.GetEncoder(),
	)
	if err != nil {
		return PatchedStableInputRootPathValue{}, fmt.Errorf("failed to create input root: %w", err)
	}

	// Invoke "pwd".
	keyPatcher := model_core.NewReferenceMessagePatcher[dag.ObjectContentsWalker]()
	actionResult := e.GetActionResultValue(PatchedActionResultKey{
		Message: &model_analysis_pb.ActionResult_Key{
			PlatformPkixPublicKey: repoPlatform.Message.ExecPkixPublicKey,
			CommandReference: keyPatcher.AddReference(
				createdCommand.Contents.GetReference(),
				dag.NewSimpleObjectContentsWalker(createdCommand.Contents, createdCommand.Metadata),
			),
			InputRootReference: keyPatcher.AddReference(
				createdInputRoot.Contents.GetReference(),
				dag.NewSimpleObjectContentsWalker(createdInputRoot.Contents, createdInputRoot.Metadata),
			),
			ExecutionTimeout:   &durationpb.Duration{Seconds: 60},
			ExitCodeMustBeZero: true,
		},
		Patcher: keyPatcher,
	})
	if !actionResult.IsSet() {
		return PatchedStableInputRootPathValue{}, evaluation.ErrMissingDependency
	}

	// Capture the standard output of "pwd" and trim the trailing
	// newline character that it adds.
	outputs, err := c.getOutputsFromActionResult(ctx, actionResult, directoryDereferencers)
	if err != nil {
		return PatchedStableInputRootPathValue{}, fmt.Errorf("failed to obtain outputs from action result: %w", err)
	}

	stdoutEntry, err := model_filesystem.NewFileContentsEntryFromProto(
		model_core.NewNestedMessage(outputs, outputs.Message.Stdout),
		referenceFormat,
	)
	if err != nil {
		return PatchedStableInputRootPathValue{}, fmt.Errorf("invalid standard output entry: %w", err)
	}
	stdout, err := fileReader.FileReadAll(ctx, stdoutEntry, 1<<20)
	if err != nil {
		return PatchedStableInputRootPathValue{}, fmt.Errorf("failed to read standard output: %w", err)
	}

	return model_core.NewSimplePatchedMessage[dag.ObjectContentsWalker](
		&model_analysis_pb.StableInputRootPath_Value{
			InputRootPath: strings.TrimSuffix(string(stdout), "\n"),
		},
	), nil
}

func (c *baseComputer) ComputeStableInputRootPathObjectValue(ctx context.Context, key *model_analysis_pb.StableInputRootPathObject_Key, e StableInputRootPathObjectEnvironment) (*model_starlark.BarePath, error) {
	stableInputRootPath := e.GetStableInputRootPathValue(&model_analysis_pb.StableInputRootPath_Key{})
	if !stableInputRootPath.IsSet() {
		return nil, evaluation.ErrMissingDependency
	}
	// TODO: This currently assumes UNIX-based paths. We should
	// likely add an option on the platform that controls the
	// pathname format.
	var resolver model_starlark.PathResolver
	if err := path.Resolve(path.UNIXFormat.NewParser(stableInputRootPath.Message.InputRootPath), &resolver); err != nil {
		return nil, fmt.Errorf("failed to resolve stable input root path: %w", err)
	}
	return resolver.CurrentPath, nil
}
