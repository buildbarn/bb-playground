package command

import (
	"context"
	"errors"
	"io/fs"
	"sync"
	"time"

	model_core "github.com/buildbarn/bb-playground/pkg/model/core"
	model_encoding "github.com/buildbarn/bb-playground/pkg/model/encoding"
	model_filesystem "github.com/buildbarn/bb-playground/pkg/model/filesystem"
	pg_vfs "github.com/buildbarn/bb-playground/pkg/model/filesystem/virtual"
	model_parser "github.com/buildbarn/bb-playground/pkg/model/parser"
	model_command_pb "github.com/buildbarn/bb-playground/pkg/proto/model/command"
	model_filesystem_pb "github.com/buildbarn/bb-playground/pkg/proto/model/filesystem"
	remoteworker_pb "github.com/buildbarn/bb-playground/pkg/proto/remoteworker"
	dag_pb "github.com/buildbarn/bb-playground/pkg/proto/storage/dag"
	"github.com/buildbarn/bb-playground/pkg/remoteworker"
	"github.com/buildbarn/bb-playground/pkg/storage/dag"
	"github.com/buildbarn/bb-playground/pkg/storage/object"
	object_namespacemapping "github.com/buildbarn/bb-playground/pkg/storage/object/namespacemapping"
	re_clock "github.com/buildbarn/bb-remote-execution/pkg/clock"
	re_filesystem "github.com/buildbarn/bb-remote-execution/pkg/filesystem"
	"github.com/buildbarn/bb-remote-execution/pkg/filesystem/virtual"
	runner_pb "github.com/buildbarn/bb-remote-execution/pkg/proto/runner"
	"github.com/buildbarn/bb-storage/pkg/clock"
	"github.com/buildbarn/bb-storage/pkg/filesystem/path"
	"github.com/buildbarn/bb-storage/pkg/util"
	"github.com/google/uuid"

	"golang.org/x/sync/semaphore"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// Filenames of objects to be created inside the build directory.
var (
	stdoutComponent              = path.MustNewComponent("stdout")
	stderrComponent              = path.MustNewComponent("stderr")
	inputRootDirectoryComponent  = path.MustNewComponent("root")
	serverLogsDirectoryComponent = path.MustNewComponent("server_logs")
	temporaryDirectoryComponent  = path.MustNewComponent("tmp")
	checkReadinessComponent      = path.MustNewComponent("check_readiness")
	stableInputRootComponent     = path.MustNewComponent("stable")
)

// capturingErrorLogger is an error logger that stores up to a single
// error. When the error is stored, a context cancelation function is
// invoked. This is used by localBuildExecutor to kill a build action in
// case an I/O error occurs on the FUSE file system.
type capturingErrorLogger struct {
	lock   sync.Mutex
	cancel context.CancelFunc
	error  error
}

func (el *capturingErrorLogger) Log(err error) {
	el.lock.Lock()
	defer el.lock.Unlock()

	if el.cancel != nil {
		el.error = err
		el.cancel()
		el.cancel = nil
	}
}

func (el *capturingErrorLogger) GetError() error {
	el.lock.Lock()
	defer el.lock.Unlock()

	return el.error
}

type TopLevelDirectory interface {
	AddChild(ctx context.Context, name path.Component, child virtual.DirectoryChild) error
	RemoveChild(name path.Component)
}

type localExecutor struct {
	objectDownloader               object.Downloader[object.GlobalReference]
	dagUploaderClient              dag_pb.UploaderClient
	objectContentsWalkerSemaphore  *semaphore.Weighted
	topLevelDirectory              TopLevelDirectory
	handleAllocator                virtual.StatefulHandleAllocator
	filePool                       re_filesystem.FilePool
	symlinkFactory                 virtual.SymlinkFactory
	initialContentsSorter          virtual.Sorter
	hiddenFilesMatcher             virtual.StringMatcher
	runner                         runner_pb.RunnerClient
	clock                          clock.Clock
	uuidGenerator                  util.UUIDGenerator
	maximumWritableFileUploadDelay time.Duration
	environmentVariables           map[string]string
}

func NewLocalExecutor(
	objectDownloader object.Downloader[object.GlobalReference],
	dagUploaderClient dag_pb.UploaderClient,
	objectContentsWalkerSemaphore *semaphore.Weighted,
	topLevelDirectory TopLevelDirectory,
	handleAllocator virtual.StatefulHandleAllocator,
	filePool re_filesystem.FilePool,
	symlinkFactory virtual.SymlinkFactory,
	initialContentsSorter virtual.Sorter,
	hiddenFilesMatcher virtual.StringMatcher,
	runner runner_pb.RunnerClient,
	clock clock.Clock,
	uuidGenerator util.UUIDGenerator,
	maximumWritableFileUploadDelay time.Duration,
	environmentVariables map[string]string,
) remoteworker.Executor[*model_command_pb.Action] {
	return &localExecutor{
		objectDownloader:               objectDownloader,
		dagUploaderClient:              dagUploaderClient,
		objectContentsWalkerSemaphore:  objectContentsWalkerSemaphore,
		topLevelDirectory:              topLevelDirectory,
		handleAllocator:                handleAllocator,
		filePool:                       filePool,
		symlinkFactory:                 symlinkFactory,
		initialContentsSorter:          initialContentsSorter,
		hiddenFilesMatcher:             hiddenFilesMatcher,
		runner:                         runner,
		clock:                          clock,
		uuidGenerator:                  uuidGenerator,
		maximumWritableFileUploadDelay: maximumWritableFileUploadDelay,
		environmentVariables:           environmentVariables,
	}
}

func (e *localExecutor) CheckReadiness(ctx context.Context) error {
	return nil
}

func (e *localExecutor) gatherArguments(elements model_core.Message[[]*model_command_pb.ArgumentList_Element], out *[]string) error {
	for _, element := range elements.Message {
		switch level := element.Level.(type) {
		case *model_command_pb.ArgumentList_Element_Leaf:
			*out = append(*out, level.Leaf)
		case *model_command_pb.ArgumentList_Element_Parent:
			panic("TODO")
		default:
			return status.Error(codes.InvalidArgument, "Argument list element has an unknown level type")
		}
	}
	return nil
}

func (e *localExecutor) gatherEnvironmentVariables(elements model_core.Message[[]*model_command_pb.EnvironmentVariableList_Element], out map[string]string) error {
	for _, element := range elements.Message {
		switch level := element.Level.(type) {
		case *model_command_pb.EnvironmentVariableList_Element_Leaf_:
			out[level.Leaf.Name] = level.Leaf.Value
		case *model_command_pb.EnvironmentVariableList_Element_Parent:
			panic("TODO")
		default:
			return status.Error(codes.InvalidArgument, "Environment variable list element has an unknown level type")
		}
	}
	return nil
}

func captureLog(ctx context.Context, buildDirectory virtual.PrepopulatedDirectory, name path.Component, writableFileUploadDelay <-chan struct{}, fileCreationParameters *model_filesystem.FileCreationParameters) (model_core.PatchedMessage[*model_filesystem_pb.FileContents, dag.ObjectContentsWalker], error) {
	stdoutFile, err := buildDirectory.LookupChild(stdoutComponent)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return model_core.PatchedMessage[*model_filesystem_pb.FileContents, dag.ObjectContentsWalker]{}, nil
		}
		return model_core.PatchedMessage[*model_filesystem_pb.FileContents, dag.ObjectContentsWalker]{}, util.StatusWrap(err, "Failed to look up file")
	}

	openReadFrozen := virtual.ApplyOpenReadFrozen{
		WritableFileDelay: writableFileUploadDelay,
	}
	if stdoutFile.GetNode().VirtualApply(&openReadFrozen) {
		if openReadFrozen.Err != nil {
			return model_core.PatchedMessage[*model_filesystem_pb.FileContents, dag.ObjectContentsWalker]{}, util.StatusWrap(openReadFrozen.Err, "Failed to open file")
		}
		fileContents, err := model_filesystem.CreateChunkDiscardingFileMerkleTree(ctx, fileCreationParameters, openReadFrozen.Reader)
		if err != nil {
			return model_core.PatchedMessage[*model_filesystem_pb.FileContents, dag.ObjectContentsWalker]{}, util.StatusWrap(openReadFrozen.Err, "Failed to create file Merkle tree")
		}
		return fileContents, nil
	}

	return model_core.PatchedMessage[*model_filesystem_pb.FileContents, dag.ObjectContentsWalker]{}, status.Error(codes.InvalidArgument, "File is of an incorrect type")
}

func (e *localExecutor) Execute(ctx context.Context, action *model_command_pb.Action, executionTimeout time.Duration, executionEvents chan<- proto.Message) (proto.Message, time.Duration, remoteworker_pb.CurrentState_Completed_Result) {
	namespace, err := object.NewNamespace(action.Namespace)
	if err != nil {
		return &model_command_pb.Result{
			Status: status.Convert(util.StatusWrap(err, "Invalid namespace")).Proto(),
		}, 0, remoteworker_pb.CurrentState_Completed_FAILED
	}
	objectDownloader := object_namespacemapping.NewNamespaceAddingDownloader(e.objectDownloader, namespace)

	// Fetch the Command message, so that we know the arguments and
	// environment variables of the process to spawn.
	commandEncoder, err := model_encoding.NewBinaryEncoderFromProto(
		action.CommandEncoders,
		uint32(namespace.ReferenceFormat.GetMaximumObjectSizeBytes()),
	)
	if err != nil {
		return &model_command_pb.Result{
			Status: status.Convert(util.StatusWrap(err, "Invalid command encoders")).Proto(),
		}, 0, remoteworker_pb.CurrentState_Completed_FAILED
	}
	commandReader := model_parser.NewStorageBackedParsedObjectReader(
		objectDownloader,
		commandEncoder,
		model_parser.NewMessageObjectParser[object.LocalReference, model_command_pb.Command](),
	)

	commandReference, err := namespace.NewLocalReference(action.CommandReference)
	if err != nil {
		return &model_command_pb.Result{
			Status: status.Convert(util.StatusWrap(err, "Invalid command reference")).Proto(),
		}, 0, remoteworker_pb.CurrentState_Completed_FAILED
	}
	command, _, err := commandReader.ReadParsedObject(ctx, commandReference)
	if err != nil {
		return &model_command_pb.Result{
			Status: status.Convert(util.StatusWrap(err, "Failed to read command")).Proto(),
		}, 0, remoteworker_pb.CurrentState_Completed_FAILED
	}

	// Convert arguments and environment variables stored in B-trees
	// backed by storage to plain lists, so that they can be sent to
	// the runner.
	var arguments []string
	if err := e.gatherArguments(
		model_core.Message[[]*model_command_pb.ArgumentList_Element]{
			Message:            command.Message.Arguments,
			OutgoingReferences: command.OutgoingReferences,
		},
		&arguments,
	); err != nil {
		return &model_command_pb.Result{
			Status: status.Convert(util.StatusWrap(err, "Failed to gather arguments")).Proto(),
		}, 0, remoteworker_pb.CurrentState_Completed_FAILED
	}

	environmentVariables := map[string]string{}
	if err := e.gatherEnvironmentVariables(
		model_core.Message[[]*model_command_pb.EnvironmentVariableList_Element]{
			Message:            command.Message.EnvironmentVariables,
			OutgoingReferences: command.OutgoingReferences,
		},
		environmentVariables,
	); err != nil {
		return &model_command_pb.Result{
			Status: status.Convert(util.StatusWrap(err, "Failed to gather environment variables")).Proto(),
		}, 0, remoteworker_pb.CurrentState_Completed_FAILED
	}

	// Error logger to terminate execution and capture I/O error events.
	ctxWithIOError, cancelIOError := context.WithCancel(ctx)
	defer cancelIOError()
	ioErrorCapturer := &capturingErrorLogger{cancel: cancelIOError}

	fileCreationParameters, err := model_filesystem.NewFileCreationParametersFromProto(
		command.Message.FileCreationParameters,
		namespace.ReferenceFormat,
	)
	if err != nil {
		return &model_command_pb.Result{
			Status: status.Convert(util.StatusWrap(err, "Invalid file creation parameters")).Proto(),
		}, 0, remoteworker_pb.CurrentState_Completed_FAILED
	}
	directoryCreationParameters, err := model_filesystem.NewDirectoryCreationParametersFromProto(
		command.Message.DirectoryCreationParameters,
		namespace.ReferenceFormat,
	)
	if err != nil {
		return &model_command_pb.Result{
			Status: status.Convert(util.StatusWrap(err, "Invalid file creation parameters")).Proto(),
		}, 0, remoteworker_pb.CurrentState_Completed_FAILED
	}
	directoryEncoder := directoryCreationParameters.GetEncoder()

	// Create build directory and expose it via the virtual file system.
	buildDirectory := virtual.NewInMemoryPrepopulatedDirectory(
		virtual.NewHandleAllocatingFileAllocator(
			virtual.NewPoolBackedFileAllocator(e.filePool, ioErrorCapturer),
			e.handleAllocator,
		),
		e.symlinkFactory,
		ioErrorCapturer,
		e.handleAllocator,
		e.initialContentsSorter,
		e.hiddenFilesMatcher,
		e.clock,
	)
	defer buildDirectory.RemoveAllChildren(true)

	// Create subdirectories that should be present when the command
	// is executed, such as the input root directory.
	//
	// TODO: Add caching to the input root!
	inputRootReference, err := namespace.NewLocalReference(action.InputRootReference)
	if err != nil {
		return &model_command_pb.Result{
			Status: status.Convert(util.StatusWrap(err, "Invalid input root reference")).Proto(),
		}, 0, remoteworker_pb.CurrentState_Completed_FAILED
	}
	if err := buildDirectory.CreateChildren(map[path.Component]virtual.InitialChild{
		inputRootDirectoryComponent: virtual.InitialChild{}.FromDirectory(
			pg_vfs.NewObjectBackedInitialContentsFetcher(
				ctxWithIOError,
				model_parser.NewStorageBackedParsedObjectReader(
					objectDownloader,
					directoryEncoder,
					model_filesystem.NewDirectoryClusterObjectParser[object.LocalReference](
						model_parser.NewStorageBackedParsedObjectReader(
							objectDownloader,
							directoryEncoder,
							model_parser.NewMessageObjectParser[object.LocalReference, model_filesystem_pb.Leaves](),
						),
					),
				),
				pg_vfs.NewStatelessHandleAllocatingFileFactory(
					pg_vfs.NewObjectBackedFileFactory(
						ctxWithIOError,
						model_filesystem.NewFileReader(
							model_parser.NewStorageBackedParsedObjectReader(
								objectDownloader,
								fileCreationParameters.GetFileContentsListEncoder(),
								model_filesystem.NewFileContentsListObjectParser[object.LocalReference](),
							),
							model_parser.NewStorageBackedParsedObjectReader(
								objectDownloader,
								fileCreationParameters.GetChunkEncoder(),
								model_parser.NewRawObjectParser[object.LocalReference](),
							),
						),
						ioErrorCapturer,
					),
					e.handleAllocator.New(),
				),
				e.symlinkFactory,
				inputRootReference,
			),
		),
		serverLogsDirectoryComponent: virtual.InitialChild{}.FromDirectory(virtual.EmptyInitialContentsFetcher),
		temporaryDirectoryComponent:  virtual.InitialChild{}.FromDirectory(virtual.EmptyInitialContentsFetcher),
	}, false); err != nil {
		return &model_command_pb.Result{
			Status: status.Convert(util.StatusWrap(err, "Failed to create initial children of build directory")).Proto(),
		}, 0, remoteworker_pb.CurrentState_Completed_FAILED
	}

	// If the command requires a stable input root path, we should
	// attach the build directory name under a fixed name. We
	// ideally don't want to do this, because it limits prevents any
	// form of parallelism.
	var buildDirectoryName path.Component
	if command.Message.NeedsStableInputRootPath {
		buildDirectoryName = stableInputRootComponent
	} else {
		buildDirectoryName = path.MustNewComponent(uuid.Must(e.uuidGenerator()).String())
	}

	if err := e.topLevelDirectory.AddChild(ctx, buildDirectoryName, virtual.DirectoryChild{}.FromDirectory(buildDirectory)); err != nil {
		return &model_command_pb.Result{
			Status: status.Convert(util.StatusWrap(err, "Failed to attach build directory")).Proto(),
		}, 0, remoteworker_pb.CurrentState_Completed_FAILED
	}
	defer e.topLevelDirectory.RemoveChild(buildDirectoryName)

	// Invoke the command.
	buildDirectoryPath := (*path.Trace)(nil).Append(buildDirectoryName)
	ctxWithTimeout, cancelTimeout := e.clock.NewContextWithTimeout(ctxWithIOError, executionTimeout)
	runResponse, runErr := e.runner.Run(ctxWithTimeout, &runner_pb.RunRequest{
		Arguments:            arguments,
		EnvironmentVariables: environmentVariables,
		WorkingDirectory:     command.Message.WorkingDirectory,
		StdoutPath:           buildDirectoryPath.Append(stdoutComponent).GetUNIXString(),
		StderrPath:           buildDirectoryPath.Append(stderrComponent).GetUNIXString(),
		InputRootDirectory:   buildDirectoryPath.Append(inputRootDirectoryComponent).GetUNIXString(),
		TemporaryDirectory:   buildDirectoryPath.Append(temporaryDirectoryComponent).GetUNIXString(),
		ServerLogsDirectory:  buildDirectoryPath.Append(serverLogsDirectoryComponent).GetUNIXString(),
	})

	// Determine the amount of time the action ran, minus the time
	// it was delayed reading data from storage.
	cancelTimeout()
	<-ctxWithTimeout.Done()
	var virtualExecutionDuration time.Duration
	if d, ok := ctxWithTimeout.Value(re_clock.UnsuspendedDurationKey{}).(time.Duration); ok {
		virtualExecutionDuration = d
	}

	resultMessage := &model_command_pb.Result{}
	resultCode := remoteworker_pb.CurrentState_Completed_SUCCEEDED
	setError := func(err error) {
		if resultMessage.Status == nil {
			resultMessage.Status = status.Convert(err).Proto()
			if status.Code(runErr) == codes.DeadlineExceeded {
				resultCode = remoteworker_pb.CurrentState_Completed_TIMED_OUT
			} else {
				resultCode = remoteworker_pb.CurrentState_Completed_FAILED
			}
		}
	}

	// If an I/O error occurred during execution, attach any errors
	// related to it to the response first. These errors should be
	// preferred over the cancelation errors that are a result of it.
	if err := ioErrorCapturer.GetError(); err != nil {
		setError(err)
	}

	// Attach the exit code or execution error.
	if runErr == nil {
		resultMessage.ExitCode = runResponse.ExitCode
		resultMessage.AuxiliaryMetadata = append(resultMessage.AuxiliaryMetadata, runResponse.ResourceUsage...)
		if runResponse.ExitCode != 0 {
			resultCode = remoteworker_pb.CurrentState_Completed_FAILED
		}
	} else {
		setError(util.StatusWrap(runErr, "Failed to run command"))
	}

	writableFileUploadDelayCtx, writableFileUploadDelayCancel := e.clock.NewContextWithTimeout(ctx, e.maximumWritableFileUploadDelay)
	defer writableFileUploadDelayCancel()
	writableFileUploadDelayChan := writableFileUploadDelayCtx.Done()

	// Capture output files.
	var outputs model_command_pb.Outputs
	outputsPatcher := model_core.NewReferenceMessagePatcher[dag.ObjectContentsWalker]()

	if stdoutContents, err := captureLog(ctx, buildDirectory, stdoutComponent, writableFileUploadDelayChan, fileCreationParameters); err == nil {
		if stdoutContents.IsSet() {
			outputs.Stdout = stdoutContents.Message
			outputsPatcher.Merge(stdoutContents.Patcher)
		}
	} else {
		setError(util.StatusWrap(err, "Failed to capture standard output"))
	}

	if stderrContents, err := captureLog(ctx, buildDirectory, stderrComponent, writableFileUploadDelayChan, fileCreationParameters); err == nil {
		if stderrContents.IsSet() {
			outputs.Stderr = stderrContents.Message
			outputsPatcher.Merge(stderrContents.Patcher)
		}
	} else {
		setError(util.StatusWrap(err, "Failed to capture standard error"))
	}

	if proto.Size(&outputs) > 0 {
		// Action has one or more outputs. Upload them and
		// attach a reference to the result message.
		if contents, metadata, err := model_core.MarshalAndEncodePatchedMessage(
			model_core.NewPatchedMessage(&outputs, outputsPatcher),
			namespace.ReferenceFormat,
			directoryEncoder,
		); err == nil {
			outputsReference := contents.GetReference()
			if err := dag.UploadDAG(
				ctx,
				e.dagUploaderClient,
				object.GlobalReference{
					LocalReference: outputsReference,
					InstanceName:   namespace.InstanceName,
				},
				dag.NewSimpleObjectContentsWalker(contents, metadata),
				e.objectContentsWalkerSemaphore,
				object.Unlimited,
			); err == nil {
				resultMessage.OutputsReference = outputsReference.GetRawReference()
			} else {
				setError(util.StatusWrap(err, "Failed to upload outputs"))
			}
		} else {
			// TODO: Does this properly release all resources?
			setError(util.StatusWrap(err, "Failed to marshal outputs"))
		}
	}
	return resultMessage, virtualExecutionDuration, resultCode
}
