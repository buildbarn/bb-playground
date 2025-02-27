package build

import (
	"context"
	"crypto/ecdh"
	"crypto/x509"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"maps"
	"math"
	"net/url"
	"os"
	"runtime"
	"slices"
	"strings"

	"github.com/buildbarn/bb-storage/pkg/filesystem"
	"github.com/buildbarn/bb-storage/pkg/filesystem/path"
	"github.com/buildbarn/bb-storage/pkg/util"
	"github.com/buildbarn/bonanza/pkg/bazelclient/arguments"
	"github.com/buildbarn/bonanza/pkg/bazelclient/commands"
	"github.com/buildbarn/bonanza/pkg/bazelclient/logging"
	"github.com/buildbarn/bonanza/pkg/label"
	model_core "github.com/buildbarn/bonanza/pkg/model/core"
	model_encoding "github.com/buildbarn/bonanza/pkg/model/encoding"
	model_filesystem "github.com/buildbarn/bonanza/pkg/model/filesystem"
	model_build_pb "github.com/buildbarn/bonanza/pkg/proto/model/build"
	model_encoding_pb "github.com/buildbarn/bonanza/pkg/proto/model/encoding"
	model_filesystem_pb "github.com/buildbarn/bonanza/pkg/proto/model/filesystem"
	remoteexecution_pb "github.com/buildbarn/bonanza/pkg/proto/remoteexecution"
	dag_pb "github.com/buildbarn/bonanza/pkg/proto/storage/dag"
	object_pb "github.com/buildbarn/bonanza/pkg/proto/storage/object"
	"github.com/buildbarn/bonanza/pkg/remoteexecution"
	pg_starlark "github.com/buildbarn/bonanza/pkg/starlark"
	"github.com/buildbarn/bonanza/pkg/storage/dag"
	"github.com/buildbarn/bonanza/pkg/storage/object"
	"github.com/google/uuid"

	"golang.org/x/sync/errgroup"
	"golang.org/x/sync/semaphore"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/emptypb"
)

func newGRPCClient(endpoint string, commonFlags *arguments.CommonFlags) (*grpc.ClientConn, error) {
	endpointURL, err := url.Parse(endpoint)
	if err != nil {
		return nil, fmt.Errorf("invalid URL: %w", err)
	}

	var target string
	var clientCredentials credentials.TransportCredentials
	switch scheme := endpointURL.Scheme; scheme {
	case "grpc":
		target = endpointURL.Host
		clientCredentials = insecure.NewCredentials()
	case "grpcs":
		target = endpointURL.Host
		panic("TODO: TLS")
	case "unix":
		target = endpoint
		clientCredentials = insecure.NewCredentials()
	default:
		return nil, errors.New("scheme is not supported")
	}

	return grpc.NewClient(target, grpc.WithTransportCredentials(clientCredentials))
}

type localCapturableDirectoryOptions[TFile model_core.ReferenceMetadata] struct {
	fileParameters *model_filesystem.FileCreationParameters
	capturer       model_filesystem.FileMerkleTreeCapturer[TFile]
}

type localCapturableDirectory[TDirectory, TFile model_core.ReferenceMetadata] struct {
	filesystem.DirectoryCloser
	options *localCapturableDirectoryOptions[TFile]
}

func (d *localCapturableDirectory[TDirectory, TFile]) EnterCapturableDirectory(name path.Component) (*model_filesystem.CreatedDirectory[TDirectory], model_filesystem.CapturableDirectory[TDirectory, TFile], error) {
	child, err := d.DirectoryCloser.EnterDirectory(name)
	if err != nil {
		return nil, nil, err
	}
	return nil, &localCapturableDirectory[TDirectory, TFile]{
		DirectoryCloser: child,
		options:         d.options,
	}, nil
}

func (d *localCapturableDirectory[TDirectory, TFile]) OpenForFileMerkleTreeCreation(name path.Component) (model_filesystem.CapturableFile[TFile], error) {
	f, err := d.OpenRead(name)
	if err != nil {
		return nil, err
	}
	return &localCapturableFile[TFile]{
		file:    f,
		options: d.options,
	}, nil
}

type localCapturedDirectory struct {
	filesystem.DirectoryCloser
}

func (d localCapturedDirectory) EnterCapturedDirectory(name path.Component) (model_filesystem.CapturedDirectory, error) {
	child, err := d.DirectoryCloser.EnterDirectory(name)
	if err != nil {
		return nil, err
	}
	return localCapturedDirectory{
		DirectoryCloser: child,
	}, nil
}

type localCapturableFile[TFile model_core.ReferenceMetadata] struct {
	file    filesystem.FileReader
	options *localCapturableDirectoryOptions[TFile]
}

func (f *localCapturableFile[TFile]) CreateFileMerkleTree(ctx context.Context) (model_core.PatchedMessage[*model_filesystem_pb.FileContents, TFile], error) {
	defer f.Discard()
	return model_filesystem.CreateFileMerkleTree(
		ctx,
		f.options.fileParameters,
		io.NewSectionReader(f.file, 0, math.MaxInt64),
		f.options.capturer,
	)
}

func (f *localCapturableFile[TFile]) Discard() {
	f.file.Close()
	f.file = nil
}

func DoBuild(args *arguments.BuildCommand, workspacePath path.Parser) {
	logger := logging.NewLoggerFromFlags(&args.CommonFlags)
	commands.ValidateInsideWorkspace(logger, "build", workspacePath)

	remoteCacheClient, err := newGRPCClient(args.CommonFlags.RemoteCache, &args.CommonFlags)
	if err != nil {
		logger.Fatalf("Failed to create gRPC client for --remote_cache=%#v: %s", args.CommonFlags.RemoteCache, err)
	}

	// Determine the names and paths of all modules that are present
	// on the local system and need to be uploaded as part of the
	// build. First look for local_path_override() directives in
	// MODULE.bazel.
	workspaceDirectory, err := filesystem.NewLocalDirectory(workspacePath)
	if err != nil {
		logger.Fatal("Failed to open workspace directory: ", err)
	}
	moduleDotBazelFile, err := workspaceDirectory.OpenRead(path.MustNewComponent("MODULE.bazel"))
	workspaceDirectory.Close()
	if err != nil {
		logger.Fatal("Failed to open MODULE.bazel: ", err)
	}
	moduleDotBazelContents, err := io.ReadAll(io.NewSectionReader(moduleDotBazelFile, 0, math.MaxInt64))
	moduleDotBazelFile.Close()
	if err != nil {
		logger.Fatal("Failed to read MODULE.bazel: ", err)
	}
	modulePaths := map[label.Module]path.Parser{}
	moduleDotBazelHandler := NewLocalPathExtractingModuleDotBazelHandler(modulePaths, workspacePath)
	if err := pg_starlark.ParseModuleDotBazel(
		string(moduleDotBazelContents),
		label.MustNewCanonicalLabel("@@main+//:MODULE.bazel"),
		path.LocalFormat,
		moduleDotBazelHandler,
	); err != nil {
		logger.Fatal("Failed to parse MODULE.bazel: ", err)
	}
	rootModuleName, err := moduleDotBazelHandler.GetRootModuleName()
	if err != nil {
		logger.Fatal(err)
	}

	// Augment results with modules provided to --override_module.
	for _, overrideModule := range args.CommonFlags.OverrideModule {
		fields := strings.SplitN(overrideModule, "=", 2)
		if len(fields) != 2 {
			logger.Fatal("Module overrides must use the format ${module_name}=${path}")
		}
		moduleName, err := label.NewModule(fields[0])
		if err != nil {
			logger.Fatalf("Invalid module name %#v: %s", fields[0], err)
		}
		modulePaths[moduleName] = path.LocalFormat.NewParser(fields[1])
	}

	moduleNames := slices.Collect(maps.Keys(modulePaths))
	slices.SortFunc(moduleNames, func(a, b label.Module) int {
		return strings.Compare(a.String(), b.String())
	})

	// Determine parameters for creating file and directory Merkle
	// trees. Parameters include minimum/maximum sizes of the
	// resulting objects, and whether they are compressed and
	// encrypted.
	referenceFormat := object.MustNewReferenceFormat(object_pb.ReferenceFormat_SHA256_V1)
	encryptionKeyBytes, err := base64.StdEncoding.DecodeString(args.CommonFlags.RemoteEncryptionKey)
	if err != nil {
		logger.Fatalf("Failed to base64 decode value of --remote_encryption_key: %s", err)
	}
	defaultEncoders := []*model_encoding_pb.BinaryEncoder{{
		Encoder: &model_encoding_pb.BinaryEncoder_DeterministicEncrypting{
			DeterministicEncrypting: &model_encoding_pb.DeterministicEncryptingBinaryEncoder{
				EncryptionKey: encryptionKeyBytes,
			},
		},
	}}
	var chunkEncoders []*model_encoding_pb.BinaryEncoder
	if args.CommonFlags.RemoteCacheCompression {
		chunkEncoders = append(chunkEncoders, &model_encoding_pb.BinaryEncoder{
			Encoder: &model_encoding_pb.BinaryEncoder_LzwCompressing{
				LzwCompressing: &emptypb.Empty{},
			},
		})
	}
	chunkEncoders = append(chunkEncoders, defaultEncoders...)

	directoryParametersMessage := &model_filesystem_pb.DirectoryCreationParameters{
		Access: &model_filesystem_pb.DirectoryAccessParameters{
			Encoders: defaultEncoders,
		},
		DirectoryMaximumSizeBytes: 16 * 1024,
	}
	directoryParameters, err := model_filesystem.NewDirectoryCreationParametersFromProto(directoryParametersMessage, referenceFormat)
	if err != nil {
		logger.Fatal("Invalid directory creation parameters: ", err)
	}
	fileParametersMessage := &model_filesystem_pb.FileCreationParameters{
		Access: &model_filesystem_pb.FileAccessParameters{
			ChunkEncoders:            chunkEncoders,
			FileContentsListEncoders: defaultEncoders,
		},
		ChunkMinimumSizeBytes:            64 * 1024,
		ChunkMaximumSizeBytes:            256 * 1024,
		FileContentsListMinimumSizeBytes: 4 * 1024,
		FileContentsListMaximumSizeBytes: 16 * 1024,
	}
	fileParameters, err := model_filesystem.NewFileCreationParametersFromProto(fileParametersMessage, referenceFormat)
	if err != nil {
		logger.Fatal("Invalid file creation parameters: ", err)
	}

	// Construct Merkle trees for all modules that need to be
	// uploaded to storage.
	logger.Info("Scanning module sources")
	group, groupCtx := errgroup.WithContext(context.Background())
	moduleRootDirectories := make([]model_filesystem.CapturedDirectory, 0, len(moduleNames))
	createdModuleRootDirectories := make([]model_filesystem.CreatedDirectory[model_filesystem.CapturedObject], len(moduleNames))
	createMerkleTreesConcurrency := semaphore.NewWeighted(int64(runtime.NumCPU()))
	group.Go(func() error {
		for i, moduleName := range moduleNames {
			modulePath := modulePaths[moduleName]
			moduleRootDirectory, err := filesystem.NewLocalDirectory(modulePath)
			if err != nil {
				return util.StatusWrapf(err, "Failed to open root directory of module %#v", moduleName.String())
			}
			moduleRootDirectories = append(moduleRootDirectories, localCapturedDirectory{
				DirectoryCloser: moduleRootDirectory,
			})
			if err := model_filesystem.CreateDirectoryMerkleTree(
				groupCtx,
				createMerkleTreesConcurrency,
				group,
				directoryParameters,
				&localCapturableDirectory[model_filesystem.CapturedObject, model_core.NoopReferenceMetadata]{
					DirectoryCloser: moduleRootDirectory,
					options: &localCapturableDirectoryOptions[model_core.NoopReferenceMetadata]{
						fileParameters: fileParameters,
						capturer:       model_filesystem.NoopFileMerkleTreeCapturer,
					},
				},
				model_filesystem.FileDiscardingDirectoryMerkleTreeCapturer,
				&createdModuleRootDirectories[i],
			); err != nil {
				return util.StatusWrapf(err, "Failed to create directory Merkle tree for module %#v", moduleName.String())
			}
		}
		return nil
	})
	if err := group.Wait(); err != nil {
		logger.Fatal(err)
	}

	// Construct a BuildSpecification message that lists all the
	// modules and contains all of the flags to instruct what needs
	// to be built.
	buildSpecification := model_build_pb.BuildSpecification{
		RootModuleName:                  rootModuleName.String(),
		TargetPatterns:                  args.Arguments,
		DirectoryCreationParameters:     directoryParametersMessage,
		FileCreationParameters:          fileParametersMessage,
		IgnoreRootModuleDevDependencies: args.CommonFlags.IgnoreDevDependency,
		BuiltinsModuleNames:             args.CommonFlags.BuiltinsModule,
		RepoPlatform:                    args.CommonFlags.RepoPlatform,
		CommandEncoders:                 defaultEncoders,
	}
	switch args.CommonFlags.LockfileMode {
	case arguments.LockfileMode_Off:
	case arguments.LockfileMode_Update:
		buildSpecification.UseLockfile = &model_build_pb.UseLockfile{}
	case arguments.LockfileMode_Refresh:
		buildSpecification.UseLockfile = &model_build_pb.UseLockfile{
			Error: true,
		}
	case arguments.LockfileMode_Error:
		buildSpecification.UseLockfile = &model_build_pb.UseLockfile{
			MaximumCacheDuration: &durationpb.Duration{Seconds: 3600},
		}
	default:
		panic("unknown lockfile mode")
	}
	if len(args.CommonFlags.Registry) > 0 {
		buildSpecification.ModuleRegistryUrls = args.CommonFlags.Registry
	} else {
		buildSpecification.ModuleRegistryUrls = []string{"https://bcr.bazel.build/"}
	}
	buildSpecificationPatcher := model_core.NewReferenceMessagePatcher[dag.ObjectContentsWalker]()

	for i, moduleName := range moduleNames {
		createdRootDirectory := createdModuleRootDirectories[i]
		if l := createdRootDirectory.MaximumSymlinkEscapementLevels; l == nil || l.Value != 0 {
			logger.Fatalf("Module %#v contains one or more symbolic links that potentially escape the module's root directory", moduleName.String())
		}
		contents, children, err := model_core.MarshalAndEncodePatchedMessage(
			createdModuleRootDirectories[i].Message,
			referenceFormat,
			directoryParameters.GetEncoder(),
		)
		if err != nil {
			logger.Fatalf("Failed to create root directory object for module %#v: %s", moduleName.String(), err)
		}

		buildSpecification.Modules = append(
			buildSpecification.Modules,
			&model_build_pb.Module{
				Name: moduleName.String(),
				RootDirectoryReference: createdRootDirectory.ToDirectoryReference(
					buildSpecificationPatcher.AddReference(
						contents.GetReference(),
						model_filesystem.NewCapturedDirectoryWalker(
							directoryParameters.DirectoryAccessParameters,
							fileParameters,
							moduleRootDirectories[i],
							&model_filesystem.CapturedObject{
								Contents: contents,
								Children: children,
							},
						),
					),
				),
			},
		)
	}

	buildSpecificationEncoder, err := model_encoding.NewBinaryEncoderFromProto(
		defaultEncoders,
		uint32(referenceFormat.GetMaximumObjectSizeBytes()),
	)
	if err != nil {
		logger.Fatal("Failed to create build specification encoder: ", err)
	}

	buildSpecificationObject, buildSpecificationWalkers, err := model_core.MarshalAndEncodePatchedMessage(
		model_core.NewPatchedMessage(&buildSpecification, buildSpecificationPatcher),
		referenceFormat,
		buildSpecificationEncoder,
	)
	if err != nil {
		logger.Fatal("Failed to create build specification object: ", err)
	}

	logger.Info("Uploading module sources")
	instanceName := object.NewInstanceName(args.CommonFlags.RemoteInstanceName)
	buildSpecificationReference := buildSpecificationObject.GetReference()
	if err := dag.UploadDAG(
		context.Background(),
		dag_pb.NewUploaderClient(remoteCacheClient),
		object.GlobalReference{
			InstanceName:   instanceName,
			LocalReference: buildSpecificationReference,
		},
		dag.NewSimpleObjectContentsWalker(
			buildSpecificationObject,
			buildSpecificationWalkers,
		),
		semaphore.NewWeighted(10),
		object.NewLimit(&object_pb.Limit{
			Count:     1000,
			SizeBytes: 1 << 20,
		}),
	); err != nil {
		logger.Fatal("Failed to upload workspace directory: ", err)
	}

	clientPrivateKeyData, err := os.ReadFile(args.CommonFlags.RemoteExecutorClientPrivateKey)
	if err != nil {
		logger.Fatalf("Failed to read --remote_executor_client_private_key=%#v: %s", args.CommonFlags.RemoteExecutorClientPrivateKey, err)
	}
	clientPrivateKey, err := remoteexecution.ParseECDHPrivateKey(clientPrivateKeyData)
	if err != nil {
		logger.Fatalf("Failed to parse --remote_executor_client_private_key=%#v: %s", args.CommonFlags.RemoteExecutorClientPrivateKey, err)
	}

	clientCertificateChainData, err := os.ReadFile(args.CommonFlags.RemoteExecutorClientCertificateChain)
	if err != nil {
		logger.Fatalf("Failed to read --remote_executor_client_certificate_chain=%#v: %s", args.CommonFlags.RemoteExecutorClientCertificateChain, err)
	}
	clientCertificateChain, err := remoteexecution.ParseCertificateChain(clientCertificateChainData)
	if err != nil {
		logger.Fatalf("Failed to parse --remote_executor_client_certificate_chain=%#v: %s", args.CommonFlags.RemoteExecutorClientCertificateChain, err)
	}

	remoteExecutorClient, err := newGRPCClient(args.CommonFlags.RemoteExecutor, &args.CommonFlags)
	if err != nil {
		logger.Fatalf("Failed to create gRPC client for --remote_executor=%#v: %s", args.CommonFlags.RemoteExecutor, err)
	}
	builderClient := remoteexecution.NewClient[*model_build_pb.Action, emptypb.Empty, *model_build_pb.Result](
		remoteexecution_pb.NewExecutionClient(remoteExecutorClient),
		clientPrivateKey,
		clientCertificateChain,
	)

	builderPKIXPublicKey, err := base64.StdEncoding.DecodeString(args.CommonFlags.RemoteExecutorBuilderPkixPublicKey)
	if err != nil {
		logger.Fatalf("Failed to base64 decode --remote_executor_builder_pkix_public_key: %s", err)
	}
	builderPublicKey, err := x509.ParsePKIXPublicKey(builderPKIXPublicKey)
	if err != nil {
		logger.Fatalf("Failed to parse --remote_executor_builder_pkix_public_key: %s", err)
	}
	builderECDHPublicKey, ok := builderPublicKey.(*ecdh.PublicKey)
	if !ok {
		logger.Fatalf("--remote_executor_builder_pkix_public_key is not an ECDH public key")
	}

	var invocationID uuid.UUID
	if v := args.CommonFlags.InvocationId; v == "" {
		invocationID = uuid.Must(uuid.NewRandom())
	} else {
		invocationID, err = uuid.Parse(v)
		if err != nil {
			logger.Fatalf("Invalid --invocation_id=%#v: %s", v, err)
		}
	}
	var buildRequestID uuid.UUID
	if v := args.CommonFlags.BuildRequestId; v == "" {
		buildRequestID = uuid.Must(uuid.NewRandom())
	} else {
		buildRequestID, err = uuid.Parse(v)
		if err != nil {
			logger.Fatalf("Invalid --build_request_id=%#v: %s", v, err)
		}
	}

	logger.Info("Performing build")
	var result model_build_pb.Result
	var errBuild error
	for event := range builderClient.RunAction(
		context.Background(),
		builderECDHPublicKey,
		&model_build_pb.Action{
			InvocationId:   invocationID.String(),
			BuildRequestId: buildRequestID.String(),
			Namespace: object.Namespace{
				InstanceName:    instanceName,
				ReferenceFormat: referenceFormat,
			}.ToProto(),
			BuildSpecificationReference: buildSpecificationReference.GetRawReference(),
			BuildSpecificationEncoders:  defaultEncoders,
		},
		&remoteexecution_pb.Action_AdditionalData{
			ExecutionTimeout: &durationpb.Duration{Seconds: 24 * 60 * 60},
		},
		&result,
		&errBuild,
	) {
		logger.Info(event)
	}
	if errBuild != nil {
		logger.Fatal("Failed to perform build: ", errBuild)
	}
	if err := status.FromProto(result.Status); err != nil {
		logger.Fatal("Failed to perform build: ", err)
	}
	logger.Info(result)
}
