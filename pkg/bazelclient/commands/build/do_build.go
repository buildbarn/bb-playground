package build

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"net/url"
	"strings"

	"github.com/buildbarn/bb-playground/pkg/bazelclient/arguments"
	"github.com/buildbarn/bb-playground/pkg/bazelclient/commands"
	"github.com/buildbarn/bb-playground/pkg/bazelclient/logging"
	"github.com/buildbarn/bb-playground/pkg/label"
	dag_pb "github.com/buildbarn/bb-playground/pkg/proto/storage/dag"
	pg_starlark "github.com/buildbarn/bb-playground/pkg/starlark"
	"github.com/buildbarn/bb-playground/pkg/storage/dag"
	"github.com/buildbarn/bb-playground/pkg/storage/object"
	"github.com/buildbarn/bb-storage/pkg/filesystem"
	"github.com/buildbarn/bb-storage/pkg/filesystem/path"

	"golang.org/x/sync/semaphore"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
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

func DoBuild(args *arguments.BuildCommand, workspacePath path.Parser) {
	logger := logging.NewLoggerFromFlags(&args.CommonFlags)
	commands.ValidateInsideWorkspace(logger, "build", workspacePath)

	remoteCacheClient, err := newGRPCClient(args.CommonFlags.RemoteCache, &args.CommonFlags)
	if err != nil {
		logger.Fatalf("Failed to create gRPC client for --remote_cache=%#v: %s", args.CommonFlags.RemoteCache, err)
	}

	workspaceDirectory, err := filesystem.NewLocalDirectory(workspacePath)
	if err != nil {
		logger.Fatal("Failed to open workspace directory: ", err)
	}
	defer workspaceDirectory.Close()

	// Determine the names and paths of all modules that are present
	// on the local system and need to be uploaded as part of the
	// build. First look for local_path_override() directives in
	// MODULE.bazel.
	moduleDotBazelFile, err := workspaceDirectory.OpenRead(path.MustNewComponent("MODULE.bazel"))
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
	if err := pg_starlark.ParseModuleDotBazel(string(moduleDotBazelContents), path.LocalFormat, moduleDotBazelHandler); err != nil {
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
	logger.Info("ROOT MODULE NAME: ", rootModuleName)
	logger.Info("MODULE PATHS: ", modulePaths)

	// TODO!
	var rootReference object.LocalReference
	var rootObjectContentsWalker dag.ObjectContentsWalker
	var maximumUnfinalizedParentsLimit object.Limit

	if err := dag.UploadDAG(
		context.Background(),
		dag_pb.NewUploaderClient(remoteCacheClient),
		object.GlobalReference{
			InstanceName:   object.NewInstanceName(args.CommonFlags.RemoteInstanceName),
			LocalReference: rootReference,
		},
		rootObjectContentsWalker,
		semaphore.NewWeighted(10),
		maximumUnfinalizedParentsLimit,
	); err != nil {
		logger.Fatal("Failed to upload workspace directory: ", err)
	}
}
