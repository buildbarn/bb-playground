package analysis

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/buildbarn/bb-storage/pkg/filesystem/path"
	"github.com/buildbarn/bonanza/pkg/evaluation"
	model_core "github.com/buildbarn/bonanza/pkg/model/core"
	model_filesystem "github.com/buildbarn/bonanza/pkg/model/filesystem"
	model_parser "github.com/buildbarn/bonanza/pkg/model/parser"
	model_analysis_pb "github.com/buildbarn/bonanza/pkg/proto/model/analysis"
	model_filesystem_pb "github.com/buildbarn/bonanza/pkg/proto/model/filesystem"
	"github.com/buildbarn/bonanza/pkg/storage/dag"
	"github.com/buildbarn/bonanza/pkg/storage/object"
)

// reposFilePropertiesResolver resolves the properties of a file
// contained in a repo. All repos are placed in a fictive root
// directory, which allows symbolic links with targets of shape
// "../${repo}/${file}" to resolve properly.
type reposFilePropertiesResolver struct {
	context                context.Context
	directoryClusterReader model_parser.ParsedObjectReader[object.LocalReference, model_filesystem.DirectoryCluster]
	environment            FilePropertiesEnvironment

	stack          []model_filesystem.DirectoryInfo
	fileProperties model_core.Message[*model_filesystem_pb.FileProperties]
	gotTerminal    bool
}

var _ path.ComponentWalker = (*reposFilePropertiesResolver)(nil)

func (r *reposFilePropertiesResolver) getCurrentDirectory() (*model_filesystem.Directory, error) {
	directoryInfo := r.stack[len(r.stack)-1]
	cluster, _, err := r.directoryClusterReader.ReadParsedObject(r.context, directoryInfo.ClusterReference)
	if err != nil {
		return nil, fmt.Errorf("failed to read directory cluster: %w", err)
	}
	if directoryInfo.DirectoryIndex >= uint(len(cluster)) {
		return nil, fmt.Errorf("directory index %d exceeds directory cluster size %d", directoryInfo.DirectoryIndex, len(cluster))
	}
	return &cluster[directoryInfo.DirectoryIndex], nil
}

func (r *reposFilePropertiesResolver) OnDirectory(name path.Component) (path.GotDirectoryOrSymlink, error) {
	if len(r.stack) == 0 {
		// Currently within the root directory. Enter the repo
		// corresponding to the provided directory name.
		repoValue := r.environment.GetRepoValue(&model_analysis_pb.Repo_Key{
			CanonicalRepo: name.String(),
		})
		if !repoValue.IsSet() {
			return nil, evaluation.ErrMissingDependency
		}

		directoryInfo, err := model_filesystem.NewDirectoryInfoFromDirectoryReference(
			model_core.NewNestedMessage(repoValue, repoValue.Message.RootDirectoryReference),
		)
		if err != nil {
			return nil, fmt.Errorf("failed to create directory info for canonical repo %#v: %w", name.String())
		}

		r.stack = append(r.stack, directoryInfo)
		return path.GotDirectory{
			Child:        r,
			IsReversible: true,
		}, nil
	}

	d, err := r.getCurrentDirectory()
	if err != nil {
		return nil, err
	}

	n := name.String()
	directories := d.Directories
	if i, ok := sort.Find(
		len(directories),
		func(i int) int { return strings.Compare(n, directories[i].Name.String()) },
	); ok {
		r.stack = append(r.stack, directories[i].Info)
		return path.GotDirectory{
			Child:        r,
			IsReversible: true,
		}, nil
	}

	files := d.Leaves.Message.Files
	if _, ok := sort.Find(
		len(files),
		func(i int) int { return strings.Compare(n, files[i].Name) },
	); ok {
		return nil, errors.New("path resolves to a regular file, while a directory was expected")
	}

	symlinks := d.Leaves.Message.Symlinks
	if i, ok := sort.Find(
		len(symlinks),
		func(i int) int { return strings.Compare(n, symlinks[i].Name) },
	); ok {
		return path.GotSymlink{
			Parent: path.NewRelativeScopeWalker(r),
			Target: path.UNIXFormat.NewParser(symlinks[i].Target),
		}, nil
	}

	return nil, errors.New("path does not exist")
}

func (r *reposFilePropertiesResolver) OnTerminal(name path.Component) (*path.GotSymlink, error) {
	if len(r.stack) == 0 {
		return path.OnTerminalViaOnDirectory(r, name)
	}

	d, err := r.getCurrentDirectory()
	if err != nil {
		return nil, err
	}

	n := name.String()
	directories := d.Directories
	if _, ok := sort.Find(
		len(directories),
		func(i int) int { return strings.Compare(n, directories[i].Name.String()) },
	); ok {
		return nil, errors.New("path resolves to a directory, while a file was expected")
	}

	files := d.Leaves.Message.Files
	if i, ok := sort.Find(
		len(files),
		func(i int) int { return strings.Compare(n, files[i].Name) },
	); ok {
		properties := files[i].Properties
		if properties == nil {
			return nil, errors.New("path resolves to file that does not have any properties")
		}
		r.fileProperties = model_core.NewNestedMessage(d.Leaves, properties)
		r.gotTerminal = true
		return nil, nil
	}

	symlinks := d.Leaves.Message.Symlinks
	if i, ok := sort.Find(
		len(symlinks),
		func(i int) int { return strings.Compare(n, symlinks[i].Name) },
	); ok {
		return &path.GotSymlink{
			Parent: path.NewRelativeScopeWalker(r),
			Target: path.UNIXFormat.NewParser(symlinks[i].Target),
		}, nil
	}

	r.gotTerminal = true
	return nil, nil
}

func (r *reposFilePropertiesResolver) OnUp() (path.ComponentWalker, error) {
	if len(r.stack) == 0 {
		return nil, errors.New("path escapes repositories directory")
	}
	r.stack = r.stack[:len(r.stack)-1]
	return r, nil
}

func (c *baseComputer) ComputeFilePropertiesValue(ctx context.Context, key *model_analysis_pb.FileProperties_Key, e FilePropertiesEnvironment) (PatchedFilePropertiesValue, error) {
	directoryAccessParametersValue := e.GetDirectoryAccessParametersValue(&model_analysis_pb.DirectoryAccessParameters_Key{})
	if !directoryAccessParametersValue.IsSet() {
		return PatchedFilePropertiesValue{}, evaluation.ErrMissingDependency
	}
	directoryAccessParameters, err := model_filesystem.NewDirectoryAccessParametersFromProto(
		directoryAccessParametersValue.Message.DirectoryAccessParameters,
		c.buildSpecificationReference.GetReferenceFormat(),
	)
	if err != nil {
		return PatchedFilePropertiesValue{}, fmt.Errorf("invalid directory access parameters: %w", err)
	}

	resolver := reposFilePropertiesResolver{
		context: ctx,
		directoryClusterReader: model_parser.NewStorageBackedParsedObjectReader(
			c.objectDownloader,
			directoryAccessParameters.GetEncoder(),
			model_filesystem.NewDirectoryClusterObjectParser[object.LocalReference](
				model_parser.NewStorageBackedParsedObjectReader(
					c.objectDownloader,
					directoryAccessParameters.GetEncoder(),
					model_parser.NewMessageObjectParser[object.LocalReference, model_filesystem_pb.Leaves](),
				),
			),
		),
		environment: e,
	}

	canonicalRepo, ok := path.NewComponent(key.CanonicalRepo)
	if !ok {
		return PatchedFilePropertiesValue{}, fmt.Errorf("canonical repo is not a valid pathname component")
	}
	repoDirectory, err := resolver.OnDirectory(canonicalRepo)
	if err != nil {
		return PatchedFilePropertiesValue{}, fmt.Errorf("failed to resolve canonical repo directory: %w", err)
	}

	if err := path.Resolve(
		path.UNIXFormat.NewParser(key.Path),
		path.NewLoopDetectingScopeWalker(
			path.NewRelativeScopeWalker(repoDirectory.(path.GotDirectory).Child),
		),
	); err != nil {
		return PatchedFilePropertiesValue{}, fmt.Errorf("failed to resolve path: %w", err)
	}
	if !resolver.gotTerminal {
		return PatchedFilePropertiesValue{}, errors.New("path resolves to a directory")
	}

	patchedFileProperties := model_core.NewPatchedMessageFromExisting(
		resolver.fileProperties,
		func(index int) dag.ObjectContentsWalker {
			return dag.ExistingObjectContentsWalker
		},
	)
	return PatchedFilePropertiesValue{
		Message: &model_analysis_pb.FileProperties_Value{
			Exists: patchedFileProperties.Message,
		},
		Patcher: patchedFileProperties.Patcher,
	}, nil
}
