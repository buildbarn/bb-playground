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
	model_core_pb "github.com/buildbarn/bonanza/pkg/proto/model/core"
	model_filesystem_pb "github.com/buildbarn/bonanza/pkg/proto/model/filesystem"
	"github.com/buildbarn/bonanza/pkg/storage/dag"
	"github.com/buildbarn/bonanza/pkg/storage/object"
)

// reposFilePropertiesResolver resolves the properties of a file
// contained in a repo. All repos are placed in a fictive root
// directory, which allows symbolic links with targets of shape
// "../${repo}/${file}" to resolve properly.
type reposFilePropertiesResolver[TReference object.BasicReference] struct {
	context         context.Context
	directoryReader model_parser.ParsedObjectReader[TReference, model_core.Message[*model_filesystem_pb.Directory, TReference]]
	leavesReader    model_parser.ParsedObjectReader[TReference, model_core.Message[*model_filesystem_pb.Leaves, TReference]]
	environment     FilePropertiesEnvironment[TReference]

	currentDirectoryReference model_core.Message[*model_core_pb.Reference, TReference]
	stack                     []model_core.Message[*model_filesystem_pb.Directory, TReference]
	fileProperties            model_core.Message[*model_filesystem_pb.FileProperties, TReference]
	gotTerminal               bool
}

var _ path.ComponentWalker = (*reposFilePropertiesResolver[object.LocalReference])(nil)

func (r *reposFilePropertiesResolver[TReference]) getCurrentLeaves() (model_core.Message[*model_filesystem_pb.Leaves, TReference], error) {
	return model_filesystem.DirectoryGetLeaves(r.context, r.leavesReader, r.stack[len(r.stack)-1])
}

func (r *reposFilePropertiesResolver[TReference]) dereferenceCurrentDirectory() error {
	if r.currentDirectoryReference.IsSet() {
		d, err := model_parser.Dereference(r.context, r.directoryReader, r.currentDirectoryReference)
		if err != nil {
			return err
		}
		r.stack = append(r.stack, d)
		r.currentDirectoryReference.Clear()
	}
	return nil
}

func (r *reposFilePropertiesResolver[TReference]) OnDirectory(name path.Component) (path.GotDirectoryOrSymlink, error) {
	if err := r.dereferenceCurrentDirectory(); err != nil {
		return nil, err
	}
	if len(r.stack) == 0 {
		// Currently within the root directory. Enter the repo
		// corresponding to the provided directory name.
		repoValue := r.environment.GetRepoValue(&model_analysis_pb.Repo_Key{
			CanonicalRepo: name.String(),
		})
		if !repoValue.IsSet() {
			return nil, evaluation.ErrMissingDependency
		}

		r.currentDirectoryReference = model_core.NewNestedMessage(repoValue, repoValue.Message.RootDirectoryReference.GetReference())
		return path.GotDirectory{
			Child:        r,
			IsReversible: true,
		}, nil
	}

	d := r.stack[len(r.stack)-1]
	n := name.String()
	directories := d.Message.Directories
	if i, ok := sort.Find(
		len(directories),
		func(i int) int { return strings.Compare(n, directories[i].Name) },
	); ok {
		switch contents := directories[i].Contents.(type) {
		case *model_filesystem_pb.DirectoryNode_ContentsExternal:
			r.currentDirectoryReference = model_core.NewNestedMessage(d, contents.ContentsExternal.Reference)
		case *model_filesystem_pb.DirectoryNode_ContentsInline:
			r.stack = append(r.stack, model_core.NewNestedMessage(d, contents.ContentsInline))
		default:
			return nil, errors.New("unknown directory contents type")
		}
		return path.GotDirectory{
			Child:        r,
			IsReversible: true,
		}, nil
	}

	leaves, err := r.getCurrentLeaves()
	if err != nil {
		return nil, err
	}

	files := leaves.Message.Files
	if _, ok := sort.Find(
		len(files),
		func(i int) int { return strings.Compare(n, files[i].Name) },
	); ok {
		return nil, errors.New("path resolves to a regular file, while a directory was expected")
	}

	symlinks := leaves.Message.Symlinks
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

func (r *reposFilePropertiesResolver[TReference]) OnTerminal(name path.Component) (*path.GotSymlink, error) {
	if err := r.dereferenceCurrentDirectory(); err != nil {
		return nil, err
	}
	if len(r.stack) == 0 {
		return path.OnTerminalViaOnDirectory(r, name)
	}

	d := r.stack[len(r.stack)-1]
	n := name.String()
	directories := d.Message.Directories
	if _, ok := sort.Find(
		len(directories),
		func(i int) int { return strings.Compare(n, directories[i].Name) },
	); ok {
		return nil, errors.New("path resolves to a directory, while a file was expected")
	}

	leaves, err := r.getCurrentLeaves()
	if err != nil {
		return nil, err
	}

	files := leaves.Message.Files
	if i, ok := sort.Find(
		len(files),
		func(i int) int { return strings.Compare(n, files[i].Name) },
	); ok {
		properties := files[i].Properties
		if properties == nil {
			return nil, errors.New("path resolves to file that does not have any properties")
		}
		r.fileProperties = model_core.NewNestedMessage(leaves, properties)
		r.gotTerminal = true
		return nil, nil
	}

	symlinks := leaves.Message.Symlinks
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

func (r *reposFilePropertiesResolver[TReference]) OnUp() (path.ComponentWalker, error) {
	if r.currentDirectoryReference.IsSet() {
		r.currentDirectoryReference.Clear()
	} else if len(r.stack) == 0 {
		return nil, errors.New("path escapes repositories directory")
	} else {
		r.stack = r.stack[:len(r.stack)-1]
	}
	return r, nil
}

func (c *baseComputer[TReference, TMetadata]) ComputeFilePropertiesValue(ctx context.Context, key *model_analysis_pb.FileProperties_Key, e FilePropertiesEnvironment[TReference]) (PatchedFilePropertiesValue, error) {
	directoryReaders, gotDirectoryReaders := e.GetDirectoryReadersValue(&model_analysis_pb.DirectoryReaders_Key{})
	if !gotDirectoryReaders {
		return PatchedFilePropertiesValue{}, evaluation.ErrMissingDependency
	}

	resolver := reposFilePropertiesResolver[TReference]{
		context:         ctx,
		directoryReader: directoryReaders.Directory,
		leavesReader:    directoryReaders.Leaves,
		environment:     e,
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
