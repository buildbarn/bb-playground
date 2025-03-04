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
	"github.com/buildbarn/bonanza/pkg/model/core/dereference"
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
type reposFilePropertiesResolver struct {
	context               context.Context
	directoryDereferencer dereference.Dereferencer[object.OutgoingReferences, model_core.Message[*model_filesystem_pb.Directory, object.OutgoingReferences]]
	leavesDereferencer    dereference.Dereferencer[object.OutgoingReferences, model_core.Message[*model_filesystem_pb.Leaves, object.OutgoingReferences]]
	environment           FilePropertiesEnvironment

	currentDirectoryReference model_core.Message[*model_core_pb.Reference, object.OutgoingReferences]
	stack                     []model_core.Message[*model_filesystem_pb.Directory, object.OutgoingReferences]
	fileProperties            model_core.Message[*model_filesystem_pb.FileProperties, object.OutgoingReferences]
	gotTerminal               bool
}

var _ path.ComponentWalker = (*reposFilePropertiesResolver)(nil)

func (r *reposFilePropertiesResolver) getCurrentLeaves() (model_core.Message[*model_filesystem_pb.Leaves, object.OutgoingReferences], error) {
	d := r.stack[len(r.stack)-1]
	switch l := d.Message.Leaves.(type) {
	case *model_filesystem_pb.Directory_LeavesExternal:
		return dereference.Dereference(r.context, r.leavesDereferencer, model_core.NewNestedMessage(d, l.LeavesExternal.Reference))
	case *model_filesystem_pb.Directory_LeavesInline:
		return model_core.NewNestedMessage(d, l.LeavesInline), nil
	default:
		return model_core.Message[*model_filesystem_pb.Leaves, object.OutgoingReferences]{}, errors.New("unknown leaves type")
	}
}

func (r *reposFilePropertiesResolver) dereferenceCurrentDirectory() error {
	if r.currentDirectoryReference.IsSet() {
		d, err := dereference.Dereference(r.context, r.directoryDereferencer, r.currentDirectoryReference)
		if err != nil {
			return err
		}
		r.stack = append(r.stack, d)
		r.currentDirectoryReference.Clear()
	}
	return nil
}

func (r *reposFilePropertiesResolver) OnDirectory(name path.Component) (path.GotDirectoryOrSymlink, error) {
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

func (r *reposFilePropertiesResolver) OnTerminal(name path.Component) (*path.GotSymlink, error) {
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

func (r *reposFilePropertiesResolver) OnUp() (path.ComponentWalker, error) {
	if r.currentDirectoryReference.IsSet() {
		r.currentDirectoryReference.Clear()
	} else if len(r.stack) == 0 {
		return nil, errors.New("path escapes repositories directory")
	} else {
		r.stack = r.stack[:len(r.stack)-1]
	}
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
		directoryDereferencer: dereference.NewReadingDereferencer(
			model_parser.NewStorageBackedParsedObjectReader(
				c.objectDownloader,
				directoryAccessParameters.GetEncoder(),
				model_parser.NewMessageObjectParser[object.LocalReference, model_filesystem_pb.Directory](),
			),
		),
		leavesDereferencer: dereference.NewReadingDereferencer(
			model_parser.NewStorageBackedParsedObjectReader(
				c.objectDownloader,
				directoryAccessParameters.GetEncoder(),
				model_parser.NewMessageObjectParser[object.LocalReference, model_filesystem_pb.Leaves](),
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
