package filesystem

import (
	"context"
	"io"
	"math"
	"sync"
	"sync/atomic"

	model_core "github.com/buildbarn/bb-playground/pkg/model/core"
	"github.com/buildbarn/bb-playground/pkg/model/core/inlinedtree"
	model_filesystem_pb "github.com/buildbarn/bb-playground/pkg/proto/model/filesystem"
	"github.com/buildbarn/bb-playground/pkg/storage/object"
	"github.com/buildbarn/bb-storage/pkg/filesystem"
	"github.com/buildbarn/bb-storage/pkg/filesystem/path"
	"github.com/buildbarn/bb-storage/pkg/util"

	"golang.org/x/sync/errgroup"
	"golang.org/x/sync/semaphore"
	"google.golang.org/protobuf/proto"
)

type unfinalizedDirectory[TDirectory, TFile any] struct {
	leavesPatcherLock sync.Mutex
	leaves            model_core.PatchedMessage[*model_filesystem_pb.Leaves, TFile]
	directories       []unfinalizedDirectoryNode[TDirectory]

	unfinalizedCount atomic.Uint32
	parent           *unfinalizedDirectory[TDirectory, TFile]
	out              *model_core.PatchedMessage[*model_filesystem_pb.Directory, TDirectory]
}

type unfinalizedDirectoryNode[TDirectory any] struct {
	name            path.Component
	externalMessage model_core.PatchedMessage[*model_filesystem_pb.Directory, TDirectory]
}

type directoryMerkleTreeBuilder[TDirectory, TFile any] struct {
	context                     context.Context
	concurrency                 *semaphore.Weighted
	group                       *errgroup.Group
	directoryInlinedTreeOptions inlinedtree.Options
	fileParameters              *FileCreationParameters
	capturer                    DirectoryMerkleTreeCapturer[TDirectory, TFile]
}

func (b *directoryMerkleTreeBuilder[TDirectory, TFile]) walkDirectory(
	directory filesystem.Directory,
	directoryPath *path.Trace,
	parent *unfinalizedDirectory[TDirectory, TFile],
	out *model_core.PatchedMessage[*model_filesystem_pb.Directory, TDirectory],
) error {
	if b.context.Err() != nil {
		return util.StatusFromContext(b.context)
	}

	// Obtain the list of children and count them. That way we can
	// simplify the reference counting scheme. It also allows us to
	// create lists that don't need further resizing, meaning any
	// references to elements remain stable.
	entries, err := directory.ReadDir()
	if err != nil {
		return util.StatusWrapf(err, "Failed to read contents of directory %#v", directoryPath.GetUNIXString())
	}
	directoriesCount, filesCount, symlinksCount := 0, 0, 0
	for _, entry := range entries {
		switch entry.Type() {
		case filesystem.FileTypeDirectory:
			directoriesCount++
		case filesystem.FileTypeRegularFile:
			filesCount++
		case filesystem.FileTypeSymlink:
			symlinksCount++
		}
	}

	ud := unfinalizedDirectory[TDirectory, TFile]{
		leaves: model_core.PatchedMessage[*model_filesystem_pb.Leaves, TFile]{
			Message: &model_filesystem_pb.Leaves{
				Files:    make([]*model_filesystem_pb.FileNode, 0, filesCount),
				Symlinks: make([]*model_filesystem_pb.SymlinkNode, 0, symlinksCount),
			},
			Patcher: model_core.NewReferenceMessagePatcher[TFile](),
		},
		directories: make([]unfinalizedDirectoryNode[TDirectory], 0, directoriesCount),
		parent:      parent,
		out:         out,
	}
	ud.unfinalizedCount.Store(uint32(directoriesCount + filesCount + 1))

	for _, entry := range entries {
		name := entry.Name()
		switch entry.Type() {
		case filesystem.FileTypeDirectory:
			childPath := directoryPath.Append(name)
			child, err := directory.EnterDirectory(name)
			if err != nil {
				return util.StatusWrapf(err, "Failed to open directory %#v", childPath.GetUNIXString())
			}
			ud.directories = append(
				ud.directories,
				unfinalizedDirectoryNode[TDirectory]{name: name},
			)
			err = b.walkDirectory(
				child,
				childPath,
				&ud,
				&ud.directories[len(ud.directories)-1].externalMessage,
			)
			child.Close()
			if err != nil {
				return err
			}
		case filesystem.FileTypeRegularFile:
			f, err := directory.OpenRead(name)
			if err != nil {
				return util.StatusWrapf(err, "Failed to open file %#v", directoryPath.Append(name).GetUNIXString())
			}
			fileNode := &model_filesystem_pb.FileNode{
				Name: name.String(),
				Properties: &model_filesystem_pb.FileProperties{
					IsExecutable: entry.IsExecutable(),
				},
			}
			ud.leaves.Message.Files = append(ud.leaves.Message.Files, fileNode)

			if err := util.AcquireSemaphore(b.context, b.concurrency, 1); err != nil {
				f.Close()
				return err
			}
			b.group.Go(func() error {
				defer b.concurrency.Release(1)

				fileContents, err := CreateFileMerkleTree(
					b.context,
					b.fileParameters,
					io.NewSectionReader(f, 0, math.MaxInt64),
					b.capturer,
				)
				f.Close()
				if err != nil {
					return util.StatusWrapf(err, "Failed to create Merkle tree for file %#v", directoryPath.Append(name).GetUNIXString())
				}

				if fileContents.IsSet() {
					fileNode.Properties.Contents = fileContents.Message
					ud.leavesPatcherLock.Lock()
					ud.leaves.Patcher.Merge(fileContents.Patcher)
					ud.leavesPatcherLock.Unlock()
				}
				return b.maybeFinalizeDirectory(&ud)
			})
		case filesystem.FileTypeSymlink:
			targetParser, err := directory.Readlink(name)
			if err != nil {
				return util.StatusWrapf(err, "Failed to read target of symbolic link %#v", directoryPath.Append(name).GetUNIXString())
			}
			targetBuilder, scopeWalker := path.EmptyBuilder.Join(path.VoidScopeWalker)
			if err := path.Resolve(targetParser, scopeWalker); err != nil {
				return util.StatusWrapf(err, "Failed to resolve target of symbolic link %#v", directoryPath.Append(name).GetUNIXString())
			}
			ud.leaves.Message.Symlinks = append(
				ud.leaves.Message.Symlinks,
				&model_filesystem_pb.SymlinkNode{
					Name:   name.String(),
					Target: targetBuilder.GetUNIXString(),
				},
			)
		}
	}

	return b.maybeFinalizeDirectory(&ud)
}

func (b *directoryMerkleTreeBuilder[TDirectory, TFile]) maybeFinalizeDirectory(ud *unfinalizedDirectory[TDirectory, TFile]) error {
	for ; ud.unfinalizedCount.Add(^uint32(0)) == 0; ud = ud.parent {
		inlineCandidates := make([]inlinedtree.Candidate[*model_filesystem_pb.Directory, TDirectory], 0, 1+len(ud.directories))
		leavesInline := &model_filesystem_pb.Directory_LeavesInline{
			LeavesInline: ud.leaves.Message,
		}
		inlineCandidates = append(
			inlineCandidates,
			inlinedtree.Candidate[*model_filesystem_pb.Directory, TDirectory]{
				ExternalMessage: model_core.PatchedMessage[proto.Message, TDirectory]{
					Message: ud.leaves.Message,
					Patcher: model_core.MapReferenceMessagePatcherMetadata(
						ud.leaves.Patcher,
						b.capturer.CaptureFileNode,
					),
				},
				ParentAppender: func(
					directory model_core.PatchedMessage[*model_filesystem_pb.Directory, TDirectory],
					externalContents *object.Contents,
					externalChildren []TDirectory,
				) {
					if externalContents == nil {
						directory.Message.Leaves = leavesInline
					} else {
						directory.Message.Leaves = &model_filesystem_pb.Directory_LeavesExternal{
							LeavesExternal: directory.Patcher.AddReference(
								externalContents.GetReference(),
								b.capturer.CaptureDirectory(externalContents, externalChildren),
							),
						}
					}
				},
			},
		)

		for _, directoryNode := range ud.directories {
			nameStr := directoryNode.name.String()
			directoriesCount := uint32(len(directoryNode.externalMessage.Message.Directories))
			inlineDirectoryNode := model_filesystem_pb.DirectoryNode{
				Name: nameStr,
				Contents: &model_filesystem_pb.DirectoryNode_ContentsInline{
					ContentsInline: directoryNode.externalMessage.Message,
				},
			}
			inlineCandidates = append(
				inlineCandidates,
				inlinedtree.Candidate[*model_filesystem_pb.Directory, TDirectory]{
					ExternalMessage: model_core.PatchedMessage[proto.Message, TDirectory]{
						Message: directoryNode.externalMessage.Message,
						Patcher: directoryNode.externalMessage.Patcher,
					},
					ParentAppender: func(
						directory model_core.PatchedMessage[*model_filesystem_pb.Directory, TDirectory],
						externalContents *object.Contents,
						externalChildren []TDirectory,
					) {
						if externalContents == nil {
							directory.Message.Directories = append(directory.Message.Directories, &inlineDirectoryNode)
						} else {
							directory.Message.Directories = append(directory.Message.Directories, &model_filesystem_pb.DirectoryNode{
								Name: nameStr,
								Contents: &model_filesystem_pb.DirectoryNode_ContentsExternal{
									ContentsExternal: &model_filesystem_pb.DirectoryReference{
										Reference: directory.Patcher.AddReference(
											externalContents.GetReference(),
											b.capturer.CaptureLeaves(externalContents, externalChildren),
										),
										DirectoriesCount: directoriesCount,
									},
								},
							})
						}
					},
				},
			)
		}

		out, err := inlinedtree.Build(inlineCandidates, &b.directoryInlinedTreeOptions)
		if err != nil {
			return util.StatusWrap(err, "Failed to build directory")
		}
		*ud.out = out
	}
	return nil
}

// CreateDirectoryMerkleTree creates a Merkle tree that corresponds to
// the contents of a given directory. Upon success, a Directory message
// corresponding with the root directory is returned.
//
// Computation of the Merkle trees of the individual files can be done
// in parallel. These processes may terminate asynchronously, meaning
// that group.Wait() needs to be called to ensure that the capturer is
// invoked for all objects, and the output message is set.
func CreateDirectoryMerkleTree[TDirectory, TFile any](
	ctx context.Context,
	concurrency *semaphore.Weighted,
	group *errgroup.Group,
	directoryParameters *DirectoryCreationParameters,
	fileParameters *FileCreationParameters,
	directory filesystem.Directory,
	capturer DirectoryMerkleTreeCapturer[TDirectory, TFile],
	out *model_core.PatchedMessage[*model_filesystem_pb.Directory, TDirectory],
) error {
	b := directoryMerkleTreeBuilder[TDirectory, TFile]{
		context:     ctx,
		concurrency: concurrency,
		group:       group,
		directoryInlinedTreeOptions: inlinedtree.Options{
			ReferenceFormat:  directoryParameters.referenceFormat,
			Encoder:          directoryParameters.encoder,
			MaximumSizeBytes: directoryParameters.directoryMaximumSizeBytes,
		},
		fileParameters: fileParameters,
		capturer:       capturer,
	}

	var parent unfinalizedDirectory[TDirectory, TFile]
	parent.unfinalizedCount.Store(2)
	return b.walkDirectory(directory, nil, &parent, out)
}
