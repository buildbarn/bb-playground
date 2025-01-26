package filesystem

import (
	"context"
	"sync"
	"sync/atomic"

	model_core "github.com/buildbarn/bb-playground/pkg/model/core"
	"github.com/buildbarn/bb-playground/pkg/model/core/inlinedtree"
	model_core_pb "github.com/buildbarn/bb-playground/pkg/proto/model/core"
	model_filesystem_pb "github.com/buildbarn/bb-playground/pkg/proto/model/filesystem"
	"github.com/buildbarn/bb-playground/pkg/storage/object"
	"github.com/buildbarn/bb-storage/pkg/filesystem"
	"github.com/buildbarn/bb-storage/pkg/filesystem/path"
	"github.com/buildbarn/bb-storage/pkg/util"

	"golang.org/x/sync/errgroup"
	"golang.org/x/sync/semaphore"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

// CapturableDirectory is an interface for a directory that can be
// traversed by CreateFileMerkleTree().
type CapturableDirectory[TDirectory, TFile model_core.ReferenceMetadata] interface {
	// Identical to filesystem.Directory.
	Close() error
	ReadDir() ([]filesystem.FileInfo, error)
	Readlink(name path.Component) (path.Parser, error)

	// Enter a directory, so that it may be traversed. The
	// implementation has the possibility to return an existing
	// Directory message. This can be of use when computing a Merkle
	// tree that is based on an existing directory structure that is
	// altered slightly.
	EnterCapturableDirectory(name path.Component) (*CreatedDirectory[TDirectory], CapturableDirectory[TDirectory, TFile], error)

	// Open a file, so that a Merkle tree of its contents can be
	// computed. The actual Merkle tree computation is performed by
	// calling CapturableFile.CreateFileMerkleTree(). That way file
	// Merkle tree computation can happen in parallel.
	OpenForFileMerkleTreeCreation(name path.Component) (CapturableFile[TFile], error)
}

// CapturableFile is called into by CreateFileMerkleTree() to obtain a
// Merkle tree for a given file. Either one of these methods will be
// called exactly once.
type CapturableFile[TFile model_core.ReferenceMetadata] interface {
	CreateFileMerkleTree(ctx context.Context) (model_core.PatchedMessage[*model_filesystem_pb.FileContents, TFile], error)
	Discard()
}

type unfinalizedDirectory[TDirectory, TFile model_core.ReferenceMetadata] struct {
	leavesPatcherLock                    sync.Mutex
	leaves                               model_core.PatchedMessage[*model_filesystem_pb.Leaves, TFile]
	leavesMaximumSymlinkEscapementLevels *wrapperspb.UInt32Value
	directories                          []unfinalizedDirectoryNode[TDirectory]

	unfinalizedCount atomic.Uint32
	parent           *unfinalizedDirectory[TDirectory, TFile]
	out              *CreatedDirectory[TDirectory]
}

type unfinalizedDirectoryNode[TDirectory model_core.ReferenceMetadata] struct {
	name             path.Component
	createdDirectory CreatedDirectory[TDirectory]
}

type directoryMerkleTreeBuilder[TDirectory, TFile model_core.ReferenceMetadata] struct {
	context                     context.Context
	concurrency                 *semaphore.Weighted
	group                       *errgroup.Group
	directoryInlinedTreeOptions inlinedtree.Options
	capturer                    DirectoryMerkleTreeCapturer[TDirectory, TFile]
}

func (b *directoryMerkleTreeBuilder[TDirectory, TFile]) walkDirectory(
	directory CapturableDirectory[TDirectory, TFile],
	directoryPath *path.Trace,
	parent *unfinalizedDirectory[TDirectory, TFile],
	out *CreatedDirectory[TDirectory],
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
		leaves: model_core.NewSimplePatchedMessage[TFile](
			&model_filesystem_pb.Leaves{
				Files:    make([]*model_filesystem_pb.FileNode, 0, filesCount),
				Symlinks: make([]*model_filesystem_pb.SymlinkNode, 0, symlinksCount),
			},
		),
		leavesMaximumSymlinkEscapementLevels: &wrapperspb.UInt32Value{
			Value: 0,
		},
		directories: make([]unfinalizedDirectoryNode[TDirectory], 0, directoriesCount),
		parent:      parent,
		out:         out,
	}
	ud.unfinalizedCount.Store(uint32(directoriesCount + filesCount + 1))

	finalized := uint32(1)
	for _, entry := range entries {
		name := entry.Name()
		switch entry.Type() {
		case filesystem.FileTypeDirectory:
			childPath := directoryPath.Append(name)
			childExisting, childDirectory, err := directory.EnterCapturableDirectory(name)
			if err != nil {
				return util.StatusWrapf(err, "Failed to open directory %#v", childPath.GetUNIXString())
			}
			ud.directories = append(
				ud.directories,
				unfinalizedDirectoryNode[TDirectory]{name: name},
			)
			if childOut := &ud.directories[len(ud.directories)-1].createdDirectory; childExisting == nil {
				err = b.walkDirectory(
					childDirectory,
					childPath,
					&ud,
					childOut,
				)
				childDirectory.Close()
				if err != nil {
					return err
				}
			} else {
				// There is no need to recursively
				// traverse this directory, as the
				// directory's contents were computed
				// previously.
				*childOut = *childExisting
				finalized++
			}
		case filesystem.FileTypeRegularFile:
			f, err := directory.OpenForFileMerkleTreeCreation(name)
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
				f.Discard()
				return err
			}
			b.group.Go(func() error {
				defer b.concurrency.Release(1)

				fileContents, err := f.CreateFileMerkleTree(b.context)
				if err != nil {
					return util.StatusWrapf(err, "Failed to create Merkle tree for file %#v", directoryPath.Append(name).GetUNIXString())
				}

				if fileContents.IsSet() {
					fileNode.Properties.Contents = fileContents.Message
					ud.leavesPatcherLock.Lock()
					ud.leaves.Patcher.Merge(fileContents.Patcher)
					ud.leavesPatcherLock.Unlock()
				}
				return b.maybeFinalizeDirectory(&ud, 1)
			})
		case filesystem.FileTypeSymlink:
			targetParser, err := directory.Readlink(name)
			if err != nil {
				return util.StatusWrapf(err, "Failed to read target of symbolic link %#v", directoryPath.Append(name).GetUNIXString())
			}

			// Convert the symlink target to a string.
			escapementCounter := NewEscapementCountingScopeWalker()
			targetBuilder, scopeWalker := path.EmptyBuilder.Join(escapementCounter)
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

			// If the symlink target contains ".."
			// components, we may need to increase the
			// maximum symlink escapements level for this
			// directory.
			if symlinkLevels := escapementCounter.GetLevels(); symlinkLevels == nil {
				ud.leavesMaximumSymlinkEscapementLevels = nil
			} else if l := ud.leavesMaximumSymlinkEscapementLevels; l != nil && l.Value < symlinkLevels.Value {
				l.Value = symlinkLevels.Value
			}
		}
	}

	return b.maybeFinalizeDirectory(&ud, finalized)
}

func (b *directoryMerkleTreeBuilder[TDirectory, TFile]) maybeFinalizeDirectory(ud *unfinalizedDirectory[TDirectory, TFile], count uint32) error {
	for ; ud.unfinalizedCount.Add(-count) == 0; ud = ud.parent {
		count = 1
		inlineCandidates := make(inlinedtree.CandidateList[*model_filesystem_pb.Directory, TDirectory], 0, 1+len(ud.directories))
		defer inlineCandidates.Discard()

		leavesInline := &model_filesystem_pb.Directory_LeavesInline{
			LeavesInline: ud.leaves.Message,
		}
		inlineCandidates = append(
			inlineCandidates,
			inlinedtree.Candidate[*model_filesystem_pb.Directory, TDirectory]{
				ExternalMessage: model_core.NewPatchedMessage[proto.Message](
					ud.leaves.Message,
					model_core.MapReferenceMessagePatcherMetadata(
						ud.leaves.Patcher,
						func(reference object.LocalReference, metadata TFile) TDirectory {
							return b.capturer.CaptureFileNode(metadata)
						},
					),
				),
				ParentAppender: func(
					directory model_core.PatchedMessage[*model_filesystem_pb.Directory, TDirectory],
					externalContents *object.Contents,
					externalChildren []TDirectory,
				) {
					if externalContents == nil {
						directory.Message.Leaves = leavesInline
					} else {
						directory.Message.Leaves = &model_filesystem_pb.Directory_LeavesExternal{
							LeavesExternal: &model_filesystem_pb.LeavesReference{
								Reference: directory.Patcher.AddReference(
									externalContents.GetReference(),
									b.capturer.CaptureDirectory(externalContents, externalChildren),
								),
								MaximumSymlinkEscapementLevels: ud.leavesMaximumSymlinkEscapementLevels,
							},
						}
					}
				},
			},
		)

		maximumSymlinkEscapementLevels := ud.leavesMaximumSymlinkEscapementLevels
		for _, directoryNode := range ud.directories {
			nameStr := directoryNode.name.String()
			createdDirectory := directoryNode.createdDirectory
			inlineDirectoryNode := model_filesystem_pb.DirectoryNode{
				Name: nameStr,
				Contents: &model_filesystem_pb.DirectoryNode_ContentsInline{
					ContentsInline: createdDirectory.Message.Message,
				},
			}
			inlineCandidates = append(
				inlineCandidates,
				inlinedtree.Candidate[*model_filesystem_pb.Directory, TDirectory]{
					ExternalMessage: model_core.NewPatchedMessage[proto.Message](
						createdDirectory.Message.Message,
						createdDirectory.Message.Patcher,
					),
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
									ContentsExternal: createdDirectory.ToDirectoryReference(
										directory.Patcher.AddReference(
											externalContents.GetReference(),
											b.capturer.CaptureLeaves(externalContents, externalChildren),
										),
									),
								},
							})
						}
					},
				},
			)

			// If the child directory contains any symbolic
			// links, we may need to increase the maximum
			// symlink escapement levels for the parent.
			if createdDirectory.MaximumSymlinkEscapementLevels == nil {
				maximumSymlinkEscapementLevels = nil
			} else if maximumSymlinkEscapementLevels != nil {
				parentValue := int64(createdDirectory.MaximumSymlinkEscapementLevels.Value) - 1
				if int64(maximumSymlinkEscapementLevels.Value) < parentValue {
					maximumSymlinkEscapementLevels = &wrapperspb.UInt32Value{Value: uint32(parentValue)}
				}
			}
		}

		message, err := inlinedtree.Build(inlineCandidates, &b.directoryInlinedTreeOptions)
		if err != nil {
			return util.StatusWrap(err, "Failed to build directory")
		}
		*ud.out = CreatedDirectory[TDirectory]{
			Message:                        message,
			MaximumSymlinkEscapementLevels: maximumSymlinkEscapementLevels,
		}
	}
	return nil
}

// CreatedDirectory contains the message of the root directory of the
// directory hierarchy for which a Merkle tree was created. It also
// contains all of the metadata that needs to be available when creating
// a DirectoryReference message for referring to the root directory.
type CreatedDirectory[TDirectory model_core.ReferenceMetadata] struct {
	Message                        model_core.PatchedMessage[*model_filesystem_pb.Directory, TDirectory]
	MaximumSymlinkEscapementLevels *wrapperspb.UInt32Value
}

// ToDirectoryReference creates a DirectoryReference message that
// corresponds to the current directory.
func (cd *CreatedDirectory[TDirectory]) ToDirectoryReference(reference *model_core_pb.Reference) *model_filesystem_pb.DirectoryReference {
	return &model_filesystem_pb.DirectoryReference{
		Reference:                      reference,
		DirectoriesCount:               uint32(len(cd.Message.Message.Directories)),
		MaximumSymlinkEscapementLevels: cd.MaximumSymlinkEscapementLevels,
	}
}

// CreateDirectoryMerkleTree creates a Merkle tree that corresponds to
// the contents of a given directory. Upon success, a Directory message
// corresponding with the root directory is returned.
//
// Computation of the Merkle trees of the individual files can be done
// in parallel. These processes may terminate asynchronously, meaning
// that group.Wait() needs to be called to ensure that the capturer is
// invoked for all objects, and the output message is set.
func CreateDirectoryMerkleTree[TDirectory, TFile model_core.ReferenceMetadata](
	ctx context.Context,
	concurrency *semaphore.Weighted,
	group *errgroup.Group,
	directoryParameters *DirectoryCreationParameters,
	directory CapturableDirectory[TDirectory, TFile],
	capturer DirectoryMerkleTreeCapturer[TDirectory, TFile],
	out *CreatedDirectory[TDirectory],
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
		capturer: capturer,
	}

	var parent unfinalizedDirectory[TDirectory, TFile]
	parent.unfinalizedCount.Store(2)
	return b.walkDirectory(directory, nil, &parent, out)
}
