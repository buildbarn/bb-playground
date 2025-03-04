package filesystem

import (
	"context"
	"io"
	"math"
	"sync/atomic"

	"github.com/buildbarn/bb-storage/pkg/filesystem"
	"github.com/buildbarn/bb-storage/pkg/filesystem/path"
	"github.com/buildbarn/bb-storage/pkg/util"
	model_core "github.com/buildbarn/bonanza/pkg/model/core"
	model_filesystem_pb "github.com/buildbarn/bonanza/pkg/proto/model/filesystem"
	"github.com/buildbarn/bonanza/pkg/storage/dag"
	"github.com/buildbarn/bonanza/pkg/storage/object"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// CapturedDirectory is called into by CapturedDirectoryWalker to
// traverse a directory hierarchy and read file contents.
type CapturedDirectory interface {
	Close() error
	EnterCapturedDirectory(name path.Component) (CapturedDirectory, error)
	OpenRead(name path.Component) (filesystem.FileReader, error)
}

// capturedDirectoryWalkerOptions contains all state that is shared by
// all the transitively created instances of ObjectContentsWalker that
// are returned by NewCapturedDirectoryWalker().
type capturedDirectoryWalkerOptions struct {
	directoryParameters *DirectoryAccessParameters
	fileParameters      *FileCreationParameters
	rootDirectory       CapturedDirectory
}

// openFile opens a file underneath the root directory for reading. This
// method is called whenever GetContents() is called against an
// ObjectContentsWalker that is backed by a file.
func (o *capturedDirectoryWalkerOptions) openFile(pathTrace *path.Trace) (filesystem.FileReader, error) {
	var dPathTrace *path.Trace
	d := o.rootDirectory
	var dCloser io.Closer = io.NopCloser(nil)
	defer func() {
		dCloser.Close()
	}()

	components := pathTrace.ToList()
	for _, component := range components[:len(components)-1] {
		dPathTrace = dPathTrace.Append(component)
		dChild, err := d.EnterCapturedDirectory(component)
		if err != nil {
			return nil, util.StatusWrapf(err, "Failed to enter directory %#v", dPathTrace.GetUNIXString())
		}
		dCloser.Close()
		d = dChild
		dCloser = dChild
	}

	return d.OpenRead(components[len(components)-1])
}

// gatherWalkersForLeaves creates ObjectContentsWalkers for all files
// contained in the provided Leaves message.
func (o *capturedDirectoryWalkerOptions) gatherWalkersForLeaves(leaves *model_filesystem_pb.Leaves, outgoingReferences object.OutgoingReferences[object.LocalReference], pathTrace *path.Trace, walkers []dag.ObjectContentsWalker) error {
	for _, childFile := range leaves.Files {
		childPathTrace := pathTrace.Append(path.MustNewComponent(childFile.Name))
		properties := childFile.Properties
		if properties == nil {
			return status.Errorf(codes.InvalidArgument, "File %#v does not have any properties", childPathTrace.GetUNIXString())
		}
		if contents := properties.Contents; contents != nil {
			index, err := model_core.GetIndexFromReferenceMessage(contents.Reference, len(walkers))
			if err != nil {
				return util.StatusWrapf(err, "Invalid reference index for file %#v", childPathTrace.GetUNIXString())
			}
			childReference := outgoingReferences.GetOutgoingReference(index)
			if childReference.GetHeight() == 0 {
				walkers[index] = &smallFileWalker{
					options:   o,
					reference: childReference,
					pathTrace: childPathTrace,
					sizeBytes: uint32(contents.TotalSizeBytes),
				}
			} else {
				walkers[index] = &recomputingConcatenatedFileWalker{
					options:   o,
					reference: childReference,
					pathTrace: childPathTrace,
				}
			}
		}
	}
	return nil
}

// capturedDirectoryWalker is an ObjectContentsWalker that is backed by
// a CreatedObjectTree corresponding to a Directory message.
type capturedDirectoryWalker struct {
	options   *capturedDirectoryWalkerOptions
	object    *model_core.CreatedObjectTree
	pathTrace *path.Trace
}

// NewCapturedDirectoryWalker returns an implementation of
// ObjectContentsWalker that is capable of walking over a hierarchy over
// Directory and Leaves objects that were created using
// CreateDirectoryMerkleTree and captured using
// FileDiscardingDirectoryMerkleTreeCapturer. This makes it possible to
// upload such directory hierarchies to a storage server.
//
// These Merkle trees do not contain any file contents, but it is
// permitted for the storage server to request them. If that happens, we
// must reobtain them from the underlying file system. This is why the
// caller must provide a handle to the root directory on which the
// provided Merkle tree is based.
func NewCapturedDirectoryWalker(directoryParameters *DirectoryAccessParameters, fileParameters *FileCreationParameters, rootDirectory CapturedDirectory, rootObject *model_core.CreatedObjectTree) dag.ObjectContentsWalker {
	return &capturedDirectoryWalker{
		options: &capturedDirectoryWalkerOptions{
			directoryParameters: directoryParameters,
			fileParameters:      fileParameters,
			rootDirectory:       rootDirectory,
		},
		object: rootObject,
	}
}

func (w *capturedDirectoryWalker) GetContents(ctx context.Context) (*object.Contents, []dag.ObjectContentsWalker, error) {
	directory, err := w.options.directoryParameters.DecodeDirectory(w.object.Contents)
	if err != nil {
		return nil, nil, util.StatusWrapf(err, "Failed to decode directory %#v", w.pathTrace.GetUNIXString())
	}

	walkers := make([]dag.ObjectContentsWalker, w.object.Contents.GetDegree())
	if err := w.gatherWalkers(directory, w.pathTrace, walkers); err != nil {
		return nil, nil, err
	}
	return w.object.Contents, walkers, nil
}

func (w *capturedDirectoryWalker) gatherWalkers(directory *model_filesystem_pb.Directory, pathTrace *path.Trace, walkers []dag.ObjectContentsWalker) error {
	switch leaves := directory.Leaves.(type) {
	case *model_filesystem_pb.Directory_LeavesExternal:
		index, err := model_core.GetIndexFromReferenceMessage(leaves.LeavesExternal.Reference, len(walkers))
		if err != nil {
			return util.StatusWrapf(err, "Invalid reference index for leaves of directory %#v", pathTrace.GetUNIXString())
		}
		walkers[index] = &capturedLeavesWalker{
			options:   w.options,
			object:    &w.object.Metadata[index],
			pathTrace: pathTrace,
		}
	case *model_filesystem_pb.Directory_LeavesInline:
		if err := w.options.gatherWalkersForLeaves(leaves.LeavesInline, w.object.Contents, pathTrace, walkers); err != nil {
			return err
		}
	default:
		return status.Errorf(codes.InvalidArgument, "Invalid leaves type for directory %#v", pathTrace.GetUNIXString())
	}

	for _, childDirectory := range directory.Directories {
		childPathTrace := pathTrace.Append(path.MustNewComponent(childDirectory.Name))
		switch contents := childDirectory.Contents.(type) {
		case *model_filesystem_pb.DirectoryNode_ContentsExternal:
			index, err := model_core.GetIndexFromReferenceMessage(contents.ContentsExternal.Reference, len(walkers))
			if err != nil {
				return util.StatusWrapf(err, "Invalid reference index for directory %#v", childPathTrace.GetUNIXString())
			}
			walkers[index] = &capturedDirectoryWalker{
				options:   w.options,
				object:    &w.object.Metadata[index],
				pathTrace: childPathTrace,
			}
		case *model_filesystem_pb.DirectoryNode_ContentsInline:
			if err := w.gatherWalkers(contents.ContentsInline, childPathTrace, walkers); err != nil {
				return err
			}
		default:
			return status.Errorf(codes.InvalidArgument, "Invalid contents type for directory %#v", childPathTrace.GetUNIXString())
		}
	}
	return nil
}

func (w *capturedDirectoryWalker) Discard() {}

// capturedLeaves is an ObjectContentsWalker that is backed by a
// CreatedObjectTree corresponding to a Leaves message.
type capturedLeavesWalker struct {
	options   *capturedDirectoryWalkerOptions
	object    *model_core.CreatedObjectTree
	pathTrace *path.Trace
}

func (w *capturedLeavesWalker) GetContents(ctx context.Context) (*object.Contents, []dag.ObjectContentsWalker, error) {
	leaves, err := w.options.directoryParameters.DecodeLeaves(w.object.Contents)
	if err != nil {
		return nil, nil, util.StatusWrapf(err, "Failed to decode leaves of directory %#v", w.pathTrace.GetUNIXString())
	}

	walkers := make([]dag.ObjectContentsWalker, w.object.Contents.GetDegree())
	if err := w.options.gatherWalkersForLeaves(leaves, w.object.Contents, w.pathTrace, walkers); err != nil {
		return nil, nil, err
	}
	return w.object.Contents, walkers, nil
}

func (w *capturedLeavesWalker) Discard() {}

// smallFileWalker is an ObjectContentsWalker that is backed by a file
// that is small enough to store in a single chunk.
type smallFileWalker struct {
	options   *capturedDirectoryWalkerOptions
	reference object.LocalReference
	pathTrace *path.Trace
	sizeBytes uint32
}

func (w *smallFileWalker) GetContents(ctx context.Context) (*object.Contents, []dag.ObjectContentsWalker, error) {
	r, err := w.options.openFile(w.pathTrace)
	if err != nil {
		return nil, nil, util.StatusWrapf(err, "Failed to open %#v", w.pathTrace.GetUNIXString())
	}
	defer r.Close()

	data := make([]byte, w.sizeBytes)
	if n, err := r.ReadAt(data, 0); n != len(data) {
		return nil, nil, util.StatusWrapf(err, "Failed to read %#v", w.pathTrace.GetUNIXString())
	}
	contents, err := w.options.fileParameters.EncodeChunk(data)
	if err != nil {
		return nil, nil, util.StatusWrapf(err, "Failed to encode %#v", w.pathTrace.GetUNIXString())
	}
	if actualReference := contents.GetReference(); actualReference != w.reference {
		return nil, nil, status.Errorf(codes.InvalidArgument, "File %#v has reference %s, while %s was expected", w.pathTrace.GetUNIXString(), actualReference, w.reference)
	}
	return contents, nil, nil
}

func (w *smallFileWalker) Discard() {}

// recomputingConcatenatedFileWalker is an ObjectContentsWalker that is
// backed by a file that is too large to store in a single chunk. When
// GetContents() is called, the Merkle tree corresponding to this file
// is recomputed.
type recomputingConcatenatedFileWalker struct {
	options   *capturedDirectoryWalkerOptions
	reference object.LocalReference
	pathTrace *path.Trace
}

func (w *recomputingConcatenatedFileWalker) GetContents(ctx context.Context) (*object.Contents, []dag.ObjectContentsWalker, error) {
	r, err := w.options.openFile(w.pathTrace)
	if err != nil {
		return nil, nil, util.StatusWrapf(err, "Failed to open %#v", w.pathTrace.GetUNIXString())
	}
	fileContents, err := CreateFileMerkleTree(
		ctx,
		w.options.fileParameters,
		io.NewSectionReader(r, 0, math.MaxInt64),
		ChunkDiscardingFileMerkleTreeCapturer,
	)
	if err != nil {
		r.Close()
		return nil, nil, err
	}

	if !fileContents.IsSet() {
		return nil, nil, status.Errorf(codes.InvalidArgument, "File %#v no longer has any contents", w.pathTrace.GetUNIXString())
	}
	references, objects := fileContents.Patcher.SortAndSetReferences()
	if references[0] != w.reference {
		r.Close()
		return nil, nil, status.Errorf(codes.InvalidArgument, "File %#v has reference %s, while %s was expected", w.pathTrace.GetUNIXString(), references[0], w.reference)
	}

	options := &computedConcatenatedFileObjectOptions{
		fileParameters: w.options.fileParameters,
		pathTrace:      w.pathTrace,
		file:           r,
	}
	options.referenceCount.Store(1)
	wComputed := &computedConcatenatedFileWalker{
		options: options,
		object:  &objects[0],
	}
	return wComputed.GetContents(ctx)
}

func NewCapturedFileWalker(fileParameters *FileCreationParameters, r filesystem.FileReader, fileReference object.LocalReference, fileSizeBytes uint64, fileObject *model_core.CreatedObjectTree) dag.ObjectContentsWalker {
	options := &computedConcatenatedFileObjectOptions{
		fileParameters: fileParameters,
		file:           r,
	}
	options.referenceCount.Store(1)
	if fileReference.GetHeight() == 0 {
		return &concatenatedFileChunkWalker{
			options:   options,
			reference: fileReference,
			sizeBytes: uint32(fileSizeBytes),
		}
	}
	return &computedConcatenatedFileWalker{
		options: options,
		object:  fileObject,
	}
}

func (w *recomputingConcatenatedFileWalker) Discard() {}

// computedConcatenatedFileObjectOptions is an ObjectContentsWalker that
// is backed by a part of a large file for which the Merkle tree was
// recomputed.
type computedConcatenatedFileObjectOptions struct {
	fileParameters *FileCreationParameters
	pathTrace      *path.Trace
	referenceCount atomic.Uint64
	file           filesystem.FileReader
}

type computedConcatenatedFileWalker struct {
	options     *computedConcatenatedFileObjectOptions
	object      *model_core.CreatedObjectTree
	offsetBytes uint64
}

func (w *computedConcatenatedFileWalker) GetContents(ctx context.Context) (*object.Contents, []dag.ObjectContentsWalker, error) {
	fileContentsList, err := w.options.fileParameters.DecodeFileContentsList(w.object.Contents)
	if err != nil {
		return nil, nil, util.StatusWrapf(err, "Failed to decode file contents list for file %#v at offset %d", w.options.pathTrace.GetUNIXString(), w.offsetBytes)
	}
	reference := w.object.Contents.GetReference()
	walkers := make([]dag.ObjectContentsWalker, reference.GetDegree())
	offsetBytes := w.offsetBytes
	for _, part := range fileContentsList {
		index, err := model_core.GetIndexFromReferenceMessage(part.Reference, len(walkers))
		if err != nil {
			return nil, nil, util.StatusWrapf(err, "Invalid reference index for part of file %#v at offset %d", w.options.pathTrace.GetUNIXString(), offsetBytes)
		}
		partReference := w.object.Contents.GetOutgoingReference(index)
		if partReference.GetHeight() == 0 {
			walkers[index] = &concatenatedFileChunkWalker{
				options:     w.options,
				reference:   partReference,
				offsetBytes: offsetBytes,
				sizeBytes:   uint32(part.TotalSizeBytes),
			}
		} else {
			walkers[index] = &computedConcatenatedFileWalker{
				options:     w.options,
				object:      &w.object.Metadata[index],
				offsetBytes: offsetBytes,
			}
		}
		offsetBytes += part.TotalSizeBytes
	}
	w.options.referenceCount.Add(uint64(len(walkers) - 1))
	return w.object.Contents, walkers, nil
}

func (w *computedConcatenatedFileWalker) Discard() {
	if w.options.referenceCount.Add(^uint64(0)) == 0 {
		w.options.file.Close()
		w.options.file = nil
	}
	w.options = nil
}

// concatenatedFileChunkWalker is an ObjectContentsWalker that is backed
// by a single chunk of data contained in a large file.
type concatenatedFileChunkWalker struct {
	options     *computedConcatenatedFileObjectOptions
	reference   object.LocalReference
	offsetBytes uint64
	sizeBytes   uint32
}

func (w *concatenatedFileChunkWalker) GetContents(ctx context.Context) (*object.Contents, []dag.ObjectContentsWalker, error) {
	defer w.Discard()

	data := make([]byte, w.sizeBytes)
	if n, err := w.options.file.ReadAt(data, int64(w.offsetBytes)); n != len(data) {
		return nil, nil, util.StatusWrapf(err, "Failed to read %#v at offset %d", w.options.pathTrace.GetUNIXString(), w.offsetBytes)
	}
	contents, err := w.options.fileParameters.EncodeChunk(data)
	if err != nil {
		return nil, nil, util.StatusWrapf(err, "Failed to encode %#v", w.options.pathTrace.GetUNIXString())
	}
	if actualReference := contents.GetReference(); actualReference != w.reference {
		return nil, nil, status.Errorf(codes.InvalidArgument, "Chunk at offset %d in file %#v has reference %s, while %s was expected", w.offsetBytes, w.options.pathTrace.GetUNIXString(), actualReference, w.reference)
	}
	return contents, nil, nil
}

func (w *concatenatedFileChunkWalker) Discard() {
	if w.options.referenceCount.Add(^uint64(0)) == 0 {
		w.options.file.Close()
		w.options.file = nil
	}
	w.options = nil
}
