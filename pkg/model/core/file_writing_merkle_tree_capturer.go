package core

import (
	"bufio"
	"context"
	"encoding/binary"
	"errors"
	"io"
	"math"
	"sync"
	"sync/atomic"

	"github.com/buildbarn/bb-playground/pkg/storage/dag"
	"github.com/buildbarn/bb-playground/pkg/storage/object"
)

// FileWritingMerkleTreeCapturer is capable of sequentially writing the
// contents of objects to a file on disk. This can be used to
// temporarily store large Merkle trees on disk prior to being uploaded
// to a remote storage server. It may not be feasible to store the
// Merkle tree in memory.
//
// The format in which data is stored on disk is simple. For each
// object, the contents are stored literally. After every object, the
// offset of any previously written children is stored. This makes it
// possible to traverse the full graph of objects afterwards.
type FileWritingMerkleTreeCapturer struct {
	lock               sync.Mutex
	writer             *bufio.Writer
	currentOffsetBytes uint64
	savedErr           error
}

// NewFileWritingMerkleTreeCapturer creates a
// FileWritingMerkleTreeCapturer that is in the initial state (i.e.,
// assuming the output file is empty).
func NewFileWritingMerkleTreeCapturer(w io.Writer) *FileWritingMerkleTreeCapturer {
	return &FileWritingMerkleTreeCapturer{
		writer: bufio.NewWriter(w),
	}
}

// CaptureObject writes the contents of a single object to disk.
func (c *FileWritingMerkleTreeCapturer) CaptureObject(contents *object.Contents, children []FileBackedObjectLocation) FileBackedObjectLocation {
	c.lock.Lock()
	defer c.lock.Unlock()

	if c.savedErr != nil {
		return FileBackedObjectLocation{offsetBytes: math.MaxUint64}
	}

	fullData := contents.GetFullData()
	if _, err := c.writer.Write(fullData); err != nil {
		c.savedErr = err
		return FileBackedObjectLocation{offsetBytes: math.MaxUint64}
	}

	if len(children) != contents.GetDegree() {
		panic("number of children should match the degree")
	}
	for _, child := range children {
		var b [8]byte
		binary.LittleEndian.PutUint64(b[:], child.offsetBytes)
		if _, err := c.writer.Write(b[:]); err != nil {
			c.savedErr = err
			return FileBackedObjectLocation{offsetBytes: math.MaxUint64}
		}
	}

	startOffsetBytes := c.currentOffsetBytes
	c.currentOffsetBytes += uint64(len(fullData) + len(children)*8)
	return FileBackedObjectLocation{
		offsetBytes: startOffsetBytes,
	}
}

// Flush the contents of any lingering objects to disk. This function
// needs to be called to ensure that all objects can be retrieved
// afterwards.
func (c *FileWritingMerkleTreeCapturer) Flush() error {
	if c.savedErr != nil {
		return c.savedErr
	}
	return c.writer.Flush()
}

// FileBackedObjectLocation contains the location at which an object was
// written to disk. It can be used to reobtain the contents of the
// object later.
type FileBackedObjectLocation struct {
	offsetBytes uint64
}

var _ ReferenceMetadata = FileBackedObjectLocation{}

func (FileBackedObjectLocation) Discard() {}

// ExistingFileBackedObjectLocation is similar to
// dag.ExistingObjectContentsWalker. It is a placeholder value that
// results in a dag.ObjectContentsWalker that fails to complete.
var ExistingFileBackedObjectLocation = FileBackedObjectLocation{
	offsetBytes: math.MaxUint64 - 1,
}

// ReaderAtCloser is used by FileReadingObjectContentsWalkerFactory to
// access objects that were written to disk.
type ReaderAtCloser interface {
	io.ReaderAt
	io.Closer
}

// FileReadingObjectContentsWalkerFactory is a factory type for
// dag.ObjectContentsWalker that is capable of reading the contents of
// objects from disk, which were previously written by
// FileWritingMerkleTreeCapturer.
type FileReadingObjectContentsWalkerFactory struct {
	reader         ReaderAtCloser
	referenceCount atomic.Uint64
}

// NewFileReadingObjectContentsWalkerFactory creates a
// FileReadingObjectContentsWalkerFactory that is backed by a given
// file.
func NewFileReadingObjectContentsWalkerFactory(reader ReaderAtCloser) *FileReadingObjectContentsWalkerFactory {
	wf := &FileReadingObjectContentsWalkerFactory{
		reader: reader,
	}
	wf.referenceCount.Store(1)
	return wf
}

// Release the underlying file by closing it. This function must be
// invoked after all calls to CreateObjectContentsWalker have been made.
func (wf *FileReadingObjectContentsWalkerFactory) Release() {
	if wf.referenceCount.Add(^uint64(0)) == 0 {
		wf.reader.Close()
		wf.reader = nil
	}
}

// CreateObjectContentsWalker creates a dag.ObjectContentsWalker that
// reads the contents of an object from disk. The location of where the
// object is stored must be provided in the form of
// aFileBackedObjectLocation.
func (wf *FileReadingObjectContentsWalkerFactory) CreateObjectContentsWalker(reference object.LocalReference, location FileBackedObjectLocation) dag.ObjectContentsWalker {
	if wf.referenceCount.Add(1) < 2 {
		panic("invalid reference count")
	}
	return &fileReadingObjectContentsWalker{
		factory:   wf,
		reference: reference,
		location:  location,
	}
}

type fileReadingObjectContentsWalker struct {
	factory   *FileReadingObjectContentsWalkerFactory
	reference object.LocalReference
	location  FileBackedObjectLocation
}

func (w *fileReadingObjectContentsWalker) GetContents(ctx context.Context) (*object.Contents, []dag.ObjectContentsWalker, error) {
	refcountDelta := ^uint64(0)
	wf := w.factory
	w.factory = nil
	defer func() {
		if wf.referenceCount.Add(refcountDelta) == 0 {
			wf.reader.Close()
			wf.reader = nil
		}
	}()

	if w.location == ExistingFileBackedObjectLocation {
		return nil, nil, errors.New("contents for this object are not available for upload, as this object was expected to already exist")
	}

	// Restore the contents of the object.
	sizeBytes := w.reference.GetSizeBytes()
	fullData := make([]byte, sizeBytes)
	offsetBytes := w.location.offsetBytes
	if n, err := wf.reader.ReadAt(fullData, int64(offsetBytes)); n != sizeBytes {
		return nil, nil, err
	}
	contents, err := object.NewContentsFromFullData(w.reference, fullData)
	if err != nil {
		return nil, nil, err
	}

	degree := w.reference.GetDegree()
	if degree == 0 {
		return contents, nil, nil
	}

	// Construct walkers for the children of the object.
	offsets := make([]byte, degree*8)
	if n, err := wf.reader.ReadAt(offsets, int64(offsetBytes)+int64(sizeBytes)); n != len(offsets) {
		return nil, nil, err
	}
	walkers := make([]dag.ObjectContentsWalker, 0, degree)
	for i := 0; i < degree; i++ {
		walkers = append(walkers, &fileReadingObjectContentsWalker{
			factory:   wf,
			reference: contents.GetOutgoingReference(i),
			location: FileBackedObjectLocation{
				offsetBytes: binary.LittleEndian.Uint64(offsets[i*8:]),
			},
		})
	}

	// Extend the lifetime of the underlying file until all of the
	// walkers for the children are consumed.
	refcountDelta += uint64(degree)
	return contents, walkers, nil
}

func (w *fileReadingObjectContentsWalker) Discard() {
	w.factory.Release()
	w.factory = nil
}
