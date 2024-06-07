package filesystem

import (
	"sort"

	"github.com/buildbarn/bb-playground/pkg/storage/object"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type fileContentsSpan struct {
	startBytes uint64
	list       FileContentsList
}

// FileContentsIterator is a helper type for iterating over the chunks
// of a concatenated file sequentially.
type FileContentsIterator struct {
	spans              []fileContentsSpan
	initialOffsetBytes uint64
}

// NewFileContentsIterator creates a FileContentsIterator that starts
// iteration at the provided offset within the file. It is the caller's
// responsibility to ensure the provided offset is less than the size of
// the file.
func NewFileContentsIterator(root FileContentsEntry, initialOffsetBytes uint64) FileContentsIterator {
	return FileContentsIterator{
		spans: append(
			make([]fileContentsSpan, 0, root.Reference.GetHeight()+1),
			fileContentsSpan{
				list: FileContentsList{
					root,
					// Sentinel to permit calling ToNextPart()
					// while at the end of the file.
					FileContentsEntry{},
				},
			},
		),
		initialOffsetBytes: initialOffsetBytes,
	}
}

// GetCurrentPart returns the reference of the part of the file that
// contain the data corresponding with the current offset. It also
// returns the offset within the part from which data should be read,
// and the expected total size of the part.
//
// It is the caller's responsibility to track whether iteration has
// reached the end of the file. Once the end of the file has been
// reached, GetCurrentPart() may no longer be called.
func (i *FileContentsIterator) GetCurrentPart() (reference object.LocalReference, offsetBytes, sizeBytes uint64) {
	lastSpan := &i.spans[len(i.spans)-1]
	return lastSpan.list[0].Reference, i.initialOffsetBytes, lastSpan.list[0].EndBytes - lastSpan.startBytes
}

// PushFileContentsList can be invoked after GetCurrentPart() to signal
// that the current part does not refer to a chunk of data, but another
// FileContentsList. After calling this method, another call to
// GetCurrentPart() can be made to retry resolution of the part within
// the provided FileContentsList.
func (i *FileContentsIterator) PushFileContentsList(list FileContentsList) error {
	lastSpan := &i.spans[len(i.spans)-1]
	if actualSizeBytes, expectedSizeBytes := list[len(list)-1].EndBytes, lastSpan.list[0].EndBytes-lastSpan.startBytes; actualSizeBytes != expectedSizeBytes {
		return status.Errorf(codes.InvalidArgument, "Parts in the file contents list have a total size of %d bytes, while %d bytes were expected", actualSizeBytes, expectedSizeBytes)
	}
	startBytes, toSkip := uint64(0), 0
	if i.initialOffsetBytes >= list[0].EndBytes {
		// Initial offset at which we need to start reading does
		// not lie within the first part. Find the part that
		// contains the requested offset.
		n := sort.Search(len(list)-1, func(index int) bool {
			return i.initialOffsetBytes < list[index+1].EndBytes
		})
		startBytes = list[n].EndBytes
		toSkip = n + 1
	}
	i.spans = append(i.spans, fileContentsSpan{
		startBytes: startBytes,
		list:       list[toSkip:],
	})
	i.initialOffsetBytes -= startBytes
	return nil
}

// ToNextPart can be invoked after GetCurrentPart() to signal that the
// current part refers to a chunk of data. The next call to
// GetCurrentPart() will return the reference of the part that is stored
// after the current one.
func (i *FileContentsIterator) ToNextPart() {
	for len(i.spans[len(i.spans)-1].list) == 1 {
		i.spans = i.spans[:len(i.spans)-1]
	}
	lastSpan := &i.spans[len(i.spans)-1]
	lastSpan.startBytes = lastSpan.list[0].EndBytes
	lastSpan.list = lastSpan.list[1:]
	i.initialOffsetBytes = 0
}
