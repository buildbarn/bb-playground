package inlinedtree

import (
	"container/heap"

	"github.com/buildbarn/bb-playground/pkg/ds"
	"github.com/buildbarn/bb-playground/pkg/encoding"
	model_core "github.com/buildbarn/bb-playground/pkg/model/core"
	model_filesystem_pb "github.com/buildbarn/bb-playground/pkg/proto/model/filesystem"
	"github.com/buildbarn/bb-playground/pkg/storage/object"
	"github.com/buildbarn/bb-storage/pkg/util"

	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/proto"
)

// ParentAppender is invoked to append the data to be stored in the
// parent message. If externalContents is not nil, the data is about to
// be stored in an external message. If externalMessage is nil, the data
// may be inlined.
//
// This function may be invoked multiple times to determine how the size
// of the parent object is affected by either inlining the data or
// storing it externally.
type ParentAppender[TParentMessage proto.Message, TMetadata any] func(
	parent model_core.MessageWithReferences[TParentMessage, TMetadata],
	externalContents *object.Contents,
	externalMetadata []TMetadata,
)

// Candidate of data that needs to be stored in a message, but can
// optionally be stored in a child object if no space is present.
type Candidate[TParentMessage proto.Message, TMetadata any] struct {
	// Message to store in a child object if no space is present to
	// inline it into the parent object.
	ExternalMessage model_core.MessageWithReferences[proto.Message, TMetadata]
	// Function to invoke to either inline the message into the
	// output, or create a reference to the child object.
	ParentAppender ParentAppender[TParentMessage, TMetadata]
}

var marshalOptions = proto.MarshalOptions{UseCachedSize: true}

type queuedCandidate struct {
	candidateIndex           int
	inlinedSizeIncreaseBytes int
}

type queuedCandidates struct {
	ds.Slice[queuedCandidate]
}

func (l queuedCandidates) Less(i, j int) bool {
	return l.Slice[i].inlinedSizeIncreaseBytes < l.Slice[j].inlinedSizeIncreaseBytes
}

// Build a Protobuf message that contains data, either inlined into the
// message directly or stored in the form of a reference to a separate
// object.
//
// Data to be inlined needs to be provided in the form of candidates.
// For each candidate, a function needs to be provided that can either
// inline the data into the message or emit a reference to it. These
// functions are eventually applied in the same order in which they are
// provided, meaning that the order in which items are appended to lists
// is preserved.
//
// This function selects candidates to inline based on their height or
// size. It initially attempts to keep the height of the resulting
// message minimal, followed by inlining as many candidates as possible
// to fill up the remaining space.
func Build[
	TParentMessage any,
	TMetadata any,
	TParentMessagePtr interface {
		*TParentMessage
		proto.Message
	},
](
	referenceFormat object.ReferenceFormat,
	encoder encoding.BinaryEncoder,
	candidates []Candidate[TParentMessagePtr, TMetadata],
	maximumSizeBytes int,
) (model_core.MessageWithReferences[TParentMessagePtr, TMetadata], error) {
	// Start off with an empty output message.
	output := model_core.MessageWithReferences[TParentMessagePtr, TMetadata]{
		Message: TParentMessagePtr(new(TParentMessage)),
		Patcher: model_core.NewReferenceMessagePatcher[TMetadata](),
	}
	outputSizeBytes := 0

	// For each candidate, compute how much the size of the output
	// message increases, either when inlined or stored externally.
	bogusContents, err := referenceFormat.NewContents(nil, []byte("A"))
	if err != nil {
		panic(err)
	}
	candidatesToInline := make([]bool, len(candidates))
	queuedCandidates := queuedCandidates{
		Slice: make(ds.Slice[queuedCandidate], 0, len(candidates)),
	}
	for i, candidate := range candidates {
		parentInlined := model_core.MessageWithReferences[TParentMessagePtr, TMetadata]{
			Message: TParentMessagePtr(new(TParentMessage)),
		}
		candidate.ParentAppender(parentInlined, nil, nil)
		inlinedSizeBytes := candidate.ExternalMessage.Patcher.GetReferencesSizeBytes() + marshalOptions.Size(parentInlined.Message)

		parentExternal := model_core.MessageWithReferences[TParentMessagePtr, TMetadata]{
			Message: TParentMessagePtr(new(TParentMessage)),
			Patcher: model_core.NewReferenceMessagePatcher[TMetadata](),
		}
		candidate.ParentAppender(parentExternal, bogusContents, nil)
		externalSizeBytes := parentExternal.Patcher.GetReferencesSizeBytes() + marshalOptions.Size(parentExternal.Message)

		if inlinedSizeBytes <= externalSizeBytes {
			// Inlining the data is smaller than storing it
			// externally. Inline it immediately.
			candidatesToInline[i] = true
			output.Patcher.Merge(candidate.ExternalMessage.Patcher)
			outputSizeBytes += inlinedSizeBytes
		} else {
			// Inlining the data takes up more space. Queue
			// it, so that we can inline it if enough space
			// is available.
			outputSizeBytes += externalSizeBytes
			queuedCandidates.Push(queuedCandidate{
				candidateIndex:           i,
				inlinedSizeIncreaseBytes: inlinedSizeBytes - externalSizeBytes,
			})
		}
	}

	// Determine how much space is needed to inline all candidates
	// that would otherwise cause the height of the object tree to
	// increase. If space is available, inline them all. This
	// ensures that the height is minimized.
	heightAllInlined := output.Patcher.GetHeight()
	highestInlinedSizeIncreaseBytes := 0
	for _, queuedCandidate := range queuedCandidates.Slice {
		candidate := &candidates[queuedCandidate.candidateIndex]
		if h := candidate.ExternalMessage.Patcher.GetHeight(); heightAllInlined == h {
			highestInlinedSizeIncreaseBytes += queuedCandidate.inlinedSizeIncreaseBytes
		} else if heightAllInlined < h {
			heightAllInlined = h
			highestInlinedSizeIncreaseBytes = queuedCandidate.inlinedSizeIncreaseBytes
		}
	}

	if newOutputSizeBytes := outputSizeBytes + highestInlinedSizeIncreaseBytes; newOutputSizeBytes <= maximumSizeBytes {
		for i := 0; i < queuedCandidates.Len(); {
			queuedCandidate := &queuedCandidates.Slice[i]
			candidate := &candidates[queuedCandidate.candidateIndex]
			if candidate.ExternalMessage.Patcher.GetHeight() == heightAllInlined {
				candidatesToInline[queuedCandidate.candidateIndex] = true
				output.Patcher.Merge(candidate.ExternalMessage.Patcher)

				queuedCandidates.Swap(i, queuedCandidates.Len()-1)
				queuedCandidates.Pop()
			} else {
				i++
			}
		}
		outputSizeBytes = newOutputSizeBytes
	}

	// Fill the remaining space with as many candidates as possible,
	// inlining the smallest ones first. Even though this is
	// detrimental to the overall height, it reduces the total
	// number of objects significantly.
	//
	// It may be that multiple candidates share the same size. In
	// that case we could inline the candidate with the lowest
	// index. However, this has the disadvantage that the order in
	// which candidates are provided to this function matters even
	// if they pertain to different fields, making refactoring hard.
	//
	// Work around this by inlining all equally sized candidates
	// collectively. If there is no space to fit all of them, we
	// store them externally and inline less preferential candidates
	// instead.
	heap.Init(&queuedCandidates)
	var queuedCandidatesWithSameSize []queuedCandidate
	for queuedCandidates.Len() > 0 && outputSizeBytes+queuedCandidates.Slice[0].inlinedSizeIncreaseBytes <= maximumSizeBytes {
		queuedCandidatesWithSameSize = append(queuedCandidatesWithSameSize[:0], heap.Pop(&queuedCandidates).(queuedCandidate))
		inlinedSizeIncreaseBytes := queuedCandidatesWithSameSize[0].inlinedSizeIncreaseBytes
		for queuedCandidates.Len() > 0 && queuedCandidates.Slice[0].inlinedSizeIncreaseBytes == inlinedSizeIncreaseBytes {
			queuedCandidatesWithSameSize = append(queuedCandidatesWithSameSize, heap.Pop(&queuedCandidates).(queuedCandidate))
		}

		if newOutputSizeBytes := outputSizeBytes + inlinedSizeIncreaseBytes*len(queuedCandidatesWithSameSize); newOutputSizeBytes <= maximumSizeBytes {
			for _, queuedCandidate := range queuedCandidatesWithSameSize {
				candidate := &candidates[queuedCandidate.candidateIndex]
				candidatesToInline[queuedCandidate.candidateIndex] = true
				output.Patcher.Merge(candidate.ExternalMessage.Patcher)
			}
			outputSizeBytes = newOutputSizeBytes
		}
	}

	for i, candidate := range candidates {
		if candidatesToInline[i] {
			// Inline the message into the parent.
			candidate.ParentAppender(output, nil, nil)
		} else {
			// Store the message separately, and store a
			// reference in the parent.
			references, metadata := candidate.ExternalMessage.Patcher.SortAndSetReferences()
			data, err := marshalOptions.Marshal(candidate.ExternalMessage.Message)
			if err != nil {
				return model_core.MessageWithReferences[TParentMessagePtr, TMetadata]{}, util.StatusWrapfWithCode(err, codes.InvalidArgument, "Failed to marshal candidate at index %d", i)
			}
			encodedData, err := encoder.EncodeBinary(data)
			if err != nil {
				return model_core.MessageWithReferences[TParentMessagePtr, TMetadata]{}, util.StatusWrapf(err, "Failed to encode candidate at index %d", i)
			}
			contents, err := referenceFormat.NewContents(references, encodedData)
			if err != nil {
				return model_core.MessageWithReferences[TParentMessagePtr, TMetadata]{}, util.StatusWrapf(err, "Failed to create object contents for candidate at index %d", i)
			}
			candidate.ParentAppender(output, contents, metadata)
		}
	}
	return output, nil
}

type ParentAppenderForTesting ParentAppender[*model_filesystem_pb.Directory, string]
