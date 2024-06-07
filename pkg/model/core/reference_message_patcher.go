package core

import (
	"bytes"
	"math"
	"sort"

	"github.com/buildbarn/bb-playground/pkg/ds"
	"github.com/buildbarn/bb-playground/pkg/proto/model/core"
	"github.com/buildbarn/bb-playground/pkg/storage/object"
)

type referenceMessage[TMetadata any] struct {
	message  core.Reference
	metadata TMetadata
}

// ReferenceMessagePatcher keeps track of all Reference messages that
// are contained within a given Protobuf message. For each of these
// Reference messages, it stores the actual object.LocalReference that
// is associated with them.
//
// Once the Protobuf message has been fully constructed,
// SortAndSetReferences() can be used to return a sorted, deduplicated
// list of all outgoing references of the message, and to update the
// indices in the Reference messages to refer to the correct outgoing
// reference.
type ReferenceMessagePatcher[TMetadata any] struct {
	messagesByReference map[object.LocalReference][]*referenceMessage[TMetadata]
	height              int
}

// NewReferenceMessagePatcher creates a new ReferenceMessagePatcher that
// does not contain any Reference messages.
func NewReferenceMessagePatcher[TMetadata any]() *ReferenceMessagePatcher[TMetadata] {
	return &ReferenceMessagePatcher[TMetadata]{
		messagesByReference: map[object.LocalReference][]*referenceMessage[TMetadata]{},
	}
}

func (p *ReferenceMessagePatcher[TMetadata]) maybeIncreaseHeight(height int) {
	if p.height < height {
		p.height = height
	}
}

// AddReference allocates a new Reference message that is associated
// with a given object.LocalReference and caller provided metadata.
func (p *ReferenceMessagePatcher[TMetadata]) AddReference(reference object.LocalReference, metadata TMetadata) *core.Reference {
	referenceMessage := &referenceMessage[TMetadata]{
		message: core.Reference{
			Index: math.MaxUint32,
		},
		metadata: metadata,
	}
	p.messagesByReference[reference] = append(p.messagesByReference[reference], referenceMessage)
	p.maybeIncreaseHeight(reference.GetHeight() + 1)
	return &referenceMessage.message
}

// Merge multiple instances of ReferenceMessagePatcher together. This
// method can be used when multiple Protobuf messages are combined into
// a larger message, and are eventually stored as a single object.
func (p *ReferenceMessagePatcher[TMetadata]) Merge(other *ReferenceMessagePatcher[TMetadata]) {
	// Reduce the worst-case time complexity by always merging the
	// small map into the larger one.
	if len(p.messagesByReference) < len(other.messagesByReference) {
		p.messagesByReference, other.messagesByReference = other.messagesByReference, p.messagesByReference
	}
	for reference, messages := range other.messagesByReference {
		if _, ok := p.messagesByReference[reference]; ok {
			p.messagesByReference[reference] = append(p.messagesByReference[reference], messages...)
		} else {
			p.messagesByReference[reference] = messages
		}
	}
	p.maybeIncreaseHeight(other.height)

	clear(other.messagesByReference)
	other.height = 0
}

// GetHeight returns the height that the object of the Protobuf message
// would have if it were created with the current set of outgoing
// references.
func (p *ReferenceMessagePatcher[TMetadata]) GetHeight() int {
	return p.height
}

// GetReferencesSizeBytes returns the size that all of the outgoing
// references would have if an object were created with the current set
// of outgoing references.
func (p *ReferenceMessagePatcher[TMetadata]) GetReferencesSizeBytes() int {
	for reference := range p.messagesByReference {
		return len(reference.GetRawReference()) * len(p.messagesByReference)
	}
	return 0
}

// SortAndSetReferences returns a sorted list of all outgoing references
// of the Protobuf message. This list can be provided to
// object.NewContents() to construct an actual object for storage. In
// addition to that, a list of user provided metadata is returned that
// sorted along the same order.
func (p *ReferenceMessagePatcher[TMetadata]) SortAndSetReferences() (object.OutgoingReferencesList, []TMetadata) {
	// Created a sorted list of outgoing references.
	sortedReferences := referencesList{
		Slice: make(ds.Slice[object.LocalReference], 0, len(p.messagesByReference)),
	}
	for reference := range p.messagesByReference {
		sortedReferences.Slice = append(sortedReferences.Slice, reference)
	}
	sort.Sort(sortedReferences)

	// Extract metadata associated with the references. Also assign
	// indices to the Reference messages. These should both respect
	// the same order as the outgoing references.
	sortedMetadata := make([]TMetadata, 0, len(p.messagesByReference))
	for i, reference := range sortedReferences.Slice {
		referenceMessages := p.messagesByReference[reference]
		for _, rm := range referenceMessages {
			rm.message.Index = uint32(i) + 1
		}
		sortedMetadata = append(sortedMetadata, referenceMessages[0].metadata)
	}
	return object.OutgoingReferencesList(sortedReferences.Slice), sortedMetadata
}

type referencesList struct {
	ds.Slice[object.LocalReference]
}

func (l referencesList) Less(i, j int) bool {
	return bytes.Compare(
		l.Slice[i].GetRawReference(),
		l.Slice[j].GetRawReference(),
	) < 0
}
