package core

import (
	"bytes"
	"math"
	"sort"

	"github.com/buildbarn/bb-playground/pkg/ds"
	"github.com/buildbarn/bb-playground/pkg/proto/model/core"
	"github.com/buildbarn/bb-playground/pkg/storage/object"

	"google.golang.org/protobuf/reflect/protoreflect"
)

type ReferenceMetadata interface {
	Discard()
}

type NoopReferenceMetadata struct{}

func (NoopReferenceMetadata) Discard() {}

type ReferenceMetadataCreator[T any] func(index int) T

type referenceMessages[TMetadata ReferenceMetadata] struct {
	metadata TMetadata
	indices  []*uint32
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
type ReferenceMessagePatcher[TMetadata ReferenceMetadata] struct {
	messagesByReference map[object.LocalReference]referenceMessages[TMetadata]
	height              int
}

// NewReferenceMessagePatcher creates a new ReferenceMessagePatcher that
// does not contain any Reference messages.
func NewReferenceMessagePatcher[TMetadata ReferenceMetadata]() *ReferenceMessagePatcher[TMetadata] {
	return &ReferenceMessagePatcher[TMetadata]{}
}

func (p *ReferenceMessagePatcher[TMetadata]) maybeIncreaseHeight(height int) {
	if p.height < height {
		p.height = height
	}
}

// AddReference allocates a new Reference message that is associated
// with a given object.LocalReference and caller provided metadata.
func (p *ReferenceMessagePatcher[TMetadata]) AddReference(reference object.LocalReference, metadata TMetadata) *core.Reference {
	message := &core.Reference{
		Index: math.MaxUint32,
	}
	p.addReferenceMessage(message, reference, metadata)
	return message
}

func (p *ReferenceMessagePatcher[TMetadata]) addReferenceMessage(message *core.Reference, reference object.LocalReference, metadata TMetadata) {
	if p.messagesByReference == nil {
		p.messagesByReference = map[object.LocalReference]referenceMessages[TMetadata]{}
	}
	if existingMessages, ok := p.messagesByReference[reference]; ok {
		metadata.Discard()
		p.messagesByReference[reference] = referenceMessages[TMetadata]{
			metadata: existingMessages.metadata,
			indices:  append(p.messagesByReference[reference].indices, &message.Index),
		}
	} else {
		p.messagesByReference[reference] = referenceMessages[TMetadata]{
			metadata: metadata,
			indices:  []*uint32{&message.Index},
		}
		p.maybeIncreaseHeight(reference.GetHeight() + 1)
	}
}

func (p *ReferenceMessagePatcher[TMetadata]) addReferenceMessagesRecursively(message protoreflect.Message, outgoingReferences object.OutgoingReferences, createMetadata ReferenceMetadataCreator[TMetadata]) {
	if m, ok := message.Interface().(*core.Reference); ok {
		// If the reference message refers to a valid object,
		// let it be managed by the patcher. If it is invalid,
		// we at least change the index to MaxUint32, so that
		// any future attempts to resolve it will fail.
		if index, err := GetIndexFromReferenceMessage(m, outgoingReferences.GetDegree()); err == nil {
			reference := outgoingReferences.GetOutgoingReference(index)
			p.addReferenceMessage(m, reference, createMetadata(index))
		}
		m.Index = math.MaxUint32
	} else {
		message.Range(func(fieldDescriptor protoreflect.FieldDescriptor, value protoreflect.Value) bool {
			if k := fieldDescriptor.Kind(); k == protoreflect.MessageKind || k == protoreflect.GroupKind {
				if fieldDescriptor.IsList() {
					l := value.List()
					n := l.Len()
					for i := 0; i < n; i++ {
						p.addReferenceMessagesRecursively(l.Get(i).Message(), outgoingReferences, createMetadata)
					}
				} else {
					p.addReferenceMessagesRecursively(value.Message(), outgoingReferences, createMetadata)
				}
			}
			return true
		})
	}
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
	for reference, newMessages := range other.messagesByReference {
		if existingMessages, ok := p.messagesByReference[reference]; ok {
			newMessages.metadata.Discard()
			p.messagesByReference[reference] = referenceMessages[TMetadata]{
				metadata: existingMessages.metadata,
				indices:  append(existingMessages.indices, newMessages.indices...),
			}
		} else {
			p.messagesByReference[reference] = newMessages
		}
	}
	p.maybeIncreaseHeight(other.height)
	other.empty()
}

func (p *ReferenceMessagePatcher[TMetadata]) empty() {
	clear(p.messagesByReference)
	p.height = 0
}

func (p *ReferenceMessagePatcher[TMetadata]) Discard() {
	for _, messages := range p.messagesByReference {
		messages.metadata.Discard()
	}
	p.empty()
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
		for _, index := range referenceMessages.indices {
			*index = uint32(i) + 1
		}
		sortedMetadata = append(sortedMetadata, referenceMessages.metadata)
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

// MapReferenceMessagePatcherMetadata replaces a ReferenceMessagePatcher
// with a new instance that contains the same references, but has
// metadata mapped to other values, potentially of another type.
func MapReferenceMessagePatcherMetadata[TOld, TNew ReferenceMetadata](pOld *ReferenceMessagePatcher[TOld], mapMetadata func(object.LocalReference, TOld) TNew) *ReferenceMessagePatcher[TNew] {
	pNew := &ReferenceMessagePatcher[TNew]{
		messagesByReference: make(map[object.LocalReference]referenceMessages[TNew], len(pOld.messagesByReference)),
		height:              pOld.height,
	}
	for reference, oldMessages := range pOld.messagesByReference {
		pNew.messagesByReference[reference] = referenceMessages[TNew]{
			metadata: mapMetadata(reference, oldMessages.metadata),
			indices:  oldMessages.indices,
		}
	}
	pOld.empty()
	return pNew
}

type ReferenceMetadataCreatorForTesting ReferenceMetadataCreator[ReferenceMetadata]
