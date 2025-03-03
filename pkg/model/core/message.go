package core

import (
	"reflect"

	model_core_pb "github.com/buildbarn/bonanza/pkg/proto/model/core"
	"github.com/buildbarn/bonanza/pkg/storage/object"

	"google.golang.org/protobuf/proto"
)

type Message[TMessage any, TOutgoingReferences object.OutgoingReferences] struct {
	Message            TMessage
	OutgoingReferences TOutgoingReferences
}

// NewMessage is a helper function for creating instances of Message.
func NewMessage[TMessage any, TOutgoingReferences object.OutgoingReferences](
	m TMessage,
	outgoingReferences TOutgoingReferences,
) Message[TMessage, TOutgoingReferences] {
	return Message[TMessage, TOutgoingReferences]{
		Message:            m,
		OutgoingReferences: outgoingReferences,
	}
}

// NewNestedMessage is a helper function for creating instances of
// Message that refer to a message that was mebedded into another one.
func NewNestedMessage[TMessage1, TMessage2 any, TOutgoingReferences object.OutgoingReferences](parent Message[TMessage1, TOutgoingReferences], child TMessage2) Message[TMessage2, TOutgoingReferences] {
	return Message[TMessage2, TOutgoingReferences]{
		Message:            child,
		OutgoingReferences: parent.OutgoingReferences,
	}
}

func (m Message[TMessage, TOutgoingReferences]) IsSet() bool {
	// TODO: Is there a way we can implement this without
	// reflection? If not, should we remove this method and make
	// callers check the fields themselves?
	v := reflect.ValueOf(m.OutgoingReferences)
	return v.IsValid() && !v.IsNil()
}

func (m *Message[TMessage, TOutgoingReferences]) Clear() {
	*m = Message[TMessage, TOutgoingReferences]{}
}

// GetOutgoingReference is a utility function for obtaining an outgoing
// reference corresponding to Reference message that is a child of the
// current message.
func (m *Message[TMessage, TOutgoingReferences]) GetOutgoingReference(reference *model_core_pb.Reference) (object.LocalReference, error) {
	index, err := GetIndexFromReferenceMessage(reference, m.OutgoingReferences.GetDegree())
	if err != nil {
		var badReference object.LocalReference
		return badReference, err
	}
	return m.OutgoingReferences.GetOutgoingReference(index), nil
}

func MessagesEqual[TMessage proto.Message, TOutgoingReferences object.OutgoingReferences](m1, m2 Message[TMessage, TOutgoingReferences]) bool {
	degree1, degree2 := m1.OutgoingReferences.GetDegree(), m2.OutgoingReferences.GetDegree()
	if degree1 != degree2 {
		return false
	}
	for i := 0; i < degree1; i++ {
		if m1.OutgoingReferences.GetOutgoingReference(i) != m2.OutgoingReferences.GetOutgoingReference(i) {
			return false
		}
	}
	return proto.Equal(m1.Message, m2.Message)
}
