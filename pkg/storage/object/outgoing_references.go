package object

// OutgoingReferences is a list of outgoing references of an object. It
// may either be implemented by a simple slice, or it's possible to
// provide an implementation that reads references directly out of the
// object's contents.
type OutgoingReferences interface {
	GetDegree() int
	GetOutgoingReference(index int) LocalReference
	GetOutgoingReferencesList() OutgoingReferencesList
}

// OutgoingReferencesList is a list of outgoing references of an object
// that is backed by a simple slice.
type OutgoingReferencesList []LocalReference

var _ OutgoingReferences = OutgoingReferencesList{}

func (l OutgoingReferencesList) GetDegree() int {
	return len(l)
}

func (l OutgoingReferencesList) GetOutgoingReference(index int) LocalReference {
	return l[index]
}

func (l OutgoingReferencesList) GetOutgoingReferencesList() OutgoingReferencesList {
	// References are already stored in a list.
	return l
}
