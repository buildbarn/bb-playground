package object

// BasicReference is a constraint to be used on generic functions and
// types that work with references, and need to gain access to at least
// some of the basic properties of a reference.
type BasicReference interface {
	// Accessors of individual fields contained in the reference.
	GetHash() []byte
	GetSizeBytes() int
	GetHeight() int
	GetDegree() int
	GetMaximumTotalParentsSizeBytes(includeSelf bool) int

	// Binary representation.
	GetReferenceFormat() ReferenceFormat
	GetRawReference() []byte

	// Conversion to a LocalReference with the same value.
	GetLocalReference() LocalReference
}
