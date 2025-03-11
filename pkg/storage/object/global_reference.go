package object

import (
	"strings"
)

// GlobalReference uniquely identifies an object across all namespaces.
type GlobalReference struct {
	InstanceName
	LocalReference
}

// MustNewSHA256V1GlobalReference creates a global reference that uses
// reference format SHA256_V1. This function can be used as part of
// tests.
func MustNewSHA256V1GlobalReference(instanceName, hash string, sizeBytes uint32, height uint8, degree uint16, maximumTotalParentsSizeBytes uint64) GlobalReference {
	return GlobalReference{
		InstanceName:   NewInstanceName(instanceName),
		LocalReference: MustNewSHA256V1LocalReference(hash, sizeBytes, height, degree, maximumTotalParentsSizeBytes),
	}
}

// GetNamespace returns the namespace that contains the object.
func (r GlobalReference) GetNamespace() Namespace {
	return Namespace{
		InstanceName:    r.InstanceName,
		ReferenceFormat: r.LocalReference.GetReferenceFormat(),
	}
}

// Flatten a reference, so that its height and degree are both zero.
//
// This is used by the read caching backend, where we want to cache
// individual objects as opposed to graphs. When writing objects to the
// local cache, we set the height and degree to zero, so that there is
// no need to track any leases.
func (r GlobalReference) Flatten() GlobalReference {
	return GlobalReference{
		InstanceName:   r.InstanceName,
		LocalReference: r.LocalReference.Flatten(),
	}
}

// CompareByHeight returns -1 if the reference comes before another
// reference along a total order based on the references' height and
// maximum total parents size. Conversely, it returns 1 if the reference
// comes after another reference. 0 is returned if both references are
// equal.
//
// This order can be used to perform bounded parallel traversal of DAGs
// in such a way that forward progress is guaranteed.
func (r GlobalReference) CompareByHeight(other GlobalReference) int {
	if cmp := r.LocalReference.CompareByHeight(other.LocalReference); cmp != 0 {
		return cmp
	}
	return strings.Compare(r.InstanceName.value, other.InstanceName.value)
}

// WithLocalReference returns a new GlobalReference that has its value
// replaced with another.
//
// This method can be used to upgrade an outgoing reference of an object
// from a local reference to a global reference within the same
// namespace as the object.
func (r GlobalReference) WithLocalReference(localReference LocalReference) GlobalReference {
	return r.InstanceName.WithLocalReference(localReference)
}
