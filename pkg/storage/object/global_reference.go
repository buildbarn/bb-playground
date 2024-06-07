package object

import (
	"strings"
)

type GlobalReference struct {
	InstanceName
	LocalReference
}

func MustNewSHA256V1GlobalReference(instanceName, hash string, sizeBytes uint32, height uint8, degree uint16, maximumTotalParentsSizeBytes uint64) GlobalReference {
	return GlobalReference{
		InstanceName:   NewInstanceName(instanceName),
		LocalReference: MustNewSHA256V1LocalReference(hash, sizeBytes, height, degree, maximumTotalParentsSizeBytes),
	}
}

func (r GlobalReference) GetNamespace() Namespace {
	return Namespace{
		InstanceName:    r.InstanceName,
		ReferenceFormat: r.LocalReference.GetReferenceFormat(),
	}
}

func (r GlobalReference) Flatten() GlobalReference {
	return GlobalReference{
		InstanceName:   r.InstanceName,
		LocalReference: r.LocalReference.Flatten(),
	}
}

func (r GlobalReference) CompareByHeight(other GlobalReference) int {
	if cmp := r.LocalReference.CompareByHeight(other.LocalReference); cmp != 0 {
		return cmp
	}
	return strings.Compare(r.InstanceName.value, other.InstanceName.value)
}

func (r *GlobalReference) SetLocalReference(localReference LocalReference) {
	r.LocalReference = localReference
}

func (r GlobalReference) WithLocalReference(localReference LocalReference) GlobalReference {
	return r.InstanceName.WithLocalReference(localReference)
}
