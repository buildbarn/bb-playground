package object

import (
	"github.com/buildbarn/bb-playground/pkg/proto/storage/object"
)

type Limit struct {
	count     uint32
	sizeBytes uint64
}

func NewLimit(m *object.Limit) Limit {
	return Limit{
		count:     m.Count,
		sizeBytes: m.SizeBytes,
	}
}

func (l Limit) Min(other Limit) Limit {
	return Limit{
		count:     min(l.count, other.count),
		sizeBytes: min(l.sizeBytes, other.sizeBytes),
	}
}

func (l Limit) ToProto() *object.Limit {
	return &object.Limit{
		Count:     l.count,
		SizeBytes: l.sizeBytes,
	}
}

func (l *Limit) CanAcquireObjectAndChildren(reference LocalReference) bool {
	count := uint32(reference.GetHeight())
	sizeBytes := uint64(reference.GetMaximumTotalParentsSizeBytes(true))
	return count <= l.count && sizeBytes <= l.sizeBytes
}

func (l *Limit) AcquireObjectAndChildren(reference LocalReference) bool {
	count := uint32(reference.GetHeight())
	sizeBytes := uint64(reference.GetMaximumTotalParentsSizeBytes(true))
	if count > l.count || sizeBytes > l.sizeBytes {
		return false
	}

	l.count -= count
	l.sizeBytes -= sizeBytes
	return true
}

func (l *Limit) ReleaseObject(reference LocalReference) {
	if reference.GetHeight() > 0 {
		l.count += 1
		l.sizeBytes += uint64(reference.GetSizeBytes())
	}
}

func (l *Limit) ReleaseChildren(reference LocalReference) {
	if h := reference.GetHeight(); h > 0 {
		l.count += uint32(h - 1)
		l.sizeBytes += uint64(reference.GetMaximumTotalParentsSizeBytes(false))
	}
}
