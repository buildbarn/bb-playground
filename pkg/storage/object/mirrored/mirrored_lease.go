package mirrored

import (
	"github.com/buildbarn/bb-playground/pkg/encoding/varint"
	"github.com/buildbarn/bb-playground/pkg/storage/object/leasemarshaling"
	"github.com/buildbarn/bb-storage/pkg/util"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// MirroredLease is the lease type that is returned by MirroredUploader.
// It contains a pair of leases for each of the replicas.
type MirroredLease[TLeaseA, TLeaseB any] struct {
	LeaseA TLeaseA
	LeaseB TLeaseB
}

type mirroredLeaseMarshaler[TLeaseA, TLeaseB any] struct {
	marshalerA leasemarshaling.LeaseMarshaler[TLeaseA]
	marshalerB leasemarshaling.LeaseMarshaler[TLeaseB]
}

// NewMirroredLeaseMarshaler returns a marshaler that is capable of
// marshaling instances of MirroredLease. These leases are marshaled by
// first storing leases of both of the replicas, followed by storing the
// length of lease A.
func NewMirroredLeaseMarshaler[TLeaseA, TLeaseB any](marshalerA leasemarshaling.LeaseMarshaler[TLeaseA], marshalerB leasemarshaling.LeaseMarshaler[TLeaseB]) leasemarshaling.LeaseMarshaler[MirroredLease[TLeaseA, TLeaseB]] {
	return &mirroredLeaseMarshaler[TLeaseA, TLeaseB]{
		marshalerA: marshalerA,
		marshalerB: marshalerB,
	}
}

func (lm *mirroredLeaseMarshaler[TLeaseA, TLeaseB]) MarshalLease(lease MirroredLease[TLeaseA, TLeaseB], dst []byte) []byte {
	dstA := lm.marshalerA.MarshalLease(lease.LeaseA, dst)
	lengthA := len(dstA) - len(dst)
	dst = lm.marshalerB.MarshalLease(lease.LeaseB, dstA)
	return varint.AppendBackward(dst, lengthA)
}

func (lm *mirroredLeaseMarshaler[TLeaseA, TLeaseB]) UnmarshalLease(src []byte) (MirroredLease[TLeaseA, TLeaseB], error) {
	// Extract the length of lease A.
	lengthA, n := varint.ConsumeBackward[uint](src)
	if n < 0 {
		return MirroredLease[TLeaseA, TLeaseB]{}, status.Error(codes.InvalidArgument, "Invalid length of lease A")
	}
	src = src[:len(src)-n]
	if lengthA > uint(len(src)) {
		return MirroredLease[TLeaseA, TLeaseB]{}, status.Errorf(
			codes.InvalidArgument,
			"Lease A is %d bytes in size, while space inside the mirrored lease is only %d bytes in size",
			lengthA,
			len(src),
		)
	}

	// Extract leases A and B.
	leaseA, err := lm.marshalerA.UnmarshalLease(src[:lengthA])
	if err != nil {
		return MirroredLease[TLeaseA, TLeaseB]{}, util.StatusWrap(err, "Lease A")
	}
	leaseB, err := lm.marshalerB.UnmarshalLease(src[lengthA:])
	if err != nil {
		return MirroredLease[TLeaseA, TLeaseB]{}, util.StatusWrap(err, "Lease B")
	}
	return MirroredLease[TLeaseA, TLeaseB]{
		LeaseA: leaseA,
		LeaseB: leaseB,
	}, nil
}
