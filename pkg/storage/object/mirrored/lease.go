package mirrored

import (
	"github.com/buildbarn/bb-storage/pkg/util"
	"github.com/buildbarn/bonanza/pkg/encoding/varint"
	"github.com/buildbarn/bonanza/pkg/storage/object/leasemarshaling"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Lease type that is returned by Uploader. It contains a pair of leases
// for each of the replicas.
type Lease[TLeaseA, TLeaseB any] struct {
	LeaseA TLeaseA
	LeaseB TLeaseB
}

type leaseMarshaler[TLeaseA, TLeaseB any] struct {
	marshalerA leasemarshaling.LeaseMarshaler[TLeaseA]
	marshalerB leasemarshaling.LeaseMarshaler[TLeaseB]
}

// NewLeaseMarshaler returns a marshaler that is capable of marshaling
// instances of Lease. These leases are marshaled by first storing
// leases of both of the replicas, followed by storing the length of
// lease A.
func NewLeaseMarshaler[TLeaseA, TLeaseB any](marshalerA leasemarshaling.LeaseMarshaler[TLeaseA], marshalerB leasemarshaling.LeaseMarshaler[TLeaseB]) leasemarshaling.LeaseMarshaler[Lease[TLeaseA, TLeaseB]] {
	return &leaseMarshaler[TLeaseA, TLeaseB]{
		marshalerA: marshalerA,
		marshalerB: marshalerB,
	}
}

func (lm *leaseMarshaler[TLeaseA, TLeaseB]) MarshalLease(lease Lease[TLeaseA, TLeaseB], dst []byte) []byte {
	dstA := lm.marshalerA.MarshalLease(lease.LeaseA, dst)
	lengthA := len(dstA) - len(dst)
	dst = lm.marshalerB.MarshalLease(lease.LeaseB, dstA)
	return varint.AppendBackward(dst, lengthA)
}

func (lm *leaseMarshaler[TLeaseA, TLeaseB]) UnmarshalLease(src []byte) (Lease[TLeaseA, TLeaseB], error) {
	// Extract the length of lease A.
	lengthA, n := varint.ConsumeBackward[uint](src)
	if n < 0 {
		return Lease[TLeaseA, TLeaseB]{}, status.Error(codes.InvalidArgument, "Invalid length of lease A")
	}
	src = src[:len(src)-n]
	if lengthA > uint(len(src)) {
		return Lease[TLeaseA, TLeaseB]{}, status.Errorf(
			codes.InvalidArgument,
			"Lease A is %d bytes in size, while space inside the mirrored lease is only %d bytes in size",
			lengthA,
			len(src),
		)
	}

	// Extract leases A and B.
	leaseA, err := lm.marshalerA.UnmarshalLease(src[:lengthA])
	if err != nil {
		return Lease[TLeaseA, TLeaseB]{}, util.StatusWrap(err, "Lease A")
	}
	leaseB, err := lm.marshalerB.UnmarshalLease(src[lengthA:])
	if err != nil {
		return Lease[TLeaseA, TLeaseB]{}, util.StatusWrap(err, "Lease B")
	}
	return Lease[TLeaseA, TLeaseB]{
		LeaseA: leaseA,
		LeaseB: leaseB,
	}, nil
}
