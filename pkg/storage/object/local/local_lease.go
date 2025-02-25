package local

import (
	"github.com/buildbarn/bonanza/pkg/encoding/varint"
	"github.com/buildbarn/bonanza/pkg/storage/object/leasemarshaling"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type LocalLease uint64

type localLeaseMarshaler struct{}

func (lm localLeaseMarshaler) MarshalLease(lease LocalLease, dst []byte) []byte {
	return varint.AppendForward(dst, lease)
}

func (lm localLeaseMarshaler) UnmarshalLease(src []byte) (LocalLease, error) {
	v, n := varint.ConsumeForward[LocalLease](src)
	if n != len(src) {
		return 0, status.Error(codes.InvalidArgument, "Malformed lease")
	}
	return v, nil
}

var LocalLeaseMarshaler leasemarshaling.LeaseMarshaler[LocalLease] = localLeaseMarshaler{}
