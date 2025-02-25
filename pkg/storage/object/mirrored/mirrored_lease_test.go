package mirrored_test

import (
	"testing"

	"github.com/buildbarn/bb-storage/pkg/testutil"
	"github.com/buildbarn/bonanza/pkg/storage/object/mirrored"
	"github.com/stretchr/testify/require"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"go.uber.org/mock/gomock"
)

func TestMirroredLeaseMarshaler(t *testing.T) {
	ctrl := gomock.NewController(t)

	marshalerA := NewMockLeaseMarshalerForTesting(ctrl)
	marshalerB := NewMockLeaseMarshalerForTesting(ctrl)
	marshaler := mirrored.NewMirroredLeaseMarshaler(marshalerA, marshalerB)

	t.Run("Identity", func(t *testing.T) {
		// Marshal a pair of integer leases. If we unmarshal
		// these, we should get the original integer values
		// back.
		leaseTemplate := make([]byte, 0, 100000)
		for i := 0; i < 100000; i++ {
			leaseTemplate = append(leaseTemplate, byte(i))
		}

		for i := 0; i+1 < len(leaseTemplate); i += 51 {
			marshalerA.EXPECT().MarshalLease(i, gomock.Any()).
				DoAndReturn(func(lease int, dst []byte) []byte {
					return append(dst, leaseTemplate[:i]...)
				})
			marshalerB.EXPECT().MarshalLease(i+1, gomock.Any()).
				DoAndReturn(func(lease int, dst []byte) []byte {
					return append(dst, leaseTemplate[:i+1]...)
				})

			marshaledLease := marshaler.MarshalLease(mirrored.MirroredLease[any, any]{
				LeaseA: i,
				LeaseB: i + 1,
			}, nil)

			marshalerA.EXPECT().UnmarshalLease(leaseTemplate[:i]).
				Return(i, nil)
			marshalerB.EXPECT().UnmarshalLease(leaseTemplate[:i+1]).
				Return(i+1, nil)

			unmarshaledLease, err := marshaler.UnmarshalLease(marshaledLease)
			require.NoError(t, err)
			require.Equal(t, mirrored.MirroredLease[any, any]{
				LeaseA: i,
				LeaseB: i + 1,
			}, unmarshaledLease)
		}
	})

	t.Run("TrailingLengthMissing", func(t *testing.T) {
		_, err := marshaler.UnmarshalLease(nil)
		testutil.RequireEqualStatus(t, status.Error(codes.InvalidArgument, "Invalid length of lease A"), err)
	})

	t.Run("LeaseATooLarge", func(t *testing.T) {
		_, err := marshaler.UnmarshalLease([]byte{0x81})
		testutil.RequireEqualStatus(t, status.Error(codes.InvalidArgument, "Lease A is 1 bytes in size, while space inside the mirrored lease is only 0 bytes in size"), err)
	})
}
