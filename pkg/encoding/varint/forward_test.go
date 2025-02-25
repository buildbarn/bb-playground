package varint_test

import (
	"bytes"
	"math"
	"math/rand/v2"
	"testing"

	"github.com/buildbarn/bb-storage/pkg/testutil"
	"github.com/buildbarn/bonanza/pkg/encoding/varint"
	"github.com/stretchr/testify/require"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestForward(t *testing.T) {
	t.Run("Int8", func(t *testing.T) {
		t.Run("Random", func(t *testing.T) {
			for i := 0; i < 100; i++ {
				before := int8(rand.Int32())
				buf := varint.AppendForward(nil, before)
				require.Len(t, buf, varint.SizeBytes(before))

				after, n := varint.ConsumeForward[int8](buf)
				require.Less(t, 0, n)
				require.Equal(t, before, after)

				after, err := varint.ReadForward[int8](bytes.NewBuffer(buf))
				require.NoError(t, err)
				require.Equal(t, before, after)
			}
		})

		t.Run("Limits", func(t *testing.T) {
			_, n := varint.ConsumeForward[int8]([]byte{0x7f, 0x7f})
			require.Equal(t, -1, n)
			v, n := varint.ConsumeForward[int8]([]byte{0x7f, 0x80})
			require.Equal(t, 2, n)
			require.Equal(t, int8(math.MinInt8), v)
			v, n = varint.ConsumeForward[int8]([]byte{0x40, 0x7f})
			require.Equal(t, 2, n)
			require.Equal(t, int8(math.MaxInt8), v)
			_, n = varint.ConsumeForward[int8]([]byte{0x40, 0x80})
			require.Equal(t, -1, n)

			_, err := varint.ReadForward[int8](bytes.NewBuffer([]byte{0x7f, 0x7f}))
			testutil.RequireEqualStatus(t, status.Error(codes.InvalidArgument, "Value is -129, which is out of range"), err)
			v, err = varint.ReadForward[int8](bytes.NewBuffer([]byte{0x7f, 0x80}))
			require.NoError(t, err)
			require.Equal(t, int8(math.MinInt8), v)
			v, err = varint.ReadForward[int8](bytes.NewBuffer([]byte{0x40, 0x7f}))
			require.NoError(t, err)
			require.Equal(t, int8(math.MaxInt8), v)
			_, err = varint.ReadForward[int8](bytes.NewBuffer([]byte{0x40, 0x80}))
			testutil.RequireEqualStatus(t, status.Error(codes.InvalidArgument, "Value is 128, which is out of range"), err)
		})
	})

	t.Run("Uint8", func(t *testing.T) {
		t.Run("Random", func(t *testing.T) {
			for i := 0; i < 100; i++ {
				before := uint8(rand.Uint32())
				buf := varint.AppendForward(nil, before)
				require.Len(t, buf, varint.SizeBytes(before))

				after, n := varint.ConsumeForward[uint8](buf)
				require.Less(t, 0, n)
				require.Equal(t, before, after)

				after, err := varint.ReadForward[uint8](bytes.NewBuffer(buf))
				require.NoError(t, err)
				require.Equal(t, before, after)
			}
		})

		t.Run("Limits", func(t *testing.T) {
			_, n := varint.ConsumeForward[uint8]([]byte{0xff})
			require.Equal(t, -1, n)
			v, n := varint.ConsumeForward[uint8]([]byte{0x80})
			require.Equal(t, 1, n)
			require.Equal(t, uint8(0), v)
			v, n = varint.ConsumeForward[uint8]([]byte{0x40, 0xff})
			require.Equal(t, 2, n)
			require.Equal(t, uint8(math.MaxUint8), v)
			_, n = varint.ConsumeForward[uint8]([]byte{0x41, 0x00})
			require.Equal(t, -1, n)

			_, err := varint.ReadForward[uint8](bytes.NewBuffer([]byte{0xff}))
			testutil.RequireEqualStatus(t, status.Error(codes.InvalidArgument, "Value is -1, while a non-negative value was expected"), err)
			v, err = varint.ReadForward[uint8](bytes.NewBuffer([]byte{0x80}))
			require.NoError(t, err)
			require.Equal(t, uint8(0), v)
			v, err = varint.ReadForward[uint8](bytes.NewBuffer([]byte{0x40, 0xff}))
			require.NoError(t, err)
			require.Equal(t, uint8(math.MaxUint8), v)
			_, err = varint.ReadForward[uint8](bytes.NewBuffer([]byte{0x41, 0x00}))
			testutil.RequireEqualStatus(t, status.Error(codes.InvalidArgument, "Value is 256, which is out of range"), err)
		})
	})

	t.Run("Int16", func(t *testing.T) {
		t.Run("Random", func(t *testing.T) {
			for i := 0; i < 1000; i++ {
				before := int16(rand.Int32())
				buf := varint.AppendForward(nil, before)
				require.Len(t, buf, varint.SizeBytes(before))

				after, n := varint.ConsumeForward[int16](buf)
				require.Less(t, 0, n)
				require.Equal(t, before, after)

				after, err := varint.ReadForward[int16](bytes.NewBuffer(buf))
				require.NoError(t, err)
				require.Equal(t, before, after)
			}
		})

		t.Run("Limits", func(t *testing.T) {
			_, n := varint.ConsumeForward[int16]([]byte{0x3f, 0x7f, 0xff})
			require.Equal(t, -1, n)
			v, n := varint.ConsumeForward[int16]([]byte{0x3f, 0x80, 0x00})
			require.Equal(t, 3, n)
			require.Equal(t, int16(math.MinInt16), v)
			v, n = varint.ConsumeForward[int16]([]byte{0x20, 0x7f, 0xff})
			require.Equal(t, 3, n)
			require.Equal(t, int16(math.MaxInt16), v)
			_, n = varint.ConsumeForward[int16]([]byte{0x20, 0x80, 0x00})
			require.Equal(t, -1, n)

			_, err := varint.ReadForward[int16](bytes.NewBuffer([]byte{0x3f, 0x7f, 0xff}))
			testutil.RequireEqualStatus(t, status.Error(codes.InvalidArgument, "Value is -32769, which is out of range"), err)
			v, err = varint.ReadForward[int16](bytes.NewBuffer([]byte{0x3f, 0x80, 0x00}))
			require.NoError(t, err)
			require.Equal(t, int16(math.MinInt16), v)
			v, err = varint.ReadForward[int16](bytes.NewBuffer([]byte{0x20, 0x7f, 0xff}))
			require.NoError(t, err)
			require.Equal(t, int16(math.MaxInt16), v)
			_, err = varint.ReadForward[int16](bytes.NewBuffer([]byte{0x20, 0x80, 0x00}))
			testutil.RequireEqualStatus(t, status.Error(codes.InvalidArgument, "Value is 32768, which is out of range"), err)
		})
	})

	t.Run("Uint16", func(t *testing.T) {
		t.Run("Random", func(t *testing.T) {
			for i := 0; i < 1000; i++ {
				before := uint16(rand.Uint32())
				buf := varint.AppendForward(nil, before)
				require.Len(t, buf, varint.SizeBytes(before))

				after, n := varint.ConsumeForward[uint16](buf)
				require.Less(t, 0, n)
				require.Equal(t, before, after)

				after, err := varint.ReadForward[uint16](bytes.NewBuffer(buf))
				require.NoError(t, err)
				require.Equal(t, before, after)
			}
		})

		t.Run("Limits", func(t *testing.T) {
			_, n := varint.ConsumeForward[uint16]([]byte{0xff})
			require.Equal(t, -1, n)
			v, n := varint.ConsumeForward[uint16]([]byte{0x80})
			require.Equal(t, 1, n)
			require.Equal(t, uint16(0), v)
			v, n = varint.ConsumeForward[uint16]([]byte{0x20, 0xff, 0xff})
			require.Equal(t, 3, n)
			require.Equal(t, uint16(math.MaxUint16), v)
			_, n = varint.ConsumeForward[uint16]([]byte{0x21, 0x00, 0x00})
			require.Equal(t, -1, n)

			_, err := varint.ReadForward[uint16](bytes.NewBuffer([]byte{0xff}))
			testutil.RequireEqualStatus(t, status.Error(codes.InvalidArgument, "Value is -1, while a non-negative value was expected"), err)
			v, err = varint.ReadForward[uint16](bytes.NewBuffer([]byte{0x80}))
			require.NoError(t, err)
			require.Equal(t, uint16(0), v)
			v, err = varint.ReadForward[uint16](bytes.NewBuffer([]byte{0x20, 0xff, 0xff}))
			require.NoError(t, err)
			require.Equal(t, uint16(math.MaxUint16), v)
			_, err = varint.ReadForward[uint16](bytes.NewBuffer([]byte{0x21, 0x00, 0x00}))
			testutil.RequireEqualStatus(t, status.Error(codes.InvalidArgument, "Value is 65536, which is out of range"), err)
		})
	})

	t.Run("Int32", func(t *testing.T) {
		t.Run("Random", func(t *testing.T) {
			for i := 0; i < 10000; i++ {
				before := rand.Int32()
				buf := varint.AppendForward(nil, before)
				require.Len(t, buf, varint.SizeBytes(before))

				after, n := varint.ConsumeForward[int32](buf)
				require.Less(t, 0, n)
				require.Equal(t, before, after)

				after, err := varint.ReadForward[int32](bytes.NewBuffer(buf))
				require.NoError(t, err)
				require.Equal(t, before, after)
			}
		})

		t.Run("Limits", func(t *testing.T) {
			_, n := varint.ConsumeForward[int32]([]byte{0x0f, 0x7f, 0xff, 0xff, 0xff})
			require.Equal(t, -1, n)
			v, n := varint.ConsumeForward[int32]([]byte{0x0f, 0x80, 0x00, 0x00, 0x00})
			require.Equal(t, 5, n)
			require.Equal(t, int32(math.MinInt32), v)
			v, n = varint.ConsumeForward[int32]([]byte{0x08, 0x7f, 0xff, 0xff, 0xff})
			require.Equal(t, 5, n)
			require.Equal(t, int32(math.MaxInt32), v)
			_, n = varint.ConsumeForward[int32]([]byte{0x08, 0x80, 0x00, 0x00, 0x00})
			require.Equal(t, -1, n)

			_, err := varint.ReadForward[int32](bytes.NewBuffer([]byte{0x0f, 0x7f, 0xff, 0xff, 0xff}))
			testutil.RequireEqualStatus(t, status.Error(codes.InvalidArgument, "Value is -2147483649, which is out of range"), err)
			v, err = varint.ReadForward[int32](bytes.NewBuffer([]byte{0x0f, 0x80, 0x00, 0x00, 0x00}))
			require.NoError(t, err)
			require.Equal(t, int32(math.MinInt32), v)
			v, err = varint.ReadForward[int32](bytes.NewBuffer([]byte{0x08, 0x7f, 0xff, 0xff, 0xff}))
			require.NoError(t, err)
			require.Equal(t, int32(math.MaxInt32), v)
			_, err = varint.ReadForward[int32](bytes.NewBuffer([]byte{0x08, 0x80, 0x00, 0x00, 0x00}))
			testutil.RequireEqualStatus(t, status.Error(codes.InvalidArgument, "Value is 2147483648, which is out of range"), err)
		})
	})

	t.Run("Uint32", func(t *testing.T) {
		t.Run("Random", func(t *testing.T) {
			for i := 0; i < 10000; i++ {
				before := rand.Uint32()
				buf := varint.AppendForward(nil, before)
				require.Len(t, buf, varint.SizeBytes(before))

				after, n := varint.ConsumeForward[uint32](buf)
				require.Less(t, 0, n)
				require.Equal(t, before, after)

				after, err := varint.ReadForward[uint32](bytes.NewBuffer(buf))
				require.NoError(t, err)
				require.Equal(t, before, after)
			}
		})

		t.Run("Limits", func(t *testing.T) {
			_, n := varint.ConsumeForward[uint32]([]byte{0xff})
			require.Equal(t, -1, n)
			v, n := varint.ConsumeForward[uint32]([]byte{0x80})
			require.Equal(t, 1, n)
			require.Equal(t, uint32(0), v)
			v, n = varint.ConsumeForward[uint32]([]byte{0x08, 0xff, 0xff, 0xff, 0xff})
			require.Equal(t, 5, n)
			require.Equal(t, uint32(math.MaxUint32), v)
			_, n = varint.ConsumeForward[uint32]([]byte{0x09, 0x00, 0x00, 0x00, 0x00})
			require.Equal(t, -1, n)

			_, err := varint.ReadForward[uint32](bytes.NewBuffer([]byte{0xff}))
			testutil.RequireEqualStatus(t, status.Error(codes.InvalidArgument, "Value is -1, while a non-negative value was expected"), err)
			v, err = varint.ReadForward[uint32](bytes.NewBuffer([]byte{0x80}))
			require.NoError(t, err)
			require.Equal(t, uint32(0), v)
			v, err = varint.ReadForward[uint32](bytes.NewBuffer([]byte{0x08, 0xff, 0xff, 0xff, 0xff}))
			require.NoError(t, err)
			require.Equal(t, uint32(math.MaxUint32), v)
			_, err = varint.ReadForward[uint32](bytes.NewBuffer([]byte{0x09, 0x00, 0x00, 0x00, 0x00}))
			testutil.RequireEqualStatus(t, status.Error(codes.InvalidArgument, "Value is 4294967296, which is out of range"), err)
		})
	})

	t.Run("Int64", func(t *testing.T) {
		t.Run("Random", func(t *testing.T) {
			for i := 0; i < 100000; i++ {
				before := rand.Int64()
				buf := varint.AppendForward(nil, before)
				require.Len(t, buf, varint.SizeBytes(before))

				after, n := varint.ConsumeForward[int64](buf)
				require.Less(t, 0, n)
				require.Equal(t, before, after)

				after, err := varint.ReadForward[int64](bytes.NewBuffer(buf))
				require.NoError(t, err)
				require.Equal(t, before, after)
			}
		})

		t.Run("Limits", func(t *testing.T) {
			_, n := varint.ConsumeForward[int64]([]byte{0x00, 0x7f, 0x7f, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff})
			require.Equal(t, -1, n)
			v, n := varint.ConsumeForward[int64]([]byte{0x00, 0x7f, 0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00})
			require.Equal(t, 10, n)
			require.Equal(t, int64(math.MinInt64), v)
			v, n = varint.ConsumeForward[int64]([]byte{0x00, 0x40, 0x7f, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff})
			require.Equal(t, 10, n)
			require.Equal(t, int64(math.MaxInt64), v)
			_, n = varint.ConsumeForward[int64]([]byte{0x00, 0x40, 0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00})
			require.Equal(t, -1, n)

			_, err := varint.ReadForward[int64](bytes.NewBuffer([]byte{0x00, 0x7f, 0x7f, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff}))
			testutil.RequireEqualStatus(t, status.Error(codes.InvalidArgument, "Value is out of range for int64"), err)
			v, err = varint.ReadForward[int64](bytes.NewBuffer([]byte{0x00, 0x7f, 0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}))
			require.NoError(t, err)
			require.Equal(t, int64(math.MinInt64), v)
			v, err = varint.ReadForward[int64](bytes.NewBuffer([]byte{0x00, 0x40, 0x7f, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff}))
			require.NoError(t, err)
			require.Equal(t, int64(math.MaxInt64), v)
			_, err = varint.ReadForward[int64](bytes.NewBuffer([]byte{0x00, 0x40, 0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}))
			testutil.RequireEqualStatus(t, status.Error(codes.InvalidArgument, "Value is out of range for int64"), err)
		})
	})

	t.Run("Uint64", func(t *testing.T) {
		t.Run("Random", func(t *testing.T) {
			for i := 0; i < 100000; i++ {
				before := rand.Uint64()
				buf := varint.AppendForward(nil, before)
				require.Len(t, buf, varint.SizeBytes(before))

				after, n := varint.ConsumeForward[uint64](buf)
				require.Less(t, 0, n)
				require.Equal(t, before, after)

				after, err := varint.ReadForward[uint64](bytes.NewBuffer(buf))
				require.NoError(t, err)
				require.Equal(t, before, after)
			}
		})

		t.Run("Limits", func(t *testing.T) {
			_, n := varint.ConsumeForward[uint64]([]byte{0xff})
			require.Equal(t, -1, n)
			v, n := varint.ConsumeForward[uint64]([]byte{0x80})
			require.Equal(t, 1, n)
			require.Equal(t, uint64(0), v)
			v, n = varint.ConsumeForward[uint64]([]byte{0x00, 0x40, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff})
			require.Equal(t, 10, n)
			require.Equal(t, uint64(math.MaxUint64), v)
			_, n = varint.ConsumeForward[uint64]([]byte{0x00, 0x41, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00})
			require.Equal(t, -1, n)

			_, err := varint.ReadForward[uint64](bytes.NewBuffer([]byte{0xff}))
			testutil.RequireEqualStatus(t, status.Error(codes.InvalidArgument, "Value is -1, while a non-negative value was expected"), err)
			v, err = varint.ReadForward[uint64](bytes.NewBuffer([]byte{0x80}))
			require.NoError(t, err)
			require.Equal(t, uint64(0), v)
			v, err = varint.ReadForward[uint64](bytes.NewBuffer([]byte{0x00, 0x40, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff}))
			require.NoError(t, err)
			require.Equal(t, uint64(math.MaxUint64), v)
			_, err = varint.ReadForward[uint64](bytes.NewBuffer([]byte{0x00, 0x41, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}))
			testutil.RequireEqualStatus(t, status.Error(codes.InvalidArgument, "Value is out of range for int64 and uint64"), err)
		})
	})
}
