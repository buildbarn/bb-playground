package varint_test

import (
	"math"
	"math/rand/v2"
	"testing"

	"github.com/buildbarn/bonanza/pkg/encoding/varint"
	"github.com/stretchr/testify/require"
)

func TestBackward(t *testing.T) {
	t.Run("Int8", func(t *testing.T) {
		t.Run("Random", func(t *testing.T) {
			for i := 0; i < 100; i++ {
				before := int8(rand.Int32())
				buf := varint.AppendBackward(nil, before)
				require.Len(t, buf, varint.SizeBytes(before))

				after, n := varint.ConsumeBackward[int8](buf)
				require.Less(t, 0, n)
				require.Equal(t, before, after)
			}
		})

		t.Run("Limits", func(t *testing.T) {
			_, n := varint.ConsumeBackward[int8]([]byte{0x7f, 0x7f})
			require.Equal(t, -1, n)
			v, n := varint.ConsumeBackward[int8]([]byte{0x80, 0x7f})
			require.Equal(t, 2, n)
			require.Equal(t, int8(math.MinInt8), v)
			v, n = varint.ConsumeBackward[int8]([]byte{0x7f, 0x40})
			require.Equal(t, 2, n)
			require.Equal(t, int8(math.MaxInt8), v)
			_, n = varint.ConsumeBackward[int8]([]byte{0x80, 0x40})
			require.Equal(t, -1, n)
		})
	})

	t.Run("Uint8", func(t *testing.T) {
		t.Run("Random", func(t *testing.T) {
			for i := 0; i < 100; i++ {
				before := uint8(rand.Uint32())
				buf := varint.AppendBackward(nil, before)
				require.Len(t, buf, varint.SizeBytes(before))

				after, n := varint.ConsumeBackward[uint8](buf)
				require.Less(t, 0, n)
				require.Equal(t, before, after)
			}
		})

		t.Run("Limits", func(t *testing.T) {
			_, n := varint.ConsumeBackward[uint8]([]byte{0xff})
			require.Equal(t, -1, n)
			v, n := varint.ConsumeBackward[uint8]([]byte{0x80})
			require.Equal(t, 1, n)
			require.Equal(t, uint8(0), v)
			v, n = varint.ConsumeBackward[uint8]([]byte{0xff, 0x40})
			require.Equal(t, 2, n)
			require.Equal(t, uint8(math.MaxUint8), v)
			_, n = varint.ConsumeBackward[uint8]([]byte{0x00, 0x41})
			require.Equal(t, -1, n)
		})
	})

	t.Run("Int16", func(t *testing.T) {
		t.Run("Random", func(t *testing.T) {
			for i := 0; i < 1000; i++ {
				before := int16(rand.Int32())
				buf := varint.AppendBackward(nil, before)
				require.Len(t, buf, varint.SizeBytes(before))

				after, n := varint.ConsumeBackward[int16](buf)
				require.Less(t, 0, n)
				require.Equal(t, before, after)
			}
		})

		t.Run("Limits", func(t *testing.T) {
			_, n := varint.ConsumeBackward[int16]([]byte{0xff, 0x7f, 0x3f})
			require.Equal(t, -1, n)
			v, n := varint.ConsumeBackward[int16]([]byte{0x00, 0x80, 0x3f})
			require.Equal(t, 3, n)
			require.Equal(t, int16(math.MinInt16), v)
			v, n = varint.ConsumeBackward[int16]([]byte{0xff, 0x7f, 0x20})
			require.Equal(t, 3, n)
			require.Equal(t, int16(math.MaxInt16), v)
			_, n = varint.ConsumeBackward[int16]([]byte{0x00, 0x80, 0x20})
			require.Equal(t, -1, n)
		})
	})

	t.Run("Uint16", func(t *testing.T) {
		t.Run("Random", func(t *testing.T) {
			for i := 0; i < 1000; i++ {
				before := uint16(rand.Uint32())
				buf := varint.AppendBackward(nil, before)
				require.Len(t, buf, varint.SizeBytes(before))

				after, n := varint.ConsumeBackward[uint16](buf)
				require.Less(t, 0, n)
				require.Equal(t, before, after)
			}
		})

		t.Run("Limits", func(t *testing.T) {
			_, n := varint.ConsumeBackward[uint16]([]byte{0xff})
			require.Equal(t, -1, n)
			v, n := varint.ConsumeBackward[uint16]([]byte{0x80})
			require.Equal(t, 1, n)
			require.Equal(t, uint16(0), v)
			v, n = varint.ConsumeBackward[uint16]([]byte{0xff, 0xff, 0x20})
			require.Equal(t, 3, n)
			require.Equal(t, uint16(math.MaxUint16), v)
			_, n = varint.ConsumeBackward[uint16]([]byte{0x00, 0x00, 0x21})
			require.Equal(t, -1, n)
		})
	})

	t.Run("Int32", func(t *testing.T) {
		t.Run("Random", func(t *testing.T) {
			for i := 0; i < 10000; i++ {
				before := rand.Int32()
				buf := varint.AppendBackward(nil, before)
				require.Len(t, buf, varint.SizeBytes(before))

				after, n := varint.ConsumeBackward[int32](buf)
				require.Less(t, 0, n)
				require.Equal(t, before, after)
			}
		})

		t.Run("Limits", func(t *testing.T) {
			_, n := varint.ConsumeBackward[int32]([]byte{0xff, 0xff, 0xff, 0x7f, 0x0f})
			require.Equal(t, -1, n)
			v, n := varint.ConsumeBackward[int32]([]byte{0x00, 0x00, 0x00, 0x80, 0x0f})
			require.Equal(t, 5, n)
			require.Equal(t, int32(math.MinInt32), v)
			v, n = varint.ConsumeBackward[int32]([]byte{0xff, 0xff, 0xff, 0x7f, 0x08})
			require.Equal(t, 5, n)
			require.Equal(t, int32(math.MaxInt32), v)
			_, n = varint.ConsumeBackward[int32]([]byte{0x00, 0x00, 0x00, 0x80, 0x08})
			require.Equal(t, -1, n)
		})
	})

	t.Run("Uint32", func(t *testing.T) {
		t.Run("Random", func(t *testing.T) {
			for i := 0; i < 10000; i++ {
				before := rand.Uint32()
				buf := varint.AppendBackward(nil, before)
				require.Len(t, buf, varint.SizeBytes(before))

				after, n := varint.ConsumeBackward[uint32](buf)
				require.Less(t, 0, n)
				require.Equal(t, before, after)
			}
		})

		t.Run("Limits", func(t *testing.T) {
			_, n := varint.ConsumeBackward[uint32]([]byte{0xff})
			require.Equal(t, -1, n)
			v, n := varint.ConsumeBackward[uint32]([]byte{0x80})
			require.Equal(t, 1, n)
			require.Equal(t, uint32(0), v)
			v, n = varint.ConsumeBackward[uint32]([]byte{0xff, 0xff, 0xff, 0xff, 0x08})
			require.Equal(t, 5, n)
			require.Equal(t, uint32(math.MaxUint32), v)
			_, n = varint.ConsumeBackward[uint32]([]byte{0x00, 0x00, 0x00, 0x00, 0x09})
			require.Equal(t, -1, n)
		})
	})

	t.Run("Int64", func(t *testing.T) {
		t.Run("Random", func(t *testing.T) {
			for i := 0; i < 100000; i++ {
				before := rand.Int64()
				buf := varint.AppendBackward(nil, before)
				require.Len(t, buf, varint.SizeBytes(before))

				after, n := varint.ConsumeBackward[int64](buf)
				require.Less(t, 0, n)
				require.Equal(t, before, after)
			}
		})

		t.Run("Limits", func(t *testing.T) {
			_, n := varint.ConsumeBackward[int64]([]byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x7f, 0x7f, 0x00})
			require.Equal(t, -1, n)
			v, n := varint.ConsumeBackward[int64]([]byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x80, 0x7f, 0x00})
			require.Equal(t, 10, n)
			require.Equal(t, int64(math.MinInt64), v)
			v, n = varint.ConsumeBackward[int64]([]byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x7f, 0x40, 0x00})
			require.Equal(t, 10, n)
			require.Equal(t, int64(math.MaxInt64), v)
			_, n = varint.ConsumeBackward[int64]([]byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x80, 0x40, 0x00})
			require.Equal(t, -1, n)
		})
	})

	t.Run("Uint64", func(t *testing.T) {
		t.Run("Random", func(t *testing.T) {
			for i := 0; i < 100000; i++ {
				before := rand.Uint64()
				buf := varint.AppendBackward(nil, before)
				require.Len(t, buf, varint.SizeBytes(before))

				after, n := varint.ConsumeBackward[uint64](buf)
				require.Less(t, 0, n)
				require.Equal(t, before, after)
			}
		})

		t.Run("Limits", func(t *testing.T) {
			_, n := varint.ConsumeBackward[uint64]([]byte{0xff})
			require.Equal(t, -1, n)
			v, n := varint.ConsumeBackward[uint64]([]byte{0x80})
			require.Equal(t, 1, n)
			require.Equal(t, uint64(0), v)
			v, n = varint.ConsumeBackward[uint64]([]byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x40, 0x00})
			require.Equal(t, 10, n)
			require.Equal(t, uint64(math.MaxUint64), v)
			_, n = varint.ConsumeBackward[uint64]([]byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x41, 0x00})
			require.Equal(t, -1, n)
		})
	})
}
