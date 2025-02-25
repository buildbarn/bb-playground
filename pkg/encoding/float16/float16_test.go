package float16_test

import (
	"math/rand/v2"
	"testing"

	"github.com/buildbarn/bonanza/pkg/encoding/float16"
	"github.com/stretchr/testify/require"
)

func TestFloat16Identity(t *testing.T) {
	// If the exponent is zero or one, the integer and floating
	// point representations are identical.
	for i := 0; i <= 4096; i++ {
		require.Equal(t, uint64(i), float16.Float16ToUint64(uint16(i)))

		converted, ok := float16.Uint64ToFloat16RoundUp(uint64(i))
		require.True(t, ok)
		require.Equal(t, uint16(i), converted)
	}
}

func TestFloat16Rounding(t *testing.T) {
	// If the exponent goes beyond one, the precision should
	// gradually decrease. Converting from an integer to a floating
	// point value should round upwards.
	for i := 4096 + 2; i <= 8192; i += 2 {
		for j := 0; j < 2; j++ {
			converted, ok := float16.Uint64ToFloat16RoundUp(uint64(i - j))
			require.True(t, ok)
			require.Equal(t, uint64(i), float16.Float16ToUint64(converted))
		}
	}

	for i := 8192 + 4; i <= 16384; i += 4 {
		for j := 0; j < 4; j++ {
			converted, ok := float16.Uint64ToFloat16RoundUp(uint64(i - j))
			require.True(t, ok)
			require.Equal(t, uint64(i), float16.Float16ToUint64(converted))
		}
	}

	for i := 16384 + 8; i <= 32768; i += 8 {
		for j := 0; j < 8; j++ {
			converted, ok := float16.Uint64ToFloat16RoundUp(uint64(i - j))
			require.True(t, ok)
			require.Equal(t, uint64(i), float16.Float16ToUint64(converted))
		}
	}
}

func TestFloat16Max(t *testing.T) {
	// The largest value that can be represented in this format is
	// 0xfff << 30 == 0x3ffc0000000.
	require.Equal(t, uint64(0x3ffc0000000), float16.Float16ToUint64(0xffff))

	converted, ok := float16.Uint64ToFloat16RoundUp(0x3ffc0000000)
	require.True(t, ok)
	require.Equal(t, uint16(0xffff), converted)
}

func TestFloat16TooLarge(t *testing.T) {
	_, ok := float16.Uint64ToFloat16RoundUp(0x3ffc0000001)
	require.False(t, ok)
}

func TestFloat16Random(t *testing.T) {
	for i := 0; i < 10000; i++ {
		original := uint16(rand.Uint32())
		converted, ok := float16.Uint64ToFloat16RoundUp(float16.Float16ToUint64(original))
		require.True(t, ok)
		require.Equal(t, original, converted)
	}
}
