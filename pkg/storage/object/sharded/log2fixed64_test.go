package sharded_test

import (
	"math"
	"math/rand/v2"
	"slices"
	"testing"

	"github.com/buildbarn/bonanza/pkg/storage/object/sharded"
	"github.com/stretchr/testify/assert"
)

func TestLog2Fixed64(t *testing.T) {
	t.Run("Zero", func(t *testing.T) {
		// log2(0) is normally undefined. However, for our
		// purpose we want this function to work for all inputs.
		// Map it to zero.
		assert.Equal(t, uint64(0), sharded.Log2Fixed64(0))
	})

	t.Run("PowersOfTwo", func(t *testing.T) {
		// Exact powers of two should result in integer
		// logarithmic values.
		for i := 0; i < 64; i++ {
			assert.Equal(t, uint64(i)<<(64-6), sharded.Log2Fixed64(1<<i), i)
		}
	})

	t.Run("Monotonicity", func(t *testing.T) {
		// log2(x) is a monotonic function. Even though this
		// implementation only has limited precision, it should
		// preserve monotonicity.
		var numbers [1000000]uint64
		for i := range numbers {
			numbers[i] = rand.Uint64()
		}
		slices.Sort(numbers[:])
		for i := 1; i < len(numbers); i++ {
			a, b := numbers[i-1], numbers[i]
			assert.LessOrEqual(
				t,
				sharded.Log2Fixed64(a),
				sharded.Log2Fixed64(b),
				a,
				b,
			)
		}
	})

	t.Run("Precision", func(t *testing.T) {
		// Verify that the value of this function is close to
		// what's computed by its floating point counterpart.
		for i := 0; i < 1000000; i++ {
			if v := rand.Uint64(); v != 0 {
				assert.InEpsilon(
					t,
					math.Log2(float64(v)),
					float64(sharded.Log2Fixed64(v))/(1<<64/64),
					1e-5,
				)
			}
		}
	})
}
