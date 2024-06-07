package varint

import (
	"encoding/binary"
	"math/bits"

	"golang.org/x/exp/constraints"
)

// AppendBackward appends an integer value to a byte slice using a
// variable length encoding, which can be read back in a backward
// parsing manner.
//
// This encoding is identical to AppendForward, except that bytes are
// emitted in reverse order.
func AppendBackward[T constraints.Integer](dst []byte, value T) []byte {
	if value < 0 {
		v := int64(value)
		switch {
		case v >= -(1 << 6):
			// Negative 7 bit value stored in 1 byte.
			return append(dst, byte(v))
		case v >= -(1 << 13):
			// Negative 14 bit value stored in 2 bytes.
			return binary.LittleEndian.AppendUint16(dst, uint16(v)&0x7fff)
		case v >= -(1 << 20):
			// Negative 21 bit value stored in 3 bytes.
			return append(
				binary.LittleEndian.AppendUint16(dst, uint16(v)),
				byte(v>>16)&0x3f,
			)
		case v >= -(1 << 27):
			// Negative 28 bit value stored in 4 bytes.
			return binary.LittleEndian.AppendUint32(dst, uint32(v)&0x1fffffff)
		case v >= -(1 << 34):
			// Negative 35 bit value stored in 5 bytes.
			return append(
				binary.LittleEndian.AppendUint32(dst, uint32(v)),
				byte(v>>32)&0x0f,
			)
		case v >= -(1 << 41):
			// Negative 42 bit value stored in 6 bytes.
			return binary.LittleEndian.AppendUint16(
				binary.LittleEndian.AppendUint32(dst, uint32(v)),
				uint16(v>>32)&0x07ff,
			)
		case v >= -(1 << 48):
			// Negative 49 bit value stored in 7 bytes.
			return append(
				binary.LittleEndian.AppendUint16(
					binary.LittleEndian.AppendUint32(dst, uint32(v)),
					uint16(v>>32),
				),
				0x03,
			)
		case v >= -(1 << 55):
			// Negative 56 bit value stored in 8 bytes.
			return binary.LittleEndian.AppendUint64(dst, uint64(v)&0x01ffffffffffffff)
		case v >= -(1 << 62):
			// Negative 63 bit value stored in 9 bytes.
			return append(
				binary.LittleEndian.AppendUint64(dst, uint64(v)),
				0x00,
			)
		default:
			// Negative 70 bit value stored in 10 bytes.
			return append(
				binary.LittleEndian.AppendUint64(dst, uint64(v)),
				0x7f,
				0x00,
			)
		}
	} else {
		v := uint64(value)
		switch {
		case v < 1<<6:
			// Non-negative 7 bit value stored in 1 byte.
			return append(dst, byte(v)|0x80)
		case v < 1<<13:
			// Non-negative 14 bit value stored in 2 bytes.
			return binary.LittleEndian.AppendUint16(dst, uint16(v)|0x4000)
		case v < 1<<20:
			// Non-negative 21 bit value stored in 3 bytes.
			return append(
				binary.LittleEndian.AppendUint16(dst, uint16(v)),
				byte(v>>16)|0x20,
			)
		case v < 1<<27:
			// Non-negative 28 bit value stored in 4 bytes.
			return binary.LittleEndian.AppendUint32(dst, uint32(v)|0x10000000)
		case v < 1<<34:
			// Non-negative 35 bit value stored in 5 bytes.
			return append(
				binary.LittleEndian.AppendUint32(dst, uint32(v)),
				byte(v>>32)|0x08,
			)
		case v < 1<<41:
			// Non-negative 42 bit value stored in 6 bytes.
			return binary.LittleEndian.AppendUint16(
				binary.LittleEndian.AppendUint32(dst, uint32(v)),
				uint16(v>>32)|0x0400,
			)
		case v < 1<<48:
			// Non-negative 49 bit value stored in 7 bytes.
			return append(
				binary.LittleEndian.AppendUint16(
					binary.LittleEndian.AppendUint32(dst, uint32(v)),
					uint16(v>>32),
				),
				0x02,
			)
		case v < 1<<55:
			// Non-negative 56 bit value stored in 8 bytes.
			return binary.LittleEndian.AppendUint64(dst, v|0x0100000000000000)
		case v < 1<<62:
			// Non-negative 63 bit value stored in 9 bytes.
			return append(
				binary.LittleEndian.AppendUint64(dst, v|0x8000000000000000),
				0x00,
			)
		default:
			// Non-negative 70 bit value stored in 10 bytes.
			return append(
				binary.LittleEndian.AppendUint64(dst, v),
				0x40,
				0x00,
			)
		}
	}
}

// ConsumeBackward consumes an integer value from the head of a byte
// slice, which was encoded using AppendBackward().
func ConsumeBackward[T constraints.Integer](src []byte) (T, int) {
	if len(src) < 1 {
		return 0, -1
	}
	leadingZeros := bits.LeadingZeros8(uint8(src[len(src)-1]))
	length := 1 + leadingZeros
	if len(src) < length {
		return 0, -1
	}

	isUnsigned := ^T(0) > 0
	var v int64
	switch leadingZeros {
	case 0:
		// 7 bit value stored in 1 byte.
		v = int64(src[len(src)-1]) << 57 >> 57
	case 1:
		// 14 bit value stored in 2 bytes.
		v = int64(binary.LittleEndian.Uint16(src[len(src)-2:])) << 50 >> 50
	case 2:
		// 21 bit value stored in 3 bytes.
		v = (int64(src[len(src)-1]) << 59 >> 43) |
			int64(binary.LittleEndian.Uint16(src[len(src)-3:]))
	case 3:
		// 28 bit value stored in 4 bytes.
		v = int64(binary.LittleEndian.Uint32(src[len(src)-4:])) << 36 >> 36
	case 4:
		// 35 bit value stored in 5 bytes.
		v = (int64(src[len(src)-1]) << 61 >> 29) |
			int64(binary.LittleEndian.Uint32(src[len(src)-5:]))
	case 5:
		// 42 bit value stored in 6 bytes.
		v = (int64(binary.LittleEndian.Uint16(src[len(src)-2:])) << 54 >> 22) |
			int64(binary.LittleEndian.Uint32(src[len(src)-6:]))
	case 6:
		// 49 byte value stored in 7 bytes.
		v = (int64(src[len(src)-1]) << 63 >> 15) |
			(int64(binary.LittleEndian.Uint16(src[len(src)-3:])) << 32) |
			int64(binary.LittleEndian.Uint32(src[len(src)-7:]))
	case 7:
		// 56 bit value stored in 8 bytes.
		v = int64(binary.LittleEndian.Uint64(src[len(src)-8:])) << 8 >> 8
	case 8:
		leadingZeros := bits.LeadingZeros8(uint8(src[len(src)-2]))
		length += leadingZeros
		if len(src) < length {
			return 0, -1
		}

		switch {
		case leadingZeros == 0:
			// 63 bit values stored in 9 bytes.
			v = int64(binary.LittleEndian.Uint64(src[len(src)-9:])) << 1 >> 1
		case isUnsigned && src[len(src)-2] == 0x40:
			// 70 bit value stored in 10 bytes, converted to uint64.
			v := binary.LittleEndian.Uint64(src[len(src)-10:])
			converted := T(v)
			if uint64(converted) != v {
				return 0, -1
			}
			return converted, length
		case !isUnsigned && (src[len(src)-2] == 0x40 && src[len(src)-3] < 0x80) || (src[len(src)-2] == 0x7f && src[len(src)-3] >= 0x80):
			// 70 bit value stored in 10 bytes, converted to int64.
			v = int64(binary.LittleEndian.Uint64(src[len(src)-10:]))
		default:
			return 0, -1
		}
	}

	// Bounds checking against the desired integer type.
	if isUnsigned && v < 0 {
		return 0, -1
	}
	converted := T(v)
	if int64(converted) != v {
		return 0, -1
	}
	return converted, length
}
