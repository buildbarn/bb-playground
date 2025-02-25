package object

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"math"

	"github.com/buildbarn/bonanza/pkg/encoding/float16"
	"github.com/buildbarn/bonanza/pkg/proto/storage/object"
)

const (
	minimumObjectSizeBytes = 1
	maximumObjectSizeBytes = 1 << 21
	referenceSizeBytes     = 40
)

type maximumTotalParentsSizeBytesBoundsEntry struct {
	minimum uint16
	maximum uint16
}

var maximumTotalParentsSizeBytesBounds [1 << 8]maximumTotalParentsSizeBytesBoundsEntry

func init() {
	var bounds maximumTotalParentsSizeBytesBoundsEntry
	for i := 2; i <= math.MaxUint8; i++ {
		var ok bool
		bounds.minimum, ok = float16.Uint64ToFloat16RoundUp(float16.Float16ToUint64(bounds.minimum) + referenceSizeBytes)
		if !ok {
			panic("float16 is too small to express all possible maximum total parents sizes")
		}
		bounds.maximum, ok = float16.Uint64ToFloat16RoundUp(float16.Float16ToUint64(bounds.maximum) + maximumObjectSizeBytes)
		if !ok {
			panic("float16 is too small to express all possible maximum total parents sizes")
		}
		maximumTotalParentsSizeBytesBounds[i] = bounds
	}
}

type LocalReference struct {
	rawReference [referenceSizeBytes]byte
}

func MustNewSHA256V1LocalReference(hash string, sizeBytes uint32, height uint8, degree uint16, maximumTotalParentsSizeBytes uint64) LocalReference {
	var rawReference [40]byte
	if n, err := hex.Decode(rawReference[:], []byte(hash)); err != nil {
		panic(err)
	} else if n != 32 {
		panic("Wrong hash length")
	}
	binary.LittleEndian.PutUint32(rawReference[32:], sizeBytes)
	rawReference[35] = byte(height)
	binary.LittleEndian.PutUint16(rawReference[36:], degree)
	f16, ok := float16.Uint64ToFloat16RoundUp(maximumTotalParentsSizeBytes)
	if !ok {
		panic("maximumTotalParentsSizeBytes is too large")
	}
	binary.LittleEndian.PutUint16(rawReference[38:], f16)

	referenceFormat := MustNewReferenceFormat(object.ReferenceFormat_SHA256_V1)
	reference, err := referenceFormat.NewLocalReference(rawReference[:])
	if err != nil {
		panic(err)
	}
	return reference
}

func (LocalReference) GetReferenceFormat() ReferenceFormat {
	return ReferenceFormat{}
}

func (r LocalReference) GetRawReference() []byte {
	return r.rawReference[:]
}

func (r LocalReference) String() string {
	return fmt.Sprintf(
		"SHA256=%s:S=%d:H=%d:D=%d:M=%d",
		hex.EncodeToString(r.GetHash()),
		r.GetSizeBytes(),
		r.GetHeight(),
		r.GetDegree(),
		r.GetMaximumTotalParentsSizeBytes(false),
	)
}

func (r LocalReference) GetHash() []byte {
	return r.rawReference[:32]
}

func (r LocalReference) GetSizeBytes() int {
	return int(binary.LittleEndian.Uint32(r.rawReference[32:]) & 0xffffff)
}

func (r LocalReference) GetHeight() int {
	return int(r.rawReference[35])
}

func (r LocalReference) GetDegree() int {
	return int(binary.LittleEndian.Uint16(r.rawReference[36:]))
}

func (r LocalReference) getRawMaximumTotalParentsSizeBytes() uint16 {
	return binary.LittleEndian.Uint16(r.rawReference[38:])
}

func (r LocalReference) GetMaximumTotalParentsSizeBytes(includeSelf bool) int {
	sizeBytes := int(float16.Float16ToUint64(r.getRawMaximumTotalParentsSizeBytes()))
	if includeSelf && r.GetHeight() > 0 {
		sizeBytes += r.GetSizeBytes()
	}
	return sizeBytes
}

func (r LocalReference) Flatten() (flatReference LocalReference) {
	copy(flatReference.rawReference[:35], r.rawReference[:35])
	return
}

func (r LocalReference) GetLocalReference() LocalReference {
	return r
}

func (r LocalReference) WithLocalReference(localReference LocalReference) LocalReference {
	return localReference
}

// CompareByHeight returns -1 if the reference comes before another
// reference along a total order based on the references' height and
// maximum total parents size. Conversely, it returns 1 if the reference
// comes after another reference. 0 is returned if both references are
// equal.
//
// This order can be used to perform bounded parallel traversal of DAGs
// in such a way that forward progress is guaranteed.
func (r LocalReference) CompareByHeight(other LocalReference) int {
	if ha, hb := r.GetHeight(), other.GetHeight(); ha < hb {
		return -1
	} else if ha > hb {
		return 1
	}

	if sa, sb := r.GetMaximumTotalParentsSizeBytes(true), other.GetMaximumTotalParentsSizeBytes(true); sa < sb {
		return -1
	} else if sa > sb {
		return 1
	}

	// Tie breaker to ensure a total order.
	return bytes.Compare(r.GetRawReference(), other.GetRawReference())
}
