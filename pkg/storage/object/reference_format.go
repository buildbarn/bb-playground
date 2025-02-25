package object

import (
	"crypto/sha256"
	"encoding/binary"

	"github.com/buildbarn/bonanza/pkg/encoding/float16"
	"github.com/buildbarn/bonanza/pkg/proto/storage/object"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type ReferenceFormat struct{}

func NewReferenceFormat(value object.ReferenceFormat_Value) (ReferenceFormat, error) {
	if value != object.ReferenceFormat_SHA256_V1 {
		return ReferenceFormat{}, status.Error(codes.InvalidArgument, "This implementation only supports reference format SHA256_V1")
	}
	return ReferenceFormat{}, nil
}

func MustNewReferenceFormat(value object.ReferenceFormat_Value) ReferenceFormat {
	referenceFormat, err := NewReferenceFormat(value)
	if err != nil {
		panic(err)
	}
	return referenceFormat
}

func (ReferenceFormat) ToProto() object.ReferenceFormat_Value {
	return object.ReferenceFormat_SHA256_V1
}

func (ReferenceFormat) GetReferenceSizeBytes() int {
	return referenceSizeBytes
}

func (ReferenceFormat) NewLocalReference(rawReference []byte) (r LocalReference, err error) {
	// Construct the reference.
	if len(rawReference) != referenceSizeBytes {
		return LocalReference{}, status.Errorf(
			codes.InvalidArgument,
			"Reference is %d bytes in size, while SHA256_V1 references are %d bytes in size",
			len(rawReference),
			referenceSizeBytes,
		)
	}
	r.rawReference = *(*[referenceSizeBytes]byte)(rawReference)

	sizeBytes := r.GetSizeBytes()
	if sizeBytes < minimumObjectSizeBytes || sizeBytes > maximumObjectSizeBytes {
		return LocalReference{}, status.Errorf(
			codes.InvalidArgument,
			"Size is %d bytes, which lies outside the permitted range of [%d, %d] bytes",
			sizeBytes,
			minimumObjectSizeBytes,
			maximumObjectSizeBytes,
		)
	}

	// Perform validation against the reference's fields.
	maximumDegree := sizeBytes / referenceSizeBytes
	degree := r.GetDegree()
	if degree > maximumDegree {
		return LocalReference{}, status.Errorf(
			codes.InvalidArgument,
			"Degree is %d, while an object of %d bytes in size can only have a maximum degree of %d",
			degree,
			sizeBytes,
			maximumDegree,
		)
	}

	height := r.GetHeight()
	if (degree > 0) != (height > 0) {
		return LocalReference{}, status.Errorf(
			codes.InvalidArgument,
			"Degree is %d and height is %d, while both either have to be zero or non-zero",
			degree,
			height,
		)
	}

	if bounds, parentsSizeBytes := maximumTotalParentsSizeBytesBounds[height], binary.LittleEndian.Uint16(rawReference[38:]); parentsSizeBytes < bounds.minimum || parentsSizeBytes > bounds.maximum {
		return LocalReference{}, status.Errorf(
			codes.InvalidArgument,
			"Maximum total parents size is %d bytes, which at height %d lies outside the permitted range of [%d, %d] bytes",
			float16.Float16ToUint64(parentsSizeBytes),
			height,
			float16.Float16ToUint64(bounds.minimum),
			float16.Float16ToUint64(bounds.maximum),
		)
	}

	return
}

// GetBogusReference returns a reference that is valid according to all
// of the range restrictions of the reference format, but for which it's
// impossible to ever construct valid object contents.
//
// This function can be used to simulate the creation of messages,
// without actually computing their contents and references.
func (ReferenceFormat) GetBogusReference() LocalReference {
	return LocalReference{
		rawReference: [...]byte{
			// SHA-256 hash.
			0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
			0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
			0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
			0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
			// Size in bytes.
			0x01, 0x00, 0x00,
			// Height.
			0x00,
			// Degree.
			0x00, 0x00,
			// Maximum parents total size in bytes.
			0x00, 0x00,
		},
	}
}

func (ReferenceFormat) GetMaximumObjectSizeBytes() int {
	return maximumObjectSizeBytes
}

func (rf ReferenceFormat) NewContents(outgoingReferences []LocalReference, payload []byte) (*Contents, error) {
	sizeBytes := len(outgoingReferences)*referenceSizeBytes + len(payload)
	if sizeBytes < minimumObjectSizeBytes || sizeBytes > maximumObjectSizeBytes {
		return nil, status.Errorf(
			codes.InvalidArgument,
			"Size is %d bytes, which lies outside the permitted range of [%d, %d] bytes",
			sizeBytes,
			minimumObjectSizeBytes,
			maximumObjectSizeBytes,
		)
	}

	var data []byte
	var rcs referenceStatsComputer
	if len(outgoingReferences) == 0 {
		data = payload
	} else {
		data = make([]byte, 0, sizeBytes)
		for _, outgoingReference := range outgoingReferences {
			if err := rcs.addChildReference(outgoingReference); err != nil {
				return nil, err
			}
			data = append(data, outgoingReference.GetRawReference()...)
		}
		data = append(data, payload...)
	}

	var rawReference [referenceSizeBytes]byte
	*(*[32]byte)(rawReference[:]) = sha256.Sum256(data)
	binary.LittleEndian.PutUint32(rawReference[32:], uint32(sizeBytes))
	*(*[5]byte)(rawReference[35:]) = rcs.getStats()

	return &Contents{
		referenceFormat: rf,
		data:            data,
		reference:       LocalReference{rawReference: rawReference},
	}, nil
}
