package object

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"math"

	"github.com/buildbarn/bb-storage/pkg/util"
	"github.com/buildbarn/bonanza/pkg/encoding/float16"
	"github.com/buildbarn/bonanza/pkg/proto/storage/object"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Contents of an object read from storage, or to be written to storage.
type Contents struct {
	referenceFormat ReferenceFormat
	data            []byte
	reference       LocalReference
}

var _ OutgoingReferences[LocalReference] = (*Contents)(nil)

func NewContentsFromFullData(reference LocalReference, data []byte) (*Contents, error) {
	c := &Contents{
		referenceFormat: reference.GetReferenceFormat(),
		data:            data,
		reference:       reference,
	}

	if expectedSizeBytes := reference.GetSizeBytes(); len(c.data) != expectedSizeBytes {
		return nil, status.Errorf(codes.InvalidArgument, "Data is %d bytes in size, while %d bytes were expected", len(c.data), expectedSizeBytes)
	}
	actualHash := sha256.Sum256(c.data)
	if expectedHash := reference.GetHash(); !bytes.Equal(actualHash[:], expectedHash) {
		return nil, status.Errorf(codes.InvalidArgument, "Data has SHA-256 hash %s, while %s was expected", hex.EncodeToString(actualHash[:]), hex.EncodeToString(expectedHash))
	}

	if err := c.validateOutgoingReferences(); err != nil {
		return nil, err
	}
	return c, nil
}

func MustNewContents(referenceFormatValue object.ReferenceFormat_Value, references []LocalReference, payload []byte) *Contents {
	referenceFormat := MustNewReferenceFormat(referenceFormatValue)
	contents, err := referenceFormat.NewContents(references, payload)
	if err != nil {
		panic(err)
	}
	return contents
}

func (c *Contents) GetReference() LocalReference {
	return c.reference
}

func (c *Contents) GetDegree() int {
	return c.reference.GetDegree()
}

func (c *Contents) GetOutgoingReference(i int) LocalReference {
	outgoingReferences := c.data[:c.GetDegree()*referenceSizeBytes]
	return LocalReference{
		rawReference: *(*[referenceSizeBytes]byte)(outgoingReferences[i*referenceSizeBytes:]),
	}
}

func (c *Contents) DetachOutgoingReferences() OutgoingReferences[LocalReference] {
	degree := c.GetDegree()
	l := make(OutgoingReferencesList[LocalReference], 0, degree)
	for i := 0; i < degree; i++ {
		l = append(l, LocalReference{
			rawReference: *(*[referenceSizeBytes]byte)(c.data[i*referenceSizeBytes:]),
		})
	}
	return l
}

func (c *Contents) GetFullData() []byte {
	return c.data
}

func (c *Contents) GetPayload() []byte {
	return c.data[c.GetDegree()*referenceSizeBytes:]
}

func (c *Contents) cloneWithReference(r LocalReference) *Contents {
	if r == c.reference {
		return c
	}
	return &Contents{
		referenceFormat: c.referenceFormat,
		data:            c.data,
		reference:       r,
	}
}

func (c *Contents) Flatten() *Contents {
	return c.cloneWithReference(c.reference.Flatten())
}

func (c *Contents) validateOutgoingReferences() error {
	var rcs referenceStatsComputer
	degree := c.GetDegree()
	for i := 0; i < degree; i++ {
		outgoingReference, err := c.referenceFormat.NewLocalReference(c.data[i*referenceSizeBytes : (i+1)*referenceSizeBytes])
		if err != nil {
			return util.StatusWrapf(err, "Invalid reference at index %d", i)
		}
		if err := rcs.addChildReference(outgoingReference); err != nil {
			return err
		}
	}

	if expectedHeight := c.reference.GetHeight(); rcs.height != expectedHeight {
		return status.Errorf(codes.InvalidArgument, "Object has height %d, while %d was expected", rcs.height, expectedHeight)
	}

	actualMaximumTotalParentsSizeBytes, ok := float16.Uint64ToFloat16RoundUp(uint64(rcs.maximumTotalParentsSizeBytes))
	if !ok {
		panic("maximum total parents size should be computable without overflow")
	}
	if expectedMaximumTotalParentsSizeBytes := c.reference.getRawMaximumTotalParentsSizeBytes(); actualMaximumTotalParentsSizeBytes != expectedMaximumTotalParentsSizeBytes {
		return status.Errorf(
			codes.InvalidArgument,
			"Object has a maximum total parents size of %d bytes, while %d bytes were expected",
			float16.Float16ToUint64(actualMaximumTotalParentsSizeBytes),
			float16.Float16ToUint64(expectedMaximumTotalParentsSizeBytes),
		)
	}
	return nil
}

func (c *Contents) Unflatten(newReference LocalReference) (*Contents, error) {
	if *(*[35]byte)(newReference.rawReference[:]) != *(*[35]byte)(c.reference.rawReference[:]) {
		return nil, status.Error(codes.InvalidArgument, "Hash and size of flattened and unflattened references do not match")
	}
	cFlat := c.cloneWithReference(newReference)
	if err := cFlat.validateOutgoingReferences(); err != nil {
		return nil, err
	}
	return cFlat, nil
}

type referenceStatsComputer struct {
	lastRawReference             *[referenceSizeBytes]byte
	height                       int
	degree                       int
	maximumTotalParentsSizeBytes int
}

func (rcs *referenceStatsComputer) addChildReference(outgoingReference LocalReference) error {
	rawReference := &outgoingReference.rawReference
	if rcs.lastRawReference != nil && bytes.Compare(rcs.lastRawReference[:], rawReference[:]) >= 0 {
		return status.Errorf(codes.InvalidArgument, "Outgoing references at indices %d and %d are not properly sorted", rcs.degree-1, rcs.degree)
	}
	rcs.lastRawReference = rawReference

	childHeight := outgoingReference.GetHeight()
	if newHeight := childHeight + 1; rcs.height < newHeight {
		if newHeight > math.MaxUint8 {
			return status.Errorf(codes.InvalidArgument, "Outgoing reference at index %d has height %d, which is too high", rcs.degree, childHeight)
		}
		rcs.height = newHeight
	}

	rcs.degree++

	if newMaximumTotalParentsSizeBytes := outgoingReference.GetMaximumTotalParentsSizeBytes(true); rcs.maximumTotalParentsSizeBytes < newMaximumTotalParentsSizeBytes {
		rcs.maximumTotalParentsSizeBytes = newMaximumTotalParentsSizeBytes
	}
	return nil
}

func (rcs *referenceStatsComputer) getStats() (stats [5]byte) {
	stats[0] = byte(rcs.height)
	binary.LittleEndian.PutUint16(stats[1:], uint16(rcs.degree))
	f16, ok := float16.Uint64ToFloat16RoundUp(uint64(rcs.maximumTotalParentsSizeBytes))
	if !ok {
		panic("maximumTotalParentsSizeBytes is too large")
	}
	binary.LittleEndian.PutUint16(stats[3:], f16)
	return
}
