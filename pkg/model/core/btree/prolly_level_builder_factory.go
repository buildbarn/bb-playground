package btree

import (
	"hash/fnv"

	model_core "github.com/buildbarn/bb-playground/pkg/model/core"
	"github.com/buildbarn/bb-playground/pkg/model/encoding"
	"github.com/buildbarn/bb-playground/pkg/storage/object"
	"github.com/buildbarn/bb-storage/pkg/util"

	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
)

var marshalOptions = proto.MarshalOptions{UseCachedSize: true}

type prollyLevelBuilderFactory[TNode proto.Message, TMetadata any] struct {
	minimumCount       int
	minimumSizeBytes   int
	maximumSizeBytes   int
	encoder            encoding.BinaryEncoder
	referenceFormat    object.ReferenceFormat
	parentNodeComputer ParentNodeComputer[TNode, TMetadata]
}

// NewProllyLevelBuilderFactory returns a LevelBuilderFactory that is
// capable of creating LevelBuilders that perform probabilistic
// chunking, leading to the creation of a Prolly Tree:
//
// https://docs.dolthub.com/architecture/storage-engine/prolly-tree
//
// For each node that is inserted, an FNV-1a hash is computed based on
// the node's message contents and outgoing references. A cut is made
// after the node where the hash is maximal.
//
// Nodes are stored a format that can be unmarshaled as a Protobuf
// message having the following schema:
//
//	message TNodeList {
//	  repeated TNode nodes = 1;
//	}
func NewProllyLevelBuilderFactory[TNode proto.Message, TMetadata any](
	minimumCount, minimumSizeBytes, maximumSizeBytes int,
	encoder encoding.BinaryEncoder,
	referenceFormat object.ReferenceFormat,
	parentNodeComputer ParentNodeComputer[TNode, TMetadata],
) LevelBuilderFactory[TNode, TMetadata] {
	return &prollyLevelBuilderFactory[TNode, TMetadata]{
		minimumCount:       minimumCount,
		minimumSizeBytes:   minimumSizeBytes,
		maximumSizeBytes:   maximumSizeBytes,
		encoder:            encoder,
		referenceFormat:    referenceFormat,
		parentNodeComputer: parentNodeComputer,
	}
}

func (lbf *prollyLevelBuilderFactory[TNode, TMetadata]) NewLevelBuilder() LevelBuilder[TNode, TMetadata] {
	return &prollyLevelBuilder[TNode, TMetadata]{
		factory: lbf,
		cuts:    make([]prollyCut, 2),
	}
}

type prollyCut struct {
	index               int
	cumulativeSizeBytes int
	hash                uint64
}

type prollyLevelBuilder[TNode proto.Message, TMetadata any] struct {
	factory *prollyLevelBuilderFactory[TNode, TMetadata]

	childNodes          []model_core.PatchedMessage[TNode, TMetadata]
	uncutSizesBytes     []int
	totalUncutSizeBytes int
	cuts                []prollyCut
}

func (lb *prollyLevelBuilder[TNode, TMetadata]) PushChild(node model_core.PatchedMessage[TNode, TMetadata]) error {
	// Schedule the nodes for insertion into objects. Don't insert
	// them immediately, as we need to keep some at hand to ensure
	// the final object on this level respects the minimum number of
	// objects and size.
	sizeBytes := node.Patcher.GetReferencesSizeBytes() + 1 + protowire.SizeBytes(proto.Size(node.Message))
	lb.childNodes = append(lb.childNodes, node)
	lb.uncutSizesBytes = append(lb.uncutSizesBytes, sizeBytes)
	lb.totalUncutSizeBytes += sizeBytes

	lbf := lb.factory
	for len(lb.uncutSizesBytes) > lbf.minimumCount && lb.totalUncutSizeBytes-lb.uncutSizesBytes[0] > lbf.minimumSizeBytes {
		// We've got enough nodes to fill the last object on
		// this level. Add a new potential cut.
		lastCut := &lb.cuts[len(lb.cuts)-1]
		newCut := prollyCut{
			index:               lastCut.index + 1,
			cumulativeSizeBytes: lastCut.cumulativeSizeBytes + lb.uncutSizesBytes[0],
		}
		if newCut.index >= lbf.minimumCount && newCut.cumulativeSizeBytes >= lbf.minimumSizeBytes {
			// This node lies within a range where we can
			// eventually expect cuts to appear. Compute an
			// FNV-1a hash of the node, so that we can
			// perform the cut at the position where the
			// hash is maximized.
			hash := fnv.New64a()
			node := &lb.childNodes[lastCut.index]
			references, _ := node.Patcher.SortAndSetReferences()
			for _, reference := range references {
				hash.Write(reference.GetRawReference())
			}
			m, err := marshalOptions.Marshal(node.Message)
			if err != nil {
				return err
			}
			hash.Write(m)
			newCut.hash = hash.Sum64()
		}
		lb.totalUncutSizeBytes -= lb.uncutSizesBytes[0]
		lb.uncutSizesBytes = lb.uncutSizesBytes[1:]

		// Insert the new cut, potentially collapsing some of
		// the cuts we added previously.
		cutsToKeep := len(lb.cuts)
		for cutsToKeep >= 2 &&
			(lb.cuts[cutsToKeep-1].index-lb.cuts[cutsToKeep-2].index < lbf.minimumCount ||
				lb.cuts[cutsToKeep-1].cumulativeSizeBytes-lb.cuts[cutsToKeep-2].cumulativeSizeBytes < lbf.minimumSizeBytes ||
				(lb.cuts[cutsToKeep-1].hash < newCut.hash &&
					newCut.cumulativeSizeBytes-lb.cuts[cutsToKeep-2].cumulativeSizeBytes <= lbf.maximumSizeBytes)) {
			cutsToKeep--
		}
		lb.cuts = append(lb.cuts[:cutsToKeep], newCut)
	}
	return nil
}

func (lb *prollyLevelBuilder[TNode, TMetadata]) PopParent(finalize bool) (*model_core.PatchedMessage[TNode, TMetadata], error) {
	// Determine whether we've collected enough nodes to be able to
	// create a new object, and how many child nodes should go into it.
	lbf := lb.factory
	cut := lb.cuts[1]
	if finalize {
		if len(lb.childNodes) == 0 {
			return nil, nil
		}
		if cut.index < lbf.minimumCount || cut.cumulativeSizeBytes < lbf.minimumSizeBytes {
			// We've reached the end. Add all nodes for
			// which we didn't compute cuts yet, thereby
			// ensuring that the last object also respects
			// the minimum node count and size limit.
			if len(lb.cuts) > 2 {
				panic("can't have multiple cuts if the first cut is still below limits")
			}
			cut = prollyCut{
				index:               len(lb.childNodes),
				cumulativeSizeBytes: lb.cuts[len(lb.cuts)-1].cumulativeSizeBytes + lb.totalUncutSizeBytes,
			}
			lb.uncutSizesBytes = lb.uncutSizesBytes[:0]
			lb.totalUncutSizeBytes = 0
		}
	} else {
		if cut.index < lbf.minimumCount || lb.cuts[len(lb.cuts)-1].cumulativeSizeBytes < lbf.maximumSizeBytes {
			return nil, nil
		}
	}

	// Remove nodes and cuts.
	childNodes := lb.childNodes[:cut.index]
	lb.childNodes = lb.childNodes[cut.index:]
	if len(lb.cuts) > 2 {
		for i := 1; i < len(lb.cuts)-1; i++ {
			lb.cuts[i] = prollyCut{
				index:               lb.cuts[i+1].index - cut.index,
				cumulativeSizeBytes: lb.cuts[i+1].cumulativeSizeBytes - cut.cumulativeSizeBytes,
				hash:                lb.cuts[i+1].hash,
			}
		}
		lb.cuts = lb.cuts[:len(lb.cuts)-1]
	} else {
		lb.cuts = append(lb.cuts[:1], prollyCut{})
	}

	// Construct the object containing the children.
	childPatcher := model_core.NewReferenceMessagePatcher[TMetadata]()
	for _, node := range childNodes {
		childPatcher.Merge(node.Patcher)
	}
	references, metadata := childPatcher.SortAndSetReferences()

	maximumDataSizeBytes := cut.cumulativeSizeBytes - childPatcher.GetReferencesSizeBytes()
	data := make([]byte, 0, maximumDataSizeBytes)
	for i, childNode := range childNodes {
		data = protowire.AppendTag(data, 1, protowire.BytesType)
		data = protowire.AppendVarint(data, uint64(proto.Size(childNode.Message)))
		var err error
		data, err = marshalOptions.MarshalAppend(data, childNode.Message)
		if err != nil {
			return nil, util.StatusWrapfWithCode(err, codes.InvalidArgument, "Failed to marshal node at index %d", i)
		}
		childPatcher.Merge(childNode.Patcher)
	}
	if len(data) > maximumDataSizeBytes {
		panic("incorrect object size estimation")
	}
	encodedData, err := lbf.encoder.EncodeBinary(data)
	if err != nil {
		return nil, util.StatusWrap(err, "Failed to encode object")
	}
	contents, err := lbf.referenceFormat.NewContents(references, encodedData)
	if err != nil {
		return nil, util.StatusWrap(err, "Failed to create object contents")
	}

	// Construct a parent node that references the object containing
	// the children.
	childMessages := make([]TNode, 0, len(childNodes))
	for _, childNode := range childNodes {
		childMessages = append(childMessages, childNode.Message)
	}
	parentNode, err := lbf.parentNodeComputer(contents, childMessages, references, metadata)
	if err != nil {
		return nil, util.StatusWrap(err, "Failed to compute parent node")
	}
	return &parentNode, nil
}
