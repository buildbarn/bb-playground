package btree

import (
	model_core "github.com/buildbarn/bonanza/pkg/model/core"
	model_filesystem_pb "github.com/buildbarn/bonanza/pkg/proto/model/filesystem"

	"google.golang.org/protobuf/proto"
)

// NodeMerger is invoked whenever a list of tree nodes is gathered that
// should be placed as siblings in a single B-tree object. It is
// responsible for constructing the B-tree object and yielding a message
// that can be placed in the parent object to reference it.
type NodeMerger[TNode proto.Message, TMetadata model_core.ReferenceMetadata] func(model_core.PatchedMessage[[]TNode, TMetadata]) (model_core.PatchedMessage[TNode, TMetadata], error)

// NodeMergerForTesting is an instantiation of NodeMerger for generating
// mocks to be used by tests.
type NodeMergerForTesting NodeMerger[*model_filesystem_pb.FileContents, model_core.ReferenceMetadata]
