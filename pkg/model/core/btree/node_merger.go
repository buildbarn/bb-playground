package btree

import (
	model_core "github.com/buildbarn/bb-playground/pkg/model/core"
	model_filesystem_pb "github.com/buildbarn/bb-playground/pkg/proto/model/filesystem"

	"google.golang.org/protobuf/proto"
)

type NodeMerger[TNode proto.Message, TMetadata model_core.ReferenceMetadata] func(model_core.PatchedMessage[[]TNode, TMetadata]) (model_core.PatchedMessage[TNode, TMetadata], error)

type NodeMergerForTesting NodeMerger[*model_filesystem_pb.FileContents, model_core.ReferenceMetadata]
