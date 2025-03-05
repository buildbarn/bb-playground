package parser

import (
	"github.com/buildbarn/bb-storage/pkg/eviction"
	"github.com/buildbarn/bb-storage/pkg/util"
	model_parser_cfg_pb "github.com/buildbarn/bonanza/pkg/proto/configuration/model/parser"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func NewParsedObjectPoolFromConfiguration(configuration *model_parser_cfg_pb.ParsedObjectPool) (*ParsedObjectPool, error) {
	if configuration == nil {
		return nil, status.Error(codes.InvalidArgument, "No parsed object pool configuration provided")
	}
	evictionSet, err := eviction.NewSetFromConfiguration[ParsedObjectEvictionKey](configuration.CacheReplacementPolicy)
	if err != nil {
		return nil, util.StatusWrap(err, "Failed to create eviction set")
	}
	return NewParsedObjectPool(evictionSet, int(configuration.Count), int(configuration.SizeBytes)), nil
}
