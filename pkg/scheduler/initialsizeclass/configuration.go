package initialsizeclass

import (
	pb "github.com/buildbarn/bb-playground/pkg/proto/configuration/scheduler"
	"github.com/buildbarn/bb-storage/pkg/util"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// NewAnalyzerFromConfiguration creates a new initial size class
// analyzer based on options provided in a configuration file.
func NewAnalyzerFromConfiguration(configuration *pb.InitialSizeClassAnalyzerConfiguration) (Analyzer, error) {
	if configuration == nil {
		return nil, status.Error(codes.InvalidArgument, "No initial size class analyzer configuration provided")
	}

	maximumExecutionTimeout := configuration.MaximumExecutionTimeout
	if err := maximumExecutionTimeout.CheckValid(); err != nil {
		return nil, util.StatusWrap(err, "Invalid maximum execution timeout")
	}
	actionTimeoutExtractor := NewActionTimeoutExtractor(maximumExecutionTimeout.AsDuration())
	return NewFallbackAnalyzer(actionTimeoutExtractor), nil
}
