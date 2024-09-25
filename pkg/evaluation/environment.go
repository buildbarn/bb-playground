package evaluation

import (
	model_core "github.com/buildbarn/bb-playground/pkg/model/core"

	"google.golang.org/protobuf/proto"
)

type Environment interface {
	GetValue(key proto.Message) model_core.Message[proto.Message]
}
