package parser

import (
	model_core "github.com/buildbarn/bonanza/pkg/model/core"
)

type ObjectParser[TReference, TParsedObject any] interface {
	ParseObject(in model_core.Message[[]byte, TReference]) (TParsedObject, int, error)
}
