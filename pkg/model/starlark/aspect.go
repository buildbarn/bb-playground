package starlark

import (
	"errors"

	pg_label "github.com/buildbarn/bb-playground/pkg/label"
	model_core "github.com/buildbarn/bb-playground/pkg/model/core"
	model_starlark_pb "github.com/buildbarn/bb-playground/pkg/proto/model/starlark"
	"github.com/buildbarn/bb-playground/pkg/storage/dag"

	"go.starlark.net/starlark"
)

type Aspect struct {
	LateNamedValue
	definition *model_starlark_pb.Aspect_Definition
}

var (
	_ EncodableValue = &Aspect{}
	_ NamedGlobal    = &Aspect{}
)

func NewAspect(identifier *pg_label.CanonicalStarlarkIdentifier, definition *model_starlark_pb.Aspect_Definition) starlark.Value {
	return &Aspect{
		LateNamedValue: LateNamedValue{
			Identifier: identifier,
		},
		definition: definition,
	}
}

func (a *Aspect) String() string {
	return "<aspect>"
}

func (a *Aspect) Type() string {
	return "Aspect"
}

func (a *Aspect) Freeze() {}

func (a *Aspect) Truth() starlark.Bool {
	return starlark.True
}

func (a *Aspect) Hash() (uint32, error) {
	return 0, errors.New("aspect cannot be hashed")
}

func (a *Aspect) EncodeValue(path map[starlark.Value]struct{}, currentIdentifier *pg_label.CanonicalStarlarkIdentifier, options *ValueEncodingOptions) (model_core.PatchedMessage[*model_starlark_pb.Value, dag.ObjectContentsWalker], bool, error) {
	if a.Identifier == nil {
		return model_core.PatchedMessage[*model_starlark_pb.Value, dag.ObjectContentsWalker]{}, false, errors.New("aspect does not have a name")
	}
	if currentIdentifier == nil || *currentIdentifier != *a.Identifier {
		// Not the canonical identifier under which this aspect
		// is known. Emit a reference.
		return model_core.NewSimplePatchedMessage[dag.ObjectContentsWalker](
			&model_starlark_pb.Value{
				Kind: &model_starlark_pb.Value_Aspect{
					Aspect: &model_starlark_pb.Aspect{
						Kind: &model_starlark_pb.Aspect_Reference{
							Reference: a.Identifier.String(),
						},
					},
				},
			},
		), false, nil
	}

	needsCode := false
	return model_core.NewSimplePatchedMessage[dag.ObjectContentsWalker](
		&model_starlark_pb.Value{
			Kind: &model_starlark_pb.Value_Aspect{
				Aspect: &model_starlark_pb.Aspect{
					Kind: &model_starlark_pb.Aspect_Definition_{
						Definition: &model_starlark_pb.Aspect_Definition{},
					},
				},
			},
		},
	), needsCode, nil
}
