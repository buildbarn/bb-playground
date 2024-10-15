package starlark

import (
	"errors"

	pg_label "github.com/buildbarn/bb-playground/pkg/label"
	model_core "github.com/buildbarn/bb-playground/pkg/model/core"
	model_starlark_pb "github.com/buildbarn/bb-playground/pkg/proto/model/starlark"
	"github.com/buildbarn/bb-playground/pkg/storage/dag"

	"go.starlark.net/starlark"
)

type repositoryRule struct {
	LateNamedValue
}

var (
	_ EncodableValue = &repositoryRule{}
	_ NamedGlobal    = &repositoryRule{}
)

func NewRepositoryRule(identifier *pg_label.CanonicalStarlarkIdentifier, definition *model_starlark_pb.RepositoryRule_Definition) starlark.Value {
	return &repositoryRule{
		LateNamedValue: LateNamedValue{
			Identifier: identifier,
		},
	}
}

func (rr *repositoryRule) String() string {
	return "<repository_rule>"
}

func (rr *repositoryRule) Type() string {
	return "repository_rule"
}

func (rr *repositoryRule) Freeze() {}

func (rr *repositoryRule) Truth() starlark.Bool {
	return starlark.True
}

func (rr *repositoryRule) Hash() (uint32, error) {
	// TODO
	return 0, nil
}

func (rr *repositoryRule) EncodeValue(path map[starlark.Value]struct{}, currentIdentifier *pg_label.CanonicalStarlarkIdentifier, options *ValueEncodingOptions) (model_core.PatchedMessage[*model_starlark_pb.Value, dag.ObjectContentsWalker], bool, error) {
	if rr.Identifier == nil {
		return model_core.PatchedMessage[*model_starlark_pb.Value, dag.ObjectContentsWalker]{}, false, errors.New("repository_rule does not have a name")
	}
	if currentIdentifier == nil || *currentIdentifier != *rr.Identifier {
		// Not the canonical identifier under which this
		// transition is known. Emit a reference.
		return model_core.PatchedMessage[*model_starlark_pb.Value, dag.ObjectContentsWalker]{
			Message: &model_starlark_pb.Value{
				Kind: &model_starlark_pb.Value_RepositoryRule{
					RepositoryRule: &model_starlark_pb.RepositoryRule{
						Kind: &model_starlark_pb.RepositoryRule_Reference{
							Reference: rr.Identifier.String(),
						},
					},
				},
			},
			Patcher: model_core.NewReferenceMessagePatcher[dag.ObjectContentsWalker](),
		}, false, nil
	}

	return model_core.PatchedMessage[*model_starlark_pb.Value, dag.ObjectContentsWalker]{
		Message: &model_starlark_pb.Value{
			Kind: &model_starlark_pb.Value_RepositoryRule{
				RepositoryRule: &model_starlark_pb.RepositoryRule{
					Kind: &model_starlark_pb.RepositoryRule_Definition_{
						Definition: &model_starlark_pb.RepositoryRule_Definition{},
					},
				},
			},
		},
		Patcher: model_core.NewReferenceMessagePatcher[dag.ObjectContentsWalker](),
	}, false, nil
}
