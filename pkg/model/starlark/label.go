package starlark

import (
	"fmt"

	pg_label "github.com/buildbarn/bb-playground/pkg/label"
	model_core "github.com/buildbarn/bb-playground/pkg/model/core"
	model_starlark_pb "github.com/buildbarn/bb-playground/pkg/proto/model/starlark"
	"github.com/buildbarn/bb-playground/pkg/storage/dag"

	"go.starlark.net/starlark"
)

type label struct {
	value pg_label.CanonicalLabel
}

var _ EncodableValue = &label{}

func NewLabel(value pg_label.CanonicalLabel) starlark.Value {
	return label{
		value: value,
	}
}

func (l label) String() string {
	return fmt.Sprintf("Label(%#v)", l.value.String())
}

func (l label) Type() string {
	return "Label"
}

func (l label) Freeze() {}

func (l label) Truth() starlark.Bool {
	return starlark.True
}

func (l label) Hash() (uint32, error) {
	return starlark.String(l.value.String()).Hash()
}

func (l label) EncodeValue(path map[starlark.Value]struct{}, options *ValueEncodingOptions) (model_core.PatchedMessage[*model_starlark_pb.Value, dag.ObjectContentsWalker], bool, error) {
	// TODO!
	needsCode := false
	return model_core.PatchedMessage[*model_starlark_pb.Value, dag.ObjectContentsWalker]{
		Message: &model_starlark_pb.Value{
			Kind: &model_starlark_pb.Value_Label{
				Label: l.value.String(),
			},
		},
		Patcher: model_core.NewReferenceMessagePatcher[dag.ObjectContentsWalker](),
	}, needsCode, nil
}

type CanonicalRepoResolver = func(apparentRepo pg_label.ApparentRepo) (pg_label.CanonicalRepo, error)

const CanonicalRepoResolverKey = "canonical_repo_resolver"

func UnpackCanonicalLabel(thread *starlark.Thread, v starlark.Value, dst *pg_label.CanonicalLabel) error {
	switch typedV := v.(type) {
	case starlark.String:
		// Label value is a bare string. Parse it.
		canonicalPackage := pg_label.MustNewCanonicalLabel(thread.CallFrame(1).Pos.Filename()).GetCanonicalPackage()
		apparentLabel, err := canonicalPackage.AppendLabel(string(typedV))
		if err != nil {
			return err
		}
		if canonicalLabel, ok := apparentLabel.AsCanonicalLabel(); ok {
			*dst = canonicalLabel
			return nil
		}

		// Label value has an apparent repo name. Resolve it.
		apparentRepo, ok := apparentLabel.GetApparentRepo()
		if !ok {
			panic("label that is not canonical should include an apparent repo name")
		}
		canonicalRepoResolver := thread.Local(CanonicalRepoResolverKey)
		if canonicalRepoResolver == nil {
			return fmt.Errorf("label has apparent repo %#v, which cannot be resolved to a canonical repo from within this context", apparentRepo.String())
		}
		canonicalRepo, err := canonicalRepoResolver.(CanonicalRepoResolver)(apparentRepo)
		if err != nil {
			return err
		}
		*dst = apparentLabel.WithCanonicalRepo(canonicalRepo)
		return nil
	case label:
		// Label value is already wrapped in Label().
		*dst = typedV.value
		return nil
	default:
		return fmt.Errorf("got %s, want Label or str", v.Type())
	}
}
