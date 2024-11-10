package starlark

import (
	"errors"
	"fmt"

	pg_label "github.com/buildbarn/bb-playground/pkg/label"
	model_core "github.com/buildbarn/bb-playground/pkg/model/core"
	model_starlark_pb "github.com/buildbarn/bb-playground/pkg/proto/model/starlark"
	"github.com/buildbarn/bb-playground/pkg/starlark/unpack"
	"github.com/buildbarn/bb-playground/pkg/storage/dag"

	"go.starlark.net/starlark"
	"go.starlark.net/syntax"
)

type label struct {
	value pg_label.CanonicalLabel
}

var (
	_ starlark.HasAttrs = &label{}
	_ starlark.Value    = &label{}
	_ EncodableValue    = &label{}
)

func NewLabel(value pg_label.CanonicalLabel) starlark.Value {
	return label{
		value: value,
	}
}

func (l label) String() string {
	return l.value.String()
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

func (l label) Attr(name string) (starlark.Value, error) {
	switch name {
	case "name":
		return starlark.String(l.value.GetTargetName().String()), nil
	default:
		return nil, nil
	}
}

var labelAttrNames = []string{
	"name",
}

func (l label) AttrNames() []string {
	return labelAttrNames
}

func (l label) EncodeValue(path map[starlark.Value]struct{}, currentIdentifier *pg_label.CanonicalStarlarkIdentifier, options *ValueEncodingOptions) (model_core.PatchedMessage[*model_starlark_pb.Value, dag.ObjectContentsWalker], bool, error) {
	return model_core.NewSimplePatchedMessage[dag.ObjectContentsWalker](
		&model_starlark_pb.Value{
			Kind: &model_starlark_pb.Value_Label{
				Label: l.value.String(),
			},
		},
	), false, nil
}

type (
	CanonicalRepoResolver = func(fromCanonicalRepo pg_label.CanonicalRepo, toApparentRepo pg_label.ApparentRepo) (pg_label.CanonicalRepo, error)
	RootModuleResolver    = func() (pg_label.Module, error)
)

const (
	CanonicalRepoResolverKey = "canonical_repo_resolver"
	RootModuleResolverKey    = "root_module_resolver"
)

type labelOrStringUnpackerInto struct {
	basePackage pg_label.CanonicalPackage
}

func NewLabelOrStringUnpackerInto(basePackage pg_label.CanonicalPackage) unpack.UnpackerInto[pg_label.CanonicalLabel] {
	return &labelOrStringUnpackerInto{
		basePackage: basePackage,
	}
}

func (ui *labelOrStringUnpackerInto) UnpackInto(thread *starlark.Thread, v starlark.Value, dst *pg_label.CanonicalLabel) error {
	switch typedV := v.(type) {
	case starlark.String:
		// Label value is a bare string. Parse it.
		apparentLabel, err := ui.basePackage.AppendLabel(string(typedV))
		if err != nil {
			return err
		}
		if canonicalLabel, ok := apparentLabel.AsCanonicalLabel(); ok {
			*dst = canonicalLabel
			return nil
		}

		if apparentRepo, ok := apparentLabel.GetApparentRepo(); ok {
			// Label value has an apparent repo name. Resolve it.
			canonicalRepoResolver := thread.Local(CanonicalRepoResolverKey)
			if canonicalRepoResolver == nil {
				return fmt.Errorf("label has apparent repo %#v, which cannot be resolved to a canonical repo from within this context", apparentRepo.String())
			}
			canonicalRepo, err := canonicalRepoResolver.(CanonicalRepoResolver)(ui.basePackage.GetCanonicalRepo(), apparentRepo)
			if err != nil {
				return fmt.Errorf("failed to resolve apparent repo %#v: %w", apparentRepo.String(), err)
			}
			*dst = apparentLabel.WithCanonicalRepo(canonicalRepo)
			return nil
		}

		// Label is prefixed with "@@". Resolve to the root module.
		rootModuleResolver := thread.Local(RootModuleResolverKey)
		if rootModuleResolver == nil {
			return errors.New("label is prefixed with \"@@\", which cannot be resolved to the root module from within this context")
		}
		rootModule, err := rootModuleResolver.(RootModuleResolver)()
		if err != nil {
			return fmt.Errorf("failed to resolve root module: %w", err)
		}
		*dst = apparentLabel.WithCanonicalRepo(rootModule.ToModuleInstance(nil).GetBareCanonicalRepo())
		return nil
	case label:
		// Label value is already wrapped in Label().
		*dst = typedV.value
		return nil
	default:
		return fmt.Errorf("got %s, want Label or str", v.Type())
	}
}

func (ui *labelOrStringUnpackerInto) Canonicalize(thread *starlark.Thread, v starlark.Value) (starlark.Value, error) {
	var l pg_label.CanonicalLabel
	if err := ui.UnpackInto(thread, v, &l); err != nil {
		return nil, err
	}
	return label{value: l}, nil
}

func (labelOrStringUnpackerInto) GetConcatenationOperator() syntax.Token {
	return 0
}

type labelUnpackerInto struct{}

var LabelUnpackerInto unpack.UnpackerInto[pg_label.CanonicalLabel] = labelUnpackerInto{}

func (labelUnpackerInto) UnpackInto(thread *starlark.Thread, v starlark.Value, dst *pg_label.CanonicalLabel) error {
	l, ok := v.(label)
	if !ok {
		return fmt.Errorf("got %s, want Label", v.Type())
	}
	*dst = l.value
	return nil
}

func (ui labelUnpackerInto) Canonicalize(thread *starlark.Thread, v starlark.Value) (starlark.Value, error) {
	var l pg_label.CanonicalLabel
	if err := ui.UnpackInto(thread, v, &l); err != nil {
		return nil, err
	}
	return label{value: l}, nil
}

func (labelUnpackerInto) GetConcatenationOperator() syntax.Token {
	return 0
}

func currentFilePackage(thread *starlark.Thread) pg_label.CanonicalPackage {
	return pg_label.MustNewCanonicalLabel(thread.CallFrame(1).Pos.Filename()).GetCanonicalPackage()
}
