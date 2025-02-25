package starlark

import (
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync/atomic"

	pg_label "github.com/buildbarn/bonanza/pkg/label"
	model_core "github.com/buildbarn/bonanza/pkg/model/core"
	model_starlark_pb "github.com/buildbarn/bonanza/pkg/proto/model/starlark"
	"github.com/buildbarn/bonanza/pkg/storage/dag"

	"go.starlark.net/starlark"
)

type targetReference struct {
	label            pg_label.ResolvedLabel
	encodedProviders model_core.Message[[]*model_starlark_pb.Struct]

	decodedProviders []atomic.Pointer[Struct]
}

func NewTargetReference(label pg_label.ResolvedLabel, providers model_core.Message[[]*model_starlark_pb.Struct]) starlark.Value {
	return &targetReference{
		label:            label,
		encodedProviders: providers,
		decodedProviders: make([]atomic.Pointer[Struct], len(providers.Message)),
	}
}

var (
	_ EncodableValue    = (*targetReference)(nil)
	_ starlark.HasAttrs = (*targetReference)(nil)
	_ starlark.Mapping  = (*targetReference)(nil)
)

func (tr *targetReference) String() string {
	return fmt.Sprintf("<target %s>", tr.label.String())
}

func (targetReference) Type() string {
	return "Target"
}

func (targetReference) Freeze() {
}

func (targetReference) Truth() starlark.Bool {
	return starlark.True
}

func (targetReference) Hash(thread *starlark.Thread) (uint32, error) {
	return 0, errors.New("Target cannot be hashed")
}

var defaultInfoProviderIdentifier = pg_label.MustNewCanonicalStarlarkIdentifier("@@builtins_core+//:exports.bzl%DefaultInfo")

func (tr *targetReference) Attr(thread *starlark.Thread, name string) (starlark.Value, error) {
	switch name {
	case "label":
		return NewLabel(tr.label), nil
	case "data_runfiles", "default_runfiles", "files", "files_to_run":
		// Fields provided by DefaultInfo can be accessed directly.
		defaultInfoProviderValue, err := tr.getProviderValue(thread, defaultInfoProviderIdentifier)
		if err != nil {
			return nil, err
		}
		return defaultInfoProviderValue.Attr(thread, name)
	default:
		return nil, nil
	}
}

var targetReferenceAttrNames = []string{
	"data_runfiles",
	"default_runfiles",
	"files",
	"files_to_run",
	"label",
}

func (tr *targetReference) AttrNames() []string {
	return targetReferenceAttrNames
}

func (tr *targetReference) getProviderValue(thread *starlark.Thread, providerIdentifier pg_label.CanonicalStarlarkIdentifier) (*Struct, error) {
	valueDecodingOptions := thread.Local(ValueDecodingOptionsKey)
	if valueDecodingOptions == nil {
		return nil, errors.New("providers cannot be decoded from within this context")
	}

	providerIdentifierStr := providerIdentifier.String()
	index, ok := sort.Find(
		len(tr.encodedProviders.Message),
		func(i int) int {
			return strings.Compare(providerIdentifierStr, tr.encodedProviders.Message[i].ProviderInstanceProperties.GetProviderIdentifier())
		},
	)
	if !ok {
		return nil, fmt.Errorf("target %#v did not yield provider %#v", tr.label.String(), providerIdentifierStr)
	}

	strukt := tr.decodedProviders[index].Load()
	if strukt == nil {
		var err error
		strukt, err = DecodeStruct(
			model_core.Message[*model_starlark_pb.Struct]{
				Message:            tr.encodedProviders.Message[index],
				OutgoingReferences: tr.encodedProviders.OutgoingReferences,
			},
			valueDecodingOptions.(*ValueDecodingOptions),
		)
		if err != nil {
			return nil, err
		}
		tr.decodedProviders[index].Store(strukt)
	}
	return strukt, nil
}

func (tr *targetReference) Get(thread *starlark.Thread, v starlark.Value) (starlark.Value, bool, error) {
	provider, ok := v.(*Provider)
	if !ok {
		return nil, false, errors.New("keys have to be of type provider")
	}
	providerIdentifier := provider.Identifier
	if providerIdentifier == nil {
		return nil, false, errors.New("provider does not have a name")
	}
	providerValue, err := tr.getProviderValue(thread, *providerIdentifier)
	if err != nil {
		return nil, false, err
	}
	return providerValue, true, nil
}

func (tr *targetReference) EncodeValue(path map[starlark.Value]struct{}, currentIdentifier *pg_label.CanonicalStarlarkIdentifier, options *ValueEncodingOptions) (model_core.PatchedMessage[*model_starlark_pb.Value, dag.ObjectContentsWalker], bool, error) {
	return model_core.NewPatchedMessageFromExisting(
		model_core.Message[*model_starlark_pb.Value]{
			Message: &model_starlark_pb.Value{
				Kind: &model_starlark_pb.Value_TargetReference{
					TargetReference: &model_starlark_pb.TargetReference{
						Label:     tr.label.String(),
						Providers: tr.encodedProviders.Message,
					},
				},
			},
			OutgoingReferences: tr.encodedProviders.OutgoingReferences,
		},
		func(index int) dag.ObjectContentsWalker {
			return dag.ExistingObjectContentsWalker
		},
	), false, nil
}
