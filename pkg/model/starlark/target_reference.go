package starlark

import (
	"errors"
	"sort"

	pg_label "github.com/buildbarn/bb-playground/pkg/label"

	"go.starlark.net/starlark"
)

type targetReference struct {
	canonicalLabel    pg_label.CanonicalLabel
	providerInstances []ProviderInstance
}

var (
	_ starlark.Mapping = &targetReference{}
	_ starlark.Value   = &targetReference{}
)

func NewTargetReference(canonicalLabel pg_label.CanonicalLabel, providerInstances []ProviderInstance) starlark.Value {
	return &targetReference{
		canonicalLabel:    canonicalLabel,
		providerInstances: providerInstances,
	}
}

func (tr targetReference) String() string {
	return "TODO"
}

func (tr targetReference) Type() string {
	return "target_reference"
}

func (tr targetReference) Freeze() {
}

func (tr targetReference) Truth() starlark.Bool {
	return starlark.True
}

func (tr targetReference) Hash() (uint32, error) {
	return 0, nil
}

func (tr targetReference) Get(v starlark.Value) (starlark.Value, bool, error) {
	provider, ok := v.(*Provider)
	if !ok {
		return nil, false, errors.New("keys have to be of type provider")
	}
	providerIdentifier := provider.Identifier
	if providerIdentifier == nil {
		return nil, false, errors.New("provider does not have a name")
	}
	providerIdentifierStr := providerIdentifier.String()
	if i := sort.Search(
		len(tr.providerInstances),
		func(i int) bool { return tr.providerInstances[i].provider.Identifier.String() >= providerIdentifierStr },
	); i < len(tr.providerInstances) && tr.providerInstances[i].provider.Identifier.String() == providerIdentifierStr {
		return tr.providerInstances[i], true, nil
	}
	return nil, false, nil
}
