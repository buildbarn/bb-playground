package starlark

import (
	"errors"
	"fmt"
	"maps"
	"slices"
	"sort"
	"strings"
	"sync/atomic"

	pg_label "github.com/buildbarn/bb-playground/pkg/label"
	model_core "github.com/buildbarn/bb-playground/pkg/model/core"
	model_starlark_pb "github.com/buildbarn/bb-playground/pkg/proto/model/starlark"
	"github.com/buildbarn/bb-playground/pkg/starlark/unpack"
	"github.com/buildbarn/bb-playground/pkg/storage/dag"

	"go.starlark.net/starlark"
)

type rule struct {
	LateNamedValue
	definition RuleDefinition
}

var (
	_ starlark.Callable = &rule{}
	_ EncodableValue    = &rule{}
	_ NamedGlobal       = &rule{}
)

func NewRule(identifier *pg_label.CanonicalStarlarkIdentifier, definition RuleDefinition) starlark.Value {
	return &rule{
		LateNamedValue: LateNamedValue{
			Identifier: identifier,
		},
		definition: definition,
	}
}

func (r *rule) String() string {
	return "<rule>"
}

func (r *rule) Type() string {
	return "rule"
}

func (r *rule) Freeze() {}

func (r *rule) Truth() starlark.Bool {
	return starlark.True
}

func (r *rule) Hash() (uint32, error) {
	// TODO
	return 0, nil
}

func (r *rule) Name() string {
	if r.Identifier == nil {
		return "rule"
	}
	return r.Identifier.GetStarlarkIdentifier().String()
}

const TargetRegistrarKey = "target_registrar"

func (r *rule) CallInternal(thread *starlark.Thread, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
	if r.Identifier == nil {
		return nil, errors.New("rule does not have a name")
	}
	targetRegistrarValue := thread.Local(TargetRegistrarKey)
	if targetRegistrarValue == nil {
		return nil, fmt.Errorf("rule cannot be invoked from within this context")
	}
	targetRegistrar := targetRegistrarValue.(*TargetRegistrar)

	attrs, err := r.definition.GetAttrsCheap(thread)
	if err != nil {
		return nil, err
	}

	var unpackers []any
	attrNames := make([]string, 0, len(attrs))
	values := make([]*Select, len(attrs))
	currentPackage := thread.Local(CanonicalPackageKey).(pg_label.CanonicalPackage)
	for _, name := range slices.SortedFunc(
		maps.Keys(attrs),
		func(a, b pg_label.StarlarkIdentifier) int { return strings.Compare(a.String(), b.String()) },
	) {
		switch nameStr := name.String(); nameStr {
		case "deprecation", "name", "package_metadata", "tags", "testonly", "visibility":
			return nil, fmt.Errorf("rule uses attribute with reserved name %#v", nameStr)
		}
		if name.IsPublic() {
			attr := attrs[name]
			nameStr := name.String()
			if attr.defaultValue == nil {
				// Attribute is mandatory.
				unpackers = append(unpackers, nameStr)
			} else {
				// Attribute is optional.
				unpackers = append(unpackers, nameStr+"?")
			}
			unpackers = append(
				unpackers,
				unpack.Bind(
					thread,
					&values[len(attrNames)],
					NewSelectUnpackerInto(attr.attrType.GetCanonicalizer(currentPackage)),
				),
			)
			attrNames = append(attrNames, nameStr)
		}
	}

	defaultInheritableAttrs := targetRegistrar.defaultInheritableAttrs.Message

	deprecation := defaultInheritableAttrs.Deprecation
	var name string
	packageMetadata := defaultInheritableAttrs.PackageMetadata
	var tags []string
	testOnly := defaultInheritableAttrs.Testonly
	var visibility []pg_label.CanonicalLabel
	labelStringListUnpackerInto := unpack.List(unpack.Stringer(NewLabelOrStringUnpackerInto(currentPackage)))
	unpackers = append(
		unpackers,
		"name", unpack.Bind(thread, &name, unpack.Stringer(unpack.TargetName)),
		"deprecation?", unpack.Bind(thread, &deprecation, unpack.String),
		"package_metadata?", unpack.Bind(thread, &packageMetadata, labelStringListUnpackerInto),
		"tags?", unpack.Bind(thread, &tags, unpack.List(unpack.String)),
		"testonly?", unpack.Bind(thread, &testOnly, unpack.Bool),
		"visibility?", unpack.Bind(thread, &visibility, unpack.List(NewLabelOrStringUnpackerInto(currentPackage))),
	)

	if err := starlark.UnpackArgs(
		r.Identifier.GetStarlarkIdentifier().String(), args, kwargs,
		unpackers...,
	); err != nil {
		return nil, err
	}

	var attrValues []*model_starlark_pb.RuleTarget_AttrValue
	patcher := model_core.NewReferenceMessagePatcher[dag.ObjectContentsWalker]()
	valueEncodingOptions := thread.Local(ValueEncodingOptionsKey).(*ValueEncodingOptions)
	for i, attrName := range attrNames {
		if value := values[i]; value != nil {
			encodedGroups, _, err := value.EncodeGroups(
				/* path = */ map[starlark.Value]struct{}{},
				valueEncodingOptions,
			)
			if err != nil {
				return nil, err
			}
			attrValues = append(attrValues,
				&model_starlark_pb.RuleTarget_AttrValue{
					Name:       attrName,
					ValueParts: encodedGroups.Message,
				},
			)
			patcher.Merge(encodedGroups.Patcher)
		}
	}

	sort.Strings(tags)

	visibilityPackageGroup, err := targetRegistrar.getVisibilityPackageGroup(visibility)
	if err != nil {
		return nil, err
	}
	patcher.Merge(visibilityPackageGroup.Patcher)

	return starlark.None, targetRegistrar.registerTarget(
		name,
		model_core.NewPatchedMessage(
			&model_starlark_pb.Target{
				Name: name,
				Definition: &model_starlark_pb.Target_Definition{
					Kind: &model_starlark_pb.Target_Definition_RuleTarget{
						RuleTarget: &model_starlark_pb.RuleTarget{
							RuleIdentifier: r.Identifier.String(),
							AttrValues:     attrValues,
							Tags:           slices.Compact(tags),
							InheritableAttrs: &model_starlark_pb.InheritableAttrs{
								Deprecation:     deprecation,
								PackageMetadata: packageMetadata,
								Testonly:        testOnly,
								Visibility:      visibilityPackageGroup.Message,
							},
						},
					},
				},
			},
			patcher,
		),
	)
}

func (r *rule) EncodeValue(path map[starlark.Value]struct{}, currentIdentifier *pg_label.CanonicalStarlarkIdentifier, options *ValueEncodingOptions) (model_core.PatchedMessage[*model_starlark_pb.Value, dag.ObjectContentsWalker], bool, error) {
	if r.Identifier == nil {
		return model_core.PatchedMessage[*model_starlark_pb.Value, dag.ObjectContentsWalker]{}, false, errors.New("rule does not have a name")
	}
	if currentIdentifier == nil || *currentIdentifier != *r.Identifier {
		// Not the canonical identifier under which this rule is
		// known. Emit a reference.
		return model_core.NewSimplePatchedMessage[dag.ObjectContentsWalker](&model_starlark_pb.Value{
			Kind: &model_starlark_pb.Value_Rule{
				Rule: &model_starlark_pb.Rule{
					Kind: &model_starlark_pb.Rule_Reference{
						Reference: r.Identifier.String(),
					},
				},
			},
		}), false, nil
	}

	definition, needsCode, err := r.definition.Encode(path, options)
	if err != nil {
		return model_core.PatchedMessage[*model_starlark_pb.Value, dag.ObjectContentsWalker]{}, false, err
	}
	return model_core.NewPatchedMessage(
		&model_starlark_pb.Value{
			Kind: &model_starlark_pb.Value_Rule{
				Rule: &model_starlark_pb.Rule{
					Kind: &model_starlark_pb.Rule_Definition_{
						Definition: definition.Message,
					},
				},
			},
		},
		definition.Patcher,
	), needsCode, nil
}

type RuleDefinition interface {
	Encode(path map[starlark.Value]struct{}, options *ValueEncodingOptions) (model_core.PatchedMessage[*model_starlark_pb.Rule_Definition, dag.ObjectContentsWalker], bool, error)
	GetAttrsCheap(thread *starlark.Thread) (map[pg_label.StarlarkIdentifier]*Attr, error)
}

type starlarkRuleDefinition struct {
	attrs          map[pg_label.StarlarkIdentifier]*Attr
	cfg            *Transition
	execGroups     map[string]*ExecGroup
	implementation NamedFunction
	provides       []*Provider
}

func NewStarlarkRuleDefinition(
	attrs map[pg_label.StarlarkIdentifier]*Attr,
	cfg *Transition,
	execGroups map[string]*ExecGroup,
	implementation NamedFunction,
	provides []*Provider,
) RuleDefinition {
	return &starlarkRuleDefinition{
		attrs:          attrs,
		cfg:            cfg,
		execGroups:     execGroups,
		implementation: implementation,
		provides:       provides,
	}
}

func (rd *starlarkRuleDefinition) Encode(path map[starlark.Value]struct{}, options *ValueEncodingOptions) (model_core.PatchedMessage[*model_starlark_pb.Rule_Definition, dag.ObjectContentsWalker], bool, error) {
	implementation, implementationNeedsCode, err := rd.implementation.Encode(path, options)
	if err != nil {
		return model_core.PatchedMessage[*model_starlark_pb.Rule_Definition, dag.ObjectContentsWalker]{}, false, err
	}

	namedAttrs, namedAttrsNeedCode, err := encodeNamedAttrs(rd.attrs, path, options)
	if err != nil {
		return model_core.PatchedMessage[*model_starlark_pb.Rule_Definition, dag.ObjectContentsWalker]{}, false, err
	}

	execGroups := make([]*model_starlark_pb.NamedExecGroup, 0, len(rd.execGroups))
	for _, name := range slices.Sorted(maps.Keys(rd.execGroups)) {
		execGroups = append(execGroups, &model_starlark_pb.NamedExecGroup{
			Name:      name,
			ExecGroup: rd.execGroups[name].Encode(),
		})
	}

	return model_core.NewPatchedMessage(
		&model_starlark_pb.Rule_Definition{
			Implementation: implementation.Message,
			Attrs:          namedAttrs.Message,
			ExecGroups:     execGroups,
		},
		namedAttrs.Patcher,
	), implementationNeedsCode || namedAttrsNeedCode, nil
}

func (rd *starlarkRuleDefinition) GetAttrsCheap(thread *starlark.Thread) (map[pg_label.StarlarkIdentifier]*Attr, error) {
	return rd.attrs, nil
}

type protoRuleDefinition struct {
	message         model_core.Message[*model_starlark_pb.Rule_Definition]
	protoAttrsCache protoAttrsCache
}

func NewProtoRuleDefinition(message model_core.Message[*model_starlark_pb.Rule_Definition]) RuleDefinition {
	return &protoRuleDefinition{
		message: message,
	}
}

func (rd *protoRuleDefinition) Encode(path map[starlark.Value]struct{}, options *ValueEncodingOptions) (model_core.PatchedMessage[*model_starlark_pb.Rule_Definition, dag.ObjectContentsWalker], bool, error) {
	panic("rule definition was already encoded previously")
}

func (rd *protoRuleDefinition) GetAttrsCheap(thread *starlark.Thread) (map[pg_label.StarlarkIdentifier]*Attr, error) {
	return rd.protoAttrsCache.getAttrsCheap(thread, rd.message.Message.Attrs)
}

type reloadingRuleDefinition struct {
	identifier pg_label.CanonicalStarlarkIdentifier
	base       atomic.Pointer[RuleDefinition]
}

type GlobalResolver = func(identifier pg_label.CanonicalStarlarkIdentifier) (model_core.Message[*model_starlark_pb.Value], error)

const GlobalResolverKey = "global_resolver"

func NewReloadingRuleDefinition(identifier pg_label.CanonicalStarlarkIdentifier) RuleDefinition {
	return &reloadingRuleDefinition{
		identifier: identifier,
	}
}

func (rd *reloadingRuleDefinition) Encode(path map[starlark.Value]struct{}, options *ValueEncodingOptions) (model_core.PatchedMessage[*model_starlark_pb.Rule_Definition, dag.ObjectContentsWalker], bool, error) {
	panic("rule definition was already encoded previously")
}

func (rd *reloadingRuleDefinition) GetAttrsCheap(thread *starlark.Thread) (map[pg_label.StarlarkIdentifier]*Attr, error) {
	if base := rd.base.Load(); base != nil {
		return (*base).GetAttrsCheap(thread)
	}
	value, err := thread.Local(GlobalResolverKey).(GlobalResolver)(rd.identifier)
	if err != nil {
		return nil, err
	}

	valueKind, ok := value.Message.Kind.(*model_starlark_pb.Value_Rule)
	if !ok {
		return nil, fmt.Errorf("identifier %#v is not a rule", rd.identifier.String())
	}
	ruleKind, ok := valueKind.Rule.Kind.(*model_starlark_pb.Rule_Definition_)
	if !ok {
		return nil, fmt.Errorf("rule %#v does not have a definition", rd.identifier.String())
	}

	base := NewProtoRuleDefinition(model_core.Message[*model_starlark_pb.Rule_Definition]{
		Message:            ruleKind.Definition,
		OutgoingReferences: value.OutgoingReferences,
	})
	rd.base.Store(&base)

	return base.GetAttrsCheap(thread)
}

// bogusValue is a simple Starlark value type that acts as a
// placeholder. It can be used in places where we need to replace a
// value by a stub, and need to be sure the stub isn't being interpreted
// in any meaningful way.
type bogusValue struct{}

func (bogusValue) String() string {
	return "<bogus_value>"
}

func (bogusValue) Type() string {
	return "bogus_value"
}

func (bogusValue) Freeze() {}

func (bogusValue) Truth() starlark.Bool {
	return starlark.False
}

func (bogusValue) Hash() (uint32, error) {
	return 0, nil
}

type protoAttrsCache struct {
	attrsCheap atomic.Pointer[map[pg_label.StarlarkIdentifier]*Attr]
}

func (pac *protoAttrsCache) getAttrsCheap(thread *starlark.Thread, namedAttrs []*model_starlark_pb.NamedAttr) (map[pg_label.StarlarkIdentifier]*Attr, error) {
	if attrs := pac.attrsCheap.Load(); attrs != nil {
		return *attrs, nil
	}

	attrs := map[pg_label.StarlarkIdentifier]*Attr{}
	for _, namedAttr := range namedAttrs {
		name, err := pg_label.NewStarlarkIdentifier(namedAttr.Name)
		if err != nil {
			return nil, fmt.Errorf("attribute %#v: %w", namedAttr.Name, err)
		}
		if namedAttr.Attr == nil {
			return nil, fmt.Errorf("attribute %#v: missing message", namedAttr.Name)
		}
		attrType, err := DecodeAttrType(namedAttr.Attr)
		if err != nil {
			return nil, fmt.Errorf("attribute %#v: %w", namedAttr.Name, err)
		}

		// Don't bother extracting the actual default value from
		// the rule. We don't need to know these in order to
		// determine if a rule is being called properly.
		var defaultValue starlark.Value
		if namedAttr.Attr.Default != nil {
			defaultValue = bogusValue{}
		}

		attrs[name] = NewAttr(attrType, defaultValue)
	}

	pac.attrsCheap.Store(&attrs)
	return attrs, nil
}
