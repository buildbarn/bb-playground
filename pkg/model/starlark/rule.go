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
	if len(args) != 0 {
		return nil, fmt.Errorf("got %d positional arguments, want 1", len(args))
	}

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

	buildSetting, err := r.definition.GetBuildSetting(thread)
	if err != nil {
		return nil, err
	}

	var unpackers []any
	attrNames := make([]pg_label.StarlarkIdentifier, 0, len(attrs))
	values := make([]starlark.Value, len(attrs))
	currentPackage := thread.Local(CanonicalPackageKey).(pg_label.CanonicalPackage)
	for _, name := range slices.SortedFunc(
		maps.Keys(attrs),
		func(a, b pg_label.StarlarkIdentifier) int { return strings.Compare(a.String(), b.String()) },
	) {
		nameStr := name.String()
		switch nameStr {
		case "deprecation", "exec_compatible_with", "name",
			"package_metadata", "tags", "target_compatible_with",
			"testonly", "visibility":
			return nil, fmt.Errorf("rule uses attribute with reserved name %#v", nameStr)
		case "build_setting_default":
			if buildSetting != nil {
				return nil, fmt.Errorf("rule uses attribute with name \"build_setting_default\", which is reserved for build settings", nameStr)
			}
		}
		if name.IsPublic() {
			if attrs[name].defaultValue == nil {
				// Attribute is mandatory.
				unpackers = append(unpackers, nameStr)
			} else {
				// Attribute is optional.
				unpackers = append(unpackers, nameStr+"?")
			}
			unpackers = append(unpackers, &values[len(attrNames)])
			attrNames = append(attrNames, name)
		} else {
			// TODO: Visit labels from private attribute default values!
		}
	}

	defaultInheritableAttrs := targetRegistrar.defaultInheritableAttrs.Message

	var name string
	deprecation := defaultInheritableAttrs.Deprecation
	var execCompatibleWith []string
	packageMetadata := defaultInheritableAttrs.PackageMetadata
	var tags []string
	var targetCompatibleWith []string
	testOnly := defaultInheritableAttrs.Testonly
	var visibility []pg_label.CanonicalLabel
	labelStringListUnpackerInto := unpack.List(unpack.Stringer(NewLabelOrStringUnpackerInto(currentPackage)))
	unpackers = append(
		unpackers,
		"name", unpack.Bind(thread, &name, unpack.Stringer(unpack.TargetName)),
		"deprecation?", unpack.Bind(thread, &deprecation, unpack.String),
		"exec_compatible_with?", unpack.Bind(thread, &execCompatibleWith, labelStringListUnpackerInto),
		"package_metadata?", unpack.Bind(thread, &packageMetadata, labelStringListUnpackerInto),
		"tags?", unpack.Bind(thread, &tags, unpack.List(unpack.String)),
		"target_compatible_with?", unpack.Bind(thread, &targetCompatibleWith, labelStringListUnpackerInto),
		"testonly?", unpack.Bind(thread, &testOnly, unpack.Bool),
		"visibility?", unpack.Bind(thread, &visibility, unpack.List(NewLabelOrStringUnpackerInto(currentPackage))),
	)

	if buildSetting != nil {
		// TODO: Save build setting default value in target.
		var buildSettingDefault starlark.Value
		unpackers = append(
			unpackers,
			"build_setting_default",
			unpack.Bind(thread, &buildSettingDefault, unpack.Canonicalize(buildSetting.buildSettingType.GetCanonicalizer())),
		)
	}

	if err := starlark.UnpackArgs(
		r.Identifier.GetStarlarkIdentifier().String(), nil, kwargs,
		unpackers...,
	); err != nil {
		return nil, err
	}

	initializer, err := r.definition.GetInitializer(thread)
	if err != nil {
		return nil, err
	}
	if initializer != nil {
		initializerKwargs := make([]starlark.Tuple, 0, 1+len(attrNames))
		initializerKwargs = append(initializerKwargs, starlark.Tuple{
			starlark.String("name"),
			starlark.String(name),
		})
		for i, attrName := range attrNames {
			if v := values[i]; v != nil {
				initializerKwargs = append(initializerKwargs, starlark.Tuple{
					starlark.String(attrName.String()),
					v,
				})
			}
		}
		overrides, err := starlark.Call(thread, initializer, nil, initializerKwargs)
		if err != nil {
			return nil, fmt.Errorf("failed to run initializer: %w", err)
		}
		var overrideEntries map[string]starlark.Value
		if err := unpack.Dict(unpack.String, unpack.Any).UnpackInto(thread, overrides, &overrideEntries); err != nil {
			return nil, fmt.Errorf("failed to unpack initializer return value: %w", err)
		}
		for name, value := range overrideEntries {
			if name == "name" {
				// Overriding "name" is not permitted.
				continue
			}
			index, ok := sort.Find(
				len(attrNames),
				func(i int) int { return strings.Compare(name, attrNames[i].String()) },
			)
			if !ok {
				return nil, fmt.Errorf("initializer returned value for attr %#v, which does not exist", name)
			}
			values[index] = value
		}
	}

	var attrValues []*model_starlark_pb.RuleTarget_AttrValue
	patcher := model_core.NewReferenceMessagePatcher[dag.ObjectContentsWalker]()
	valueEncodingOptions := thread.Local(ValueEncodingOptionsKey).(*ValueEncodingOptions)
	for i, attrName := range attrNames {
		attr := attrs[attrName]
		unpacker := NewSelectUnpackerInto(attr.attrType.GetCanonicalizer(currentPackage))
		if attr.defaultValue != nil {
			unpacker = unpack.IfNotNone(unpacker)
		}
		value := values[i]
		if value == nil {
			value = starlark.None
		}
		var selectValue *Select
		if err := unpacker.UnpackInto(thread, value, &selectValue); err != nil {
			return nil, fmt.Errorf("invalid argument %#v: %w", attrName.String(), err)
		}
		if selectValue != nil {
			encodedGroups, _, err := selectValue.EncodeGroups(
				/* path = */ map[starlark.Value]struct{}{},
				valueEncodingOptions,
			)
			if err != nil {
				return nil, err
			}
			attrValues = append(attrValues,
				&model_starlark_pb.RuleTarget_AttrValue{
					Name:       attrName.String(),
					ValueParts: encodedGroups.Message,
				},
			)
			patcher.Merge(encodedGroups.Patcher)

			selectValue.VisitLabels(map[starlark.Value]struct{}{}, func(l pg_label.CanonicalLabel) {
				if l.GetCanonicalPackage() == currentPackage {
					targetRegistrar.registerImplicitTarget(l.GetTargetName().String())
				}
			})
		} else {
			// TODO: Also visit labels from attribute default values!
		}
	}

	sort.Strings(execCompatibleWith)
	sort.Strings(tags)
	sort.Strings(targetCompatibleWith)

	visibilityPackageGroup, err := targetRegistrar.getVisibilityPackageGroup(visibility)
	if err != nil {
		return nil, err
	}
	patcher.Merge(visibilityPackageGroup.Patcher)

	return starlark.None, targetRegistrar.registerExplicitTarget(
		name,
		model_core.NewPatchedMessage(
			&model_starlark_pb.Target_Definition{
				Kind: &model_starlark_pb.Target_Definition_RuleTarget{
					RuleTarget: &model_starlark_pb.RuleTarget{
						RuleIdentifier:       r.Identifier.String(),
						AttrValues:           attrValues,
						ExecCompatibleWith:   execCompatibleWith,
						Tags:                 slices.Compact(tags),
						TargetCompatibleWith: targetCompatibleWith,
						InheritableAttrs: &model_starlark_pb.InheritableAttrs{
							Deprecation:     deprecation,
							PackageMetadata: packageMetadata,
							Testonly:        testOnly,
							Visibility:      visibilityPackageGroup.Message,
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
	GetBuildSetting(thread *starlark.Thread) (*BuildSetting, error)
	GetInitializer(thread *starlark.Thread) (*NamedFunction, error)
}

type starlarkRuleDefinition struct {
	attrs          map[pg_label.StarlarkIdentifier]*Attr
	buildSetting   *BuildSetting
	cfg            *Transition
	execGroups     map[string]*ExecGroup
	implementation NamedFunction
	initializer    *NamedFunction
	provides       []*Provider
}

func NewStarlarkRuleDefinition(
	attrs map[pg_label.StarlarkIdentifier]*Attr,
	buildSetting *BuildSetting,
	cfg *Transition,
	execGroups map[string]*ExecGroup,
	implementation NamedFunction,
	initializer *NamedFunction,
	provides []*Provider,
) RuleDefinition {
	return &starlarkRuleDefinition{
		attrs:          attrs,
		buildSetting:   buildSetting,
		cfg:            cfg,
		execGroups:     execGroups,
		implementation: implementation,
		initializer:    initializer,
		provides:       provides,
	}
}

func (rd *starlarkRuleDefinition) Encode(path map[starlark.Value]struct{}, options *ValueEncodingOptions) (model_core.PatchedMessage[*model_starlark_pb.Rule_Definition, dag.ObjectContentsWalker], bool, error) {
	patcher := model_core.NewReferenceMessagePatcher[dag.ObjectContentsWalker]()

	var buildSetting *model_starlark_pb.BuildSetting
	if rd.buildSetting != nil {
		buildSetting = rd.buildSetting.Encode()
	}

	execGroups := make([]*model_starlark_pb.NamedExecGroup, 0, len(rd.execGroups))
	for _, name := range slices.Sorted(maps.Keys(rd.execGroups)) {
		execGroups = append(execGroups, &model_starlark_pb.NamedExecGroup{
			Name:      name,
			ExecGroup: rd.execGroups[name].Encode(),
		})
	}

	implementation, needsCode, err := rd.implementation.Encode(path, options)
	if err != nil {
		return model_core.PatchedMessage[*model_starlark_pb.Rule_Definition, dag.ObjectContentsWalker]{}, false, err
	}
	patcher.Merge(implementation.Patcher)

	var initializerMessage *model_starlark_pb.Function
	if rd.initializer != nil {
		initializer, initializerNeedsCode, err := rd.initializer.Encode(path, options)
		if err != nil {
			return model_core.PatchedMessage[*model_starlark_pb.Rule_Definition, dag.ObjectContentsWalker]{}, false, err
		}
		initializerMessage = initializer.Message
		needsCode = needsCode || initializerNeedsCode
		patcher.Merge(initializer.Patcher)
	}

	namedAttrs, namedAttrsNeedCode, err := encodeNamedAttrs(rd.attrs, path, options)
	if err != nil {
		return model_core.PatchedMessage[*model_starlark_pb.Rule_Definition, dag.ObjectContentsWalker]{}, false, err
	}
	needsCode = needsCode || namedAttrsNeedCode
	patcher.Merge(namedAttrs.Patcher)

	return model_core.NewPatchedMessage(
		&model_starlark_pb.Rule_Definition{
			Attrs:          namedAttrs.Message,
			BuildSetting:   buildSetting,
			ExecGroups:     execGroups,
			Implementation: implementation.Message,
			Initializer:    initializerMessage,
		},
		patcher,
	), needsCode, nil
}

func (rd *starlarkRuleDefinition) GetAttrsCheap(thread *starlark.Thread) (map[pg_label.StarlarkIdentifier]*Attr, error) {
	return rd.attrs, nil
}

func (rd *starlarkRuleDefinition) GetBuildSetting(thread *starlark.Thread) (*BuildSetting, error) {
	return rd.buildSetting, nil
}

func (rd *starlarkRuleDefinition) GetInitializer(thread *starlark.Thread) (*NamedFunction, error) {
	return rd.initializer, nil
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

func (rd *protoRuleDefinition) GetBuildSetting(thread *starlark.Thread) (*BuildSetting, error) {
	buildSettingMessage := rd.message.Message.BuildSetting
	if buildSettingMessage == nil {
		return nil, nil
	}
	return decodeBuildSetting(buildSettingMessage)
}

func (rd *protoRuleDefinition) GetInitializer(thread *starlark.Thread) (*NamedFunction, error) {
	if rd.message.Message.Initializer == nil {
		return nil, nil
	}
	f := NewNamedFunction(
		NewProtoNamedFunctionDefinition(
			model_core.Message[*model_starlark_pb.Function]{
				Message:            rd.message.Message.Initializer,
				OutgoingReferences: rd.message.OutgoingReferences,
			},
		),
	)
	return &f, nil
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

func (rd *reloadingRuleDefinition) getBase(thread *starlark.Thread) (RuleDefinition, error) {
	if base := rd.base.Load(); base != nil {
		return *base, nil
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
	return base, nil
}

func (rd *reloadingRuleDefinition) GetAttrsCheap(thread *starlark.Thread) (map[pg_label.StarlarkIdentifier]*Attr, error) {
	base, err := rd.getBase(thread)
	if err != nil {
		return nil, err
	}
	return base.GetAttrsCheap(thread)
}

func (rd *reloadingRuleDefinition) GetBuildSetting(thread *starlark.Thread) (*BuildSetting, error) {
	base, err := rd.getBase(thread)
	if err != nil {
		return nil, err
	}
	return base.GetBuildSetting(thread)
}

func (rd *reloadingRuleDefinition) GetInitializer(thread *starlark.Thread) (*NamedFunction, error) {
	base, err := rd.getBase(thread)
	if err != nil {
		return nil, err
	}
	return base.GetInitializer(thread)
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
