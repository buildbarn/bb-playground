package starlark

import (
	"errors"
	"fmt"
	"maps"
	"slices"
	"sort"
	"strings"
	"sync/atomic"

	pg_label "github.com/buildbarn/bonanza/pkg/label"
	model_core "github.com/buildbarn/bonanza/pkg/model/core"
	model_starlark_pb "github.com/buildbarn/bonanza/pkg/proto/model/starlark"
	"github.com/buildbarn/bonanza/pkg/starlark/unpack"

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

func (r *rule) Hash(thread *starlark.Thread) (uint32, error) {
	return 0, errors.New("rule cannot be hashed")
}

func (r *rule) Name() string {
	if r.Identifier == nil {
		return "rule"
	}
	return r.Identifier.GetStarlarkIdentifier().String()
}

const TargetRegistrarKey = "target_registrar"

var emptyTargetCompatibleWith = NewSelect(
	[]SelectGroup{
		NewSelectGroup(nil, starlark.NewList(nil), ""),
	},
	/* concatenationOperator = */ 0,
)

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

	test, err := r.definition.GetTest(thread)
	if err != nil {
		return nil, err
	}

	var mandatoryUnpackers, optionalUnpackers []any
	attrNames := make([]pg_label.StarlarkIdentifier, 0, len(attrs))
	values := make([]starlark.Value, len(attrs))
	currentPackage := thread.Local(CanonicalPackageKey).(pg_label.CanonicalPackage)
	for _, name := range slices.SortedFunc(
		maps.Keys(attrs),
		func(a, b pg_label.StarlarkIdentifier) int { return strings.Compare(a.String(), b.String()) },
	) {
		nameStr := name.String()
		switch nameStr {
		case "applicable_licenses", "deprecation",
			"exec_compatible_with", "features", "name",
			"package_metadata", "tags", "target_compatible_with",
			"testonly", "visibility":
			return nil, fmt.Errorf("rule uses attribute with reserved name %#v", nameStr)
		case "build_setting_default":
			if buildSetting != nil {
				return nil, fmt.Errorf("rule uses attribute with name \"build_setting_default\", which is reserved for build settings", nameStr)
			}
		case "args", "flaky", "local", "shard_count", "size", "timeout":
			if test {
				return nil, fmt.Errorf("rule uses attribute with name %#v, which is reserved for tests", nameStr)
			}
		}

		attr := attrs[name]
		filenameTemplate, isOutput := attr.attrType.IsOutput()
		isPublic := name.IsPublic()
		if isOutput && filenameTemplate != "" {
			// Predeclared output declared using
			// rule(outputs = ...). These are not taken as
			// explicit arguments.
		} else if isPublic {
			if attr.defaultValue == nil {
				// Attribute is mandatory.
				mandatoryUnpackers = append(mandatoryUnpackers, nameStr, &values[len(attrNames)])
			} else {
				// Attribute is optional.
				optionalUnpackers = append(optionalUnpackers, nameStr+"?", &values[len(attrNames)])
			}
		} else if false {
			// TODO: We should also add all label types
			// here, regardless of whether they are public.
			// This is needed to ensure VisitLabels() is
			// called properly.
		}

		if isPublic || isOutput {
			attrNames = append(attrNames, name)
		}
	}

	defaultInheritableAttrs := targetRegistrar.defaultInheritableAttrs.Message

	var name string
	mandatoryUnpackers = append(
		mandatoryUnpackers,
		"name", unpack.Bind(thread, &name, unpack.Stringer(unpack.TargetName)),
	)

	var applicableLicenses []string
	deprecation := defaultInheritableAttrs.Deprecation
	var execCompatibleWith []string
	var features *Select
	packageMetadata := defaultInheritableAttrs.PackageMetadata
	var tags []string
	targetCompatibleWith := emptyTargetCompatibleWith
	testOnly := defaultInheritableAttrs.Testonly
	var visibility []pg_label.ResolvedLabel
	labelUnpackerInto := NewLabelOrStringUnpackerInto(currentPackage)
	labelStringListUnpackerInto := unpack.List(unpack.Stringer(labelUnpackerInto))
	optionalUnpackers = append(
		optionalUnpackers,
		"applicable_licenses?", unpack.Bind(thread, &applicableLicenses, labelStringListUnpackerInto),
		"deprecation?", unpack.Bind(thread, &deprecation, unpack.String),
		"exec_compatible_with?", unpack.Bind(thread, &execCompatibleWith, labelStringListUnpackerInto),
		"features?", unpack.Bind(thread, &features, NewSelectUnpackerInto(unpack.Canonicalize(unpack.List(unpack.String)))),
		"package_metadata?", unpack.Bind(thread, &packageMetadata, labelStringListUnpackerInto),
		"tags?", unpack.Bind(thread, &tags, unpack.IfNotNone(unpack.List(unpack.String))),
		"target_compatible_with?", unpack.Bind(thread, &targetCompatibleWith, NewSelectUnpackerInto(unpack.Canonicalize(unpack.List(labelUnpackerInto)))),
		"testonly?", unpack.Bind(thread, &testOnly, unpack.IfNotNone(sloppyBoolUnpackerInto{})),
		"visibility?", unpack.Bind(thread, &visibility, unpack.IfNotNone(unpack.List(NewLabelOrStringUnpackerInto(currentPackage)))),
	)

	var buildSettingDefault starlark.Value
	if buildSetting != nil {
		mandatoryUnpackers = append(
			mandatoryUnpackers,
			"build_setting_default",
			unpack.Bind(thread, &buildSettingDefault, unpack.Canonicalize(buildSetting.buildSettingType.GetCanonicalizer())),
		)
	}

	if test {
		var args []string
		flaky := false
		local := false
		shardCount := 1
		size := "medium"
		var timeout string
		optionalUnpackers = append(
			optionalUnpackers,
			"args?",
			unpack.Bind(thread, &args, unpack.List(unpack.String)),
			"flaky?",
			unpack.Bind(thread, &flaky, sloppyBoolUnpackerInto{}),
			"local?",
			unpack.Bind(thread, &local, sloppyBoolUnpackerInto{}),
			"shard_count?",
			unpack.Bind(thread, &shardCount, unpack.Int[int]()),
			"size?",
			unpack.Bind(thread, &size, unpack.String),
			"timeout?",
			unpack.Bind(thread, &timeout, unpack.String),
		)
	}

	if err := starlark.UnpackArgs(
		r.Identifier.GetStarlarkIdentifier().String(), nil, kwargs,
		append(mandatoryUnpackers, optionalUnpackers...)...,
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
	patcher := model_core.NewReferenceMessagePatcher[model_core.CreatedObjectTree]()
	valueEncodingOptions := thread.Local(ValueEncodingOptionsKey).(*ValueEncodingOptions)
	for i, attrName := range attrNames {
		attr := attrs[attrName]
		value := values[i]

		// If the attr is a predeclared output declared using
		// rule(outputs = ...), compute its actual value.
		filenameTemplate, isOutput := attr.attrType.IsOutput()
		if isOutput && filenameTemplate != "" {
			targetName, err := pg_label.NewTargetName(strings.ReplaceAll(filenameTemplate, "%{name}", name))
			if err != nil {
				return nil, fmt.Errorf("invalid target name for predeclared output %#v with filename template %#v: %w", attrName, filenameTemplate, err)
			}
			value = NewLabel(currentPackage.AppendTargetName(targetName).AsResolved())
		}

		canonicalizer := attr.attrType.GetCanonicalizer(currentPackage)
		if attr.defaultValue != nil {
			canonicalizer = unpack.IfNotNoneCanonicalizer(canonicalizer)
		}
		if value == nil {
			value = starlark.None
		}
		var selectValue *Select
		if err := NewSelectUnpackerInto(canonicalizer).UnpackInto(thread, value, &selectValue); err != nil {
			return nil, fmt.Errorf("invalid argument %#v: %w", attrName.String(), err)
		}

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

		// TODO: Visit the default value in case no explicit
		// value is set.
		//
		// TODO: If attr.attrType.IsOutput(), register labels as
		// explicit targets.
		if _, isOutput := attr.attrType.IsOutput(); isOutput {
			if err := selectValue.VisitLabels(thread, map[starlark.Value]struct{}{}, func(l pg_label.ResolvedLabel) error {
				if canonicalLabel, err := l.AsCanonical(); err == nil {
					if canonicalLabel.GetCanonicalPackage() == currentPackage {
						targetName := canonicalLabel.GetTargetName().String()
						if err := targetRegistrar.registerExplicitTarget(
							targetName,
							model_core.NewPatchedMessage(
								&model_starlark_pb.Target_Definition{
									Kind: &model_starlark_pb.Target_Definition_PredeclaredOutputFileTarget{
										PredeclaredOutputFileTarget: &model_starlark_pb.PredeclaredOutputFileTarget{
											OwnerTargetName: name,
										},
									},
								},
								patcher,
							),
						); err != nil {
							return err
						}
					}
				}
				return nil
			}); err != nil {
				return nil, err
			}
		} else {
			if err := selectValue.VisitLabels(thread, map[starlark.Value]struct{}{}, func(l pg_label.ResolvedLabel) error {
				if canonicalLabel, err := l.AsCanonical(); err == nil {
					if canonicalLabel.GetCanonicalPackage() == currentPackage {
						targetRegistrar.registerImplicitTarget(l.GetTargetName().String())
					}
				}
				return nil
			}); err != nil {
				return nil, err
			}
		}
	}

	var encodedBuildSettingDefault model_core.PatchedMessage[*model_starlark_pb.Value, model_core.CreatedObjectTree]
	if buildSetting != nil {
		encodedBuildSettingDefault, _, err = EncodeValue(
			buildSettingDefault,
			/* path = */ map[starlark.Value]struct{}{},
			/* identifier = */ nil,
			valueEncodingOptions,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to encode \"build_setting_default\": %w", err)
		}
		patcher.Merge(encodedBuildSettingDefault.Patcher)
	}

	if len(applicableLicenses) > 0 {
		if len(packageMetadata) > 0 {
			return nil, fmt.Errorf("\"applicable_licenses\" and \"package_metadata\" cannot be specified at the same time")
		}
		packageMetadata = applicableLicenses
	}

	targetCompatibleWithGroups, _, err := targetCompatibleWith.EncodeGroups(
		/* path = */ map[starlark.Value]struct{}{},
		valueEncodingOptions,
	)
	if err != nil {
		return nil, err
	}
	patcher.Merge(targetCompatibleWithGroups.Patcher)

	sort.Strings(execCompatibleWith)
	sort.Strings(tags)

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
						TargetCompatibleWith: targetCompatibleWithGroups.Message,
						InheritableAttrs: &model_starlark_pb.InheritableAttrs{
							Deprecation:     deprecation,
							PackageMetadata: packageMetadata,
							Testonly:        testOnly,
							Visibility:      visibilityPackageGroup.Message,
						},
						BuildSettingDefault: encodedBuildSettingDefault.Message,
					},
				},
			},
			patcher,
		),
	)
}

func (r *rule) EncodeValue(path map[starlark.Value]struct{}, currentIdentifier *pg_label.CanonicalStarlarkIdentifier, options *ValueEncodingOptions) (model_core.PatchedMessage[*model_starlark_pb.Value, model_core.CreatedObjectTree], bool, error) {
	if r.Identifier == nil {
		return model_core.PatchedMessage[*model_starlark_pb.Value, model_core.CreatedObjectTree]{}, false, errors.New("rule does not have a name")
	}
	if currentIdentifier == nil || *currentIdentifier != *r.Identifier {
		// Not the canonical identifier under which this rule is
		// known. Emit a reference.
		return model_core.NewSimplePatchedMessage[model_core.CreatedObjectTree](&model_starlark_pb.Value{
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
		return model_core.PatchedMessage[*model_starlark_pb.Value, model_core.CreatedObjectTree]{}, false, err
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
	Encode(path map[starlark.Value]struct{}, options *ValueEncodingOptions) (model_core.PatchedMessage[*model_starlark_pb.Rule_Definition, model_core.CreatedObjectTree], bool, error)
	GetAttrsCheap(thread *starlark.Thread) (map[pg_label.StarlarkIdentifier]*Attr, error)
	GetBuildSetting(thread *starlark.Thread) (*BuildSetting, error)
	GetInitializer(thread *starlark.Thread) (*NamedFunction, error)
	GetTest(thread *starlark.Thread) (bool, error)
}

type starlarkRuleDefinition struct {
	attrs          map[pg_label.StarlarkIdentifier]*Attr
	buildSetting   *BuildSetting
	cfg            *Transition
	execGroups     map[string]*ExecGroup
	implementation NamedFunction
	initializer    *NamedFunction
	provides       []*Provider
	test           bool
	subrules       []*Subrule
}

func NewStarlarkRuleDefinition(
	attrs map[pg_label.StarlarkIdentifier]*Attr,
	buildSetting *BuildSetting,
	cfg *Transition,
	execGroups map[string]*ExecGroup,
	implementation NamedFunction,
	initializer *NamedFunction,
	provides []*Provider,
	test bool,
	subrules []*Subrule,
) RuleDefinition {
	return &starlarkRuleDefinition{
		attrs:          attrs,
		buildSetting:   buildSetting,
		cfg:            cfg,
		execGroups:     execGroups,
		implementation: implementation,
		initializer:    initializer,
		provides:       provides,
		test:           test,
		subrules:       subrules,
	}
}

func (rd *starlarkRuleDefinition) Encode(path map[starlark.Value]struct{}, options *ValueEncodingOptions) (model_core.PatchedMessage[*model_starlark_pb.Rule_Definition, model_core.CreatedObjectTree], bool, error) {
	patcher := model_core.NewReferenceMessagePatcher[model_core.CreatedObjectTree]()

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
		return model_core.PatchedMessage[*model_starlark_pb.Rule_Definition, model_core.CreatedObjectTree]{}, false, err
	}
	patcher.Merge(implementation.Patcher)

	var initializerMessage *model_starlark_pb.Function
	if rd.initializer != nil {
		initializer, initializerNeedsCode, err := rd.initializer.Encode(path, options)
		if err != nil {
			return model_core.PatchedMessage[*model_starlark_pb.Rule_Definition, model_core.CreatedObjectTree]{}, false, err
		}
		initializerMessage = initializer.Message
		needsCode = needsCode || initializerNeedsCode
		patcher.Merge(initializer.Patcher)
	}

	namedAttrs, namedAttrsNeedCode, err := encodeNamedAttrs(rd.attrs, path, options)
	if err != nil {
		return model_core.PatchedMessage[*model_starlark_pb.Rule_Definition, model_core.CreatedObjectTree]{}, false, err
	}
	needsCode = needsCode || namedAttrsNeedCode
	patcher.Merge(namedAttrs.Patcher)

	cfgTransitionIdentifier := ""
	if rd.cfg != nil {
		cfgTransitionIdentifier, err = rd.cfg.GetUserDefinedTransitionIdentifier()
		if err != nil {
			return model_core.PatchedMessage[*model_starlark_pb.Rule_Definition, model_core.CreatedObjectTree]{}, false, err
		}
	}

	subruleIdentifiers := make([]string, 0, len(rd.subrules))
	for i, subrule := range rd.subrules {
		if subrule.Identifier == nil {
			return model_core.PatchedMessage[*model_starlark_pb.Rule_Definition, model_core.CreatedObjectTree]{}, false, fmt.Errorf("subrule at index %d does not have an identifier", i)
		}
		subruleIdentifiers = append(subruleIdentifiers, subrule.Identifier.String())
	}
	sort.Strings(subruleIdentifiers)

	return model_core.NewPatchedMessage(
		&model_starlark_pb.Rule_Definition{
			Attrs:                   namedAttrs.Message,
			BuildSetting:            buildSetting,
			CfgTransitionIdentifier: cfgTransitionIdentifier,
			ExecGroups:              execGroups,
			Implementation:          implementation.Message,
			Initializer:             initializerMessage,
			Test:                    rd.test,
			SubruleIdentifiers:      slices.Compact(subruleIdentifiers),
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

func (rd *starlarkRuleDefinition) GetTest(thread *starlark.Thread) (bool, error) {
	return rd.test, nil
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

func (rd *protoRuleDefinition) Encode(path map[starlark.Value]struct{}, options *ValueEncodingOptions) (model_core.PatchedMessage[*model_starlark_pb.Rule_Definition, model_core.CreatedObjectTree], bool, error) {
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
			model_core.NewNestedMessage(rd.message, rd.message.Message.Initializer),
		),
	)
	return &f, nil
}

func (rd *protoRuleDefinition) GetTest(thread *starlark.Thread) (bool, error) {
	return rd.message.Message.Test, nil
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

func (rd *reloadingRuleDefinition) Encode(path map[starlark.Value]struct{}, options *ValueEncodingOptions) (model_core.PatchedMessage[*model_starlark_pb.Rule_Definition, model_core.CreatedObjectTree], bool, error) {
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

	base := NewProtoRuleDefinition(model_core.NewNestedMessage(value, ruleKind.Definition))
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

func (rd *reloadingRuleDefinition) GetTest(thread *starlark.Thread) (bool, error) {
	base, err := rd.getBase(thread)
	if err != nil {
		return false, err
	}
	return base.GetTest(thread)
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

func (bogusValue) Hash(thread *starlark.Thread) (uint32, error) {
	return 0, errors.New("bogus_value cannot be hashed")
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
