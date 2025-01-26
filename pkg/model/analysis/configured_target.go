package analysis

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	"sort"
	"strings"

	"github.com/buildbarn/bb-playground/pkg/evaluation"
	"github.com/buildbarn/bb-playground/pkg/label"
	model_core "github.com/buildbarn/bb-playground/pkg/model/core"
	model_starlark "github.com/buildbarn/bb-playground/pkg/model/starlark"
	model_analysis_pb "github.com/buildbarn/bb-playground/pkg/proto/model/analysis"
	model_starlark_pb "github.com/buildbarn/bb-playground/pkg/proto/model/starlark"
	"github.com/buildbarn/bb-playground/pkg/starlark/unpack"
	"github.com/buildbarn/bb-playground/pkg/storage/dag"

	"google.golang.org/protobuf/encoding/protojson"

	"go.starlark.net/starlark"
	"go.starlark.net/starlarkstruct"
	"go.starlark.net/syntax"
)

var (
	constraintValueInfoProviderIdentifier = label.MustNewCanonicalStarlarkIdentifier("@@builtins_core+//:exports.bzl%ConstraintValueInfo")
	defaultInfoInfoProviderIdentifier     = label.MustNewCanonicalStarlarkIdentifier("@@builtins_core+//:exports.bzl%DefaultInfo")
)

type constraintValuesToConstraintsEnvironment interface {
	GetConfiguredTargetValue(*model_analysis_pb.ConfiguredTarget_Key) model_core.Message[*model_analysis_pb.ConfiguredTarget_Value]
	GetVisibleTargetValue(*model_analysis_pb.VisibleTarget_Key) model_core.Message[*model_analysis_pb.VisibleTarget_Value]
}

// constraintValuesToConstraints converts a list of labels of constraint
// values to a list of Constraint messages that include both the
// constraint setting and constraint value labels. These can be used to
// perform matching of constraints.
func (c *baseComputer) constraintValuesToConstraints(e constraintValuesToConstraintsEnvironment, fromPackage label.CanonicalPackage, constraintValues []string) ([]*model_analysis_pb.Constraint, error) {
	constraints := make(map[string]string, len(constraintValues))
	missingDependencies := false
	for _, constraintValue := range constraintValues {
		visibleTarget := e.GetVisibleTargetValue(&model_analysis_pb.VisibleTarget_Key{
			FromPackage: fromPackage.String(),
			ToLabel:     constraintValue,
		})
		if !visibleTarget.IsSet() {
			missingDependencies = true
			continue
		}

		constrainValueInfoProvider, err := getProviderFromConfiguredTarget(e, visibleTarget.Message.Label, constraintValueInfoProviderIdentifier)
		if err != nil {
			if errors.Is(err, evaluation.ErrMissingDependency) {
				missingDependencies = true
				continue
			}
			return nil, err
		}

		var actualConstraintSetting, actualConstraintValue, defaultConstraintValue *string
		for _, field := range constrainValueInfoProvider.Message {
			switch field.Name {
			case "constraint":
				constraintSettingInfoProvider, ok := field.Value.GetKind().(*model_starlark_pb.Value_Struct)
				if !ok {
					return nil, fmt.Errorf("field \"constraint\" of ConstraintValueInfo provider of target %#v is not a struct")
				}
				for _, field := range constraintSettingInfoProvider.Struct.Fields {
					switch field.Name {
					case "default_constraint_value":
						switch v := field.Value.GetKind().(type) {
						case *model_starlark_pb.Value_Label:
							defaultConstraintValue = &v.Label
						case *model_starlark_pb.Value_None:
						default:
							return nil, fmt.Errorf("field \"constraint.default_constraint_value\" of ConstraintValueInfo provider of target %#v is not a Label or None")
						}
					case "label":
						v, ok := field.Value.GetKind().(*model_starlark_pb.Value_Label)
						if !ok {
							return nil, fmt.Errorf("field \"constraint.label\" of ConstraintValueInfo provider of target %#v is not a Label")
						}
						actualConstraintSetting = &v.Label
					}
				}
			case "label":
				v, ok := field.Value.GetKind().(*model_starlark_pb.Value_Label)
				if !ok {
					return nil, fmt.Errorf("field \"label\" of ConstraintValueInfo provider of target %#v is not a Label")
				}
				actualConstraintValue = &v.Label
			}
		}
		if actualConstraintSetting == nil {
			return nil, fmt.Errorf("ConstraintValueInfo provider of target %#v does not contain field \"constraint.label\"")
		}
		if actualConstraintValue == nil {
			return nil, fmt.Errorf("ConstraintValueInfo provider of target %#v does not contain field \"label\"")
		}
		effectiveConstraintValue := *actualConstraintValue
		if defaultConstraintValue != nil && effectiveConstraintValue == *defaultConstraintValue {
			effectiveConstraintValue = ""
		}

		if _, ok := constraints[*actualConstraintSetting]; ok {
			return nil, fmt.Errorf("got multiple constraint values for constraint setting %#v", *actualConstraintSetting)
		}
		constraints[*actualConstraintSetting] = effectiveConstraintValue

	}
	if missingDependencies {
		return nil, evaluation.ErrMissingDependency
	}

	sortedConstraints := make([]*model_analysis_pb.Constraint, 0, len(constraints))
	for _, constraintSetting := range slices.Sorted(maps.Keys(constraints)) {
		sortedConstraints = append(
			sortedConstraints,
			&model_analysis_pb.Constraint{
				Setting: constraintSetting,
				Value:   constraints[constraintSetting],
			},
		)
	}
	return sortedConstraints, nil
}

func (c *baseComputer) ComputeConfiguredTargetValue(ctx context.Context, key *model_analysis_pb.ConfiguredTarget_Key, e ConfiguredTargetEnvironment) (PatchedConfiguredTargetValue, error) {
	targetLabel, err := label.NewCanonicalLabel(key.Label)
	if err != nil {
		return PatchedConfiguredTargetValue{}, fmt.Errorf("invalid target label: %w", err)
	}
	targetValue := e.GetTargetValue(&model_analysis_pb.Target_Key{
		Label: targetLabel.String(),
	})
	if !targetValue.IsSet() {
		return PatchedConfiguredTargetValue{}, evaluation.ErrMissingDependency
	}

	switch targetKind := targetValue.Message.Definition.GetKind().(type) {
	case *model_starlark_pb.Target_Definition_RuleTarget:
		ruleTarget := targetKind.RuleTarget

		allBuiltinsModulesNames := e.GetBuiltinsModuleNamesValue(&model_analysis_pb.BuiltinsModuleNames_Key{})
		ruleValue := e.GetCompiledBzlFileGlobalValue(&model_analysis_pb.CompiledBzlFileGlobal_Key{
			Identifier: ruleTarget.RuleIdentifier,
		})
		if !allBuiltinsModulesNames.IsSet() || !ruleValue.IsSet() {
			return PatchedConfiguredTargetValue{}, evaluation.ErrMissingDependency
		}
		v, ok := ruleValue.Message.Global.GetKind().(*model_starlark_pb.Value_Rule)
		if !ok {
			return PatchedConfiguredTargetValue{}, fmt.Errorf("%#v is not a rule", ruleTarget.RuleIdentifier)
		}
		d, ok := v.Rule.Kind.(*model_starlark_pb.Rule_Definition_)
		if !ok {
			return PatchedConfiguredTargetValue{}, fmt.Errorf("%#v is not a rule definition", ruleTarget.RuleIdentifier)
		}
		ruleDefinition := model_core.Message[*model_starlark_pb.Rule_Definition]{
			Message:            d.Definition,
			OutgoingReferences: ruleValue.OutgoingReferences,
		}

		thread := c.newStarlarkThread(ctx, e, allBuiltinsModulesNames.Message.BuiltinsModuleNames)
		returnValue, err := starlark.Call(
			thread,
			model_starlark.NewNamedFunction(
				model_starlark.NewProtoNamedFunctionDefinition(
					model_core.Message[*model_starlark_pb.Function]{
						Message:            ruleDefinition.Message.Implementation,
						OutgoingReferences: ruleDefinition.OutgoingReferences,
					},
				),
			),
			/* args = */ starlark.Tuple{
				&ruleContext{
					computer:       c,
					context:        ctx,
					environment:    e,
					thread:         thread,
					targetLabel:    targetLabel,
					ruleDefinition: ruleDefinition,
					ruleTarget: model_core.Message[*model_starlark_pb.RuleTarget]{
						Message:            ruleTarget,
						OutgoingReferences: targetValue.OutgoingReferences,
					},
					attrs:       make([]starlark.Value, len(ruleDefinition.Message.Attrs)),
					singleFiles: make([]starlark.Value, len(ruleDefinition.Message.Attrs)),
					execGroups:  make([]*ruleContextExecGroupState, len(ruleDefinition.Message.ExecGroups)),
				},
			},
			/* kwargs = */ nil,
		)
		if err != nil {
			if !errors.Is(err, evaluation.ErrMissingDependency) {
				var evalErr *starlark.EvalError
				if errors.As(err, &evalErr) {
					return PatchedConfiguredTargetValue{}, errors.New(evalErr.Backtrace())
				}
			}
			return PatchedConfiguredTargetValue{}, err
		}

		var providerInstances []model_starlark.ProviderInstance
		if err := unpack.IfNotNone(
			unpack.List(unpack.Type[model_starlark.ProviderInstance]("struct")),
		).UnpackInto(thread, returnValue, &providerInstances); err != nil {
			return PatchedConfiguredTargetValue{}, err
		}

		encodedProviderInstances := map[string]model_core.PatchedMessage[*model_starlark_pb.Struct, dag.ObjectContentsWalker]{}
		for _, providerInstance := range providerInstances {
			v, _, err := providerInstance.EncodeStruct(map[starlark.Value]struct{}{}, c.getValueEncodingOptions(targetLabel))
			if err != nil {
				return PatchedConfiguredTargetValue{}, err
			}
			providerIdentifier := v.Message.ProviderIdentifier
			if _, ok := encodedProviderInstances[providerIdentifier]; ok {
				return PatchedConfiguredTargetValue{}, fmt.Errorf("implementation function returned multiple structs for provider %#v", providerIdentifier)
			}
			encodedProviderInstances[v.Message.ProviderIdentifier] = v
		}

		patcher := model_core.NewReferenceMessagePatcher[dag.ObjectContentsWalker]()
		sortedProviderInstances := make([]*model_starlark_pb.Struct, 0, len(encodedProviderInstances))
		for _, providerIdentifier := range slices.Sorted(maps.Keys(encodedProviderInstances)) {
			providerInstance := encodedProviderInstances[providerIdentifier]
			sortedProviderInstances = append(sortedProviderInstances, providerInstance.Message)
			patcher.Merge(providerInstance.Patcher)
		}

		return model_core.NewPatchedMessage(
			&model_analysis_pb.ConfiguredTarget_Value{
				ProviderInstances: sortedProviderInstances,
			},
			patcher,
		), nil
	case *model_starlark_pb.Target_Definition_SourceFileTarget:
		return model_core.NewSimplePatchedMessage[dag.ObjectContentsWalker](
			&model_analysis_pb.ConfiguredTarget_Value{
				// TODO: Inject DefaultInfo.
			},
		), nil
	default:
		return PatchedConfiguredTargetValue{}, errors.New("only source file targets and rule targets can be configured")
	}
}

type ruleContext struct {
	computer    *baseComputer
	context     context.Context
	environment ConfiguredTargetEnvironment
	// TODO: This field should not exist. Unfortunately, methods
	// like the ones provided by starlark.Mapping do not take a
	// thread.
	thread         *starlark.Thread
	targetLabel    label.CanonicalLabel
	ruleDefinition model_core.Message[*model_starlark_pb.Rule_Definition]
	ruleTarget     model_core.Message[*model_starlark_pb.RuleTarget]
	attrs          []starlark.Value
	singleFiles    []starlark.Value
	execGroups     []*ruleContextExecGroupState
}

var _ starlark.HasAttrs = (*ruleContext)(nil)

func (rc *ruleContext) String() string {
	return fmt.Sprintf("<ctx for %s>", rc.targetLabel.String())
}

func (ruleContext) Type() string {
	return "ctx"
}

func (ruleContext) Freeze() {
}

func (ruleContext) Truth() starlark.Bool {
	return starlark.True
}

func (ruleContext) Hash() (uint32, error) {
	return 0, nil
}

func (rc *ruleContext) Attr(name string) (starlark.Value, error) {
	switch name {
	case "attr":
		return &ruleContextAttr{
			ruleContext: rc,
		}, nil
	case "executable":
		return &ruleContextExecutable{
			ruleContext: rc,
		}, nil
	case "file":
		return &ruleContextFile{
			ruleContext: rc,
		}, nil
	case "label":
		return model_starlark.NewLabel(rc.targetLabel), nil
	case "runfiles":
		return starlark.NewBuiltin("ctx.runfiles", rc.doRunfiles), nil
	case "toolchains":
		execGroups := rc.ruleDefinition.Message.ExecGroups
		execGroupIndex, ok := sort.Find(
			len(execGroups),
			func(i int) int { return strings.Compare("", execGroups[i].Name) },
		)
		if !ok {
			return nil, errors.New("rule does not have a default exec group")
		}
		return &toolchainContext{
			ruleContext:    rc,
			execGroupIndex: execGroupIndex,
		}, nil
	}
	return nil, nil
}

func (ruleContext) AttrNames() []string {
	return []string{
		"attr",
		"label",
		"runfiles",
		"toolchains",
	}
}

func (ruleContext) doRunfiles(thread *starlark.Thread, b *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
	var files []starlark.Value
	var transitiveFiles *model_starlark.Depset
	var symlinks starlark.Value
	var rootSymlinks starlark.Value
	if err := starlark.UnpackArgs(
		b.Name(), args, kwargs,
		"files?", unpack.Bind(thread, &files, unpack.List(unpack.Any)),
		"transitive_files?", unpack.Bind(thread, &transitiveFiles, unpack.IfNotNone(unpack.Type[*model_starlark.Depset]("depset"))),
		"symlinks?", &symlinks,
		"root_symlinks?", &rootSymlinks,
	); err != nil {
		return nil, err
	}
	return model_starlark.NewRunfiles(), nil
}

func (rc *ruleContext) getAttrValueParts(namedAttr *model_starlark_pb.NamedAttr) (model_core.Message[[]*model_starlark_pb.Value], error) {
	ruleTargetAttrValues := rc.ruleTarget.Message.AttrValues
	if ruleTargetIndex, ok := sort.Find(
		len(ruleTargetAttrValues),
		func(i int) int { return strings.Compare(namedAttr.Name, ruleTargetAttrValues[i].Name) },
	); ok {
		// Rule target provides an explicit value for this
		// attribute. Process any select() statements in the
		// value parts.
		selectGroups := ruleTargetAttrValues[ruleTargetIndex].ValueParts
		valueParts := make([]*model_starlark_pb.Value, 0, len(selectGroups))
		for _, selectGroup := range selectGroups {
			valuePart, err := getValueFromSelectGroup(rc.environment, selectGroup, false)
			if err != nil {
				return model_core.Message[[]*model_starlark_pb.Value]{}, err
			}
			valueParts = append(valueParts, valuePart)
		}
		return model_core.Message[[]*model_starlark_pb.Value]{
			Message:            valueParts,
			OutgoingReferences: rc.ruleTarget.OutgoingReferences,
		}, nil
	}

	// No value provided. Use the default value from
	// the rule definition.
	return model_core.Message[[]*model_starlark_pb.Value]{
		Message:            []*model_starlark_pb.Value{namedAttr.Attr.GetDefault()},
		OutgoingReferences: rc.ruleDefinition.OutgoingReferences,
	}, nil
}

type ruleContextAttr struct {
	ruleContext *ruleContext
}

var _ starlark.HasAttrs = (*ruleContextAttr)(nil)

func (ruleContextAttr) String() string {
	return "<ctx.attr>"
}

func (ruleContextAttr) Type() string {
	return "ctx.attr"
}

func (ruleContextAttr) Freeze() {
}

func (ruleContextAttr) Truth() starlark.Bool {
	return starlark.True
}

func (ruleContextAttr) Hash() (uint32, error) {
	return 0, nil
}

func (rca *ruleContextAttr) Attr(name string) (starlark.Value, error) {
	rc := rca.ruleContext
	ruleDefinitionAttrs := rc.ruleDefinition.Message.Attrs
	ruleDefinitionAttrIndex, ok := sort.Find(
		len(ruleDefinitionAttrs),
		func(i int) int { return strings.Compare(name, ruleDefinitionAttrs[i].Name) },
	)
	if !ok {
		return nil, fmt.Errorf("rule does not have an attr named %#v", name)
	}
	attr := rc.attrs[ruleDefinitionAttrIndex]
	if attr == nil {
		// Decode the values of the parts of the attribute.
		valueParts, err := rc.getAttrValueParts(ruleDefinitionAttrs[ruleDefinitionAttrIndex])
		if err != nil {
			return nil, err
		}
		decodedParts := make([]starlark.Value, 0, len(valueParts.Message))
		for _, valuePart := range valueParts.Message {
			decodedPart, err := model_starlark.DecodeValue(
				model_core.Message[*model_starlark_pb.Value]{
					Message:            valuePart,
					OutgoingReferences: valueParts.OutgoingReferences,
				},
				/* currentIdentifier = */ nil,
				rc.computer.getValueDecodingOptions(rc.context, func(canonicalLabel label.CanonicalLabel) (starlark.Value, error) {
					return &targetReference{
						ruleContext:   rc,
						originalLabel: canonicalLabel,
					}, nil
				}),
			)
			if err != nil {
				return nil, err
			}
			decodedParts = append(decodedParts, decodedPart)
		}

		// Combine the values of the parts into a single value.
		if len(decodedParts) == 0 {
			return nil, errors.New("attr value does not have any parts")
		}
		attr = decodedParts[0]
		concatenationOperator := syntax.PLUS
		if _, ok := attr.(*starlark.Dict); ok {
			concatenationOperator = syntax.PIPE
		}
		for _, decodedPart := range decodedParts[1:] {
			var err error
			attr, err = starlark.Binary(concatenationOperator, attr, decodedPart)
			if err != nil {
				return nil, err
			}
		}

		// Cache attr value for subsequent lookups.
		attr.Freeze()
		rc.attrs[ruleDefinitionAttrIndex] = attr
	}
	return attr, nil
}

func (rca *ruleContextAttr) AttrNames() []string {
	attrs := rca.ruleContext.ruleDefinition.Message.Attrs
	attrNames := make([]string, 0, len(attrs))
	for _, attr := range attrs {
		attrNames = append(attrNames, attr.Name)
	}
	return attrNames
}

type ruleContextExecutable struct {
	ruleContext *ruleContext
}

var _ starlark.HasAttrs = (*ruleContextExecutable)(nil)

func (ruleContextExecutable) String() string {
	return "<ctx.executable>"
}

func (ruleContextExecutable) Type() string {
	return "ctx.executable"
}

func (ruleContextExecutable) Freeze() {
}

func (ruleContextExecutable) Truth() starlark.Bool {
	return starlark.True
}

func (ruleContextExecutable) Hash() (uint32, error) {
	return 0, nil
}

func (rce *ruleContextExecutable) Attr(name string) (starlark.Value, error) {
	rc := rce.ruleContext
	ruleDefinitionAttrs := rc.ruleDefinition.Message.Attrs
	_, ok := sort.Find(
		len(ruleDefinitionAttrs),
		func(i int) int { return strings.Compare(name, ruleDefinitionAttrs[i].Name) },
	)
	if !ok {
		return nil, fmt.Errorf("rule does not have an attr named %#v", name)
	}
	return nil, fmt.Errorf("TODO: get executable %#v", name)
}

func (ruleContextExecutable) AttrNames() []string {
	panic("TODO")
}

type ruleContextFile struct {
	ruleContext *ruleContext
}

var _ starlark.HasAttrs = (*ruleContextFile)(nil)

func (ruleContextFile) String() string {
	return "<ctx.file>"
}

func (ruleContextFile) Type() string {
	return "ctx.file"
}

func (ruleContextFile) Freeze() {
}

func (ruleContextFile) Truth() starlark.Bool {
	return starlark.True
}

func (ruleContextFile) Hash() (uint32, error) {
	return 0, nil
}

func (rcf *ruleContextFile) Attr(name string) (starlark.Value, error) {
	rc := rcf.ruleContext
	ruleDefinitionAttrs := rc.ruleDefinition.Message.Attrs
	ruleDefinitionAttrIndex, ok := sort.Find(
		len(ruleDefinitionAttrs),
		func(i int) int { return strings.Compare(name, ruleDefinitionAttrs[i].Name) },
	)
	if !ok {
		return nil, fmt.Errorf("rule does not have an attr named %#v", name)
	}

	file := rc.singleFiles[ruleDefinitionAttrIndex]
	if file == nil {
		labelType, ok := ruleDefinitionAttrs[ruleDefinitionAttrIndex].Attr.GetType().(*model_starlark_pb.Attr_Label)
		if !ok {
			return nil, fmt.Errorf("attr %#v is not of type of label", name)
		}
		if !labelType.Label.AllowSingleFile {
			return nil, fmt.Errorf("attr %#v does not have allow_single_file set", name)
		}

		// Decode the values of the parts of the attribute.
		valueParts, err := rc.getAttrValueParts(ruleDefinitionAttrs[ruleDefinitionAttrIndex])
		if err != nil {
			return nil, err
		}
		if len(valueParts.Message) != 1 {
			return nil, errors.New("labels cannot consist of multiple value parts")
		}

		// TODO: Get configured target corresponding to label
		// and extract file from DefaultInfo provider.
		file = starlark.None

		// Cache attr value for subsequent lookups.
		file.Freeze()
		rc.singleFiles[ruleDefinitionAttrIndex] = file
	}
	return file, nil
}

func (rcf *ruleContextFile) AttrNames() []string {
	var attrNames []string
	for _, namedAttr := range rcf.ruleContext.ruleDefinition.Message.Attrs {
		if labelType, ok := namedAttr.Attr.GetType().(*model_starlark_pb.Attr_Label); ok && labelType.Label.AllowSingleFile {
			attrNames = append(attrNames, namedAttr.Name)
		}
	}
	return attrNames
}

type targetReference struct {
	ruleContext   *ruleContext
	originalLabel label.CanonicalLabel
	visibleLabel  *label.CanonicalLabel

	configuredTarget model_core.Message[*model_analysis_pb.ConfiguredTarget_Value]
	providerValues   []*starlarkstruct.Struct
}

var (
	_ starlark.HasAttrs = (*targetReference)(nil)
	_ starlark.Mapping  = (*targetReference)(nil)
)

func (tr *targetReference) getVisibleLabel() (label.CanonicalLabel, error) {
	if tr.visibleLabel != nil {
		return *tr.visibleLabel, nil
	}

	rc := tr.ruleContext
	visibleTargetValue := rc.environment.GetVisibleTargetValue(&model_analysis_pb.VisibleTarget_Key{
		FromPackage: rc.targetLabel.GetCanonicalPackage().String(),
		ToLabel:     tr.originalLabel.String(),
	})
	if !visibleTargetValue.IsSet() {
		var bad label.CanonicalLabel
		return bad, evaluation.ErrMissingDependency
	}
	visibleLabel, err := label.NewCanonicalLabel(visibleTargetValue.Message.Label)
	if err != nil {
		var bad label.CanonicalLabel
		return bad, err
	}
	tr.visibleLabel = &visibleLabel
	return visibleLabel, nil
}

func (tr *targetReference) String() string {
	return fmt.Sprintf("<target %s>", tr.originalLabel)
}

func (targetReference) Type() string {
	return "Target"
}

func (targetReference) Freeze() {
}

func (targetReference) Truth() starlark.Bool {
	return starlark.True
}

func (targetReference) Hash() (uint32, error) {
	return 0, nil
}

func (tr *targetReference) Attr(name string) (starlark.Value, error) {
	switch name {
	case "label":
		visibleLabel, err := tr.getVisibleLabel()
		if err != nil {
			return nil, err
		}
		return model_starlark.NewLabel(visibleLabel), nil
	case "original_label":
		return model_starlark.NewLabel(tr.originalLabel), nil
	case "files", "files_to_run", "data_runfiles", "default_runfiles":
		// Fields provided by DefaultInfo can be accessed directly.
		defaultInfoProviderValue, err := tr.getProviderValue(defaultInfoInfoProviderIdentifier)
		if err != nil {
			return nil, err
		}
		return defaultInfoProviderValue.Attr(name)
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
	"original_label",
}

func (tr *targetReference) AttrNames() []string {
	return targetReferenceAttrNames
}

func (tr *targetReference) getProviderValue(providerIdentifier label.CanonicalStarlarkIdentifier) (*starlarkstruct.Struct, error) {
	visibleLabel, err := tr.getVisibleLabel()
	if err != nil {
		return nil, err
	}

	// Configure the target if it's not been configured yet.
	rc := tr.ruleContext
	if !tr.configuredTarget.IsSet() {
		configuredTarget := rc.environment.GetConfiguredTargetValue(&model_analysis_pb.ConfiguredTarget_Key{
			Label: visibleLabel.String(),
		})
		if !configuredTarget.IsSet() {
			return nil, evaluation.ErrMissingDependency
		}
		tr.configuredTarget = configuredTarget
		tr.providerValues = make([]*starlarkstruct.Struct, len(configuredTarget.Message.ProviderInstances))
	}

	// Look up the provider.
	providerIdentifierStr := providerIdentifier.String()
	providerInstances := tr.configuredTarget.Message.ProviderInstances
	providerIndex, ok := sort.Find(
		len(providerInstances),
		func(i int) int {
			return strings.Compare(providerIdentifierStr, providerInstances[i].ProviderIdentifier)
		},
	)
	if !ok {
		return nil, fmt.Errorf("target %#v did not yield provider %#v", visibleLabel.String(), providerIdentifierStr)
	}

	// Decode the provider value if not accessed before.
	if providerValue := tr.providerValues[providerIndex]; providerValue != nil {
		return providerValue, nil
	}
	providerValue, err := model_starlark.DecodeStruct(
		model_core.Message[*model_starlark_pb.Struct]{
			Message:            providerInstances[providerIndex],
			OutgoingReferences: tr.configuredTarget.OutgoingReferences,
		},
		rc.computer.getValueDecodingOptions(rc.context, func(canonicalLabel label.CanonicalLabel) (starlark.Value, error) {
			return model_starlark.NewLabel(canonicalLabel), nil
		}),
	)
	if err != nil {
		return nil, err
	}
	tr.providerValues[providerIndex] = providerValue
	return providerValue, nil
}

func (tr *targetReference) Get(v starlark.Value) (starlark.Value, bool, error) {
	provider, ok := v.(*model_starlark.Provider)
	if !ok {
		return nil, false, errors.New("keys have to be of type provider")
	}
	providerIdentifier := provider.Identifier
	if providerIdentifier == nil {
		return nil, false, errors.New("provider does not have a name")
	}
	providerValue, err := tr.getProviderValue(*providerIdentifier)
	if err != nil {
		return nil, false, err
	}
	return model_starlark.NewProviderInstance(providerValue, *providerIdentifier), true, nil
}

type toolchainContext struct {
	ruleContext    *ruleContext
	execGroupIndex int
}

var _ starlark.Mapping = (*toolchainContext)(nil)

func (toolchainContext) String() string {
	return "<toolchain context>"
}

func (toolchainContext) Type() string {
	return "ToolchainContext"
}

func (toolchainContext) Freeze() {
}

func (toolchainContext) Truth() starlark.Bool {
	return starlark.True
}

func (toolchainContext) Hash() (uint32, error) {
	return 0, nil
}

func (tc *toolchainContext) Get(v starlark.Value) (starlark.Value, bool, error) {
	rc := tc.ruleContext
	thread := rc.thread
	labelUnpackerInto := unpack.Stringer(model_starlark.NewLabelOrStringUnpackerInto(model_starlark.CurrentFilePackage(thread, 0)))
	var toolchainType string
	if err := labelUnpackerInto.UnpackInto(thread, v, &toolchainType); err != nil {
		return nil, false, err
	}

	namedExecGroup := rc.ruleDefinition.Message.ExecGroups[tc.execGroupIndex]
	execGroupDefinition := namedExecGroup.ExecGroup
	if execGroupDefinition == nil {
		return nil, false, errors.New("rule definition lacks exec group definition")
	}

	toolchains := execGroupDefinition.Toolchains
	toolchainIndex, ok := sort.Find(
		len(toolchains),
		func(i int) int { return strings.Compare(toolchainType, toolchains[i].ToolchainType) },
	)
	if !ok {
		return nil, false, fmt.Errorf("exec group %#v does not depend on toolchain type %#v", namedExecGroup.Name, toolchainType)
	}

	if rc.execGroups[tc.execGroupIndex] == nil {
		execCompatibleWith, err := rc.computer.constraintValuesToConstraints(
			rc.environment,
			rc.targetLabel.GetCanonicalPackage(),
			execGroupDefinition.ExecCompatibleWith,
		)
		if err != nil {
			return nil, false, evaluation.ErrMissingDependency
		}
		resolvedToolchains := rc.environment.GetResolvedToolchainsValue(&model_analysis_pb.ResolvedToolchains_Key{
			ExecCompatibleWith: execCompatibleWith,
			// TODO: Fill this in properly!
			TargetConstraints: []*model_analysis_pb.Constraint{
				{
					Setting: "@@platforms+//cpu",
					Value:   "@@platforms+//cpu:aarch64",
				},
				{
					Setting: "@@platforms+//os",
					Value:   "@@platforms+//os:osx",
				},
			},
			Toolchains: execGroupDefinition.Toolchains,
		})
		if !resolvedToolchains.IsSet() {
			return nil, false, evaluation.ErrMissingDependency
		}
		return nil, false, errors.New("GOT RESOLVED TOOLCHAINS: " + protojson.Format(resolvedToolchains.Message))
	}

	return nil, false, fmt.Errorf("TOOLCHAIN INDEX %d", toolchainIndex)
}

type ruleContextExecGroupState struct{}

type getProviderFromConfiguredTargetEnvironment interface {
	GetConfiguredTargetValue(key *model_analysis_pb.ConfiguredTarget_Key) model_core.Message[*model_analysis_pb.ConfiguredTarget_Value]
}

// getProviderFromConfiguredTarget looks up a single provider that is
// provided by a configured target
func getProviderFromConfiguredTarget(e getProviderFromConfiguredTargetEnvironment, targetLabel string, providerIdentifier label.CanonicalStarlarkIdentifier) (model_core.Message[[]*model_starlark_pb.NamedValue], error) {
	configuredTargetValue := e.GetConfiguredTargetValue(&model_analysis_pb.ConfiguredTarget_Key{
		Label: targetLabel,
	})
	if !configuredTargetValue.IsSet() {
		return model_core.Message[[]*model_starlark_pb.NamedValue]{}, evaluation.ErrMissingDependency
	}

	providerIdentifierStr := providerIdentifier.String()
	providerInstances := configuredTargetValue.Message.ProviderInstances
	if providerIndex, ok := sort.Find(
		len(providerInstances),
		func(i int) int {
			return strings.Compare(providerIdentifierStr, providerInstances[i].ProviderIdentifier)
		},
	); ok {
		return model_core.Message[[]*model_starlark_pb.NamedValue]{
			Message:            providerInstances[providerIndex].Fields,
			OutgoingReferences: configuredTargetValue.OutgoingReferences,
		}, nil
	}
	return model_core.Message[[]*model_starlark_pb.NamedValue]{}, fmt.Errorf("target did not yield provider %#v", providerIdentifierStr)
}
