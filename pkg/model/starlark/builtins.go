package starlark

import (
	"errors"
	"fmt"
	"maps"
	"slices"
	"sort"
	"unicode/utf8"

	pg_label "github.com/buildbarn/bonanza/pkg/label"
	model_core "github.com/buildbarn/bonanza/pkg/model/core"
	model_starlark_pb "github.com/buildbarn/bonanza/pkg/proto/model/starlark"
	"github.com/buildbarn/bonanza/pkg/starlark/unpack"
	"github.com/buildbarn/bonanza/pkg/storage/object"

	"go.starlark.net/lib/json"
	"go.starlark.net/starlark"
	"go.starlark.net/syntax"
)

var providersListUnpackerInto = unpack.Or([]unpack.UnpackerInto[[][]*Provider]{
	unpack.Singleton(unpack.List(unpack.Type[*Provider]("provider"))),
	unpack.List(unpack.List(unpack.Type[*Provider]("provider"))),
})

type allowFilesBoolUnpackerInto struct{}

func (allowFilesBoolUnpackerInto) UnpackInto(thread *starlark.Thread, v starlark.Value, dst *[]string) error {
	var allowFiles bool
	if err := unpack.Bool.UnpackInto(thread, v, &allowFiles); err != nil {
		return err
	}
	if allowFiles {
		*dst = []string{""}
	} else {
		*dst = nil
	}
	return nil
}

func (allowFilesBoolUnpackerInto) Canonicalize(thread *starlark.Thread, v starlark.Value) (starlark.Value, error) {
	var allowFiles bool
	if err := unpack.Bool.UnpackInto(thread, v, &allowFiles); err != nil {
		return nil, err
	}
	if allowFiles {
		return starlark.NewList([]starlark.Value{starlark.String("")}), nil
	}
	return starlark.NewList(nil), nil
}

func (allowFilesBoolUnpackerInto) GetConcatenationOperator() syntax.Token {
	return syntax.PLUS
}

// allowFilesUnpackerInto can be used to unpack allow_files arguments,
// which either take a list of permitted file extensions or a Boolean
// value. When the Boolean value is set to true, all file extensions are
// accepted.
var allowFilesUnpackerInto = unpack.IfNotNone(unpack.Or([]unpack.UnpackerInto[[]string]{
	allowFilesBoolUnpackerInto{},
	unpack.List(unpack.String),
}))

const (
	CanonicalPackageKey = "canonical_package"
	CurrentCtxKey       = "current_ctx"
	GlobExpanderKey     = "glob_expander"
)

type GlobExpander = func(include, exclude []string, includeDirectories bool) ([]pg_label.TargetName, error)

// sortAndDeduplicateSuffixes sorts the strings in a given list,
// lexicographically comparing characters in reverse order. Any entries
// that have another entry as its suffix are also dropped.
//
// This function can be used to normalize a list of file extensions that
// should be matched. By sorting the strings in reverse order, it is
// possible to do efficient lookups.
func sortAndDeduplicateSuffixes(list []string) []string {
	slices.SortFunc(
		list,
		func(s1, s2 string) int {
			for {
				r1, len1 := utf8.DecodeLastRuneInString(s1)
				r2, len2 := utf8.DecodeLastRuneInString(s2)
				switch {
				case len1 == 0 && len2 == 0:
					return 0
				case len1 == 0:
					return -1
				case len2 == 0:
					return 1
				case r1 < r2:
					return -1
				case r1 > r2:
					return 1
				}
				s1 = s1[:len(s1)-len1]
				s2 = s2[:len(s2)-len2]
			}
		},
	)
	return slices.CompactFunc(
		list,
		func(a, b string) bool {
			diff := len(b) - len(a)
			return diff >= 0 && a == b[diff:]
		},
	)
}

var commonBuiltins = starlark.StringDict{
	"Label": starlark.NewBuiltin(
		"Label",
		func(thread *starlark.Thread, b *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
			if len(args) != 1 {
				return nil, fmt.Errorf("%s: got %d positional arguments, want 1", b.Name(), len(args))
			}
			var input pg_label.ResolvedLabel
			if err := starlark.UnpackArgs(
				b.Name(), args, kwargs,
				"input", unpack.Bind(thread, &input, NewLabelOrStringUnpackerInto(CurrentFilePackage(thread, 1))),
			); err != nil {
				return nil, err
			}
			return NewLabel(input), nil
		},
	),
	"select": starlark.NewBuiltin(
		"select",
		func(thread *starlark.Thread, b *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
			if len(args) != 1 {
				return nil, fmt.Errorf("%s: got %d positional arguments, want 1", b.Name(), len(args))
			}

			var conditions map[pg_label.ResolvedLabel]starlark.Value
			noMatchError := ""
			if err := starlark.UnpackArgs(
				b.Name(), args, kwargs,
				"conditions", unpack.Bind(thread, &conditions, unpack.Dict(NewLabelOrStringUnpackerInto(CurrentFilePackage(thread, 1)), unpack.Any)),
				"no_match_error?", unpack.Bind(thread, &noMatchError, unpack.String),
			); err != nil {
				return nil, err
			}

			// Even though select() takes the default
			// condition as part of the dictionary, we store
			// the default value separately. Extract it.
			var defaultValue starlark.Value
			for label, value := range conditions {
				if label.GetPackagePath() == "conditions" && label.GetTargetName().String() == "default" {
					if defaultValue != nil {
						return nil, fmt.Errorf("%s: got multiple default conditions", b.Name())
					}
					delete(conditions, label)
					defaultValue = value
				}
			}

			return NewSelect(
				[]SelectGroup{NewSelectGroup(conditions, defaultValue, noMatchError)},
				/* concatenationOperator = */ 0,
			), nil
		},
	),
}

var BuildFileBuiltins = starlark.StringDict{
	"package": starlark.NewBuiltin(
		"package",
		func(thread *starlark.Thread, b *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
			targetRegistrar := thread.Local(TargetRegistrarKey).(*TargetRegistrar[object.LocalReference])
			if targetRegistrar.setDefaultInheritableAttrs {
				return nil, fmt.Errorf("%s: function can only be invoked once", b.Name())
			}
			if len(targetRegistrar.targets) > 0 {
				return nil, fmt.Errorf("%s: function can only be invoked before rule targets", b.Name())
			}

			newDefaultAttrs, err := getDefaultInheritableAttrs(
				thread,
				b,
				args,
				kwargs,
				targetRegistrar.defaultInheritableAttrs,
				targetRegistrar.inlinedTreeOptions,
			)
			if err != nil {
				return nil, err
			}

			// We are not interested in embedding the new
			// PatchedMessage into anything directly.
			// Instead, we want to be able to pull copies of
			// it when we encounter targets that don't have
			// an explicit visibility.
			newDefaultInheritableAttrs, metadata := newDefaultAttrs.SortAndSetReferences()
			targetRegistrar.defaultInheritableAttrs = newDefaultInheritableAttrs
			targetRegistrar.createDefaultInheritableAttrsMetadata = func(index int) model_core.CreatedObjectTree {
				return metadata[index]
			}
			targetRegistrar.setDefaultInheritableAttrs = true
			return starlark.None, nil
		},
	),
}

func labelSetting(thread *starlark.Thread, b *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple, flag bool) (starlark.Value, error) {
	targetRegistrar := thread.Local(TargetRegistrarKey).(*TargetRegistrar[object.LocalReference])
	if targetRegistrar == nil {
		return nil, errors.New("targets cannot be registered from within this context")
	}
	currentPackage := thread.Local(CanonicalPackageKey).(pg_label.CanonicalPackage)

	var name string
	var buildSettingDefault string
	var visibility []pg_label.ResolvedLabel
	labelOrStringUnpackerInto := NewLabelOrStringUnpackerInto(currentPackage)
	if err := starlark.UnpackArgs(
		b.Name(), args, kwargs,
		"name", unpack.Bind(thread, &name, unpack.Stringer(unpack.TargetName)),
		// Extension: allow build_setting_default to be set to
		// None to implement command line flags that don't point
		// to anything by default.
		"build_setting_default", unpack.Bind(thread, &buildSettingDefault, unpack.IfNotNone(unpack.Stringer(labelOrStringUnpackerInto))),
		"visibility?", unpack.Bind(thread, &visibility, unpack.IfNotNone(unpack.List(labelOrStringUnpackerInto))),
	); err != nil {
		return nil, err
	}

	visibilityPackageGroup, err := targetRegistrar.getVisibilityPackageGroup(visibility)
	if err != nil {
		return nil, err
	}

	return starlark.None, targetRegistrar.registerExplicitTarget(
		name,
		model_core.NewPatchedMessage(
			&model_starlark_pb.Target_Definition{
				Kind: &model_starlark_pb.Target_Definition_LabelSetting{
					LabelSetting: &model_starlark_pb.LabelSetting{
						BuildSettingDefault: buildSettingDefault,
						Flag:                flag,
						Visibility:          visibilityPackageGroup.Message,
					},
				},
			},
			visibilityPackageGroup.Patcher,
		),
	)
}

func stringDictToStructFields(in starlark.StringDict) map[string]any {
	out := make(map[string]any, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

var BzlFileBuiltins starlark.StringDict

func init() {
	BzlFileBuiltins = starlark.StringDict{
		"analysis_test_transition": starlark.NewBuiltin(
			"analysis_test_transition",
			func(thread *starlark.Thread, b *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
				var settings map[string]string
				if err := starlark.UnpackArgs(
					b.Name(), args, kwargs,
					"settings", unpack.Bind(thread, &settings, unpack.Dict(unpack.String, unpack.String)),
				); err != nil {
					return nil, err
				}
				return nil, errors.New("not implemented")
			},
		),
		"aspect": starlark.NewBuiltin(
			"aspect",
			func(thread *starlark.Thread, b *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
				if len(args) > 1 {
					return nil, fmt.Errorf("%s: got %d positional arguments, want at most 1", b.Name(), len(args))
				}
				var implementation NamedFunction
				var attrAspects []string
				attrs := map[pg_label.StarlarkIdentifier]*Attr{}
				doc := ""
				execGroups := map[string]*ExecGroup{}
				var fragments []string
				var provides []*Provider
				var requiredAspectProviders [][]*Provider
				var requiredProviders [][]*Provider
				var requires []*Aspect
				var toolchains []*ToolchainType
				if err := starlark.UnpackArgs(
					b.Name(), args, kwargs,
					// Positional arguments.
					"implementation", unpack.Bind(thread, &implementation, NamedFunctionUnpackerInto),
					// Keyword arguments.
					"attr_aspects?", unpack.Bind(thread, &attrAspects, unpack.List(unpack.String)),
					"attrs?", unpack.Bind(thread, &attrs, unpack.Dict(unpack.StarlarkIdentifier, unpack.Type[*Attr]("attr.*"))),
					"doc?", unpack.Bind(thread, &doc, unpack.String),
					"exec_groups?", unpack.Bind(thread, &execGroups, unpack.Dict(unpack.String, unpack.Type[*ExecGroup]("exec_group"))),
					"fragments?", unpack.Bind(thread, &fragments, unpack.List(unpack.String)),
					"provides?", unpack.Bind(thread, &provides, unpack.List(unpack.Type[*Provider]("provider"))),
					"required_aspect_providers?", unpack.Bind(thread, &requiredAspectProviders, providersListUnpackerInto),
					"required_providers?", unpack.Bind(thread, &requiredProviders, providersListUnpackerInto),
					"requires?", unpack.Bind(thread, &requires, unpack.List(unpack.Type[*Aspect]("Aspect"))),
					"toolchains?", unpack.Bind(thread, &toolchains, unpack.List(ToolchainTypeUnpackerInto)),
				); err != nil {
					return nil, err
				}
				return NewAspect(nil, &model_starlark_pb.Aspect_Definition{}), nil
			},
		),
		"attr": NewStructFromDict[object.LocalReference](nil, map[string]any{
			"bool": starlark.NewBuiltin(
				"attr.bool",
				func(thread *starlark.Thread, b *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
					if len(args) > 0 {
						return nil, fmt.Errorf("%s: got %d positional arguments, want 0", b.Name(), len(args))
					}

					var defaultValue starlark.Value = starlark.False
					doc := ""
					mandatory := false
					if err := starlark.UnpackArgs(
						b.Name(), args, kwargs,
						"default?", &defaultValue,
						"doc?", unpack.Bind(thread, &doc, unpack.String),
						"mandatory?", unpack.Bind(thread, &mandatory, unpack.Bool),
					); err != nil {
						return nil, err
					}

					attrType := BoolAttrType
					if mandatory {
						defaultValue = nil
					} else {
						var err error
						defaultValue, err = attrType.GetCanonicalizer(CurrentFilePackage(thread, 1)).
							Canonicalize(thread, defaultValue)
						if err != nil {
							return nil, fmt.Errorf("%s: for parameter default: %w", b.Name(), err)
						}
					}
					return NewAttr(attrType, defaultValue), nil
				},
			),
			"int": starlark.NewBuiltin(
				"attr.int",
				func(thread *starlark.Thread, b *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
					if len(args) > 0 {
						return nil, fmt.Errorf("%s: got %d positional arguments, want 0", b.Name(), len(args))
					}

					var defaultValue starlark.Value = starlark.MakeInt(0)
					doc := ""
					mandatory := false
					var values []int32
					if err := starlark.UnpackArgs(
						b.Name(), args, kwargs,
						"default?", &defaultValue,
						"doc?", unpack.Bind(thread, &doc, unpack.String),
						"mandatory?", unpack.Bind(thread, &mandatory, unpack.Bool),
						"values?", unpack.Bind(thread, &values, unpack.List(unpack.Int[int32]())),
					); err != nil {
						return nil, err
					}

					attrType := NewIntAttrType(values)
					if mandatory {
						defaultValue = nil
					} else {
						var err error
						defaultValue, err = attrType.GetCanonicalizer(CurrentFilePackage(thread, 1)).
							Canonicalize(thread, defaultValue)
						if err != nil {
							return nil, fmt.Errorf("%s: for parameter default: %w", b.Name(), err)
						}
					}
					return NewAttr(attrType, defaultValue), nil
				},
			),
			"label": starlark.NewBuiltin(
				"attr.label",
				func(thread *starlark.Thread, b *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
					if len(args) > 0 {
						return nil, fmt.Errorf("%s: got %d positional arguments, want 0", b.Name(), len(args))
					}

					var allowFiles []string
					var allowRules []string
					var allowSingleFile []string
					var aspects []*Aspect
					var cfg TransitionDefinition
					var defaultValue starlark.Value = starlark.None
					doc := ""
					executable := false
					var flags []string
					mandatory := false
					providers := [][]*Provider{{}}
					if err := starlark.UnpackArgs(
						b.Name(), args, kwargs,
						"allow_files?", unpack.Bind(thread, &allowFiles, allowFilesUnpackerInto),
						"allow_rules?", unpack.Bind(thread, &allowRules, unpack.IfNotNone(unpack.List(unpack.String))),
						"allow_single_file?", unpack.Bind(thread, &allowSingleFile, allowFilesUnpackerInto),
						"aspects?", unpack.Bind(thread, &aspects, unpack.List(unpack.Type[*Aspect]("aspect"))),
						"cfg?", unpack.Bind(thread, &cfg, TransitionDefinitionUnpackerInto),
						"default?", &defaultValue,
						"doc?", unpack.Bind(thread, &doc, unpack.String),
						"executable?", unpack.Bind(thread, &executable, unpack.Bool),
						"flags?", unpack.Bind(thread, &flags, unpack.List(unpack.String)),
						"mandatory?", unpack.Bind(thread, &mandatory, unpack.Bool),
						"providers?", unpack.Bind(thread, &providers, providersListUnpackerInto),
					); err != nil {
						return nil, err
					}
					if len(allowSingleFile) > 0 {
						if len(allowFiles) > 0 {
							return nil, errors.New("allow_files and allow_single_file cannot be specified at the same time")
						}
						allowFiles = allowSingleFile
					}
					if cfg == nil {
						if executable {
							return nil, errors.New("cfg must be provided when executable=True")
						}
						cfg = TargetTransitionDefinition
					}

					attrType := NewLabelAttrType(!mandatory, len(allowSingleFile) > 0, executable, sortAndDeduplicateSuffixes(allowFiles), cfg)
					if mandatory {
						defaultValue = nil
					} else {
						if err := unpack.Or([]unpack.UnpackerInto[starlark.Value]{
							unpack.Canonicalize(NamedFunctionUnpackerInto),
							unpack.Canonicalize(attrType.GetCanonicalizer(CurrentFilePackage(thread, 1))),
						}).UnpackInto(thread, defaultValue, &defaultValue); err != nil {
							return nil, fmt.Errorf("%s: for parameter default: %w", b.Name(), err)
						}
					}
					return NewAttr(attrType, defaultValue), nil
				},
			),
			"label_keyed_string_dict": starlark.NewBuiltin(
				"attr.label_keyed_string_dict",
				func(thread *starlark.Thread, b *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
					if len(args) > 1 {
						return nil, fmt.Errorf("%s: got %d positional arguments, want at most 1", b.Name(), len(args))
					}

					allowEmpty := true
					var allowFiles []string
					var aspects []*Aspect
					cfg := TargetTransitionDefinition
					var defaultValue starlark.Value = starlark.NewDict(0)
					doc := ""
					mandatory := false
					providers := [][]*Provider{{}}
					if err := starlark.UnpackArgs(
						b.Name(), args, kwargs,
						// Positional arguments.
						"allow_empty?", unpack.Bind(thread, &allowEmpty, unpack.Bool),
						// Keyword arguments.
						"allow_files?", unpack.Bind(thread, &allowFiles, allowFilesUnpackerInto),
						"aspects?", unpack.Bind(thread, &aspects, unpack.List(unpack.Type[*Aspect]("aspect"))),
						"cfg?", unpack.Bind(thread, &cfg, TransitionDefinitionUnpackerInto),
						"default?", &defaultValue,
						"doc?", unpack.Bind(thread, &doc, unpack.String),
						"mandatory?", unpack.Bind(thread, &mandatory, unpack.Bool),
						"providers?", unpack.Bind(thread, &providers, providersListUnpackerInto),
					); err != nil {
						return nil, err
					}

					attrType := NewLabelKeyedStringDictAttrType(sortAndDeduplicateSuffixes(allowFiles), cfg)
					if mandatory {
						defaultValue = nil
					} else {
						if err := unpack.Or([]unpack.UnpackerInto[starlark.Value]{
							unpack.Canonicalize(NamedFunctionUnpackerInto),
							unpack.Canonicalize(attrType.GetCanonicalizer(CurrentFilePackage(thread, 1))),
						}).UnpackInto(thread, defaultValue, &defaultValue); err != nil {
							return nil, fmt.Errorf("%s: for parameter default: %w", b.Name(), err)
						}
					}
					return NewAttr(attrType, defaultValue), nil
				},
			),
			"label_list": starlark.NewBuiltin(
				"attr.label_list",
				func(thread *starlark.Thread, b *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
					if len(args) > 1 {
						return nil, fmt.Errorf("%s: got %d positional arguments, want at most 1", b.Name(), len(args))
					}

					allowEmpty := true
					var allowFiles []string
					var allowRules []string
					var aspects []*Aspect
					cfg := TargetTransitionDefinition
					var defaultValue starlark.Value = starlark.NewList(nil)
					doc := ""
					var flags []string
					mandatory := false
					providers := [][]*Provider{{}}
					if err := starlark.UnpackArgs(
						b.Name(), args, kwargs,
						// Positional arguments.
						"allow_empty?", unpack.Bind(thread, &allowEmpty, unpack.Bool),
						// Keyword arguments.
						"allow_files?", unpack.Bind(thread, &allowFiles, allowFilesUnpackerInto),
						"allow_rules?", unpack.Bind(thread, &allowRules, unpack.IfNotNone(unpack.List(unpack.String))),
						"aspects?", unpack.Bind(thread, &aspects, unpack.List(unpack.Type[*Aspect]("aspect"))),
						"cfg?", unpack.Bind(thread, &cfg, TransitionDefinitionUnpackerInto),
						"default?", &defaultValue,
						"doc?", unpack.Bind(thread, &doc, unpack.String),
						"flags?", unpack.Bind(thread, &flags, unpack.List(unpack.String)),
						"mandatory?", unpack.Bind(thread, &mandatory, unpack.Bool),
						"providers?", unpack.Bind(thread, &providers, providersListUnpackerInto),
					); err != nil {
						return nil, err
					}

					attrType := NewLabelListAttrType(sortAndDeduplicateSuffixes(allowFiles), cfg)
					if mandatory {
						defaultValue = nil
					} else {
						if err := unpack.Or([]unpack.UnpackerInto[starlark.Value]{
							unpack.Canonicalize(NamedFunctionUnpackerInto),
							unpack.Canonicalize(attrType.GetCanonicalizer(CurrentFilePackage(thread, 1))),
						}).UnpackInto(thread, defaultValue, &defaultValue); err != nil {
							return nil, fmt.Errorf("%s: for parameter default: %w", b.Name(), err)
						}
					}
					return NewAttr(attrType, defaultValue), nil
				},
			),
			"output": starlark.NewBuiltin(
				"attr.output",
				func(thread *starlark.Thread, b *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
					if len(args) > 0 {
						return nil, fmt.Errorf("%s: got %d positional arguments, want 0", b.Name(), len(args))
					}

					doc := ""
					mandatory := false
					if err := starlark.UnpackArgs(
						b.Name(), args, kwargs,
						"doc?", unpack.Bind(thread, &doc, unpack.String),
						"mandatory?", unpack.Bind(thread, &mandatory, unpack.Bool),
					); err != nil {
						return nil, err
					}

					return NewAttr(NewOutputAttrType(""), nil), nil
				},
			),
			"output_list": starlark.NewBuiltin(
				"attr.output_list",
				func(thread *starlark.Thread, b *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
					if len(args) > 0 {
						return nil, fmt.Errorf("%s: got %d positional arguments, want 0", b.Name(), len(args))
					}

					allowEmpty := false
					doc := ""
					mandatory := false
					if err := starlark.UnpackArgs(
						b.Name(), args, kwargs,
						// Positional arguments.
						"allow_empty?", unpack.Bind(thread, &allowEmpty, unpack.Bool),
						// Keyword arguments.
						"doc?", unpack.Bind(thread, &doc, unpack.String),
						"mandatory?", unpack.Bind(thread, &mandatory, unpack.Bool),
					); err != nil {
						return nil, err
					}

					return NewAttr(NewOutputListAttrType(), nil), nil
				},
			),
			"string": starlark.NewBuiltin(
				"attr.string",
				func(thread *starlark.Thread, b *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
					if len(args) > 0 {
						return nil, fmt.Errorf("%s: got %d positional arguments, want 0", b.Name(), len(args))
					}

					var defaultValue starlark.Value = starlark.String("")
					doc := ""
					mandatory := false
					var values []string
					if err := starlark.UnpackArgs(
						b.Name(), args, kwargs,
						"default?", &defaultValue,
						"doc?", unpack.Bind(thread, &doc, unpack.String),
						"mandatory?", unpack.Bind(thread, &mandatory, unpack.Bool),
						"values?", unpack.Bind(thread, &values, unpack.List(unpack.String)),
					); err != nil {
						return nil, err
					}

					attrType := NewStringAttrType(values)
					if mandatory {
						defaultValue = nil
					} else {
						if err := unpack.Or([]unpack.UnpackerInto[starlark.Value]{
							unpack.Canonicalize(NamedFunctionUnpackerInto),
							unpack.Canonicalize(attrType.GetCanonicalizer(CurrentFilePackage(thread, 1))),
						}).UnpackInto(thread, defaultValue, &defaultValue); err != nil {
							return nil, fmt.Errorf("%s: for parameter default: %w", b.Name(), err)
						}
					}
					return NewAttr(attrType, defaultValue), nil
				},
			),
			"string_dict": starlark.NewBuiltin(
				"attr.string_dict",
				func(thread *starlark.Thread, b *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
					if len(args) > 1 {
						return nil, fmt.Errorf("%s: got %d positional arguments, want at most 2", b.Name(), len(args))
					}

					var allowEmpty bool
					var defaultValue starlark.Value = starlark.NewDict(0)
					doc := ""
					mandatory := false
					if err := starlark.UnpackArgs(
						b.Name(), args, kwargs,
						// Positional arguments.
						"allow_empty?", unpack.Bind(thread, &allowEmpty, unpack.Bool),
						// Keyword arguments.
						"default?", &defaultValue,
						"doc?", unpack.Bind(thread, &doc, unpack.String),
						"mandatory?", unpack.Bind(thread, &mandatory, unpack.Bool),
					); err != nil {
						return nil, err
					}

					attrType := NewStringDictAttrType()
					if mandatory {
						defaultValue = nil
					} else {
						if err := unpack.Or([]unpack.UnpackerInto[starlark.Value]{
							unpack.Canonicalize(NamedFunctionUnpackerInto),
							unpack.Canonicalize(attrType.GetCanonicalizer(CurrentFilePackage(thread, 1))),
						}).UnpackInto(thread, defaultValue, &defaultValue); err != nil {
							return nil, fmt.Errorf("%s: for parameter default: %w", b.Name(), err)
						}
					}
					return NewAttr(attrType, defaultValue), nil
				},
			),
			"string_list": starlark.NewBuiltin(
				"attr.string_list",
				func(thread *starlark.Thread, b *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
					if len(args) > 2 {
						return nil, fmt.Errorf("%s: got %d positional arguments, want at most 2", b.Name(), len(args))
					}

					mandatory := false
					var allowEmpty bool
					var defaultValue starlark.Value = starlark.NewList(nil)
					doc := ""
					if err := starlark.UnpackArgs(
						b.Name(), args, kwargs,
						// Positional arguments.
						"mandatory?", unpack.Bind(thread, &mandatory, unpack.Bool),
						"allow_empty?", unpack.Bind(thread, &allowEmpty, unpack.Bool),
						// Keyword arguments.
						"default?", &defaultValue,
						"doc?", unpack.Bind(thread, &doc, unpack.String),
					); err != nil {
						return nil, err
					}

					attrType := NewStringListAttrType()
					if mandatory {
						defaultValue = nil
					} else {
						if err := unpack.Or([]unpack.UnpackerInto[starlark.Value]{
							unpack.Canonicalize(NamedFunctionUnpackerInto),
							unpack.Canonicalize(attrType.GetCanonicalizer(CurrentFilePackage(thread, 1))),
						}).UnpackInto(thread, defaultValue, &defaultValue); err != nil {
							return nil, fmt.Errorf("%s: for parameter default: %w", b.Name(), err)
						}
					}
					return NewAttr(attrType, defaultValue), nil
				},
			),
			"string_list_dict": starlark.NewBuiltin(
				"attr.string_list_dict",
				func(thread *starlark.Thread, b *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
					if len(args) > 1 {
						return nil, fmt.Errorf("%s: got %d positional arguments, want at most 1", b.Name(), len(args))
					}
					var allowEmpty bool
					var defaultValue starlark.Value = starlark.NewDict(0)
					doc := ""
					mandatory := false
					if err := starlark.UnpackArgs(
						b.Name(), args, kwargs,
						// Positional arguments.
						"allow_empty?", unpack.Bind(thread, &allowEmpty, unpack.Bool),
						// Keyword arguments.
						"default?", &defaultValue,
						"doc?", unpack.Bind(thread, &doc, unpack.String),
						"mandatory?", unpack.Bind(thread, &mandatory, unpack.Bool),
					); err != nil {
						return nil, err
					}

					attrType := NewStringListDictAttrType()
					if mandatory {
						defaultValue = nil
					} else {
						if err := unpack.Or([]unpack.UnpackerInto[starlark.Value]{
							unpack.Canonicalize(NamedFunctionUnpackerInto),
							unpack.Canonicalize(attrType.GetCanonicalizer(CurrentFilePackage(thread, 1))),
						}).UnpackInto(thread, defaultValue, &defaultValue); err != nil {
							return nil, fmt.Errorf("%s: for parameter default: %w", b.Name(), err)
						}
					}
					return NewAttr(attrType, defaultValue), nil
				},
			),
		}),
		"config": NewStructFromDict[object.LocalReference](nil, map[string]any{
			"bool": starlark.NewBuiltin(
				"config.bool",
				func(thread *starlark.Thread, b *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
					if len(args) > 0 {
						return nil, fmt.Errorf("%s: got %d positional arguments, want 0", b.Name(), len(args))
					}
					flag := false
					if err := starlark.UnpackArgs(
						b.Name(), args, kwargs,
						"flag?", unpack.Bind(thread, &flag, unpack.Bool),
					); err != nil {
						return nil, err
					}
					return NewBuildSetting(BoolBuildSettingType, flag), nil
				},
			),
			"exec": starlark.NewBuiltin(
				"config.exec",
				func(thread *starlark.Thread, b *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
					execGroup := ""
					if err := starlark.UnpackArgs(
						b.Name(), args, kwargs,
						"exec_group?", unpack.Bind(thread, &execGroup, unpack.IfNotNone(unpack.String)),
					); err != nil {
						return nil, err
					}
					return NewTransition(
						NewReferenceTransitionDefinition(
							&model_starlark_pb.Transition_Reference{
								Kind: &model_starlark_pb.Transition_Reference_ExecGroup{
									ExecGroup: execGroup,
								},
							},
						),
					), nil
				},
			),
			"int": starlark.NewBuiltin(
				"config.int",
				func(thread *starlark.Thread, b *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
					if len(args) > 0 {
						return nil, fmt.Errorf("%s: got %d positional arguments, want 0", b.Name(), len(args))
					}
					flag := false
					if err := starlark.UnpackArgs(
						b.Name(), args, kwargs,
						"flag?", unpack.Bind(thread, &flag, unpack.Bool),
					); err != nil {
						return nil, err
					}
					return NewBuildSetting(IntBuildSettingType, flag), nil
				},
			),
			"none": starlark.NewBuiltin(
				"config.none",
				func(thread *starlark.Thread, b *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
					if err := starlark.UnpackArgs(b.Name(), args, kwargs); err != nil {
						return nil, err
					}
					return NewTransition(NoneTransitionDefinition), nil
				},
			),
			"string": starlark.NewBuiltin(
				"config.string",
				func(thread *starlark.Thread, b *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
					if len(args) > 0 {
						return nil, fmt.Errorf("%s: got %d positional arguments, want 0", b.Name(), len(args))
					}
					flag := false
					if err := starlark.UnpackArgs(
						b.Name(), args, kwargs,
						"flag?", unpack.Bind(thread, &flag, unpack.Bool),
					); err != nil {
						return nil, err
					}
					return NewBuildSetting(StringBuildSettingType, flag), nil
				},
			),
			"string_list": starlark.NewBuiltin(
				"config.string_list",
				func(thread *starlark.Thread, b *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
					if len(args) > 0 {
						return nil, fmt.Errorf("%s: got %d positional arguments, want 0", b.Name(), len(args))
					}
					flag := false
					repeatable := false
					if err := starlark.UnpackArgs(
						b.Name(), args, kwargs,
						"flag?", unpack.Bind(thread, &flag, unpack.Bool),
						"repeatable?", unpack.Bind(thread, &repeatable, unpack.Bool),
					); err != nil {
						return nil, err
					}
					return NewBuildSetting(NewStringListBuildSettingType(repeatable), flag), nil
				},
			),
			"target": starlark.NewBuiltin(
				"config.target",
				func(thread *starlark.Thread, b *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
					if err := starlark.UnpackArgs(b.Name(), args, kwargs); err != nil {
						return nil, err
					}
					return NewTransition(TargetTransitionDefinition), nil
				},
			),
			"unconfigured": starlark.NewBuiltin(
				"config.unconfigured",
				func(thread *starlark.Thread, b *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
					if err := starlark.UnpackArgs(b.Name(), args, kwargs); err != nil {
						return nil, err
					}
					return NewTransition(UnconfiguredTransitionDefinition), nil
				},
			),
		}),
		"config_common": NewStructFromDict[object.LocalReference](nil, map[string]any{
			"toolchain_type": starlark.NewBuiltin(
				"config_common.toolchain_type",
				func(thread *starlark.Thread, b *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
					if len(args) > 1 {
						return nil, fmt.Errorf("%s: got %d positional arguments, want at most 1", b.Name(), len(args))
					}
					var name pg_label.ResolvedLabel
					mandatory := true
					if err := starlark.UnpackArgs(
						b.Name(), args, kwargs,
						"name", unpack.Bind(thread, &name, NewLabelOrStringUnpackerInto(CurrentFilePackage(thread, 1))),
						"mandatory?", unpack.Bind(thread, &mandatory, unpack.Bool),
					); err != nil {
						return nil, err
					}
					return NewToolchainType(name, mandatory), nil
				},
			),
		}),
		"configuration_field": starlark.NewBuiltin(
			"configuration_field",
			func(thread *starlark.Thread, b *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
				var fragment string
				var name string
				if err := starlark.UnpackArgs(
					b.Name(), args, kwargs,
					"fragment", unpack.Bind(thread, &fragment, unpack.String),
					"name", unpack.Bind(thread, &name, unpack.String),
				); err != nil {
					return nil, err
				}

				// Don't provide actual support for late-bound
				// defaults. Instead map each of them to the
				// respective command line option used by Bazel.
				switch fragment {
				case "apple":
					switch name {
					case "xcode_config_label":
						return NewLabel(pg_label.MustNewResolvedLabel("@@bazel_tools+//command_line_option:xcode_version_config")), nil
					}
				case "bazel_py":
					switch name {
					case "python_top":
						return NewLabel(pg_label.MustNewResolvedLabel("@@bazel_tools+//command_line_option:python_top")), nil
					}
				case "coverage":
					switch name {
					case "output_generator":
						return NewLabel(pg_label.MustNewResolvedLabel("@@bazel_tools+//command_line_option:coverage_output_generator")), nil
					}
				case "cpp":
					switch name {
					case "cs_fdo_profile":
						return NewLabel(pg_label.MustNewResolvedLabel("@@bazel_tools+//command_line_option:cs_fdo_profile")), nil
					case "custom_malloc":
						return NewLabel(pg_label.MustNewResolvedLabel("@@bazel_tools+//command_line_option:custom_malloc")), nil
					case "fdo_optimize":
						return NewLabel(pg_label.MustNewResolvedLabel("@@bazel_tools+//command_line_option:fdo_optimize")), nil
					case "fdo_prefetch_hints":
						return NewLabel(pg_label.MustNewResolvedLabel("@@bazel_tools+//command_line_option:fdo_prefetch_hints")), nil
					case "fdo_profile":
						return NewLabel(pg_label.MustNewResolvedLabel("@@bazel_tools+//command_line_option:fdo_profile")), nil
					case "libc_top":
						return NewLabel(pg_label.MustNewResolvedLabel("@@bazel_tools+//command_line_option:grte_top")), nil
					case "memprof_profile":
						return NewLabel(pg_label.MustNewResolvedLabel("@@bazel_tools+//command_line_option:memprof_profile")), nil
					case "propeller_optimize":
						return NewLabel(pg_label.MustNewResolvedLabel("@@bazel_tools+//command_line_option:propeller_optimize")), nil
					case "proto_profile_path":
						return NewLabel(pg_label.MustNewResolvedLabel("@@bazel_tools+//command_line_option:proto_profile_path")), nil
					case "target_libc_top_DO_NOT_USE_ONLY_FOR_CC_TOOLCHAIN":
						return starlark.None, nil
					case "xbinary_fdo":
						return NewLabel(pg_label.MustNewResolvedLabel("@@bazel_tools+//command_line_option:xbinary_fdo")), nil
					case "zipper":
						return starlark.None, nil
					}
				case "java":
					switch name {
					case "java_toolchain_bytecode_optimizer":
						return NewLabel(pg_label.MustNewResolvedLabel("@@bazel_tools+//command_line_option:proguard_top")), nil
					case "local_java_optimization_configuration":
						return NewLabel(pg_label.MustNewResolvedLabel("@@bazel_tools+//command_line_option:experimental_local_java_optimization_configuration")), nil
					}
				case "proto":
					switch name {
					case "proto_compiler":
						return NewLabel(pg_label.MustNewResolvedLabel("@@bazel_tools+//command_line_option:proto_compiler")), nil
					case "proto_toolchain_for_cc":
						return NewLabel(pg_label.MustNewResolvedLabel("@@bazel_tools+//command_line_option:proto_toolchain_for_cc")), nil
					case "proto_toolchain_for_java":
						return NewLabel(pg_label.MustNewResolvedLabel("@@bazel_tools+//command_line_option:proto_toolchain_for_java")), nil
					case "proto_toolchain_for_java_lite":
						return NewLabel(pg_label.MustNewResolvedLabel("@@bazel_tools+//command_line_option:proto_toolchain_for_javalite")), nil
					}
				case "py":
					switch name {
					case "native_rules_allowlist":
						return NewLabel(pg_label.MustNewResolvedLabel("@@bazel_tools+//command_line_option:python_native_rules_allowlist")), nil
					}
				}

				return nil, fmt.Errorf("this implementation of configuration_field() does not support fragment %#v and name %#v", fragment, name)
			},
		),
		"depset": starlark.NewBuiltin(
			"depset",
			func(thread *starlark.Thread, b *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
				if len(args) > 2 {
					return nil, fmt.Errorf("%s: got %d positional arguments, want at most 2", b.Name(), len(args))
				}
				var direct []starlark.Value
				order := "default"
				// TODO: This should use TReference!
				var transitive []*Depset[object.LocalReference]
				if err := starlark.UnpackArgs(
					b.Name(), args, kwargs,
					// Positional arguments.
					"direct?", unpack.Bind(thread, &direct, unpack.List(unpack.Any)),
					// Keyword arguments.
					"order?", unpack.Bind(thread, &order, unpack.String),
					"transitive?", unpack.Bind(thread, &transitive, unpack.List(unpack.Type[*Depset[object.LocalReference]]("depset"))),
				); err != nil {
					return nil, err
				}
				var orderValue model_starlark_pb.Depset_Order
				switch order {
				case "default":
					orderValue = model_starlark_pb.Depset_DEFAULT
				case "postorder":
					orderValue = model_starlark_pb.Depset_POSTORDER
				case "preorder":
					orderValue = model_starlark_pb.Depset_PREORDER
				case "topological":
					orderValue = model_starlark_pb.Depset_TOPOLOGICAL
				default:
					return nil, fmt.Errorf("unknown order %#v", order)
				}
				return NewDepset(thread, direct, transitive, orderValue)
			},
		),
		"exec_group": starlark.NewBuiltin(
			"exec_group",
			func(thread *starlark.Thread, b *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
				if len(args) > 0 {
					return nil, fmt.Errorf("%s: got %d positional arguments, want 0", b.Name(), len(args))
				}
				var execCompatibleWith []pg_label.ResolvedLabel
				var toolchains []*ToolchainType
				if err := starlark.UnpackArgs(
					b.Name(), args, kwargs,
					"exec_compatible_with?", unpack.Bind(thread, &execCompatibleWith, unpack.List(NewLabelOrStringUnpackerInto(CurrentFilePackage(thread, 1)))),
					"toolchains?", unpack.Bind(thread, &toolchains, unpack.List(ToolchainTypeUnpackerInto)),
				); err != nil {
					return nil, err
				}
				return NewExecGroup(execCompatibleWith, toolchains), nil
			},
		),
		"json": NewStructFromDict[object.LocalReference](nil, stringDictToStructFields(json.Module.Members)),
		"module_extension": starlark.NewBuiltin(
			"module_extension",
			func(thread *starlark.Thread, b *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
				if len(args) > 1 {
					return nil, fmt.Errorf("%s: got %d positional arguments, want at most 1", b.Name(), len(args))
				}
				var implementation NamedFunction
				archDependent := false
				doc := ""
				var environ []string
				osDependent := false
				var tagClasses map[pg_label.StarlarkIdentifier]*TagClass
				if err := starlark.UnpackArgs(
					b.Name(), args, kwargs,
					// Positional arguments.
					"implementation", unpack.Bind(thread, &implementation, NamedFunctionUnpackerInto),
					// Keyword arguments.
					"arch_dependent?", unpack.Bind(thread, &archDependent, unpack.Bool),
					"doc?", unpack.Bind(thread, &doc, unpack.String),
					"environ?", unpack.Bind(thread, &environ, unpack.List(unpack.String)),
					"os_dependent?", unpack.Bind(thread, &osDependent, unpack.Bool),
					"tag_classes?", unpack.Bind(thread, &tagClasses, unpack.Dict(unpack.StarlarkIdentifier, unpack.Type[*TagClass]("tag_class"))),
				); err != nil {
					return nil, err
				}
				return NewModuleExtension(NewStarlarkModuleExtensionDefinition(implementation, tagClasses)), nil
			},
		),
		"native": NewStructFromDict[object.LocalReference](nil, map[string]any{
			"alias": starlark.NewBuiltin(
				"native.alias",
				func(thread *starlark.Thread, b *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
					targetRegistrar := thread.Local(TargetRegistrarKey).(*TargetRegistrar[object.LocalReference])
					if targetRegistrar == nil {
						return nil, errors.New("targets cannot be registered from within this context")
					}
					currentPackage := thread.Local(CanonicalPackageKey).(pg_label.CanonicalPackage)

					var name string
					var actual *Select
					deprecation := ""
					var tags []string
					var visibility []pg_label.ResolvedLabel
					labelOrStringUnpackerInto := NewLabelOrStringUnpackerInto(currentPackage)
					if err := starlark.UnpackArgs(
						b.Name(), args, kwargs,
						"name", unpack.Bind(thread, &name, unpack.Stringer(unpack.TargetName)),
						"actual", unpack.Bind(thread, &actual, NewSelectUnpackerInto(labelOrStringUnpackerInto)),
						"deprecation?", unpack.Bind(thread, &deprecation, unpack.String),
						"tags?", unpack.Bind(thread, &tags, unpack.List(unpack.String)),
						"visibility?", unpack.Bind(thread, &visibility, unpack.IfNotNone(unpack.List(labelOrStringUnpackerInto))),
					); err != nil {
						return nil, err
					}

					patcher := model_core.NewReferenceMessagePatcher[model_core.CreatedObjectTree]()
					var visibilityPackageGroup *model_starlark_pb.PackageGroup
					if len(visibility) == 0 {
						// Unlike rule targets, exports_files()
						// defaults to public visibility.
						visibilityPackageGroup = &model_starlark_pb.PackageGroup{
							Tree: &model_starlark_pb.PackageGroup_Subpackages{
								IncludeSubpackages: true,
							},
						}
					} else {
						m, err := targetRegistrar.getVisibilityPackageGroup(visibility)
						if err != nil {
							return nil, err
						}
						patcher.Merge(m.Patcher)
						visibilityPackageGroup = m.Message
					}

					valueEncodingOptions := thread.Local(ValueEncodingOptionsKey).(*ValueEncodingOptions)
					actualGroups, _, err := actual.EncodeGroups(
						/* path = */ map[starlark.Value]struct{}{},
						valueEncodingOptions,
					)
					if err != nil {
						return nil, err
					}
					if l := len(actualGroups.Message); l != 1 {
						return nil, fmt.Errorf("\"actual\" is a select() that contains %d groups, while 1 group was expected", l)
					}
					patcher.Merge(actualGroups.Patcher)

					actual.VisitLabels(thread, map[starlark.Value]struct{}{}, func(l pg_label.ResolvedLabel) error {
						if canonicalLabel, err := l.AsCanonical(); err == nil {
							if canonicalLabel.GetCanonicalPackage() == currentPackage {
								targetRegistrar.registerImplicitTarget(l.GetTargetName().String())
							}
						}
						return nil
					})

					return starlark.None, targetRegistrar.registerExplicitTarget(
						name,
						model_core.NewPatchedMessage(
							&model_starlark_pb.Target_Definition{
								Kind: &model_starlark_pb.Target_Definition_Alias{
									Alias: &model_starlark_pb.Alias{
										Actual:     actualGroups.Message[0],
										Visibility: visibilityPackageGroup,
									},
								},
							},
							patcher,
						),
					)
				},
			),
			"bazel_version": starlark.String("8.0.0"),
			"current_ctx": starlark.NewBuiltin(
				"native.current_ctx",
				func(thread *starlark.Thread, b *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
					// This function is an extension. It
					// is provided to implement functions
					// like cc_internal.actions2ctx_cheat().
					// It should be removed once the C++
					// rules have been fully converted to
					// Starlark.
					if err := starlark.UnpackArgs(b.Name(), args, kwargs); err != nil {
						return nil, err
					}
					currentCtx := thread.Local(CurrentCtxKey)
					if currentCtx == nil {
						return nil, errors.New("this function can only be called from within a rule implementation function")
					}
					return currentCtx.(starlark.Value), nil
				},
			),
			"exports_files": starlark.NewBuiltin(
				"native.exports_files",
				func(thread *starlark.Thread, b *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
					targetRegistrar := thread.Local(TargetRegistrarKey).(*TargetRegistrar[object.LocalReference])
					if targetRegistrar == nil {
						return nil, errors.New("targets cannot be registered from within this context")
					}
					currentPackage := thread.Local(CanonicalPackageKey).(pg_label.CanonicalPackage)

					if len(args) > 1 {
						return nil, fmt.Errorf("%s: got %d positional arguments, want at most 1", b.Name(), len(args))
					}
					var srcs []string
					var visibility []pg_label.ResolvedLabel
					if err := starlark.UnpackArgs(
						b.Name(), args, kwargs,
						// Positional arguments.
						"srcs", unpack.Bind(thread, &srcs, unpack.List(unpack.Stringer(unpack.TargetName))),
						// Keyword arguments.
						"visibility?", unpack.Bind(thread, &visibility, unpack.List(NewLabelOrStringUnpackerInto(currentPackage))),
					); err != nil {
						return nil, err
					}

					for _, src := range srcs {
						// TODO: Only create the package group once.
						visibilityPackageGroup, err := targetRegistrar.getVisibilityPackageGroup(visibility)
						if err != nil {
							return nil, err
						}

						if err := targetRegistrar.registerExplicitTarget(
							src,
							model_core.NewPatchedMessage(
								&model_starlark_pb.Target_Definition{
									Kind: &model_starlark_pb.Target_Definition_SourceFileTarget{
										SourceFileTarget: &model_starlark_pb.SourceFileTarget{
											Visibility: visibilityPackageGroup.Message,
										},
									},
								},
								visibilityPackageGroup.Patcher,
							),
						); err != nil {
							return nil, err
						}
					}
					return starlark.None, nil
				},
			),
			"glob": starlark.NewBuiltin(
				"native.glob",
				func(thread *starlark.Thread, b *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
					globExpander := thread.Local(GlobExpanderKey)
					if globExpander == nil {
						return nil, fmt.Errorf("globs cannot be expanded within this context")
					}

					var include []string
					var exclude []string
					excludeDirectories := 1
					allowEmpty := false
					if err := starlark.UnpackArgs(
						b.Name(), args, kwargs,
						"include", unpack.Bind(thread, &include, unpack.List(unpack.String)),
						"exclude?", unpack.Bind(thread, &exclude, unpack.List(unpack.String)),
						"exclude_sirectories?", unpack.Bind(thread, &excludeDirectories, unpack.Int[int]()),
						"allow_empty?", unpack.Bind(thread, &allowEmpty, unpack.Bool),
					); err != nil {
						return nil, err
					}

					sort.Strings(include)
					sort.Strings(exclude)
					targetNames, err := globExpander.(GlobExpander)(include, exclude, excludeDirectories == 0)
					if err != nil {
						return nil, err
					}
					if len(targetNames) == 0 && !allowEmpty {
						return nil, errors.New("glob does not match any source files")
					}

					labels := make([]starlark.Value, 0, len(targetNames))
					for _, targetName := range targetNames {
						labels = append(labels, starlark.String(targetName.String()))
					}
					return starlark.NewList(labels), nil
				},
			),
			"label_flag": starlark.NewBuiltin(
				"native.label_flag",
				func(thread *starlark.Thread, b *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
					return labelSetting(thread, b, args, kwargs, true)
				},
			),
			"label_setting": starlark.NewBuiltin(
				"native.label_setting",
				func(thread *starlark.Thread, b *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
					return labelSetting(thread, b, args, kwargs, false)
				},
			),
			"package_group": starlark.NewBuiltin(
				"native.package_group",
				func(thread *starlark.Thread, b *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
					targetRegistrar := thread.Local(TargetRegistrarKey).(*TargetRegistrar[object.LocalReference])
					if targetRegistrar == nil {
						return nil, errors.New("targets cannot be registered from within this context")
					}
					currentPackage := thread.Local(CanonicalPackageKey).(pg_label.CanonicalPackage)

					var name string
					var packages []string
					var includes []string
					if err := starlark.UnpackArgs(
						b.Name(), args, kwargs,
						"name", unpack.Bind(thread, &name, unpack.Stringer(unpack.TargetName)),
						"packages?", unpack.Bind(thread, &packages, unpack.List(unpack.String)),
						"includes?", unpack.Bind(thread, &includes, unpack.List(unpack.Stringer(NewLabelOrStringUnpackerInto(currentPackage)))),
					); err != nil {
						return nil, err
					}

					slices.Sort(includes)
					return starlark.None, targetRegistrar.registerExplicitTarget(
						name,
						model_core.NewSimplePatchedMessage[model_core.CreatedObjectTree](
							&model_starlark_pb.Target_Definition{
								Kind: &model_starlark_pb.Target_Definition_PackageGroup{
									PackageGroup: &model_starlark_pb.PackageGroup{
										// TODO: Set tree!
										IncludePackageGroups: slices.Compact(includes),
									},
								},
							},
						),
					)
				},
			),
			"package_name": starlark.NewBuiltin(
				"native.package_name",
				func(thread *starlark.Thread, b *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
					canonicalPackage := thread.Local(CanonicalPackageKey)
					if canonicalPackage == nil {
						return nil, errors.New("package name cannot be obtained from within this context")
					}

					if err := starlark.UnpackArgs(b.Name(), args, kwargs); err != nil {
						return nil, err
					}

					return starlark.String(canonicalPackage.(pg_label.CanonicalPackage).GetPackagePath()), nil
				},
			),
			"package_relative_label": starlark.NewBuiltin(
				"native.package_relative_label",
				func(thread *starlark.Thread, b *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
					// This function is identical to Label(),
					// except that it resolves the label
					// relative to the package for which targets
					// are being computed, as opposed to the
					// package containing the .bzl file.
					canonicalPackage := thread.Local(CanonicalPackageKey)
					if canonicalPackage == nil {
						return nil, errors.New("package relative labels cannot be resolved from within this context")
					}

					if len(args) != 1 {
						return nil, fmt.Errorf("%s: got %d positional arguments, want 1", b.Name(), len(args))
					}
					var input pg_label.ResolvedLabel
					if err := starlark.UnpackArgs(
						b.Name(), args, kwargs,
						"input", unpack.Bind(thread, &input, NewLabelOrStringUnpackerInto(canonicalPackage.(pg_label.CanonicalPackage))),
					); err != nil {
						return nil, err
					}
					return NewLabel(input), nil
				},
			),
			"starlark_doc_extract": starlark.NewBuiltin(
				"native.starlark_doc_extract",
				func(thread *starlark.Thread, b *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
					return nil, errors.New("TODO: Implement native.starlark_doc_extract()")
				},
			),
		}),
		"provider": starlark.NewBuiltin(
			"provider",
			func(thread *starlark.Thread, b *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
				if len(args) > 1 {
					return nil, fmt.Errorf("%s: got %d positional arguments, want at most 1", b.Name(), len(args))
				}
				doc := ""
				dictLike := false
				var fields any
				var init *NamedFunction
				if err := starlark.UnpackArgs(
					b.Name(), args, kwargs,
					// Positional arguments.
					"doc?", unpack.Bind(thread, &doc, unpack.String),
					// Keyword arguments.
					"dict_like?", unpack.Bind(thread, &dictLike, unpack.Bool),
					"fields?", unpack.Bind(thread, &fields, unpack.IfNotNone(unpack.Or([]unpack.UnpackerInto[any]{
						unpack.Decay(unpack.Dict(unpack.String, unpack.String)),
						unpack.Decay(unpack.List(unpack.String)),
					}))),
					"init?", unpack.Bind(thread, &init, unpack.Pointer(NamedFunctionUnpackerInto)),
				); err != nil {
					return nil, err
				}

				var fieldNames []string
				switch f := fields.(type) {
				case nil:
				case []string:
					fieldNames = f
				case map[string]string:
					fieldNames = slices.Collect(maps.Keys(f))
				default:
					panic("unknown type")
				}
				sort.Strings(fieldNames)

				provider := NewProvider(
					NewProviderInstanceProperties(nil, dictLike),
					slices.Compact(fieldNames),
					init,
				)
				if init == nil {
					return provider, nil
				}
				return starlark.Tuple{
					provider,
					// TODO: Return raw provider value!
					starlark.None,
				}, nil
			},
		),
		"repository_rule": starlark.NewBuiltin(
			"repository_rule",
			func(thread *starlark.Thread, b *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
				if len(args) > 1 {
					return nil, fmt.Errorf("%s: got %d positional arguments, want at most 1", b.Name(), len(args))
				}
				var implementation NamedFunction
				attrs := map[pg_label.StarlarkIdentifier]*Attr{}
				configure := false
				doc := ""
				var environ []string
				local := false
				if err := starlark.UnpackArgs(
					b.Name(), args, kwargs,
					// Positional arguments.
					"implementation", unpack.Bind(thread, &implementation, NamedFunctionUnpackerInto),
					// Keyword arguments.
					"attrs?", unpack.Bind(thread, &attrs, unpack.Dict(unpack.StarlarkIdentifier, unpack.Type[*Attr]("attr.*"))),
					"configure?", unpack.Bind(thread, &configure, unpack.Bool),
					"doc?", unpack.Bind(thread, &doc, unpack.String),
					"environ?", unpack.Bind(thread, &environ, unpack.List(unpack.String)),
					"local?", unpack.Bind(thread, &local, unpack.Bool),
				); err != nil {
					return nil, err
				}
				return NewRepositoryRule(nil, NewStarlarkRepositoryRuleDefinition(implementation, attrs)), nil
			},
		),
		"rule": starlark.NewBuiltin(
			"rule",
			func(thread *starlark.Thread, b *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
				if len(args) > 1 {
					return nil, fmt.Errorf("%s: got %d positional arguments, want at most 1", b.Name(), len(args))
				}
				var implementation NamedFunction
				attrs := map[pg_label.StarlarkIdentifier]*Attr{}
				var buildSetting *BuildSetting
				var cfg *Transition
				doc := ""
				execGroups := map[string]*ExecGroup{}
				executable := false
				var execCompatibleWith []pg_label.ResolvedLabel
				var fragments []string
				var hostFragments []string
				var initializer *NamedFunction
				outputs := map[pg_label.StarlarkIdentifier]string{}
				var provides []*Provider
				var subrules []*Subrule
				test := false
				var toolchains []*ToolchainType
				if err := starlark.UnpackArgs(
					b.Name(), args, kwargs,
					// Positional arguments.
					"implementation", unpack.Bind(thread, &implementation, NamedFunctionUnpackerInto),
					// Keyword arguments.
					"attrs?", unpack.Bind(thread, &attrs, unpack.Dict(unpack.StarlarkIdentifier, unpack.Type[*Attr]("attr.*"))),
					"build_setting?", unpack.Bind(thread, &buildSetting, unpack.IfNotNone(unpack.Type[*BuildSetting]("config.*"))),
					"cfg?", unpack.Bind(thread, &cfg, unpack.IfNotNone(unpack.Type[*Transition]("transition"))),
					"doc?", unpack.Bind(thread, &doc, unpack.String),
					"executable?", unpack.Bind(thread, &executable, unpack.Bool),
					"exec_compatible_with?", unpack.Bind(thread, &execCompatibleWith, unpack.List(NewLabelOrStringUnpackerInto(CurrentFilePackage(thread, 1)))),
					"exec_groups?", unpack.Bind(thread, &execGroups, unpack.Dict(unpack.String, unpack.Type[*ExecGroup]("exec_group"))),
					"fragments?", unpack.Bind(thread, &fragments, unpack.List(unpack.String)),
					"host_fragments?", unpack.Bind(thread, &hostFragments, unpack.List(unpack.String)),
					"initializer?", unpack.Bind(thread, &initializer, unpack.IfNotNone(unpack.Pointer(NamedFunctionUnpackerInto))),
					"outputs?", unpack.Bind(thread, &outputs, unpack.Dict(unpack.StarlarkIdentifier, unpack.String)),
					"provides?", unpack.Bind(thread, &provides, unpack.List(unpack.Type[*Provider]("provider"))),
					"subrules?", unpack.Bind(thread, &subrules, unpack.List(unpack.Type[*Subrule]("subrule"))),
					"test?", unpack.Bind(thread, &test, unpack.Bool),
					"toolchains?", unpack.Bind(thread, &toolchains, unpack.List(ToolchainTypeUnpackerInto)),
				); err != nil {
					return nil, err
				}

				if _, ok := execGroups[""]; ok {
					return nil, errors.New("cannot explicitly declare exec_group with name \"\"")
				}
				execGroups[""] = NewExecGroup(execCompatibleWith, toolchains)

				// Convert predeclared outputs to
				// attr.output(), with the filename
				// template as the attr's default value.
				for name, template := range outputs {
					if _, ok := attrs[name]; ok {
						return nil, fmt.Errorf("predeclared output %#v has the same name as existing attr", name)
					}
					attrs[name] = NewAttr(NewOutputAttrType(template), starlark.String(template))
				}

				return NewRule[object.LocalReference](nil, NewStarlarkRuleDefinition(
					attrs,
					buildSetting,
					cfg,
					execGroups,
					implementation,
					initializer,
					provides,
					test,
					subrules,
				)), nil
			},
		),
		"struct": starlark.NewBuiltin(
			"struct",
			func(thread *starlark.Thread, b *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
				if len(args) != 0 {
					return nil, fmt.Errorf("%s: got %d positional arguments, want 0", b.Name(), len(args))
				}
				entries := make(map[string]any, len(kwargs))
				for _, kwarg := range kwargs {
					entries[string(kwarg[0].(starlark.String))] = kwarg[1]
				}
				return NewStructFromDict[object.LocalReference](nil, entries), nil
			},
		),
		"subrule": starlark.NewBuiltin(
			"subrule",
			func(thread *starlark.Thread, b *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
				if len(args) > 1 {
					return nil, fmt.Errorf("%s: got %d positional arguments, want at most 1", b.Name(), len(args))
				}
				var implementation NamedFunction
				attrs := map[pg_label.StarlarkIdentifier]*Attr{}
				var fragments []string
				var subrules []*Subrule
				var toolchains []*ToolchainType
				if err := starlark.UnpackArgs(
					b.Name(), args, kwargs,
					// Positional arguments.
					"implementation", unpack.Bind(thread, &implementation, NamedFunctionUnpackerInto),
					// Keyword arguments.
					"attrs?", unpack.Bind(thread, &attrs, unpack.Dict(unpack.StarlarkIdentifier, unpack.Type[*Attr]("attr.*"))),
					"fragments?", unpack.Bind(thread, &fragments, unpack.List(unpack.String)),
					"subrules?", unpack.Bind(thread, &subrules, unpack.List(unpack.Type[*Subrule]("subrule"))),
					"toolchains?", unpack.Bind(thread, &toolchains, unpack.List(ToolchainTypeUnpackerInto)),
				); err != nil {
					return nil, err
				}

				return NewSubrule(nil, NewStarlarkSubruleDefinition(
					attrs,
					implementation,
					subrules,
				)), nil
			},
		),
		"tag_class": starlark.NewBuiltin(
			"tag_class",
			func(thread *starlark.Thread, b *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
				if len(args) > 1 {
					return nil, fmt.Errorf("%s: got %d positional arguments, want at most 1", b.Name(), len(args))
				}
				attrs := map[pg_label.StarlarkIdentifier]*Attr{}
				doc := ""
				if err := starlark.UnpackArgs(
					b.Name(), args, kwargs,
					// Positional arguments.
					"attrs?", unpack.Bind(thread, &attrs, unpack.Dict(unpack.StarlarkIdentifier, unpack.Type[*Attr]("attr.*"))),
					// Keyword arguments.
					"doc?", unpack.Bind(thread, &doc, unpack.String),
				); err != nil {
					return nil, err
				}
				return NewTagClass(NewStarlarkTagClassDefinition(attrs)), nil
			},
		),
		"transition": starlark.NewBuiltin(
			"transition",
			func(thread *starlark.Thread, b *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
				if len(args) > 0 {
					return nil, fmt.Errorf("%s: got %d positional arguments, want 0", b.Name(), len(args))
				}
				var implementation NamedFunction
				var inputs []string
				var outputs []string
				if err := starlark.UnpackArgs(
					b.Name(), args, kwargs,
					"implementation", unpack.Bind(thread, &implementation, NamedFunctionUnpackerInto),
					// Don't convert inputs and outputs to labels.
					// The provided strings should not be
					// normalized, as they need to become the keys
					// of the dict provided to the implementation
					// function.
					"inputs", unpack.Bind(thread, &inputs, unpack.List(unpack.String)),
					"outputs", unpack.Bind(thread, &outputs, unpack.List(unpack.String)),
				); err != nil {
					return nil, err
				}
				sort.Strings(inputs)
				sort.Strings(outputs)
				return NewTransition(
					NewUserDefinedTransitionDefinition(
						nil,
						implementation,
						slices.Compact(inputs),
						slices.Compact(outputs),
					),
				), nil
			},
		),
		"visibility": starlark.NewBuiltin(
			"visibility",
			func(thread *starlark.Thread, b *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
				// TODO: Implement!
				return starlark.None, nil
			},
		),
	}
}

func init() {
	for name, builtin := range commonBuiltins {
		BuildFileBuiltins[name] = builtin
		BzlFileBuiltins[name] = builtin
	}
}
