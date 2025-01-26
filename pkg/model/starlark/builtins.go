package starlark

import (
	"errors"
	"fmt"
	"sort"

	pg_label "github.com/buildbarn/bb-playground/pkg/label"
	model_core "github.com/buildbarn/bb-playground/pkg/model/core"
	model_starlark_pb "github.com/buildbarn/bb-playground/pkg/proto/model/starlark"
	"github.com/buildbarn/bb-playground/pkg/starlark/unpack"
	"github.com/buildbarn/bb-playground/pkg/storage/dag"

	"go.starlark.net/lib/json"
	"go.starlark.net/starlark"
	"go.starlark.net/starlarkstruct"
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
	GlobExpanderKey     = "glob_expander"
)

type GlobExpander = func(include, exclude []string, includeDirectories bool) ([]pg_label.TargetName, error)

var commonBuiltins = starlark.StringDict{
	"Label": starlark.NewBuiltin(
		"Label",
		func(thread *starlark.Thread, b *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
			if len(args) != 1 {
				return nil, fmt.Errorf("%s: got %d positional arguments, want 1", b.Name(), len(args))
			}
			var input pg_label.CanonicalLabel
			if err := starlark.UnpackArgs(
				b.Name(), args, kwargs,
				"input", unpack.Bind(thread, &input, NewLabelOrStringUnpackerInto(CurrentFilePackage(thread, 1))),
			); err != nil {
				return nil, err
			}
			return NewLabel(input), nil
		},
	),
	"glob": starlark.NewBuiltin(
		"glob",
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
	"select": starlark.NewBuiltin(
		"select",
		func(thread *starlark.Thread, b *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
			if len(args) != 1 {
				return nil, fmt.Errorf("%s: got %d positional arguments, want 1", b.Name(), len(args))
			}

			var conditions map[pg_label.CanonicalLabel]starlark.Value
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
				if label.GetCanonicalPackage().GetPackagePath() == "conditions" && label.GetTargetName().String() == "default" {
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
			targetRegistrar := thread.Local(TargetRegistrarKey).(*TargetRegistrar)
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
			targetRegistrar.createDefaultInheritableAttrsMetadata = func(index int) dag.ObjectContentsWalker {
				return metadata[index]
			}
			targetRegistrar.setDefaultInheritableAttrs = true
			return starlark.None, nil
		},
	),
}

var BzlFileBuiltins = starlark.StringDict{
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
			var fragments []string
			var provides []*Provider
			var requiredAspectProviders [][]*Provider
			var requiredProviders [][]*Provider
			var toolchains []*ToolchainType
			if err := starlark.UnpackArgs(
				b.Name(), args, kwargs,
				// Positional arguments.
				"implementation", unpack.Bind(thread, &implementation, NamedFunctionUnpackerInto),
				// Keyword arguments.
				"attr_aspects?", unpack.Bind(thread, &attrAspects, unpack.List(unpack.String)),
				"attrs?", unpack.Bind(thread, &attrs, unpack.Dict(unpack.StarlarkIdentifier, unpack.Type[*Attr]("attr.*"))),
				"doc?", unpack.Bind(thread, &doc, unpack.String),
				"fragments?", unpack.Bind(thread, &fragments, unpack.List(unpack.String)),
				"provides?", unpack.Bind(thread, &provides, unpack.List(unpack.Type[*Provider]("provider"))),
				"required_aspect_providers?", unpack.Bind(thread, &requiredAspectProviders, providersListUnpackerInto),
				"required_providers?", unpack.Bind(thread, &requiredProviders, providersListUnpackerInto),
				"toolchains?", unpack.Bind(thread, &toolchains, unpack.List(ToolchainTypeUnpackerInto)),
			); err != nil {
				return nil, err
			}
			return NewAspect(nil, &model_starlark_pb.Aspect_Definition{}), nil
		},
	),
	"attr": starlarkstruct.FromStringDict(starlarkstruct.Default, starlark.StringDict{
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
				var cfg starlark.Value
				var defaultValue starlark.Value = starlark.None
				doc := ""
				executable := false
				var flags []string
				mandatory := false
				providers := [][]*Provider{{}}
				if err := starlark.UnpackArgs(
					b.Name(), args, kwargs,
					"allow_files?", unpack.Bind(thread, &allowFiles, allowFilesUnpackerInto),
					"allow_rules?", unpack.Bind(thread, &allowRules, unpack.List(unpack.String)),
					"allow_single_file?", unpack.Bind(thread, &allowSingleFile, allowFilesUnpackerInto),
					"aspects?", unpack.Bind(thread, &aspects, unpack.List(unpack.Type[*Aspect]("aspect"))),
					"cfg?", &cfg,
					"default?", &defaultValue,
					"doc?", unpack.Bind(thread, &doc, unpack.String),
					"executable?", unpack.Bind(thread, &executable, unpack.Bool),
					"flags?", unpack.Bind(thread, &flags, unpack.List(unpack.String)),
					"mandatory?", unpack.Bind(thread, &mandatory, unpack.Bool),
					"providers?", unpack.Bind(thread, &providers, providersListUnpackerInto),
				); err != nil {
					return nil, err
				}
				if len(allowFiles) > 0 && len(allowSingleFile) > 0 {
					return nil, errors.New("allow_files and allow_single_file cannot be specified at the same time")
				}

				attrType := NewLabelAttrType(!mandatory, len(allowSingleFile) > 0, executable)
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
				var cfg starlark.Value
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
					"cfg?", &cfg,
					"default?", &defaultValue,
					"doc?", unpack.Bind(thread, &doc, unpack.String),
					"mandatory?", unpack.Bind(thread, &mandatory, unpack.Bool),
					"providers?", unpack.Bind(thread, &providers, providersListUnpackerInto),
				); err != nil {
					return nil, err
				}

				attrType := NewLabelKeyedStringDictAttrType()
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
				var cfg starlark.Value
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
					"allow_rules?", unpack.Bind(thread, &allowRules, unpack.List(unpack.String)),
					"aspects?", unpack.Bind(thread, &aspects, unpack.List(unpack.Type[*Aspect]("aspect"))),
					"cfg?", &cfg,
					"default?", &defaultValue,
					"doc?", unpack.Bind(thread, &doc, unpack.String),
					"flags?", unpack.Bind(thread, &flags, unpack.List(unpack.String)),
					"mandatory?", unpack.Bind(thread, &mandatory, unpack.Bool),
					"providers?", unpack.Bind(thread, &providers, providersListUnpackerInto),
				); err != nil {
					return nil, err
				}

				attrType := NewLabelListAttrType()
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

				return NewAttr(OutputAttrType, nil), nil
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
	"config": starlarkstruct.FromStringDict(starlarkstruct.Default, starlark.StringDict{
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
				return NewExecTransitionFactory(execGroup), nil
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
	}),
	"config_common": starlarkstruct.FromStringDict(starlarkstruct.Default, starlark.StringDict{
		"toolchain_type": starlark.NewBuiltin(
			"config_common.toolchain_type",
			func(thread *starlark.Thread, b *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
				if len(args) > 1 {
					return nil, fmt.Errorf("%s: got %d positional arguments, want at most 1", b.Name(), len(args))
				}
				var name pg_label.CanonicalLabel
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
			// TODO!
			return NewLabel(pg_label.MustNewCanonicalLabel("@@foo+")), nil
		},
	),
	"depset": starlark.NewBuiltin(
		"depset",
		func(thread *starlark.Thread, b *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
			if len(args) > 2 {
				return nil, fmt.Errorf("%s: got %d positional arguments, want at most 2", b.Name(), len(args))
			}
			var direct starlark.Value
			order := "default"
			var transitive starlark.Value
			if err := starlark.UnpackArgs(
				b.Name(), args, kwargs,
				// Positional arguments.
				"direct?", &direct,
				// Keyword arguments.
				"fields?", unpack.Bind(thread, &order, unpack.String),
				"init?", &transitive,
			); err != nil {
				return nil, err
			}
			return NewDepset(), nil
		},
	),
	"exec_group": starlark.NewBuiltin(
		"exec_group",
		func(thread *starlark.Thread, b *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
			if len(args) > 0 {
				return nil, fmt.Errorf("%s: got %d positional arguments, want 0", b.Name(), len(args))
			}
			var execCompatibleWith []pg_label.CanonicalLabel
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
	"json": starlarkstruct.FromStringDict(starlarkstruct.Default, json.Module.Members),
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
	"native": starlarkstruct.FromStringDict(starlarkstruct.Default, starlark.StringDict{
		"alias": starlark.NewBuiltin(
			"native.alias",
			func(thread *starlark.Thread, b *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
				targetRegistrar := thread.Local(TargetRegistrarKey).(*TargetRegistrar)
				if targetRegistrar == nil {
					return nil, errors.New("targets cannot be registered from within this context")
				}
				currentPackage := thread.Local(CanonicalPackageKey).(pg_label.CanonicalPackage)

				var name string
				var actual *Select
				deprecation := ""
				var visibility []pg_label.CanonicalLabel
				labelOrStringUnpackerInto := NewLabelOrStringUnpackerInto(currentPackage)
				if err := starlark.UnpackArgs(
					b.Name(), args, kwargs,
					"name", unpack.Bind(thread, &name, unpack.Stringer(unpack.TargetName)),
					"actual", unpack.Bind(thread, &actual, NewSelectUnpackerInto(labelOrStringUnpackerInto)),
					"deprecation?", unpack.Bind(thread, &deprecation, unpack.String),
					"visibility?", unpack.Bind(thread, &visibility, unpack.IfNotNone(unpack.List(labelOrStringUnpackerInto))),
				); err != nil {
					return nil, err
				}

				patcher := model_core.NewReferenceMessagePatcher[dag.ObjectContentsWalker]()
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

				actual.VisitLabels(map[starlark.Value]struct{}{}, func(l pg_label.CanonicalLabel) {
					if l.GetCanonicalPackage() == currentPackage {
						targetRegistrar.registerImplicitTarget(l.GetTargetName().String())
					}
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
		"exports_files": starlark.NewBuiltin(
			"native.exports_files",
			func(thread *starlark.Thread, b *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
				targetRegistrar := thread.Local(TargetRegistrarKey).(*TargetRegistrar)
				if targetRegistrar == nil {
					return nil, errors.New("targets cannot be registered from within this context")
				}
				currentPackage := thread.Local(CanonicalPackageKey).(pg_label.CanonicalPackage)

				if len(args) != 1 {
					return nil, fmt.Errorf("%s: got %d positional arguments, want at 1", b.Name(), len(args))
				}
				var targetNames []string
				var visibility []pg_label.CanonicalLabel
				if err := starlark.UnpackArgs(
					b.Name(), args, kwargs,
					// Positional arguments.
					"target_names", unpack.Bind(thread, &targetNames, unpack.List(unpack.Stringer(unpack.TargetName))),
					// Keyword arguments.
					"visibility?", unpack.Bind(thread, &visibility, unpack.List(NewLabelOrStringUnpackerInto(currentPackage))),
				); err != nil {
					return nil, err
				}

				for _, targetName := range targetNames {
					// TODO: Only create the package group once.
					visibilityPackageGroup, err := targetRegistrar.getVisibilityPackageGroup(visibility)
					if err != nil {
						return nil, err
					}

					if err := targetRegistrar.registerExplicitTarget(
						targetName,
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
				var input pg_label.CanonicalLabel
				if err := starlark.UnpackArgs(
					b.Name(), args, kwargs,
					"input", unpack.Bind(thread, &input, NewLabelOrStringUnpackerInto(canonicalPackage.(pg_label.CanonicalPackage))),
				); err != nil {
					return nil, err
				}
				return NewLabel(input), nil
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
			var fields starlark.Value
			var init *NamedFunction
			if err := starlark.UnpackArgs(
				b.Name(), args, kwargs,
				// Positional arguments.
				"doc?", unpack.Bind(thread, &doc, unpack.String),
				// Keyword arguments.
				"fields?", &fields,
				"init?", unpack.Bind(thread, &init, unpack.Pointer(NamedFunctionUnpackerInto)),
			); err != nil {
				return nil, err
			}
			provider := NewProvider(nil, init)
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
			var execCompatibleWith []pg_label.CanonicalLabel
			var fragments []string
			var hostFragments []string
			var initializer *NamedFunction
			outputs := map[string]string{}
			var provides []*Provider
			var subrules starlark.Value
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
				"outputs?", unpack.Bind(thread, &outputs, unpack.Dict(unpack.String, unpack.String)),
				"provides?", unpack.Bind(thread, &provides, unpack.List(unpack.Type[*Provider]("provider"))),
				"subrules?", &subrules,
				"test?", unpack.Bind(thread, &test, unpack.Bool),
				"toolchains?", unpack.Bind(thread, &toolchains, unpack.List(ToolchainTypeUnpackerInto)),
			); err != nil {
				return nil, err
			}

			if _, ok := execGroups[""]; ok {
				return nil, errors.New("cannot explicitly declare exec_group with name \"\"")
			}
			execGroups[""] = NewExecGroup(execCompatibleWith, toolchains)

			return NewRule(nil, NewStarlarkRuleDefinition(
				attrs,
				buildSetting,
				cfg,
				execGroups,
				implementation,
				initializer,
				provides,
			)), nil
		},
	),
	"struct": starlark.NewBuiltin("struct", starlarkstruct.Make),
	"subrule": starlark.NewBuiltin(
		"subrule",
		func(thread *starlark.Thread, b *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
			if len(args) > 1 {
				return nil, fmt.Errorf("%s: got %d positional arguments, want at most 1", b.Name(), len(args))
			}
			var implementation NamedFunction
			attrs := map[pg_label.StarlarkIdentifier]*Attr{}
			var fragments []string
			var toolchains []*ToolchainType
			if err := starlark.UnpackArgs(
				b.Name(), args, kwargs,
				// Positional arguments.
				"implementation", unpack.Bind(thread, &implementation, NamedFunctionUnpackerInto),
				// Keyword arguments.
				"attrs?", unpack.Bind(thread, &attrs, unpack.Dict(unpack.StarlarkIdentifier, unpack.Type[*Attr]("attr.*"))),
				"fragments?", unpack.Bind(thread, &fragments, unpack.List(unpack.String)),
				"toolchains?", unpack.Bind(thread, &toolchains, unpack.List(ToolchainTypeUnpackerInto)),
			); err != nil {
				return nil, err
			}
			return NewSubrule(nil, &model_starlark_pb.Subrule_Definition{}), nil
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
			var inputs []pg_label.CanonicalLabel
			var outputs []pg_label.CanonicalLabel
			canonicalLabelListUnpackerInto := unpack.List(NewLabelOrStringUnpackerInto(CurrentFilePackage(thread, 1)))
			if err := starlark.UnpackArgs(
				b.Name(), args, kwargs,
				"implementation", unpack.Bind(thread, &implementation, NamedFunctionUnpackerInto),
				"inputs", unpack.Bind(thread, &inputs, canonicalLabelListUnpackerInto),
				"outputs", unpack.Bind(thread, &outputs, canonicalLabelListUnpackerInto),
			); err != nil {
				return nil, err
			}
			return NewTransition(nil, &model_starlark_pb.Transition_Definition{}), nil
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

func init() {
	for name, builtin := range commonBuiltins {
		BuildFileBuiltins[name] = builtin
		BzlFileBuiltins[name] = builtin
	}
}
