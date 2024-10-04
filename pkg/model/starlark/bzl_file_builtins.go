package starlark

import (
	"fmt"

	pg_label "github.com/buildbarn/bb-playground/pkg/label"
	"github.com/buildbarn/bb-playground/pkg/starlark/unpack"

	"go.starlark.net/starlark"
)

var BzlFileBuiltins = starlark.StringDict{
	"_builtins": NewStruct(map[string]starlark.Value{
		"internal": NewStruct(map[string]starlark.Value{
			"CcNativeLibraryInfo": NewProviderValue(
				pg_label.MustNewCanonicalLabel("@@bazel_tools+//foo:ccnative.bzl"),
				"CcNativeLibraryInfo",
				nil,
			),
			"cc_common": NewStruct(map[string]starlark.Value{}),
			"cc_internal": NewStruct(map[string]starlark.Value{
				"empty_compilation_outputs": NewIndirectFunction(
					pg_label.MustNewCanonicalLabel("@@bazel_tools+//foo:emptycomp.bzl"),
					"empty_compilation_outputs",
				),
			}),
		}),
		"toplevel": NewStruct(map[string]starlark.Value{
			"CcInfo": NewProviderValue(
				pg_label.MustNewCanonicalLabel("@@bazel_tools+//foo:ccinfo.bzl"),
				"CcInfo",
				nil,
			),
			"PackageSpecificationInfo": NewProviderValue(
				pg_label.MustNewCanonicalLabel("@@bazel_tools+//foo:packagespecinfo.bzl"),
				"PackageSpecificationInfo",
				nil,
			),
			"ProguardSpecProvider": NewProviderValue(
				pg_label.MustNewCanonicalLabel("@@bazel_tools+//foo:proguardspec.bzl"),
				"ProguardSpecProvider",
				nil,
			),
			"config_common": NewStruct(map[string]starlark.Value{
				"toolchain_type": starlark.NewBuiltin(
					"_builtins.toplevel.config_common.toolchain_type",
					func(thread *starlark.Thread, b *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
						return starlark.String("TODO"), nil
					},
				),
			}),
			"coverage_common": NewStruct(map[string]starlark.Value{}),
			"proto_common": NewStruct(map[string]starlark.Value{
				"incompatible_enable_proto_toolchain_resolution": starlark.NewBuiltin(
					"_builtins.toplevel.proto_common.incompatible_enable_proto_toolchain_resolution",
					func(thread *starlark.Thread, b *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
						return starlark.True, nil
					},
				),
			}),
		}),
	}),
	"CcInfo": NewProviderValue(
		pg_label.MustNewCanonicalLabel("@@bazel_tools+//foo:ccinfo.bzl"),
		"CcInfo",
		nil,
	),
	"DefaultInfo": NewProviderValue(
		pg_label.MustNewCanonicalLabel("@@bazel_tools+//foo:defaultinfo.bzl"),
		"DefaultInfo",
		nil,
	),
	"Label": starlark.NewBuiltin(
		"Label",
		func(thread *starlark.Thread, b *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
			if len(args) < 1 {
				return nil, fmt.Errorf("%s: got %d positional arguments, want at least 1", b.Name(), len(args))
			}
			var input pg_label.CanonicalLabel
			if err := starlark.UnpackArgs(
				b.Name(), args, kwargs,
				"input", unpack.Bind(thread, &input, UnpackCanonicalLabel),
			); err != nil {
				return nil, err
			}
			return NewLabel(input), nil
		},
	),
	"OutputGroupInfo": NewProviderValue(
		pg_label.MustNewCanonicalLabel("@@bazel_tools+//foo:outputgroup.bzl"),
		"OutputGroupInfo",
		nil,
	),
	"RunEnvironmentInfo": NewProviderValue(
		pg_label.MustNewCanonicalLabel("@@bazel_tools+//foo:runenvir.bzl"),
		"RunEnvironmentInfo",
		nil,
	),
	"apple_common": NewStruct(map[string]starlark.Value{}),
	"aspect": starlark.NewBuiltin(
		"aspect",
		func(thread *starlark.Thread, b *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
			panic("TODO")
		},
	),
	"attr": NewStruct(map[string]starlark.Value{
		"bool": starlark.NewBuiltin(
			"attr.bool",
			func(thread *starlark.Thread, b *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
				if len(args) > 0 {
					return nil, fmt.Errorf("%s: got %d positional arguments, want 0", b.Name(), len(args))
				}
				defaultValue := false
				doc := ""
				mandatory := false
				if err := starlark.UnpackArgs(
					b.Name(), args, kwargs,
					"default?", unpack.Bind(thread, &defaultValue, unpack.Bool),
					"doc?", unpack.Bind(thread, &doc, unpack.String),
					"mandatory?", unpack.Bind(thread, &mandatory, unpack.Bool),
				); err != nil {
					return nil, err
				}
				return NewAttr(NewBoolAttrType(defaultValue), mandatory), nil
			},
		),
		"int": starlark.NewBuiltin(
			"attr.int",
			func(thread *starlark.Thread, b *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
				if len(args) > 0 {
					return nil, fmt.Errorf("%s: got %d positional arguments, want 0", b.Name(), len(args))
				}
				defaultValue := int32(0)
				doc := ""
				mandatory := false
				var values []int32
				if err := starlark.UnpackArgs(
					b.Name(), args, kwargs,
					"default?", unpack.Bind(thread, &defaultValue, unpack.Int),
					"doc?", unpack.Bind(thread, &doc, unpack.String),
					"mandatory?", unpack.Bind(thread, &mandatory, unpack.Bool),
					"values?", unpack.Bind(thread, &values, unpack.List[int32](unpack.Int)),
				); err != nil {
					return nil, err
				}
				return NewAttr(NewIntAttrType(defaultValue), mandatory), nil
			},
		),
		"label": starlark.NewBuiltin(
			"attr.label",
			func(thread *starlark.Thread, b *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
				if len(args) > 0 {
					return nil, fmt.Errorf("%s: got %d positional arguments, want 0", b.Name(), len(args))
				}
				var allowFiles starlark.Value
				var allowSingleFile starlark.Value
				var cfg starlark.Value
				var defaultValue *pg_label.CanonicalLabel
				doc := ""
				executable := false
				var flags []string
				mandatory := false
				var providers starlark.Value
				if err := starlark.UnpackArgs(
					b.Name(), args, kwargs,
					"allow_files?", &allowFiles,
					"allow_single_file?", &allowSingleFile,
					"cfg?", &cfg,
					"default?", unpack.Bind(thread, &defaultValue, unpack.IfNotNone(unpack.Pointer(UnpackCanonicalLabel))),
					"doc?", unpack.Bind(thread, &doc, unpack.String),
					"executable?", unpack.Bind(thread, &executable, unpack.Bool),
					"flags?", unpack.Bind(thread, &flags, unpack.List(unpack.String)),
					"mandatory?", unpack.Bind(thread, &mandatory, unpack.Bool),
					"providers?", &providers,
				); err != nil {
					return nil, err
				}
				return NewAttr(NewLabelAttrType(defaultValue), mandatory), nil
			},
		),
		"label_keyed_string_dict": starlark.NewBuiltin(
			"attr.label_keyed_string_dict",
			func(thread *starlark.Thread, b *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
				if len(args) > 1 {
					return nil, fmt.Errorf("%s: got %d positional arguments, want at most 1", b.Name(), len(args))
				}
				allowEmpty := true
				var allowFiles starlark.Value
				var cfg starlark.Value
				var defaultValues map[pg_label.CanonicalLabel]string
				doc := ""
				mandatory := false
				var providers starlark.Value
				if err := starlark.UnpackArgs(
					b.Name(), args, kwargs,
					// Positional arguments.
					"allow_empty?", unpack.Bind(thread, &allowEmpty, unpack.Bool),
					// Keyword arguments.
					"allow_files?", &allowFiles,
					"cfg?", &cfg,
					"default?", unpack.Bind(thread, &defaultValues, unpack.Dict(UnpackCanonicalLabel, unpack.String)),
					"doc?", unpack.Bind(thread, &doc, unpack.String),
					"mandatory?", unpack.Bind(thread, &mandatory, unpack.Bool),
					"providers?", &providers,
				); err != nil {
					return nil, err
				}
				return NewAttr(NewLabelKeyedStringDictAttrType(allowEmpty, defaultValues), mandatory), nil
			},
		),
		"label_list": starlark.NewBuiltin(
			"attr.label_list",
			func(thread *starlark.Thread, b *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
				if len(args) > 1 {
					return nil, fmt.Errorf("%s: got %d positional arguments, want at most 1", b.Name(), len(args))
				}
				allowEmpty := true
				var allowFiles starlark.Value
				var allowRules []string
				var cfg starlark.Value
				var defaultValues []pg_label.CanonicalLabel
				doc := ""
				var flags []string
				mandatory := false
				var providers starlark.Value
				if err := starlark.UnpackArgs(
					b.Name(), args, kwargs,
					// Positional arguments.
					"allow_empty?", unpack.Bind(thread, &allowEmpty, unpack.Bool),
					// Keyword arguments.
					"allow_files?", &allowFiles,
					"allow_rules?", unpack.Bind(thread, &allowRules, unpack.List(unpack.String)),
					"cfg?", &cfg,
					"default?", unpack.Bind(thread, &defaultValues, unpack.List(UnpackCanonicalLabel)),
					"doc?", unpack.Bind(thread, &doc, unpack.String),
					"flags?", unpack.Bind(thread, &flags, unpack.List(unpack.String)),
					"mandatory?", unpack.Bind(thread, &mandatory, unpack.Bool),
					"providers?", &providers,
				); err != nil {
					return nil, err
				}
				return NewAttr(NewLabelListAttrType(allowEmpty, defaultValues), mandatory), nil
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
				return NewAttr(OutputAttrType, mandatory), nil
			},
		),
		"string": starlark.NewBuiltin(
			"attr.string",
			func(thread *starlark.Thread, b *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
				if len(args) > 0 {
					return nil, fmt.Errorf("%s: got %d positional arguments, want 0", b.Name(), len(args))
				}
				defaultValue := ""
				doc := ""
				mandatory := false
				var values []string
				if err := starlark.UnpackArgs(
					b.Name(), args, kwargs,
					"default?", unpack.Bind(thread, &defaultValue, unpack.String),
					"doc?", unpack.Bind(thread, &doc, unpack.String),
					"mandatory?", unpack.Bind(thread, &mandatory, unpack.Bool),
					"values?", unpack.Bind(thread, &values, unpack.List(unpack.String)),
				); err != nil {
					return nil, err
				}
				return NewAttr(NewStringAttrType(defaultValue), mandatory), nil
			},
		),
		"string_dict": starlark.NewBuiltin(
			"attr.string_dict",
			func(thread *starlark.Thread, b *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
				if len(args) > 1 {
					return nil, fmt.Errorf("%s: got %d positional arguments, want at most 2", b.Name(), len(args))
				}
				var allowEmpty bool
				var defaultValues map[string]string
				doc := ""
				mandatory := false
				if err := starlark.UnpackArgs(
					b.Name(), args, kwargs,
					// Positional arguments.
					"allow_empty?", unpack.Bind(thread, &allowEmpty, unpack.Bool),
					// Keyword arguments.
					"default?", unpack.Bind(thread, &defaultValues, unpack.Dict(unpack.String, unpack.String)),
					"doc?", unpack.Bind(thread, &doc, unpack.String),
					"mandatory?", unpack.Bind(thread, &mandatory, unpack.Bool),
				); err != nil {
					return nil, err
				}
				return NewAttr(NewStringDictAttrType(defaultValues), mandatory), nil
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
				var defaultValues []string
				doc := ""
				if err := starlark.UnpackArgs(
					b.Name(), args, kwargs,
					// Positional arguments.
					"mandatory?", unpack.Bind(thread, &mandatory, unpack.Bool),
					"allow_empty?", unpack.Bind(thread, &allowEmpty, unpack.Bool),
					// Keyword arguments.
					"default?", unpack.Bind(thread, &defaultValues, unpack.List(unpack.String)),
					"doc?", unpack.Bind(thread, &doc, unpack.String),
				); err != nil {
					return nil, err
				}
				return NewAttr(NewStringListAttrType(defaultValues), mandatory), nil
			},
		),
	}),
	"cc_common": NewStruct(map[string]starlark.Value{}),
	"config": NewStruct(map[string]starlark.Value{
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
	"config_common": NewStruct(map[string]starlark.Value{}),
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
	"coverage_common": NewStruct(map[string]starlark.Value{}),
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
			var toolchains []pg_label.CanonicalLabel
			if err := starlark.UnpackArgs(
				b.Name(), args, kwargs,
				"toolchains?", unpack.Bind(thread, &toolchains, unpack.List(UnpackCanonicalLabel)),
			); err != nil {
				return nil, err
			}
			return NewExecGroup(), nil
		},
	),
	"json": NewStruct(map[string]starlark.Value{
		"decode": starlark.NewBuiltin(
			"json.decode",
			func(thread *starlark.Thread, b *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
				panic("TODO")
			},
		),
		"encode": starlark.NewBuiltin(
			"json.encode",
			func(thread *starlark.Thread, b *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
				panic("TODO")
			},
		),
		"encode_indent": starlark.NewBuiltin(
			"json.encode_indent",
			func(thread *starlark.Thread, b *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
				panic("TODO")
			},
		),
		"indent": starlark.NewBuiltin(
			"indent",
			func(thread *starlark.Thread, b *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
				panic("TODO")
			},
		),
	}),
	"native": starlark.NewBuiltin(
		"native",
		func(thread *starlark.Thread, b *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
			panic("TODO")
		},
	),
	"platform_common": NewStruct(map[string]starlark.Value{}),
	"provider": starlark.NewBuiltin(
		"provider",
		func(thread *starlark.Thread, b *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
			if len(args) > 1 {
				return nil, fmt.Errorf("%s: got %d positional arguments, want at most 1", b.Name(), len(args))
			}
			doc := ""
			var fields starlark.Value
			var init starlark.Value = starlark.None
			if err := starlark.UnpackArgs(
				b.Name(), args, kwargs,
				// Positional arguments.
				"doc?", unpack.Bind(thread, &doc, unpack.String),
				// Keyword arguments.
				"fields?", &fields,
				"init?", &init,
			); err != nil {
				return nil, err
			}
			filename := pg_label.MustNewCanonicalLabel(thread.CallFrame(1).Pos.Filename())
			if init == starlark.None {
				return NewProviderValue(filename, "foo", nil), nil
			}
			return starlark.Tuple{
				NewProviderValue(filename, "foo", init),
				NewProviderValue(filename, "foo", nil),
			}, nil
		},
	),
	"repository_rule": starlark.NewBuiltin(
		"repository_rule",
		func(thread *starlark.Thread, b *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
			if len(args) > 1 {
				return nil, fmt.Errorf("%s: got %d positional arguments, want at most 1", b.Name(), len(args))
			}
			var implementation starlark.Value
			if err := starlark.UnpackArgs(
				b.Name(), args, kwargs,
				// Positional arguments.
				"implementation", &implementation,
				// Keyword arguments.
				// TODO.
			); err != nil {
				return nil, err
			}
			// TODO.
			filename := pg_label.MustNewCanonicalLabel(thread.CallFrame(1).Pos.Filename())
			return NewRepositoryRuleValue(filename, "foo"), nil
		},
	),
	"rule": starlark.NewBuiltin(
		"rule",
		func(thread *starlark.Thread, b *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
			if len(args) > 1 {
				return nil, fmt.Errorf("%s: got %d positional arguments, want at most 1", b.Name(), len(args))
			}
			var implementation starlark.Value
			var attrs starlark.Value
			var buildSetting starlark.Value
			doc := ""
			executable := false
			var fragments []string
			var provides starlark.Value
			test := false
			var toolchains starlark.Value
			if err := starlark.UnpackArgs(
				b.Name(), args, kwargs,
				// Positional arguments.
				"implementation", &implementation,
				// Keyword arguments.
				"attrs?", &attrs,
				"build_setting?", &buildSetting,
				"doc?", unpack.Bind(thread, &doc, unpack.String),
				"executable?", unpack.Bind(thread, &executable, unpack.Bool),
				"fragments?", unpack.Bind(thread, &fragments, unpack.List(unpack.String)),
				"provides?", &provides,
				"test?", unpack.Bind(thread, &test, unpack.Bool),
				"toolchains?", &toolchains,
			); err != nil {
				return nil, err
			}
			// TODO
			filename := pg_label.MustNewCanonicalLabel(thread.CallFrame(1).Pos.Filename())
			return NewRuleValue(filename, "foo"), nil
		},
	),
	"select": starlark.NewBuiltin(
		"select",
		func(thread *starlark.Thread, b *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
			panic("TODO")
		},
	),
	"struct": starlark.NewBuiltin(
		"struct",
		func(thread *starlark.Thread, b *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
			if len(args) > 0 {
				return nil, fmt.Errorf("%s: got %d positional arguments, want 0", b.Name(), len(args))
			}
			fields := make(map[string]starlark.Value, len(kwargs))
			for _, kwarg := range kwargs {
				fields[string(kwarg[0].(starlark.String))] = kwarg[1]
			}
			return NewStruct(fields), nil
		},
	),
	"subrule": starlark.NewBuiltin(
		"subrule",
		func(thread *starlark.Thread, b *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
			if len(args) > 1 {
				return nil, fmt.Errorf("%s: got %d positional arguments, want at most 1", b.Name(), len(args))
			}
			var implementation starlark.Value
			var attrs starlark.Value
			var fragments []string
			var toolchains starlark.Value
			if err := starlark.UnpackArgs(
				b.Name(), args, kwargs,
				// Positional arguments.
				"implementation", &implementation,
				// Keyword arguments.
				"attrs?", &attrs,
				"fragments?", unpack.Bind(thread, &fragments, unpack.List(unpack.String)),
				"toolchains?", &toolchains,
			); err != nil {
				return nil, err
			}
			// TODO
			filename := pg_label.MustNewCanonicalLabel(thread.CallFrame(1).Pos.Filename())
			return NewSubruleValue(filename, "foo"), nil
		},
	),
	"transition": starlark.NewBuiltin(
		"transition",
		func(thread *starlark.Thread, b *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
			if len(args) > 0 {
				return nil, fmt.Errorf("%s: got %d positional arguments, want 0", b.Name(), len(args))
			}
			var implementation starlark.Value
			var inputs []pg_label.CanonicalLabel
			var outputs []pg_label.CanonicalLabel
			if err := starlark.UnpackArgs(
				b.Name(), args, kwargs,
				"implementation", &implementation,
				"inputs", unpack.Bind(thread, &inputs, unpack.List(UnpackCanonicalLabel)),
				"outputs", unpack.Bind(thread, &outputs, unpack.List(UnpackCanonicalLabel)),
			); err != nil {
				return nil, err
			}
			return NewTransitionValue(implementation, inputs, outputs), nil
		},
	),
}
