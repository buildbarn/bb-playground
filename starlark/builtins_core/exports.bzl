AnalysisFailureInfo = provider()
AnalysisTestResultInfo = provider()
CcLauncherInfo = provider()
CcToolchainConfigInfo = provider()
CcToolchainInfo = provider()
ConfigSettingInfo = provider()
ConstraintSettingInfo = provider()
ConstraintValueInfo = provider()
DebugPackageInfo = provider()
DeclaredToolchainInfo = provider()
ExecutionInfo = provider()
FilesToRunProvider = provider()
InstrumentedFilesInfo = provider()
OutputGroupInfo = provider(dict_like = True)
PackageSpecificationInfo = provider()
PlatformInfo = provider()
ProguardSpecProvider = provider()
PyInfo = provider()
RunEnvironmentInfo = provider()
StaticallyLinkedMarkerProvider = provider()
ToolchainInfo = provider()
ToolchainTypeInfo = provider()

def _cc_info_init(
        *,
        cc_native_library_info = None,
        compilation_context = None,
        debug_context = None,
        linking_context = None):
    if not debug_context:
        debug_context = builtins_internal_cc_common_create_debug_context()
    transitive_native_libraries = cc_native_library_info.libraries_to_link if cc_native_library_info else depset()
    return {
        "debug_context": lambda: debug_context,
        "compilation_context": compilation_context or builtins_internal_cc_common_create_compilation_context(),
        "linking_context": linking_context or builtins_internal_cc_common_create_linking_context(),
        "transitive_native_libraries": lambda: transitive_native_libraries,
    }

CcInfo, _CcInfoRaw = provider(init = _cc_info_init)

def _cc_native_library_info_init(*, libraries_to_link = None):
    return {
        "libraries_to_link": libraries_to_link or depset(),
    }

CcNativeLibraryInfo, _CcNativeLibraryInfoRaw = provider(init = _cc_native_library_info_init)

def _default_info_init(*, data_runfiles = None, default_runfiles = None, executable = None, files = None, runfiles = None):
    if runfiles:
        if data_runfiles or default_runfiles:
            fail("cannot specify \"runfiles\" together with \"data_runfiles\" or \"default_runfiles\"")
        default_runfiles = runfiles

    return {
        "data_runfiles": data_runfiles,
        "default_runfiles": default_runfiles,
        "files": files,
        "files_to_run": FilesToRunProvider(
            executable = executable,
            repo_mapping_manifest = None,
            runfiles_manifest = None,
        ),
    }

DefaultInfo, _DefaultInfoRaw = provider(init = _default_info_init)

def _template_variable_info_init(variables):
    return {"variables": variables}

TemplateVariableInfo, _TemplateVariableInfoRaw = provider(init = _template_variable_info_init)

def _cc_proto_library_impl(ctx):
    fail("TODO")

cc_proto_library = rule(
    implementation = _cc_proto_library_impl,
    attrs = {
        "deps": attr.label_list(),
    },
)

def cc_toolchain_suite(**kwargs):
    pass

def _config_setting_impl(ctx):
    # TODO: Implement.
    return []

config_setting = rule(
    implementation = _config_setting_impl,
    attrs = {
        "constraint_values": attr.label_list(providers = [ConstraintValueInfo]),
        "define_values": attr.string_dict(),
        "flag_values": attr.label_keyed_string_dict(),
        "values": attr.string_dict(),
    },
    provides = [ConfigSettingInfo],
)

def _constraint_setting_impl(ctx):
    default_constraint_value = ctx.attr.default_constraint_value
    return [ConstraintSettingInfo(
        default_constraint_value = ctx.attr.default_constraint_value.label if ctx.attr.default_constraint_value else None,
        has_default_constraint_value = bool(ctx.attr.default_constraint_value),
        label = ctx.label,
    )]

constraint_setting = rule(
    implementation = _constraint_setting_impl,
    attrs = {
        "default_constraint_value": attr.label(
            # Prevent cyclic dependency between the constraint_setting()
            # and the default constraint_value().
            cfg = config.unconfigured(),
            providers = [ConstraintValueInfo],
        ),
    },
    provides = [ConstraintSettingInfo],
)

def _constraint_value_impl(ctx):
    return [ConstraintValueInfo(
        constraint = ctx.attr.constraint_setting[ConstraintSettingInfo],
        label = ctx.label,
    )]

constraint_value = rule(
    implementation = _constraint_value_impl,
    attrs = {
        "constraint_setting": attr.label(
            mandatory = True,
            providers = [ConstraintSettingInfo],
        ),
    },
    provides = [ConstraintValueInfo],
)

def _filegroup_impl(ctx):
    files = []
    runfiles = []
    if ctx.attr.output_group:
        for src in ctx.attr.srcs:
            files.append(getattr(src[OutputGroupInfo], ctx.attr.output_group))
    else:
        for src in ctx.attr.srcs:
            default_info = src[DefaultInfo]
            files.append(default_info.files)
            runfiles.append(default_info.default_runfiles)

    for data in ctx.attr.data:
        runfiles.append(data[DefaultInfo].default_runfiles)

    return [DefaultInfo(
        files = depset(direct = [], transitive = files),
        runfiles = ctx.runfiles(
            files = ctx.files.data,
        ).merge_all(runfiles),
    )]

filegroup = rule(
    implementation = _filegroup_impl,
    attrs = {
        "data": attr.label_list(allow_files = True),
        "output_group": attr.string(),
        "srcs": attr.label_list(allow_files = True),
    },
)

def _genrule_impl(ctx):
    fail("TODO")

genrule = rule(
    implementation = _genrule_impl,
    attrs = {
        "cmd": attr.string(),
        "cmd_bash": attr.string(),
        "cmd_bat": attr.string(),
        "cmd_ps": attr.string(),
        "executable": attr.bool(),
        "local": attr.bool(),
        "message": attr.string(),
        "output_licenses": attr.string_list(),
        "output_to_bindir": attr.bool(),
        "outs": attr.output_list(mandatory = True),
        "srcs": attr.label_list(allow_files = True),
        "toolchains": attr.label_list(),
        "tools": attr.label_list(allow_files = True),
    },
)

def _java_proto_library_impl(ctx):
    fail("TODO")

java_proto_library = rule(
    implementation = _java_proto_library_impl,
    attrs = {
        "deps": attr.label_list(),
    },
)

def licenses(license_types):
    # This function is deprecated. Licenses can nowadays be attached in
    # the form of metadata. Provide a no-op stub.
    pass

def _platform_impl(ctx):
    # Convert all constraint values to a dict mapping the constraint
    # setting to the corresponding value.
    constraints = {}
    for value in ctx.attr.constraint_values:
        value_info = value[ConstraintValueInfo]
        setting_label = value_info.constraint.label
        value_label = value_info.label
        if setting_label in constraints:
            fail("constraint_values contains multiple values for constraint setting %s: %s and %s" % (
                setting_label,
                constraints[setting_label],
                value_label,
            ))

        # If the value is equal to the setting's default value, we store
        # None. This allows us to filter them out later, as there is no
        # point in tracking these.
        if value_label == value_info.constraint.default_constraint_value:
            value_label = None
        constraints[setting_label] = value_label

    exec_pkix_public_key = ctx.attr.exec_pkix_public_key
    repository_os_arch = ctx.attr.repository_os_arch
    repository_os_environ = ctx.attr.repository_os_environ
    repository_os_name = ctx.attr.repository_os_name

    # Inherit properties from the parent platform.
    if ctx.attr.parents:
        if len(ctx.attr.parents) != 1:
            fail("providing multiple parents is not supported")
        parent = ctx.attr.parents[0][PlatformInfo]
        constraints = parent.constraints | constraints
        exec_pkix_public_key = exec_pkix_public_key or parent.exec_pkix_public_key
        repository_os_arch = repository_os_arch or parent.repository_os_arch
        repository_os_environ = repository_os_environ or parent.repository_os_environ
        repository_os_name = repository_os_name or parent.repository_os_name

    return [PlatformInfo(
        constraints = {
            setting: value
            for setting, value in constraints.items()
            if value
        },
        exec_pkix_public_key = exec_pkix_public_key,
        repository_os_arch = repository_os_arch,
        repository_os_environ = repository_os_environ,
        repository_os_name = repository_os_name,
    )]

platform = rule(
    implementation = _platform_impl,
    attrs = {
        "constraint_values": attr.label_list(
            doc = """
            The combination of constraint choices that this platform
            comprises. In order for a platform to apply to a given
            environment, the environment must have at least the values
            in this list.

            Each constraint_value in this list must be for a different
            constraint_setting. For example, you cannot define a
            platform that requires the cpu architecture to be both
            @platforms//cpu:x86_64 and @platforms//cpu:arm.
            """,
            providers = [ConstraintValueInfo],
        ),
        "exec_pkix_public_key": attr.string(
            doc = """
            When the platform is used for execution, the X25519 public
            key in PKIX form that identifies the execution platform. The
            key needs to be provided in base64 encoded form, without the
            PEM header/footer.
            """,
        ),
        "parents": attr.label_list(
            doc = """
            The label of a platform target that this platform should
            inherit from. Although the attribute takes a list, there
            should be no more than one platform present. Any
            constraint_settings not set directly on this platform will
            be found in the parent platform. See the section on Platform
            Inheritance for details.
            """,
            providers = [PlatformInfo],
        ),
        "repository_os_arch": attr.string(
            doc = """
            If this platform is used as a platform for executing
            commands as part of module extensions or repository rules,
            the name of the architecture to announce via
            repository_os.arch.

            This attribute should match the value of the "os.arch" Java
            property converted to lower case (e.g., "aarch64" for ARM64,
            "amd64" for x86-64, "x86" for x86-32).
            """,
        ),
        "repository_os_environ": attr.string_dict(
            doc = """
            If this platform is used as a platform for executing
            commands as part of module extensions or repository rules,
            environment variables to announce via repository_os.environ.
            """,
        ),
        "repository_os_name": attr.string(
            doc = """
            If this platform is used as a platform for executing
            commands as part of module extensions or repository rules,
            the operating system name to announce via
            repository_os.name.

            This attribute should match the value of the "os.name" Java
            property converted to lower case (e.g., "linux", "mac os x",
            "windows 10").
            """,
        ),
    },
    provides = [PlatformInfo],
)

def _sh_test_impl(ctx):
    fail("TODO: implement")

sh_test = rule(
    _sh_test_impl,
    attrs = {
        "data": attr.label_list(allow_files = True),
        "deps": attr.label_list(),
        "srcs": attr.label_list(allow_files = True),
    },
    test = True,
)

def _test_suite_impl(ctx):
    fail("TODO: implement")

test_suite = rule(
    _test_suite_impl,
)

def _toolchain_impl(ctx):
    return [DeclaredToolchainInfo(
        target_settings = [
            target_setting.label
            for target_setting in ctx.attr.target_settings
        ],
        toolchain = ctx.attr.toolchain.label,
        toolchain_type = ctx.attr.toolchain_type[ToolchainTypeInfo].type_label,
    )]

toolchain = rule(
    implementation = _toolchain_impl,
    attrs = {
        "target_settings": attr.label_list(
            providers = [ConfigSettingInfo],
        ),
        "toolchain": attr.label(
            # Prevent configuring toolchains that are not used.
            cfg = config.unconfigured(),
            mandatory = True,
            providers = [ToolchainInfo],
        ),
        "toolchain_type": attr.label(
            mandatory = True,
            providers = [ToolchainTypeInfo],
        ),
    },
    provides = [DeclaredToolchainInfo],
)

def _toolchain_type_impl(ctx):
    return [ToolchainTypeInfo(
        type_label = ctx.label,
    )]

toolchain_type = rule(
    implementation = _toolchain_type_impl,
    provides = [ToolchainTypeInfo],
)

def coverage_common_instrumented_files_info(
        ctx,
        *,
        coverage_environment = {},
        coverage_support_files = [],
        dependency_attributes = [],
        extensions = None,
        metadata_files = [],
        reported_to_actual_sources = None,
        source_attributes = []):
    return InstrumentedFilesInfo(
        # TODO: instrumented_files.
        metadata_files = depset(metadata_files),
    )

def proto_common_do_not_use_external_proto_infos():
    return []

def proto_common_do_not_use_incompatible_enable_proto_toolchain_resolution():
    # This option be controlled by command line option
    # --incompatible_enable_proto_toolchain_resolution.
    return False

def builtins_internal_apple_common_dotted_version(v):
    # TODO: Provide a proper implementation.
    return v

def builtins_internal_cc_common_action_is_enabled(*, feature_configuration, action_name):
    return action_name in ["c++-link-executable", "strip"]

def builtins_internal_cc_common_check_private_api(allowlist = []):
    pass

def _create_compilation_outputs(objects, pic_objects, lto_compilation_context):
    # TODO: Where do we get these from?
    dwo_files = depset()
    pic_dwo_files = depset()

    return struct(
        _dwo_files = dwo_files,
        _objects = objects,
        _pic_dwo_files = pic_dwo_files,
        _pic_objects = pic_objects,
        lto_compilation_context = lambda: lto_compilation_context,

        # We unfortunately also need to provide access to these in the
        # form of lists.
        dwo_files = lambda: dwo_files.to_list(),
        objects = objects.to_list(),
        pic_dwo_files = lambda: pic_dwo_files.to_list(),
        pic_objects = pic_objects.to_list(),

        # TODO: Where do these come from?
        files_to_compile = lambda parse_headers = False, use_pic = False: depset(),
        gcno_files = lambda: [],
        header_tokens = lambda: [],
        module_files = lambda: [],
        pic_gcno_files = lambda: [],
        temps = lambda: depset(),
    )

def builtins_internal_cc_common_compile(
        *,
        actions,
        cc_toolchain,
        feature_configuration,
        name,
        additional_exported_hdrs = None,
        additional_include_scanning_roots = [],
        additional_inputs = [],
        additional_module_maps = [],
        code_coverage_enabled = None,
        compilation_contexts = [],
        conly_flags = [],
        copts_filter = None,
        cxx_flags = [],
        defines = [],
        disallow_nopic_outputs = False,
        disallow_pic_outputs = False,
        do_not_generate_module_map = None,
        framework_includes = [],
        hdrs_checking_mode = None,
        implementation_compilation_contexts = [],
        include_prefix = "",
        includes = [],
        language = None,
        local_defines = [],
        loose_includes = [],
        module_interfaces = [],
        module_map = None,
        non_compilation_additional_inputs = [],
        private_hdrs = [],
        propagate_module_map_to_compile_action = None,
        public_hdrs = [],
        purpose = None,
        quote_includes = [],
        separate_module_headers = [],
        srcs = [],
        strip_include_prefix = "",
        system_includes = [],
        textual_hdrs = [],
        user_compile_flags = [],
        variables_extension = None):
    # TODO: Fill this in properly.
    compilation_context = builtins_internal_cc_common_create_compilation_context()

    # TODO: Fill this in properly.
    srcs_compilation_outputs = _create_compilation_outputs(
        objects = depset(),
        pic_objects = depset(),
        lto_compilation_context = struct(TODO_lto_compilation_context = True),
    )

    return compilation_context, srcs_compilation_outputs

def builtins_internal_cc_common_configure_features(
        ctx,
        cc_toolchain = None,
        language = None,
        requested_features = [],
        unsupported_features = []):
    return struct(
        is_enabled = lambda feature: False,
    )

def builtins_internal_cc_common_create_cc_toolchain_config_info(
        ctx,
        toolchain_identifier,
        compiler,
        features = [],
        action_configs = [],
        artifact_name_patterns = [],
        cxx_builtin_include_directories = [],
        host_system_name = None,
        target_system_name = None,
        target_cpu = None,
        target_libc = None,
        abi_version = None,
        abi_libc_version = None,
        tool_paths = [],
        make_variables = [],
        builtin_sysroot = None):
    tool_paths_tuples = [
        [tool.name, tool.path]
        for tool in tool_paths
    ]
    return CcToolchainConfigInfo(
        _action_configs = action_configs,
        _artifact_name_patterns = {
            pattern.category_name: struct(prefix = pattern.prefix, extension = pattern.extension)
            for pattern in artifact_name_patterns
        },
        _features = features,
        abi_libc_version = lambda: abi_libc_version,
        abi_version = lambda: abi_version,
        builtin_sysroot = lambda: builtin_sysroot,
        compiler = lambda: compiler,
        cxx_builtin_include_directories = lambda: cxx_builtin_include_directories,
        make_variables = lambda: make_variables,
        target_cpu = lambda: target_cpu,
        target_libc = lambda: target_libc,
        target_system_name = lambda: target_system_name,
        tool_paths = lambda: tool_paths_tuples,
        toolchain_id = lambda: toolchain_identifier,
    )

def _create_compilation_context(
        *,
        additional_inputs,
        headers,
        module_map,
        transitive_modules):
    virtual_to_original_headers = depset()
    return struct(
        additional_inputs = lambda: additional_inputs,
        headers = headers,
        module_map = module_map,
        transitive_modules = transitive_modules,
        validation_artifacts = depset(),
        virtual_to_original_headers = lambda: virtual_to_original_headers,
    )

def builtins_internal_cc_common_create_compilation_context(
        *,
        actions = None,
        add_public_headers_to_modular_headers = None,
        defines = None,
        dependent_cc_compilation_contexts = [],
        direct_private_headers = [],
        direct_public_headers = [],
        direct_textual_headers = [],
        exported_dependent_cc_compilation_contexts = [],
        external_includes = None,
        framework_includes = None,
        header_module = None,
        headers = None,
        headers_checking_mode = None,
        includes = None,
        label = None,
        local_defines = None,
        loose_hdrs_dirs = [],
        module_map = None,
        non_code_inputs = [],
        pic_header_module = None,
        propagate_module_map_to_compile_action = False,
        purpose = None,
        quote_includes = None,
        separate_module = None,
        separate_module_headers = [],
        separate_pic_module = None,
        system_includes = None,
        virtual_to_original_headers = None):
    # TODO: We should also add direct_module_maps.
    additional_inputs_transitive = []
    if non_code_inputs:
        additional_inputs_transitive.append(non_code_inputs)
    additional_inputs = depset(transitive = additional_inputs_transitive)

    modules_direct = []
    if header_module:
        modules_direct.append(header_module)
    if separate_module:
        modules_direct.append(separate_module)
    modules = depset(modules_direct)

    pic_modules_direct = []
    if pic_header_module:
        pic_modules_direct.append(pic_header_module)
    if separate_pic_module:
        pic_modules_direct.append(separate_pic_module)
    pic_modules = depset(pic_modules_direct)

    return _create_compilation_context(
        additional_inputs = additional_inputs,
        headers = headers or depset(),
        module_map = module_map,
        transitive_modules = lambda use_pic: pic_modules if use_pic else modules,
    )

def _create_lto_compilation_context():
    return struct(
        lto_bitcode_inputs = lambda: {},
    )

def builtins_internal_cc_common_create_compilation_outputs(
        *,
        dwo_objects,
        pic_dwo_objects,
        lto_compilation_context = None,
        objects = None,
        pic_objects = None):
    return _create_compilation_outputs(
        objects = objects or depset(),
        pic_objects = pic_objects or depset(),
        lto_compilation_context = lto_compilation_context or _create_lto_compilation_context(),
    )

def builtins_internal_cc_common_create_compile_variables(
        cc_toolchain,
        feature_configuration,
        source_file = None,
        output_file = None,
        user_compile_flags = None,
        include_directories = None,
        quote_include_directories = None,
        system_include_directories = None,
        framework_include_directories = None,
        preprocessor_defines = None,
        thinlto_index = None,
        thinlto_input_bitcode_file = None,
        thinlto_output_object_file = None,
        use_pic = False,
        add_legacy_cxx_options = False,
        variables_extension = None,
        strip_opts = None,
        input_file = None):
    pass

def _create_debug_context(dwo_files, pic_dwo_files):
    return struct(
        dwo_files = dwo_files,
        pic_dwo_files = pic_dwo_files,
        # TODO: How do we set these?
        files = depset(),
        pic_files = depset(),
    )

def builtins_internal_cc_common_create_debug_context(compilation_outputs = None):
    return _create_debug_context(
        dwo_files = compilation_outputs._dwo_files if compilation_outputs else depset(),
        pic_dwo_files = compilation_outputs._pic_dwo_files if compilation_outputs else depset(),
    )

def builtins_internal_cc_common_create_linker_input(
        *,
        owner,
        additional_inputs = None,
        libraries = None,
        linkstamps = None,
        user_link_flags = None):
    return struct(
        additional_inputs = tuple(additional_inputs.to_list()) if additional_inputs else (),
        libraries = tuple(libraries.to_list()) if libraries else (),
        linkstamps = tuple(linkstamps.to_list()) if linkstamps else (),
        user_link_flags = tuple(user_link_flags) if user_link_flags else (),
    )

def builtins_internal_cc_common_create_linking_context(
        *,
        additional_inputs = None,
        extra_link_time_library = None,
        libraries_to_link = None,
        linker_inputs = None,
        owner = None,
        user_link_flags = None):
    return struct(
        extra_link_time_libraries = lambda: struct(
            # TODO: Return proper depsets.
            build_libraries = lambda ctx, static_mode, for_dynamic_library: (depset(), depset()),
        ),
        linker_inputs = linker_inputs or depset(),
    )

def builtins_internal_cc_common_create_module_map(
        *,
        file,
        name,
        umbrella_header):
    return struct(
        file = lambda: file,
        umbrella_header = lambda: umbrella_header,
    )

def builtins_internal_cc_common_get_environment_variables(
        feature_configuration,
        action_name,
        variables):
    return {}

def builtins_internal_cc_common_get_execution_requirements(
        *,
        action_name,
        feature_configuration):
    return []

def builtins_internal_cc_common_get_memory_inefficient_command_line(
        feature_configuration,
        action_name,
        variables):
    return ["TODO"]

def builtins_internal_cc_common_get_tool_for_action(feature_configuration, action_name):
    return "/TODO/get/tool/for/action"

def builtins_internal_cc_common_get_tool_requirement_for_action(*, action_name, feature_configuration):
    return []

def builtins_internal_cc_common_merge_compilation_contexts(compilation_contexts = [], non_exported_compilation_contexts = []):
    additional_inputs = depset(transitive = [
        cc.additional_inputs()
        for cc in compilation_contexts
    ])
    headers = depset(transitive = [
        cc.headers
        for cc in compilation_contexts
    ])
    modules = depset(transitive = [
        cc.transitive_modules(use_pic = False)
        for cc in compilation_contexts
    ])
    pic_modules = depset(transitive = [
        cc.transitive_modules(use_pic = True)
        for cc in compilation_contexts
    ])

    return _create_compilation_context(
        additional_inputs = additional_inputs,
        headers = headers,
        module_map = None,
        transitive_modules = lambda use_pic: pic_modules if use_pic else modules,
    )

def builtins_internal_cc_common_merge_compilation_outputs(*, compilation_outputs = []):
    return _create_compilation_outputs(
        objects = depset(
            transitive = [
                co._objects
                for co in compilation_outputs
            ],
        ),
        pic_objects = depset(
            transitive = [
                co._pic_objects
                for co in compilation_outputs
            ],
        ),
        lto_compilation_context = _create_lto_compilation_context(),
    )

def builtins_internal_cc_common_merge_debug_context(debug_contexts = []):
    return _create_debug_context(
        dwo_files = depset(
            transitive = [
                dc.dwo_files
                for dc in debug_contexts
            ],
        ),
        pic_dwo_files = depset(
            transitive = [
                dc.pic_dwo_files
                for dc in debug_contexts
            ],
        ),
    )

def builtins_internal_cc_common_merge_linking_contexts(linking_contexts = []):
    return builtins_internal_cc_common_create_linking_context(
        linker_inputs = depset(
            transitive = [
                lc.linker_inputs
                for lc in linking_contexts
            ],
        ),
    )

def builtins_internal_cc_internal_actions2ctx_cheat(actions):
    return actions._cc_internal_actions2ctx_cheat

def builtins_internal_cc_internal_cc_toolchain_features(*, toolchain_config_info, tools_directory):
    return struct(
        _artifact_name_patterns = toolchain_config_info._artifact_name_patterns,
    )

def builtins_internal_cc_internal_cc_toolchain_variables(vars):
    return "TODO"

def builtins_internal_cc_internal_collect_libraries_to_link(
        linker_inputs,
        cc_toolchain,
        feature_configuration,
        output,
        dynamic_library_solib_symlink_output,
        link_type,
        linking_mode,
        is_native_deps,
        need_whole_archive,
        solib_dir,
        toolchain_libraries_solib_dir,
        allow_lto_indexing,
        lto_mapping,
        workspace_name):
    return struct(
        all_runtime_library_search_directories = depset(),
        expanded_linker_inputs = [],
        libraries_to_link = [],
        library_search_directories = depset(),
    )

def builtins_internal_cc_internal_convert_library_to_link_list_to_linker_input_list(libraries_to_link, static_mode, for_dynamic_library, support_dynamic_linker):
    # TODO!
    return []

def builtins_internal_cc_internal_create_cc_launcher_info(*, cc_info, compilation_outputs):
    return CcLauncherInfo(
        cc_info = lambda: cc_info,
        compilation_outputs = lambda: compilation_outputs,
    )

def builtins_internal_cc_internal_dynamic_library_soname(actions, path, preserve_name):
    return "TODO"

def builtins_internal_cc_internal_empty_compilation_outputs():
    return _create_compilation_outputs(
        objects = depset(),
        pic_objects = depset(),
        lto_compilation_context = _create_lto_compilation_context(),
    )

def builtins_internal_cc_internal_escape_label(label):
    return "".join([
        "_U" if c == "_" else "_S" if c == "/" else "_B" if c == "\\" else "_C" if c == ":" else "_A" if c == "@" else c
        for c in (label.repo_name + "@" + label.package + ":" + label.name).elems()
    ])

# Artifact name patterns that are registered by default.
# Obtained from ArtifactCategory.java.
default_artifact_name_patterns = {
    "STATIC_LIBRARY": struct(prefix = "lib", extension = ".a"),
    "ALWAYSLINK_STATIC_LIBRARY": struct(prefix = "lib", extension = ".lo"),
    "DYNAMIC_LIBRARY": struct(prefix = "lib", extension = ".so"),
    "EXECUTABLE": struct(prefix = "", extension = ""),
    "INTERFACE_LIBRARY": struct(prefix = "lib", extension = ".ifso"),
    "PIC_FILE": struct(prefix = "", extension = ".pic"),
    "INCLUDED_FILE_LIST": struct(prefix = "", extension = ".d"),
    "SERIALIZED_DIAGNOSTICS_FILE": struct(prefix = "", extension = ".dia"),
    "OBJECT_FILE": struct(prefix = "", extension = ".o"),
    "PIC_OBJECT_FILE": struct(prefix = "", extension = ".pic.o"),
    "CPP_MODULE": struct(prefix = "", extension = ".pcm"),
    "CPP_MODULE_GCM": struct(prefix = "", extension = ".gcm"),
    "CPP_MODULE_IFC": struct(prefix = "", extension = ".ifc"),
    "CPP_MODULES_INFO": struct(prefix = "", extension = ".CXXModules.json"),
    "CPP_MODULES_DDI": struct(prefix = "", extension = ".ddi"),
    "CPP_MODULES_MODMAP": struct(prefix = "", extension = ".modmap"),
    "CPP_MODULES_MODMAP_INPUT": struct(prefix = "", extension = ".modmap.input"),
    "GENERATED_ASSEMBLY": struct(prefix = "", extension = ".s"),
    "PROCESSED_HEADER": struct(prefix = "", extension = ".processed"),
    "GENERATED_HEADER": struct(prefix = "", extension = ".h"),
    "PREPROCESSED_C_SOURCE": struct(prefix = "", extension = ".i"),
    "PREPROCESSED_CPP_SOURCE": struct(prefix = "", extension = ".ii"),
    "COVERAGE_DATA_FILE": struct(prefix = "", extension = ".gcno"),
    "CLIF_OUTPUT_PROTO": struct(prefix = "", extension = ".opb"),
}

def builtins_internal_cc_internal_get_artifact_name_for_category(cc_toolchain, category, output_name):
    pattern = cc_toolchain._toolchain_features._artifact_name_patterns.get(category)
    if not pattern:
        pattern = default_artifact_name_patterns[category]

    output_parts = output_name.split("/")
    output_parts[-1] = pattern.prefix + output_parts[-1] + pattern.extension
    return "/".join(output_parts)

def builtins_internal_cc_internal_get_link_args(
        *,
        action_name,
        build_variables,
        feature_configuration,
        parameter_file_type):
    return struct()

def builtins_internal_cc_internal_licenses(ctx):
    return None

def builtins_internal_cc_internal_wrap_link_actions(actions, build_config = None, use_shareable_artifact_factory = False):
    return actions

def builtins_internal_java_common_internal_do_not_use__check_java_toolchain_is_declared_on_rule():
    return "TODO"

def builtins_internal_java_common_internal_do_not_use__google_legacy_api_enabled():
    return "TODO"

def builtins_internal_java_common_internal_do_not_use__incompatible_java_info_merge_runtime_module_flags():
    return "TODO"

def builtins_internal_java_common_internal_do_not_use_check_provider_instances():
    return "TODO"

def builtins_internal_java_common_internal_do_not_use_collect_native_deps_dirs():
    return "TODO"

def builtins_internal_java_common_internal_do_not_use_create_compilation_action():
    return "TODO"

def builtins_internal_java_common_internal_do_not_use_create_header_compilation_action():
    return "TODO"

def builtins_internal_java_common_internal_do_not_use_expand_java_opts():
    return "TODO"

def builtins_internal_java_common_internal_do_not_use_get_runtime_classpath_for_archive():
    return "TODO"

def builtins_internal_java_common_internal_do_not_use_incompatible_disable_non_executable_java_binary():
    return "TODO"

def builtins_internal_java_common_internal_do_not_use_target_kind():
    return "TODO"

def builtins_internal_java_common_internal_do_not_use_tokenize_javacopts():
    return "TODO"

def builtins_internal_java_common_internal_do_not_use_wrap_java_info():
    return "TODO"

def builtins_json_encode_indent(x, **kwargs):
    return json.indent(json.encode(x), **kwargs)

exported_rules = {
    "alias": native.alias,
    "cc_proto_library": cc_proto_library,
    "cc_toolchain_suite": cc_toolchain_suite,
    "config_setting": config_setting,
    "constraint_setting": constraint_setting,
    "constraint_value": constraint_value,
    "exports_files": native.exports_files,
    "filegroup": filegroup,
    "genrule": genrule,
    "glob": native.glob,
    "java_proto_library": java_proto_library,
    "label_flag": native.label_flag,
    "label_setting": native.label_setting,
    "licenses": licenses,
    "package_group": native.package_group,
    "platform": platform,
    "sh_test": sh_test,
    "test_suite": test_suite,
    "toolchain": toolchain,
    "toolchain_type": toolchain_type,
}
exported_toplevels = {
    "AnalysisFailureInfo": AnalysisFailureInfo,
    "AnalysisTestResultInfo": AnalysisTestResultInfo,
    "DefaultInfo": DefaultInfo,
    "OutputGroupInfo": OutputGroupInfo,
    "RunEnvironmentInfo": RunEnvironmentInfo,
    "CcInfo": CcInfo,
    "CcToolchainConfigInfo": CcToolchainConfigInfo,
    "DebugPackageInfo": DebugPackageInfo,
    "InstrumentedFilesInfo": InstrumentedFilesInfo,
    "PackageSpecificationInfo": PackageSpecificationInfo,
    "ProguardSpecProvider": ProguardSpecProvider,
    "PyInfo": PyInfo,
    "config_common": struct(
        toolchain_type = config_common.toolchain_type,
    ),
    "coverage_common": struct(
        instrumented_files_info = coverage_common_instrumented_files_info,
    ),
    "json": struct(
        decode = json.decode,
        encode = json.encode,
        # starlark-go does not support json.encode_indent().
        # TODO: Should we get it added?
        encode_indent = builtins_json_encode_indent,
        indent = json.indent,
    ),
    "platform_common": struct(
        TemplateVariableInfo = TemplateVariableInfo,
        ToolchainInfo = ToolchainInfo,
    ),
    "proto_common_do_not_use": struct(
        external_proto_infos = proto_common_do_not_use_external_proto_infos,
        incompatible_enable_proto_toolchain_resolution = proto_common_do_not_use_incompatible_enable_proto_toolchain_resolution,
    ),
    "testing": struct(
        ExecutionInfo = ExecutionInfo,
    ),
}

exported_toplevels["_builtins"] = struct(
    internal = struct(
        CcNativeLibraryInfo = CcNativeLibraryInfo,
        StaticallyLinkedMarkerProvider = StaticallyLinkedMarkerProvider,
        apple_common = struct(
            dotted_version = builtins_internal_apple_common_dotted_version,
        ),
        cc_common = struct(
            CcToolchainInfo = CcToolchainInfo,
            action_is_enabled = builtins_internal_cc_common_action_is_enabled,
            check_private_api = builtins_internal_cc_common_check_private_api,
            compile = builtins_internal_cc_common_compile,
            configure_features = builtins_internal_cc_common_configure_features,
            create_cc_toolchain_config_info = builtins_internal_cc_common_create_cc_toolchain_config_info,
            create_compilation_context = builtins_internal_cc_common_create_compilation_context,
            create_compilation_outputs = builtins_internal_cc_common_create_compilation_outputs,
            create_compile_variables = builtins_internal_cc_common_create_compile_variables,
            create_debug_context = builtins_internal_cc_common_create_debug_context,
            create_linker_input = builtins_internal_cc_common_create_linker_input,
            create_linking_context = builtins_internal_cc_common_create_linking_context,
            create_module_map = builtins_internal_cc_common_create_module_map,
            do_not_use_tools_cpp_compiler_present = None,
            get_environment_variables = builtins_internal_cc_common_get_environment_variables,
            get_execution_requirements = builtins_internal_cc_common_get_execution_requirements,
            get_memory_inefficient_command_line = builtins_internal_cc_common_get_memory_inefficient_command_line,
            get_tool_for_action = builtins_internal_cc_common_get_tool_for_action,
            get_tool_requirement_for_action = builtins_internal_cc_common_get_tool_requirement_for_action,
            merge_compilation_contexts = builtins_internal_cc_common_merge_compilation_contexts,
            merge_compilation_outputs = builtins_internal_cc_common_merge_compilation_outputs,
            merge_debug_context = builtins_internal_cc_common_merge_debug_context,
            merge_linking_contexts = builtins_internal_cc_common_merge_linking_contexts,
        ),
        cc_internal = struct(
            actions2ctx_cheat = builtins_internal_cc_internal_actions2ctx_cheat,
            cc_toolchain_features = builtins_internal_cc_internal_cc_toolchain_features,
            cc_toolchain_variables = builtins_internal_cc_internal_cc_toolchain_variables,
            collect_libraries_to_link = builtins_internal_cc_internal_collect_libraries_to_link,
            convert_library_to_link_list_to_linker_input_list = builtins_internal_cc_internal_convert_library_to_link_list_to_linker_input_list,
            create_cc_launcher_info = builtins_internal_cc_internal_create_cc_launcher_info,
            dynamic_library_soname = builtins_internal_cc_internal_dynamic_library_soname,
            empty_compilation_outputs = builtins_internal_cc_internal_empty_compilation_outputs,
            escape_label = builtins_internal_cc_internal_escape_label,
            get_artifact_name_for_category = builtins_internal_cc_internal_get_artifact_name_for_category,
            get_link_args = builtins_internal_cc_internal_get_link_args,
            launcher_provider = CcLauncherInfo,
            licenses = builtins_internal_cc_internal_licenses,
            wrap_link_actions = builtins_internal_cc_internal_wrap_link_actions,
        ),
        java_common_internal_do_not_use = struct(
            _check_java_toolchain_is_declared_on_rule = builtins_internal_java_common_internal_do_not_use__check_java_toolchain_is_declared_on_rule,
            _google_legacy_api_enabled = builtins_internal_java_common_internal_do_not_use__google_legacy_api_enabled,
            _incompatible_java_info_merge_runtime_module_flags = builtins_internal_java_common_internal_do_not_use__incompatible_java_info_merge_runtime_module_flags,
            check_provider_instances = builtins_internal_java_common_internal_do_not_use_check_provider_instances,
            collect_native_deps_dirs = builtins_internal_java_common_internal_do_not_use_collect_native_deps_dirs,
            create_compilation_action = builtins_internal_java_common_internal_do_not_use_create_compilation_action,
            create_header_compilation_action = builtins_internal_java_common_internal_do_not_use_create_header_compilation_action,
            expand_java_opts = builtins_internal_java_common_internal_do_not_use_expand_java_opts,
            get_runtime_classpath_for_archive = builtins_internal_java_common_internal_do_not_use_get_runtime_classpath_for_archive,
            incompatible_disable_non_executable_java_binary = builtins_internal_java_common_internal_do_not_use_incompatible_disable_non_executable_java_binary,
            target_kind = builtins_internal_java_common_internal_do_not_use_target_kind,
            tokenize_javacopts = builtins_internal_java_common_internal_do_not_use_tokenize_javacopts,
            wrap_java_info = builtins_internal_java_common_internal_do_not_use_wrap_java_info,
        ),
        objc_internal = struct(),
        py_builtins = struct(),
    ),
    toplevel = struct(**exported_toplevels),
)
