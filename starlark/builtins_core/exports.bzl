AnalysisFailureInfo = provider()
AnalysisTestResultInfo = provider()
CcInfo = provider()
CcLauncherInfo = provider()
CcNativeLibraryInfo = provider()
CcToolchainConfigInfo = provider()
CcToolchainInfo = provider()
ConfigSettingInfo = provider()
ConstraintSettingInfo = provider()
ConstraintValueInfo = provider()
DebugPackageInfo = provider()
DeclaredToolchainInfo = provider()
DefaultInfo = provider()
ExecutionInfo = provider()
InstrumentedFilesInfo = provider()
OutputGroupInfo = provider()
PackageSpecificationInfo = provider()
PlatformInfo = provider()
ProguardSpecProvider = provider()
RunEnvironmentInfo = provider()
StaticallyLinkedMarkerProvider = provider()
TemplateVariableInfo = provider()
ToolchainInfo = provider()
ToolchainTypeInfo = provider()

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
    # TODO: Provide an actual implementation that constructs a
    # DefaultInfo object.
    return []

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

def _toolchain_impl(ctx):
    return [DeclaredToolchainInfo(
        target_settings = [
            target_setting.label
            for target_setting in ctx.attr.target_settings
        ],
        # Don't use .label, as that would cause the underlying toolchain
        # to be configured, leading to unnecessary downloads. It is the
        # responsibility of the analyzer to expand any aliases.
        toolchain = ctx.attr.toolchain.original_label,
        toolchain_type = ctx.attr.toolchain_type[ToolchainTypeInfo].type_label,
    )]

toolchain = rule(
    implementation = _toolchain_impl,
    attrs = {
        "target_settings": attr.label_list(
            providers = [ConfigSettingInfo],
        ),
        "toolchain": attr.label(
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

def proto_common_incompatible_enable_proto_toolchain_resolution():
    return True

def builtins_internal_apple_common_dotted_version(v):
    return [int(p) for p in v.split(".")]

def builtins_internal_cc_common_check_private_api(allowlist = []):
    pass

def builtins_internal_cc_internal_empty_compilation_outputs():
    return "TODO"

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

exported_rules = {
    "alias": native.alias,
    "config_setting": config_setting,
    "constraint_setting": constraint_setting,
    "constraint_value": constraint_value,
    "exports_files": native.exports_files,
    "filegroup": filegroup,
    "genrule": genrule,
    "licenses": licenses,
    "platform": platform,
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
    "config_common": struct(
        toolchain_type = config_common.toolchain_type,
    ),
    "coverage_common": struct(),
    "platform_common": struct(
        TemplateVariableInfo = TemplateVariableInfo,
        ToolchainInfo = ToolchainInfo,
    ),
    "proto_common": struct(
        incompatible_enable_proto_toolchain_resolution = proto_common_incompatible_enable_proto_toolchain_resolution,
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
            check_private_api = builtins_internal_cc_common_check_private_api,
            do_not_use_tools_cpp_compiler_present = None,
        ),
        cc_internal = struct(
            empty_compilation_outputs = builtins_internal_cc_internal_empty_compilation_outputs,
            launcher_provider = CcLauncherInfo,
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
