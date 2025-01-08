AnalysisFailureInfo = provider()
AnalysisTestResultInfo = provider()
CcInfo = provider()
CcLauncherInfo = provider()
CcNativeLibraryInfo = provider()
CcToolchainConfigInfo = provider()
CcToolchainInfo = provider()
ConfigSettingInfo = provider()
ConstraintValueInfo = provider()
ConstraintSettingInfo = provider()
DebugPackageInfo = provider()
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
    fail("TODO: Implement")

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
    return [ConstraintSettingInfo(
        label = ctx.label,
    )]

constraint_setting = rule(
    implementation = _constraint_setting_impl,
    default_exec_group = False,
    provides = [ConstraintSettingInfo],
    # TODO: Provide the default_constraint_setting attribute. How can we
    # offer this without causing cycles? Maybe change attr.label() to
    # provide a flag to not inspect the configured target?
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
    default_exec_group = False,
    provides = [ConstraintValueInfo],
)

def _filegroup_impl(ctx):
    fail("TODO")

filegroup = rule(
    implementation = _filegroup_impl,
    attrs = {
        "data": attr.label_list(allow_files = True),
        "output_group": attr.string(),
        "srcs": attr.label_list(allow_files = True),
    },
    default_exec_group = False,
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
        constraints = constraints,
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
    # platform() cannot contain any exec_groups, as that would cause a
    # cyclic dependency when configuring these targets.
    default_exec_group = False,
    provides = [PlatformInfo],
)

def _toolchain_impl(ctx):
    pass

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

def builtins_internal_cc_internal_empty_compilation_outputs():
    return "TODO"

def builtins_internal_java_common_internal_do_not_use_google_legacy_api_enabled():
    return "TODO"

def builtins_internal_java_common_internal_do_not_use_incompatible_disable_non_executable_java_binary():
    return "TODO"

exported_rules = {
    "alias": native.alias,
    "config_setting": config_setting,
    "constraint_setting": constraint_setting,
    "constraint_value": constraint_value,
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
            do_not_use_tools_cpp_compiler_present = None,
        ),
        cc_internal = struct(
            empty_compilation_outputs = builtins_internal_cc_internal_empty_compilation_outputs,
            launcher_provider = CcLauncherInfo,
        ),
        java_common_internal_do_not_use = struct(
            _google_legacy_api_enabled = builtins_internal_java_common_internal_do_not_use_google_legacy_api_enabled,
            incompatible_disable_non_executable_java_binary = builtins_internal_java_common_internal_do_not_use_incompatible_disable_non_executable_java_binary,
        ),
        objc_internal = struct(),
        py_builtins = struct(),
    ),
    toplevel = struct(**exported_toplevels),
)
