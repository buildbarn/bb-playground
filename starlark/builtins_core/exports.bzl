CcInfo = provider()
CcLauncherInfo = provider()
CcNativeLibraryInfo = provider()
CcToolchainConfigInfo = provider()
CcToolchainInfo = provider()
DebugPackageInfo = provider()
DefaultInfo = provider()
InstrumentedFilesInfo = provider()
OutputGroupInfo = provider()
PackageSpecificationInfo = provider()
ProguardSpecProvider = provider()
RunEnvironmentInfo = provider()
StaticallyLinkedMarkerProvider = provider()
TemplateVariableInfo = provider()
ToolchainInfo = provider()

def _filegroup_impl(ctx):
    fail("TODO")

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
        "tools": attr.label_list(allow_files = True),
    },
)

def _platform_impl(ctx):
    fail("TODO")

platform = rule(
    implementation = _platform_impl,
    attrs = {
        "constraint_values": attr.label_list(),
        "exec_properties": attr.string_dict(),
        "parents": attr.label_list(),
    },
    # platform() cannot contain any exec_groups, as that would cause a
    # cyclic dependency when configuring these targets.
    default_exec_group = False,
)

def proto_common_incompatible_enable_proto_toolchain_resolution():
    return True

def builtins_internal_cc_internal_empty_compilation_outputs():
    return "TODO"

def builtins_internal_java_common_internal_do_not_use_google_legacy_api_enabled():
    return "TODO"

def builtins_internal_java_common_internal_do_not_use_incompatible_disable_non_executable_java_binary():
    return "TODO"

exported_rules = {
    "filegroup": filegroup,
    "genrule": genrule,
    "platform": platform,
}
exported_toplevels = {
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
    "testing": struct(),
}

exported_toplevels["_builtins"] = struct(
    internal = struct(
        CcNativeLibraryInfo = CcNativeLibraryInfo,
        StaticallyLinkedMarkerProvider = StaticallyLinkedMarkerProvider,
        apple_common = struct(),
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
