_CcInfo = provider()
_CcLauncherInfo = provider()
_CcNativeLibraryInfo = provider()
_CcToolchainConfigInfo = provider()
_CcToolchainInfo = provider()
_DebugPackageInfo = provider()
_DefaultInfo = provider()
_InstrumentedFilesInfo = provider()
_OutputGroupInfo = provider()
_PackageSpecificationInfo = provider()
_ProguardSpecProvider = provider()
_RunEnvironmentInfo = provider()
_StaticallyLinkedMarkerProvider = provider()
_TemplateVariableInfo = provider()
_ToolchainInfo = provider()

def _filegroup_impl(ctx):
    fail("TODO")

_filegroup = rule(
    implementation = _filegroup_impl,
    attrs = {
        "data": attr.label_list(allow_files = True),
        "output_group": attr.string(),
        "srcs": attr.label_list(allow_files = True),
    },
)

def _genrule_impl(ctx):
    fail("TODO")

_genrule = rule(
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

def _config_common_toolchain_type(name, *, mandatory = True):
    return config_common.toolchain_type(name, mandatory = mandatory)

def _proto_common_incompatible_enable_proto_toolchain_resolution():
    return True

def _builtins_internal_cc_internal_empty_compilation_outputs():
    return "TODO"

def _builtins_internal_java_common_internal_do_not_use_google_legacy_api_enabled():
    return "TODO"

def _builtins_internal_java_common_internal_do_not_use_incompatible_disable_non_executable_java_binary():
    return "TODO"

exported_rules = {
    "filegroup": _filegroup,
    "genrule": _genrule,
}
exported_toplevels = {
    "DefaultInfo": _DefaultInfo,
    "OutputGroupInfo": _OutputGroupInfo,
    "RunEnvironmentInfo": _RunEnvironmentInfo,
    "CcInfo": _CcInfo,
    "CcToolchainConfigInfo": _CcToolchainConfigInfo,
    "DebugPackageInfo": _DebugPackageInfo,
    "InstrumentedFilesInfo": _InstrumentedFilesInfo,
    "PackageSpecificationInfo": _PackageSpecificationInfo,
    "ProguardSpecProvider": _ProguardSpecProvider,
    "config_common": struct(
        toolchain_type = _config_common_toolchain_type,
    ),
    "coverage_common": struct(),
    "platform_common": struct(
        TemplateVariableInfo = _TemplateVariableInfo,
        ToolchainInfo = _ToolchainInfo,
    ),
    "proto_common": struct(
        incompatible_enable_proto_toolchain_resolution = _proto_common_incompatible_enable_proto_toolchain_resolution,
    ),
    "testing": struct(),
}

exported_toplevels["_builtins"] = struct(
    internal = struct(
        CcNativeLibraryInfo = _CcNativeLibraryInfo,
        StaticallyLinkedMarkerProvider = _StaticallyLinkedMarkerProvider,
        apple_common = struct(),
        cc_common = struct(
            CcToolchainInfo = _CcToolchainInfo,
            do_not_use_tools_cpp_compiler_present = None,
        ),
        cc_internal = struct(
            empty_compilation_outputs = _builtins_internal_cc_internal_empty_compilation_outputs,
            launcher_provider = _CcLauncherInfo,
        ),
        java_common_internal_do_not_use = struct(
            _google_legacy_api_enabled = _builtins_internal_java_common_internal_do_not_use_google_legacy_api_enabled,
            incompatible_disable_non_executable_java_binary = _builtins_internal_java_common_internal_do_not_use_incompatible_disable_non_executable_java_binary,
        ),
        objc_internal = struct(),
        py_builtins = struct(),
    ),
    toplevel = struct(**exported_toplevels),
)
