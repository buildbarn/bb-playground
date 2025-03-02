package starlark

import (
	"errors"
	"maps"
	"slices"
	"sort"

	pg_label "github.com/buildbarn/bonanza/pkg/label"
	model_core "github.com/buildbarn/bonanza/pkg/model/core"
	model_starlark_pb "github.com/buildbarn/bonanza/pkg/proto/model/starlark"

	"go.starlark.net/starlark"
)

type ExecGroup struct {
	execCompatibleWith []string
	toolchains         []*ToolchainType
}

func NewExecGroup(execCompatibleWith []pg_label.ResolvedLabel, toolchains []*ToolchainType) *ExecGroup {
	execCompatibleWithStrings := make([]string, 0, len(execCompatibleWith))
	for _, label := range execCompatibleWith {
		execCompatibleWithStrings = append(execCompatibleWithStrings, label.String())
	}
	sort.Strings(execCompatibleWithStrings)

	// Bazel permits listing the same toolchain multiple types, and
	// with different properties. Deduplicate and merge them.
	toolchainsMap := map[string]*ToolchainType{}
	for _, toolchain := range toolchains {
		key := toolchain.toolchainType.String()
		if existingToolchain, ok := toolchainsMap[key]; ok {
			toolchainsMap[key] = existingToolchain.Merge(toolchain)
		} else {
			toolchainsMap[key] = toolchain
		}
	}
	deduplicatedToolchains := make([]*ToolchainType, 0, len(toolchainsMap))
	for _, key := range slices.Sorted(maps.Keys(toolchainsMap)) {
		deduplicatedToolchains = append(deduplicatedToolchains, toolchainsMap[key])
	}

	return &ExecGroup{
		execCompatibleWith: slices.Compact(execCompatibleWithStrings),
		toolchains:         deduplicatedToolchains,
	}
}

func (ExecGroup) String() string {
	return "<exec_group>"
}

func (ExecGroup) Type() string {
	return "exec_group"
}

func (ExecGroup) Freeze() {}

func (ExecGroup) Truth() starlark.Bool {
	return starlark.True
}

func (ExecGroup) Hash(thread *starlark.Thread) (uint32, error) {
	return 0, errors.New("exec_group cannot be hashed")
}

func (eg *ExecGroup) Encode() *model_starlark_pb.ExecGroup {
	execGroup := model_starlark_pb.ExecGroup{
		ExecCompatibleWith: eg.execCompatibleWith,
		Toolchains:         make([]*model_starlark_pb.ToolchainType, 0, len(eg.toolchains)),
	}
	for _, toolchain := range eg.toolchains {
		execGroup.Toolchains = append(execGroup.Toolchains, toolchain.Encode())
	}
	return &execGroup
}

func (eg *ExecGroup) EncodeValue(path map[starlark.Value]struct{}, currentIdentifier *pg_label.CanonicalStarlarkIdentifier, options *ValueEncodingOptions) (model_core.PatchedMessage[*model_starlark_pb.Value, model_core.CreatedObjectTree], bool, error) {
	return model_core.NewSimplePatchedMessage[model_core.CreatedObjectTree](
		&model_starlark_pb.Value{
			Kind: &model_starlark_pb.Value_ExecGroup{
				ExecGroup: eg.Encode(),
			},
		},
	), false, nil
}
