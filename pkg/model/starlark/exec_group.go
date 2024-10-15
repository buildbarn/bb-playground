package starlark

import (
	"maps"
	"slices"
	"sort"

	pg_label "github.com/buildbarn/bb-playground/pkg/label"
	model_starlark_pb "github.com/buildbarn/bb-playground/pkg/proto/model/starlark"

	"go.starlark.net/starlark"
)

type ExecGroup struct {
	execCompatibleWith []string
	toolchains         []*ToolchainType
}

func NewExecGroup(execCompatibleWith []pg_label.CanonicalLabel, toolchains []*ToolchainType) *ExecGroup {
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

func (l *ExecGroup) String() string {
	return "<exec_group>"
}

func (l *ExecGroup) Type() string {
	return "exec_group"
}

func (l *ExecGroup) Freeze() {}

func (l *ExecGroup) Truth() starlark.Bool {
	return starlark.True
}

func (l *ExecGroup) Hash() (uint32, error) {
	// TODO
	return 0, nil
}

func (l *ExecGroup) Encode() *model_starlark_pb.ExecGroup {
	execGroup := model_starlark_pb.ExecGroup{
		ExecCompatibleWith: l.execCompatibleWith,
		Toolchains:         make([]*model_starlark_pb.ToolchainType, 0, len(l.toolchains)),
	}
	for _, toolchain := range l.toolchains {
		execGroup.Toolchains = append(execGroup.Toolchains, toolchain.Encode())
	}
	return &execGroup
}
