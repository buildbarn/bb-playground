package starlark

import (
	"fmt"

	"go.starlark.net/starlark"
)

type buildSetting struct {
	buildSettingType BuildSettingType
	flag             bool
}

func NewBuildSetting(buildSettingType BuildSettingType, flag bool) starlark.Value {
	return &buildSetting{
		buildSettingType: buildSettingType,
		flag:             flag,
	}
}

func (l *buildSetting) String() string {
	return fmt.Sprintf("<config.%s>", l.buildSettingType.Type())
}

func (l *buildSetting) Type() string {
	return "config." + l.buildSettingType.Type()
}

func (l *buildSetting) Freeze() {}

func (l *buildSetting) Truth() starlark.Bool {
	return starlark.True
}

func (l *buildSetting) Hash() (uint32, error) {
	// TODO
	return 0, nil
}

type BuildSettingType interface {
	Type() string
}

type boolBuildSettingType struct{}

var BoolBuildSettingType BuildSettingType = boolBuildSettingType{}

func (boolBuildSettingType) Type() string {
	return "bool"
}

type intBuildSettingType struct{}

var IntBuildSettingType BuildSettingType = intBuildSettingType{}

func (intBuildSettingType) Type() string {
	return "int"
}

type stringBuildSettingType struct{}

var StringBuildSettingType BuildSettingType = stringBuildSettingType{}

func (stringBuildSettingType) Type() string {
	return "string"
}

type stringListBuildSettingType struct {
	repeatable bool
}

func NewStringListBuildSettingType(repeatable bool) BuildSettingType {
	return stringListBuildSettingType{
		repeatable: repeatable,
	}
}

func (stringListBuildSettingType) Type() string {
	return "string_list"
}
