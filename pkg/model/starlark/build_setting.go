package starlark

import (
	"fmt"

	model_starlark_pb "github.com/buildbarn/bb-playground/pkg/proto/model/starlark"
	"github.com/buildbarn/bb-playground/pkg/starlark/unpack"

	"google.golang.org/protobuf/types/known/emptypb"

	"go.starlark.net/starlark"
)

type BuildSetting struct {
	buildSettingType BuildSettingType
	flag             bool
}

var _ starlark.Value = (*BuildSetting)(nil)

func NewBuildSetting(buildSettingType BuildSettingType, flag bool) *BuildSetting {
	return &BuildSetting{
		buildSettingType: buildSettingType,
		flag:             flag,
	}
}

func (bs *BuildSetting) String() string {
	return fmt.Sprintf("<config.%s>", bs.buildSettingType.Type())
}

func (bs *BuildSetting) Type() string {
	return "config." + bs.buildSettingType.Type()
}

func (bs *BuildSetting) Freeze() {}

func (bs *BuildSetting) Truth() starlark.Bool {
	return starlark.True
}

func (bs *BuildSetting) Hash() (uint32, error) {
	// TODO
	return 0, nil
}

func (bs *BuildSetting) Encode() *model_starlark_pb.BuildSetting {
	buildSetting := model_starlark_pb.BuildSetting{
		Flag: bs.flag,
	}
	bs.buildSettingType.Encode(&buildSetting)
	return &buildSetting
}

type BuildSettingType interface {
	Type() string
	Encode(out *model_starlark_pb.BuildSetting)
	GetCanonicalizer() unpack.Canonicalizer
}

type boolBuildSettingType struct{}

var BoolBuildSettingType BuildSettingType = boolBuildSettingType{}

func (boolBuildSettingType) Type() string {
	return "bool"
}

func (boolBuildSettingType) Encode(out *model_starlark_pb.BuildSetting) {
	out.Type = &model_starlark_pb.BuildSetting_Bool{
		Bool: &emptypb.Empty{},
	}
}

func (boolBuildSettingType) GetCanonicalizer() unpack.Canonicalizer {
	return unpack.Bool
}

type intBuildSettingType struct{}

var IntBuildSettingType BuildSettingType = intBuildSettingType{}

func (intBuildSettingType) Type() string {
	return "int"
}

func (intBuildSettingType) Encode(out *model_starlark_pb.BuildSetting) {
	out.Type = &model_starlark_pb.BuildSetting_Int{
		Int: &emptypb.Empty{},
	}
}

func (intBuildSettingType) GetCanonicalizer() unpack.Canonicalizer {
	return unpack.Int[int32]()
}

type stringBuildSettingType struct{}

var StringBuildSettingType BuildSettingType = stringBuildSettingType{}

func (stringBuildSettingType) Type() string {
	return "string"
}

func (stringBuildSettingType) Encode(out *model_starlark_pb.BuildSetting) {
	out.Type = &model_starlark_pb.BuildSetting_String_{
		String_: &emptypb.Empty{},
	}
}

func (stringBuildSettingType) GetCanonicalizer() unpack.Canonicalizer {
	return unpack.String
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

func (bst stringListBuildSettingType) Encode(out *model_starlark_pb.BuildSetting) {
	out.Type = &model_starlark_pb.BuildSetting_StringList{
		StringList: &model_starlark_pb.BuildSetting_StringListType{
			Repeatable: bst.repeatable,
		},
	}
}

func (stringListBuildSettingType) GetCanonicalizer() unpack.Canonicalizer {
	return unpack.List(unpack.String)
}
