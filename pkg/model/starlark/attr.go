package starlark

import (
	"fmt"

	pg_label "github.com/buildbarn/bb-playground/pkg/label"
	model_core "github.com/buildbarn/bb-playground/pkg/model/core"
	model_starlark_pb "github.com/buildbarn/bb-playground/pkg/proto/model/starlark"
	"github.com/buildbarn/bb-playground/pkg/starlark/unpack"
	"github.com/buildbarn/bb-playground/pkg/storage/dag"

	"google.golang.org/protobuf/types/known/emptypb"

	"go.starlark.net/starlark"
)

type Attr struct {
	attrType     AttrType
	defaultValue starlark.Value
}

var _ EncodableValue = &Attr{}

func NewAttr(attrType AttrType, defaultValue starlark.Value) *Attr {
	return &Attr{
		attrType:     attrType,
		defaultValue: defaultValue,
	}
}

func (a *Attr) String() string {
	return fmt.Sprintf("<attr.%s>", a.attrType.Type())
}

func (a *Attr) Type() string {
	return "attr." + a.attrType.Type()
}

func (Attr) Freeze() {}

func (Attr) Truth() starlark.Bool {
	return starlark.True
}

func (Attr) Hash() (uint32, error) {
	// TODO
	return 0, nil
}

func (a *Attr) Encode(path map[starlark.Value]struct{}, options *ValueEncodingOptions) (model_core.PatchedMessage[*model_starlark_pb.Attr, dag.ObjectContentsWalker], bool, error) {
	patcher := model_core.NewReferenceMessagePatcher[dag.ObjectContentsWalker]()
	var attr model_starlark_pb.Attr
	var needsCode bool

	if a.defaultValue != nil {
		defaultValue, defaultValueNeedsCode, err := EncodeValue(a.defaultValue, path, nil, options)
		if err != nil {
			return model_core.PatchedMessage[*model_starlark_pb.Attr, dag.ObjectContentsWalker]{}, false, err
		}
		attr.Default = defaultValue.Message
		patcher.Merge(defaultValue.Patcher)
		needsCode = needsCode || defaultValueNeedsCode
	}

	a.attrType.Encode(&attr)
	return model_core.PatchedMessage[*model_starlark_pb.Attr, dag.ObjectContentsWalker]{
		Message: &attr,
		Patcher: patcher,
	}, needsCode, nil
}

func (a *Attr) EncodeValue(path map[starlark.Value]struct{}, currentIdentifier *pg_label.CanonicalStarlarkIdentifier, options *ValueEncodingOptions) (model_core.PatchedMessage[*model_starlark_pb.Value, dag.ObjectContentsWalker], bool, error) {
	attr, needsCode, err := a.Encode(path, options)
	if err != nil {
		return model_core.PatchedMessage[*model_starlark_pb.Value, dag.ObjectContentsWalker]{}, false, err
	}
	return model_core.PatchedMessage[*model_starlark_pb.Value, dag.ObjectContentsWalker]{
		Message: &model_starlark_pb.Value{
			Kind: &model_starlark_pb.Value_Attr{
				Attr: attr.Message,
			},
		},
		Patcher: attr.Patcher,
	}, needsCode, nil
}

type AttrType interface {
	Type() string
	Encode(out *model_starlark_pb.Attr)
	GetCanonicalizer(currentPackage pg_label.CanonicalPackage) unpack.Canonicalizer
}

type boolAttrType struct{}

var BoolAttrType AttrType = boolAttrType{}

func (boolAttrType) Type() string {
	return "bool"
}

func (boolAttrType) Encode(out *model_starlark_pb.Attr) {
	out.Type = &model_starlark_pb.Attr_Bool{
		Bool: &emptypb.Empty{},
	}
}

func (boolAttrType) GetCanonicalizer(currentPackage pg_label.CanonicalPackage) unpack.Canonicalizer {
	return unpack.Bool
}

type intAttrType struct {
	values []int32
}

func NewIntAttrType(values []int32) AttrType {
	return &intAttrType{
		values: values,
	}
}

func (intAttrType) Type() string {
	return "int"
}

func (at *intAttrType) Encode(out *model_starlark_pb.Attr) {
	out.Type = &model_starlark_pb.Attr_Int{
		Int: &model_starlark_pb.Attr_IntType{
			Values: at.values,
		},
	}
}

func (intAttrType) GetCanonicalizer(currentPackage pg_label.CanonicalPackage) unpack.Canonicalizer {
	return unpack.Int[int32]()
}

type intListAttrType struct {
	values []int32
}

func NewIntListAttrType() AttrType {
	return &intListAttrType{}
}

func (intListAttrType) Type() string {
	return "int"
}

func (intListAttrType) Encode(out *model_starlark_pb.Attr) {
	out.Type = &model_starlark_pb.Attr_IntList{
		IntList: &model_starlark_pb.Attr_IntListType{},
	}
}

func (intListAttrType) GetCanonicalizer(currentPackage pg_label.CanonicalPackage) unpack.Canonicalizer {
	return unpack.List(unpack.Int[int32]())
}

type labelAttrType struct {
	allowNone bool
}

func NewLabelAttrType(allowNone bool) AttrType {
	return &labelAttrType{
		allowNone: allowNone,
	}
}

func (labelAttrType) Type() string {
	return "label"
}

func (at *labelAttrType) Encode(out *model_starlark_pb.Attr) {
	out.Type = &model_starlark_pb.Attr_Label{
		Label: &model_starlark_pb.Attr_LabelType{
			AllowNone: at.allowNone,
		},
	}
}

func (ui *labelAttrType) GetCanonicalizer(currentPackage pg_label.CanonicalPackage) unpack.Canonicalizer {
	canonicalizer := NewLabelOrStringUnpackerInto(currentPackage)
	if ui.allowNone {
		canonicalizer = unpack.IfNotNone(canonicalizer)
	}
	return canonicalizer
}

type labelKeyedStringDictAttrType struct{}

func NewLabelKeyedStringDictAttrType() AttrType {
	return &labelKeyedStringDictAttrType{}
}

func (labelKeyedStringDictAttrType) Type() string {
	return "label_keyed_string_dict"
}

func (labelKeyedStringDictAttrType) Encode(out *model_starlark_pb.Attr) {
	out.Type = &model_starlark_pb.Attr_LabelKeyedStringDict{
		LabelKeyedStringDict: &model_starlark_pb.Attr_LabelKeyedStringDictType{},
	}
}

func (labelKeyedStringDictAttrType) GetCanonicalizer(currentPackage pg_label.CanonicalPackage) unpack.Canonicalizer {
	return unpack.Dict(NewLabelOrStringUnpackerInto(currentPackage), unpack.String)
}

type labelListAttrType struct{}

func NewLabelListAttrType() AttrType {
	return &labelListAttrType{}
}

func (labelListAttrType) Type() string {
	return "label_list"
}

func (labelListAttrType) Encode(out *model_starlark_pb.Attr) {
	out.Type = &model_starlark_pb.Attr_LabelList{
		LabelList: &model_starlark_pb.Attr_LabelListType{},
	}
}

func (labelListAttrType) GetCanonicalizer(currentPackage pg_label.CanonicalPackage) unpack.Canonicalizer {
	return unpack.List(NewLabelOrStringUnpackerInto(currentPackage))
}

type outputAttrType struct{}

var OutputAttrType AttrType = outputAttrType{}

func (outputAttrType) Type() string {
	return "output"
}

func (outputAttrType) Encode(out *model_starlark_pb.Attr) {
	out.Type = &model_starlark_pb.Attr_Output{
		Output: &emptypb.Empty{},
	}
}

func (outputAttrType) GetCanonicalizer(currentPackage pg_label.CanonicalPackage) unpack.Canonicalizer {
	return unpack.TargetName
}

type outputListAttrType struct{}

func NewOutputListAttrType() AttrType {
	return &outputListAttrType{}
}

func (at *outputListAttrType) Type() string {
	return "output_list"
}

func (at *outputListAttrType) Encode(out *model_starlark_pb.Attr) {
	out.Type = &model_starlark_pb.Attr_OutputList{
		OutputList: &model_starlark_pb.Attr_OutputListType{},
	}
}

func (at *outputListAttrType) GetCanonicalizer(currentPackage pg_label.CanonicalPackage) unpack.Canonicalizer {
	return unpack.List(unpack.TargetName)
}

type stringAttrType struct {
	values []string
}

func NewStringAttrType(values []string) AttrType {
	return &stringAttrType{
		values: values,
	}
}

func (stringAttrType) Type() string {
	return "string"
}

func (stringAttrType) Encode(out *model_starlark_pb.Attr) {
	out.Type = &model_starlark_pb.Attr_String_{
		String_: &model_starlark_pb.Attr_StringType{},
	}
}

func (stringAttrType) GetCanonicalizer(currentPackage pg_label.CanonicalPackage) unpack.Canonicalizer {
	return unpack.String
}

type stringDictAttrType struct{}

func NewStringDictAttrType() AttrType {
	return &stringDictAttrType{}
}

func (stringDictAttrType) Type() string {
	return "string_dict"
}

func (stringDictAttrType) Encode(out *model_starlark_pb.Attr) {
	out.Type = &model_starlark_pb.Attr_StringDict{
		StringDict: &model_starlark_pb.Attr_StringDictType{},
	}
}

func (stringDictAttrType) GetCanonicalizer(currentPackage pg_label.CanonicalPackage) unpack.Canonicalizer {
	return unpack.Dict(unpack.String, unpack.String)
}

type stringListAttrType struct{}

func NewStringListAttrType() AttrType {
	return &stringListAttrType{}
}

func (stringListAttrType) Type() string {
	return "string_list"
}

func (stringListAttrType) Encode(out *model_starlark_pb.Attr) {
	out.Type = &model_starlark_pb.Attr_StringList{
		StringList: &model_starlark_pb.Attr_StringListType{},
	}
}

func (stringListAttrType) GetCanonicalizer(currentPackage pg_label.CanonicalPackage) unpack.Canonicalizer {
	return unpack.List(unpack.String)
}

type stringListDictAttrType struct{}

func NewStringListDictAttrType() AttrType {
	return &stringListDictAttrType{}
}

func (stringListDictAttrType) Type() string {
	return "string_list_dict"
}

func (at *stringListDictAttrType) Encode(out *model_starlark_pb.Attr) {
	out.Type = &model_starlark_pb.Attr_StringListDict{
		StringListDict: &model_starlark_pb.Attr_StringListDictType{},
	}
}

func (stringListDictAttrType) GetCanonicalizer(currentPackage pg_label.CanonicalPackage) unpack.Canonicalizer {
	return unpack.Dict(unpack.String, unpack.List(unpack.String))
}
