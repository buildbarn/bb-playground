package starlark

import (
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"

	pg_label "github.com/buildbarn/bonanza/pkg/label"
	model_core "github.com/buildbarn/bonanza/pkg/model/core"
	model_starlark_pb "github.com/buildbarn/bonanza/pkg/proto/model/starlark"
	"github.com/buildbarn/bonanza/pkg/starlark/unpack"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/emptypb"

	"go.starlark.net/starlark"
	"go.starlark.net/syntax"
)

type Attr struct {
	attrType     AttrType
	defaultValue starlark.Value
}

var (
	_ EncodableValue      = &Attr{}
	_ starlark.Comparable = &Attr{}
)

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

func (a *Attr) Hash(thread *starlark.Thread) (uint32, error) {
	return 0, fmt.Errorf("attr.%s cannot be hashed", a.attrType.Type())
}

func (a *Attr) Encode(path map[starlark.Value]struct{}, options *ValueEncodingOptions) (model_core.PatchedMessage[*model_starlark_pb.Attr, model_core.CreatedObjectTree], bool, error) {
	patcher := model_core.NewReferenceMessagePatcher[model_core.CreatedObjectTree]()
	var attr model_starlark_pb.Attr
	var needsCode bool

	if a.defaultValue != nil {
		defaultValue, defaultValueNeedsCode, err := EncodeValue(a.defaultValue, path, nil, options)
		if err != nil {
			return model_core.PatchedMessage[*model_starlark_pb.Attr, model_core.CreatedObjectTree]{}, false, err
		}
		attr.Default = defaultValue.Message
		patcher.Merge(defaultValue.Patcher)
		needsCode = needsCode || defaultValueNeedsCode
	}

	if err := a.attrType.Encode(&attr); err != nil {
		return model_core.PatchedMessage[*model_starlark_pb.Attr, model_core.CreatedObjectTree]{}, false, err
	}
	return model_core.NewPatchedMessage(&attr, patcher), needsCode, nil
}

func (a *Attr) EncodeValue(path map[starlark.Value]struct{}, currentIdentifier *pg_label.CanonicalStarlarkIdentifier, options *ValueEncodingOptions) (model_core.PatchedMessage[*model_starlark_pb.Value, model_core.CreatedObjectTree], bool, error) {
	attr, needsCode, err := a.Encode(path, options)
	if err != nil {
		return model_core.PatchedMessage[*model_starlark_pb.Value, model_core.CreatedObjectTree]{}, false, err
	}
	return model_core.NewPatchedMessage(
		&model_starlark_pb.Value{
			Kind: &model_starlark_pb.Value_Attr{
				Attr: attr.Message,
			},
		},
		attr.Patcher,
	), needsCode, nil
}

func (a *Attr) CompareSameType(thread *starlark.Thread, op syntax.Token, other starlark.Value, depth int) (bool, error) {
	// Compare the types.
	var m1, m2 model_starlark_pb.Attr
	if err := a.attrType.Encode(&m1); err != nil {
		return false, err
	}
	a2 := other.(*Attr)
	if err := a2.attrType.Encode(&m2); err != nil {
		return false, err
	}
	switch op {
	case syntax.EQL:
		if !proto.Equal(&m1, &m2) {
			return false, nil
		}
	case syntax.NEQ:
		if !proto.Equal(&m1, &m2) {
			return true, nil
		}
	default:
		return false, errors.New("attr.* can only be compared for equality")
	}

	// Compare the default values.
	return starlark.Compare(thread, op, a.defaultValue, a2.defaultValue)
}

type AttrType interface {
	Type() string
	Encode(out *model_starlark_pb.Attr) error
	GetCanonicalizer(currentPackage pg_label.CanonicalPackage) unpack.Canonicalizer
	IsOutput() (filenameTemplate string, ok bool)
}

// sloppyBoolUnpackerInto can be used to unpack Starlark Boolean values.
// For compatibility with Bazel, it also accepts integers with values
// zero and one, which it converts to False and True, respectively.
type sloppyBoolUnpackerInto struct{}

func (sloppyBoolUnpackerInto) UnpackInto(thread *starlark.Thread, v starlark.Value, dst *bool) error {
	if vInt, ok := v.(starlark.Int); ok {
		if n, ok := vInt.Int64(); ok {
			switch n {
			case 0:
				*dst = false
				return nil
			case 1:
				*dst = true
				return nil
			}
		}
	}
	return unpack.Bool.UnpackInto(thread, v, dst)
}

func (ui sloppyBoolUnpackerInto) Canonicalize(thread *starlark.Thread, v starlark.Value) (starlark.Value, error) {
	var b bool
	if err := ui.UnpackInto(thread, v, &b); err != nil {
		return nil, err
	}
	return starlark.Bool(b), nil
}

func (sloppyBoolUnpackerInto) GetConcatenationOperator() syntax.Token {
	return 0
}

type boolAttrType struct{}

var BoolAttrType AttrType = boolAttrType{}

func (boolAttrType) Type() string {
	return "bool"
}

func (boolAttrType) Encode(out *model_starlark_pb.Attr) error {
	out.Type = &model_starlark_pb.Attr_Bool{
		Bool: &emptypb.Empty{},
	}
	return nil
}

func (boolAttrType) GetCanonicalizer(currentPackage pg_label.CanonicalPackage) unpack.Canonicalizer {
	return sloppyBoolUnpackerInto{}
}

func (boolAttrType) IsOutput() (string, bool) {
	return "", false
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

func (at *intAttrType) Encode(out *model_starlark_pb.Attr) error {
	out.Type = &model_starlark_pb.Attr_Int{
		Int: &model_starlark_pb.Attr_IntType{
			Values: at.values,
		},
	}
	return nil
}

func (intAttrType) GetCanonicalizer(currentPackage pg_label.CanonicalPackage) unpack.Canonicalizer {
	return unpack.Int[int32]()
}

func (intAttrType) IsOutput() (string, bool) {
	return "", false
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

func (intListAttrType) Encode(out *model_starlark_pb.Attr) error {
	out.Type = &model_starlark_pb.Attr_IntList{
		IntList: &model_starlark_pb.Attr_IntListType{},
	}
	return nil
}

func (intListAttrType) GetCanonicalizer(currentPackage pg_label.CanonicalPackage) unpack.Canonicalizer {
	return unpack.List(unpack.Int[int32]())
}

func (intListAttrType) IsOutput() (string, bool) {
	return "", false
}

type labelAttrType struct {
	allowNone       bool
	allowSingleFile bool
	executable      bool
	valueAllowFiles []string
	valueCfg        TransitionDefinition
}

func NewLabelAttrType(allowNone, allowSingleFile, executable bool, valueAllowFiles []string, valueCfg TransitionDefinition) AttrType {
	return &labelAttrType{
		allowNone:       allowNone,
		allowSingleFile: allowSingleFile,
		executable:      executable,
		valueAllowFiles: valueAllowFiles,
		valueCfg:        valueCfg,
	}
}

func (labelAttrType) Type() string {
	return "label"
}

func (at *labelAttrType) Encode(out *model_starlark_pb.Attr) error {
	valueCfg, err := at.valueCfg.EncodeReference()
	if err != nil {
		return err
	}
	out.Type = &model_starlark_pb.Attr_Label{
		Label: &model_starlark_pb.Attr_LabelType{
			AllowNone:       at.allowNone,
			AllowSingleFile: at.allowSingleFile,
			Executable:      at.executable,
			ValueOptions: &model_starlark_pb.Attr_LabelOptions{
				AllowFiles: at.valueAllowFiles,
				Cfg:        valueCfg,
			},
		},
	}
	return nil
}

func (ui *labelAttrType) GetCanonicalizer(currentPackage pg_label.CanonicalPackage) unpack.Canonicalizer {
	canonicalizer := NewLabelOrStringUnpackerInto(currentPackage)
	if ui.allowNone {
		canonicalizer = unpack.IfNotNone(canonicalizer)
	}
	return canonicalizer
}

func (labelAttrType) IsOutput() (string, bool) {
	return "", false
}

type labelKeyedStringDictAttrType struct {
	dictKeyAllowFiles []string
	dictKeyCfg        TransitionDefinition
}

func NewLabelKeyedStringDictAttrType(dictKeyAllowFiles []string, dictKeyCfg TransitionDefinition) AttrType {
	return &labelKeyedStringDictAttrType{
		dictKeyAllowFiles: dictKeyAllowFiles,
		dictKeyCfg:        dictKeyCfg,
	}
}

func (labelKeyedStringDictAttrType) Type() string {
	return "label_keyed_string_dict"
}

func (at *labelKeyedStringDictAttrType) Encode(out *model_starlark_pb.Attr) error {
	dictKeyCfg, err := at.dictKeyCfg.EncodeReference()
	if err != nil {
		return err
	}
	out.Type = &model_starlark_pb.Attr_LabelKeyedStringDict{
		LabelKeyedStringDict: &model_starlark_pb.Attr_LabelKeyedStringDictType{
			DictKeyOptions: &model_starlark_pb.Attr_LabelOptions{
				AllowFiles: at.dictKeyAllowFiles,
				Cfg:        dictKeyCfg,
			},
		},
	}
	return nil
}

func (labelKeyedStringDictAttrType) GetCanonicalizer(currentPackage pg_label.CanonicalPackage) unpack.Canonicalizer {
	return unpack.Dict(NewLabelOrStringUnpackerInto(currentPackage), unpack.String)
}

func (labelKeyedStringDictAttrType) IsOutput() (string, bool) {
	return "", false
}

type labelListAttrType struct {
	listValueAllowFiles []string
	listValueCfg        TransitionDefinition
}

func NewLabelListAttrType(listValueAllowFiles []string, listValueCfg TransitionDefinition) AttrType {
	return &labelListAttrType{
		listValueAllowFiles: listValueAllowFiles,
		listValueCfg:        listValueCfg,
	}
}

func (labelListAttrType) Type() string {
	return "label_list"
}

func (at *labelListAttrType) Encode(out *model_starlark_pb.Attr) error {
	listValueCfg, err := at.listValueCfg.EncodeReference()
	if err != nil {
		return err
	}
	out.Type = &model_starlark_pb.Attr_LabelList{
		LabelList: &model_starlark_pb.Attr_LabelListType{
			ListValueOptions: &model_starlark_pb.Attr_LabelOptions{
				AllowFiles: at.listValueAllowFiles,
				Cfg:        listValueCfg,
			},
		},
	}
	return nil
}

func (labelListAttrType) GetCanonicalizer(currentPackage pg_label.CanonicalPackage) unpack.Canonicalizer {
	return unpack.List(NewLabelOrStringUnpackerInto(currentPackage))
}

func (labelListAttrType) IsOutput() (string, bool) {
	return "", false
}

type outputAttrType struct {
	filenameTemplate string
}

func NewOutputAttrType(filenameTemplate string) AttrType {
	return &outputAttrType{
		filenameTemplate: filenameTemplate,
	}
}

func (outputAttrType) Type() string {
	return "output"
}

func (at *outputAttrType) Encode(out *model_starlark_pb.Attr) error {
	out.Type = &model_starlark_pb.Attr_Output{
		Output: &model_starlark_pb.Attr_OutputType{
			FilenameTemplate: at.filenameTemplate,
		},
	}
	return nil
}

func (outputAttrType) GetCanonicalizer(currentPackage pg_label.CanonicalPackage) unpack.Canonicalizer {
	return NewLabelOrStringUnpackerInto(currentPackage)
}

func (at *outputAttrType) IsOutput() (string, bool) {
	return at.filenameTemplate, true
}

type outputListAttrType struct{}

func NewOutputListAttrType() AttrType {
	return &outputListAttrType{}
}

func (at *outputListAttrType) Type() string {
	return "output_list"
}

func (at *outputListAttrType) Encode(out *model_starlark_pb.Attr) error {
	out.Type = &model_starlark_pb.Attr_OutputList{
		OutputList: &model_starlark_pb.Attr_OutputListType{},
	}
	return nil
}

func (at *outputListAttrType) GetCanonicalizer(currentPackage pg_label.CanonicalPackage) unpack.Canonicalizer {
	return unpack.List(NewLabelOrStringUnpackerInto(currentPackage))
}

func (outputListAttrType) IsOutput() (string, bool) {
	return "", true
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

func (stringAttrType) Encode(out *model_starlark_pb.Attr) error {
	out.Type = &model_starlark_pb.Attr_String_{
		String_: &model_starlark_pb.Attr_StringType{},
	}
	return nil
}

func (stringAttrType) GetCanonicalizer(currentPackage pg_label.CanonicalPackage) unpack.Canonicalizer {
	return unpack.String
}

func (stringAttrType) IsOutput() (string, bool) {
	return "", false
}

type stringDictAttrType struct{}

func NewStringDictAttrType() AttrType {
	return &stringDictAttrType{}
}

func (stringDictAttrType) Type() string {
	return "string_dict"
}

func (stringDictAttrType) Encode(out *model_starlark_pb.Attr) error {
	out.Type = &model_starlark_pb.Attr_StringDict{
		StringDict: &model_starlark_pb.Attr_StringDictType{},
	}
	return nil
}

func (stringDictAttrType) GetCanonicalizer(currentPackage pg_label.CanonicalPackage) unpack.Canonicalizer {
	return unpack.Dict(unpack.String, unpack.String)
}

func (stringDictAttrType) IsOutput() (string, bool) {
	return "", false
}

type stringListAttrType struct{}

func NewStringListAttrType() AttrType {
	return &stringListAttrType{}
}

func (stringListAttrType) Type() string {
	return "string_list"
}

func (stringListAttrType) Encode(out *model_starlark_pb.Attr) error {
	out.Type = &model_starlark_pb.Attr_StringList{
		StringList: &model_starlark_pb.Attr_StringListType{},
	}
	return nil
}

func (stringListAttrType) GetCanonicalizer(currentPackage pg_label.CanonicalPackage) unpack.Canonicalizer {
	return unpack.List(unpack.String)
}

func (stringListAttrType) IsOutput() (string, bool) {
	return "", false
}

type stringListDictAttrType struct{}

func NewStringListDictAttrType() AttrType {
	return &stringListDictAttrType{}
}

func (stringListDictAttrType) Type() string {
	return "string_list_dict"
}

func (at *stringListDictAttrType) Encode(out *model_starlark_pb.Attr) error {
	out.Type = &model_starlark_pb.Attr_StringListDict{
		StringListDict: &model_starlark_pb.Attr_StringListDictType{},
	}
	return nil
}

func (stringListDictAttrType) GetCanonicalizer(currentPackage pg_label.CanonicalPackage) unpack.Canonicalizer {
	return unpack.Dict(unpack.String, unpack.List(unpack.String))
}

func (stringListDictAttrType) IsOutput() (string, bool) {
	return "", false
}

func encodeNamedAttrs(attrs map[pg_label.StarlarkIdentifier]*Attr, path map[starlark.Value]struct{}, options *ValueEncodingOptions) (model_core.PatchedMessage[[]*model_starlark_pb.NamedAttr, model_core.CreatedObjectTree], bool, error) {
	encodedAttrs := make([]*model_starlark_pb.NamedAttr, 0, len(attrs))
	patcher := model_core.NewReferenceMessagePatcher[model_core.CreatedObjectTree]()
	needsCode := false
	for _, name := range slices.SortedFunc(
		maps.Keys(attrs),
		func(a, b pg_label.StarlarkIdentifier) int { return strings.Compare(a.String(), b.String()) },
	) {
		attr, attrNeedsCode, err := attrs[name].Encode(path, options)
		if err != nil {
			return model_core.PatchedMessage[[]*model_starlark_pb.NamedAttr, model_core.CreatedObjectTree]{}, false, fmt.Errorf("attr %#v: %w", name, err)
		}
		encodedAttrs = append(encodedAttrs, &model_starlark_pb.NamedAttr{
			Name: name.String(),
			Attr: attr.Message,
		})
		patcher.Merge(attr.Patcher)
		needsCode = needsCode || attrNeedsCode
	}
	return model_core.NewPatchedMessage(encodedAttrs, patcher), needsCode, nil
}
