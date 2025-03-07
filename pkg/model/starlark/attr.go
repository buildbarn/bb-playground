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
	"github.com/buildbarn/bonanza/pkg/storage/object"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/emptypb"

	"go.starlark.net/starlark"
	"go.starlark.net/syntax"
)

type Attr[TReference any, TMetadata model_core.CloneableReferenceMetadata] struct {
	attrType     AttrType
	defaultValue starlark.Value
}

var (
	_ EncodableValue[object.LocalReference, model_core.CloneableReferenceMetadata] = (*Attr[object.LocalReference, model_core.CloneableReferenceMetadata])(nil)
	_ starlark.Comparable                                                          = (*Attr[object.LocalReference, model_core.CloneableReferenceMetadata])(nil)
)

func NewAttr[TReference any, TMetadata model_core.CloneableReferenceMetadata](attrType AttrType, defaultValue starlark.Value) *Attr[TReference, TMetadata] {
	return &Attr[TReference, TMetadata]{
		attrType:     attrType,
		defaultValue: defaultValue,
	}
}

func (a *Attr[TReference, TMetadata]) String() string {
	return fmt.Sprintf("<attr.%s>", a.attrType.Type())
}

func (a *Attr[TReference, TMetadata]) Type() string {
	return "attr." + a.attrType.Type()
}

func (Attr[TReference, TMetadata]) Freeze() {}

func (Attr[TReference, TMetadata]) Truth() starlark.Bool {
	return starlark.True
}

func (a *Attr[TReference, TMetadata]) Hash(thread *starlark.Thread) (uint32, error) {
	return 0, fmt.Errorf("attr.%s cannot be hashed", a.attrType.Type())
}

func (a *Attr[TReference, TMetadata]) Encode(path map[starlark.Value]struct{}, options *ValueEncodingOptions[TReference, TMetadata]) (model_core.PatchedMessage[*model_starlark_pb.Attr, TMetadata], bool, error) {
	patcher := model_core.NewReferenceMessagePatcher[TMetadata]()
	var attr model_starlark_pb.Attr
	var needsCode bool

	if a.defaultValue != nil {
		defaultValue, defaultValueNeedsCode, err := EncodeValue[TReference, TMetadata](a.defaultValue, path, nil, options)
		if err != nil {
			return model_core.PatchedMessage[*model_starlark_pb.Attr, TMetadata]{}, false, err
		}
		attr.Default = defaultValue.Message
		patcher.Merge(defaultValue.Patcher)
		needsCode = needsCode || defaultValueNeedsCode
	}

	if err := a.attrType.Encode(&attr); err != nil {
		return model_core.PatchedMessage[*model_starlark_pb.Attr, TMetadata]{}, false, err
	}
	return model_core.NewPatchedMessage(&attr, patcher), needsCode, nil
}

func (a *Attr[TReference, TMetadata]) EncodeValue(path map[starlark.Value]struct{}, currentIdentifier *pg_label.CanonicalStarlarkIdentifier, options *ValueEncodingOptions[TReference, TMetadata]) (model_core.PatchedMessage[*model_starlark_pb.Value, TMetadata], bool, error) {
	attr, needsCode, err := a.Encode(path, options)
	if err != nil {
		return model_core.PatchedMessage[*model_starlark_pb.Value, TMetadata]{}, false, err
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

func (a *Attr[TReference, TMetadata]) CompareSameType(thread *starlark.Thread, op syntax.Token, other starlark.Value, depth int) (bool, error) {
	// Compare the types.
	var m1, m2 model_starlark_pb.Attr
	if err := a.attrType.Encode(&m1); err != nil {
		return false, err
	}
	a2 := other.(*Attr[TReference, TMetadata])
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

type labelAttrType[TReference any, TMetadata model_core.CloneableReferenceMetadata] struct {
	allowNone       bool
	allowSingleFile bool
	executable      bool
	valueAllowFiles []string
	valueCfg        TransitionDefinition[TReference, TMetadata]
}

func NewLabelAttrType[TReference any, TMetadata model_core.CloneableReferenceMetadata](allowNone, allowSingleFile, executable bool, valueAllowFiles []string, valueCfg TransitionDefinition[TReference, TMetadata]) AttrType {
	return &labelAttrType[TReference, TMetadata]{
		allowNone:       allowNone,
		allowSingleFile: allowSingleFile,
		executable:      executable,
		valueAllowFiles: valueAllowFiles,
		valueCfg:        valueCfg,
	}
}

func (labelAttrType[TReference, TMetadata]) Type() string {
	return "label"
}

func (at *labelAttrType[TReference, TMetadata]) Encode(out *model_starlark_pb.Attr) error {
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

func (ui *labelAttrType[TReference, TMetadata]) GetCanonicalizer(currentPackage pg_label.CanonicalPackage) unpack.Canonicalizer {
	canonicalizer := NewLabelOrStringUnpackerInto[TReference, TMetadata](currentPackage)
	if ui.allowNone {
		canonicalizer = unpack.IfNotNone(canonicalizer)
	}
	return canonicalizer
}

func (labelAttrType[TReference, TMetadata]) IsOutput() (string, bool) {
	return "", false
}

type labelKeyedStringDictAttrType[TReference any, TMetadata model_core.CloneableReferenceMetadata] struct {
	dictKeyAllowFiles []string
	dictKeyCfg        TransitionDefinition[TReference, TMetadata]
}

func NewLabelKeyedStringDictAttrType[TReference any, TMetadata model_core.CloneableReferenceMetadata](dictKeyAllowFiles []string, dictKeyCfg TransitionDefinition[TReference, TMetadata]) AttrType {
	return &labelKeyedStringDictAttrType[TReference, TMetadata]{
		dictKeyAllowFiles: dictKeyAllowFiles,
		dictKeyCfg:        dictKeyCfg,
	}
}

func (labelKeyedStringDictAttrType[TReference, TMetadata]) Type() string {
	return "label_keyed_string_dict"
}

func (at *labelKeyedStringDictAttrType[TReference, TMetadata]) Encode(out *model_starlark_pb.Attr) error {
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

func (labelKeyedStringDictAttrType[TReference, TMetadata]) GetCanonicalizer(currentPackage pg_label.CanonicalPackage) unpack.Canonicalizer {
	return unpack.Dict(NewLabelOrStringUnpackerInto[TReference, TMetadata](currentPackage), unpack.String)
}

func (labelKeyedStringDictAttrType[TReference, TMetadata]) IsOutput() (string, bool) {
	return "", false
}

type labelListAttrType[TReference any, TMetadata model_core.CloneableReferenceMetadata] struct {
	listValueAllowFiles []string
	listValueCfg        TransitionDefinition[TReference, TMetadata]
}

func NewLabelListAttrType[TReference any, TMetadata model_core.CloneableReferenceMetadata](listValueAllowFiles []string, listValueCfg TransitionDefinition[TReference, TMetadata]) AttrType {
	return &labelListAttrType[TReference, TMetadata]{
		listValueAllowFiles: listValueAllowFiles,
		listValueCfg:        listValueCfg,
	}
}

func (labelListAttrType[TReference, TMetadata]) Type() string {
	return "label_list"
}

func (at *labelListAttrType[TReference, TMetadata]) Encode(out *model_starlark_pb.Attr) error {
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

func (labelListAttrType[TReference, TMetadata]) GetCanonicalizer(currentPackage pg_label.CanonicalPackage) unpack.Canonicalizer {
	return unpack.List(NewLabelOrStringUnpackerInto[TReference, TMetadata](currentPackage))
}

func (labelListAttrType[TReference, TMetadata]) IsOutput() (string, bool) {
	return "", false
}

type outputAttrType[TReference any, TMetadata model_core.CloneableReferenceMetadata] struct {
	filenameTemplate string
}

func NewOutputAttrType[TReference any, TMetadata model_core.CloneableReferenceMetadata](filenameTemplate string) AttrType {
	return &outputAttrType[TReference, TMetadata]{
		filenameTemplate: filenameTemplate,
	}
}

func (outputAttrType[TReference, TMetadata]) Type() string {
	return "output"
}

func (at *outputAttrType[TReference, TMetadata]) Encode(out *model_starlark_pb.Attr) error {
	out.Type = &model_starlark_pb.Attr_Output{
		Output: &model_starlark_pb.Attr_OutputType{
			FilenameTemplate: at.filenameTemplate,
		},
	}
	return nil
}

func (outputAttrType[TReference, TMetadata]) GetCanonicalizer(currentPackage pg_label.CanonicalPackage) unpack.Canonicalizer {
	return NewLabelOrStringUnpackerInto[TReference, TMetadata](currentPackage)
}

func (at *outputAttrType[TReference, TMetadata]) IsOutput() (string, bool) {
	return at.filenameTemplate, true
}

type outputListAttrType[TReference any, TMetadata model_core.CloneableReferenceMetadata] struct{}

func NewOutputListAttrType[TReference any, TMetadata model_core.CloneableReferenceMetadata]() AttrType {
	return &outputListAttrType[TReference, TMetadata]{}
}

func (at *outputListAttrType[TReference, TMetadata]) Type() string {
	return "output_list"
}

func (at *outputListAttrType[TReference, TMetadata]) Encode(out *model_starlark_pb.Attr) error {
	out.Type = &model_starlark_pb.Attr_OutputList{
		OutputList: &model_starlark_pb.Attr_OutputListType{},
	}
	return nil
}

func (at *outputListAttrType[TReference, TMetadata]) GetCanonicalizer(currentPackage pg_label.CanonicalPackage) unpack.Canonicalizer {
	return unpack.List(NewLabelOrStringUnpackerInto[TReference, TMetadata](currentPackage))
}

func (outputListAttrType[TReference, TMetadata]) IsOutput() (string, bool) {
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

func encodeNamedAttrs[TReference any, TMetadata model_core.CloneableReferenceMetadata](attrs map[pg_label.StarlarkIdentifier]*Attr[TReference, TMetadata], path map[starlark.Value]struct{}, options *ValueEncodingOptions[TReference, TMetadata]) (model_core.PatchedMessage[[]*model_starlark_pb.NamedAttr, TMetadata], bool, error) {
	encodedAttrs := make([]*model_starlark_pb.NamedAttr, 0, len(attrs))
	patcher := model_core.NewReferenceMessagePatcher[TMetadata]()
	needsCode := false
	for _, name := range slices.SortedFunc(
		maps.Keys(attrs),
		func(a, b pg_label.StarlarkIdentifier) int { return strings.Compare(a.String(), b.String()) },
	) {
		attr, attrNeedsCode, err := attrs[name].Encode(path, options)
		if err != nil {
			return model_core.PatchedMessage[[]*model_starlark_pb.NamedAttr, TMetadata]{}, false, fmt.Errorf("attr %#v: %w", name, err)
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
