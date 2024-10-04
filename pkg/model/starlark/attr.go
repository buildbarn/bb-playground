package starlark

import (
	"fmt"

	pg_label "github.com/buildbarn/bb-playground/pkg/label"
	model_core "github.com/buildbarn/bb-playground/pkg/model/core"
	model_starlark_pb "github.com/buildbarn/bb-playground/pkg/proto/model/starlark"
	"github.com/buildbarn/bb-playground/pkg/storage/dag"

	"go.starlark.net/starlark"
)

type attr struct {
	attrType  AttrType
	mandatory bool
}

var _ EncodableValue = &attr{}

func NewAttr(attrType AttrType, mandatory bool) starlark.Value {
	return &attr{
		attrType:  attrType,
		mandatory: mandatory,
	}
}

func (l *attr) String() string {
	return fmt.Sprintf("<attr.%s>", l.attrType.Type())
}

func (l *attr) Type() string {
	return "attr." + l.attrType.Type()
}

func (l *attr) Freeze() {}

func (l *attr) Truth() starlark.Bool {
	return starlark.True
}

func (l *attr) Hash() (uint32, error) {
	// TODO
	return 0, nil
}

func (l *attr) EncodeValue(path map[starlark.Value]struct{}, options *ValueEncodingOptions) (model_core.PatchedMessage[*model_starlark_pb.Value, dag.ObjectContentsWalker], bool, error) {
	return model_core.PatchedMessage[*model_starlark_pb.Value, dag.ObjectContentsWalker]{
		Message: &model_starlark_pb.Value{
			Kind: &model_starlark_pb.Value_Attr{
				Attr: &model_starlark_pb.Attr{
					Mandatory: l.mandatory,
				},
			},
		},
		Patcher: model_core.NewReferenceMessagePatcher[dag.ObjectContentsWalker](),
	}, false, nil
}

type AttrType interface {
	Type() string
}

type boolAttrType struct {
	defaultValue bool
}

func NewBoolAttrType(defaultValue bool) AttrType {
	return &boolAttrType{
		defaultValue: defaultValue,
	}
}

func (boolAttrType) Type() string {
	return "bool"
}

type intAttrType struct {
	defaultValue int32
}

func NewIntAttrType(defaultValue int32) AttrType {
	return &intAttrType{
		defaultValue: defaultValue,
	}
}

func (intAttrType) Type() string {
	return "int"
}

type labelAttrType struct {
	defaultValue *pg_label.CanonicalLabel
}

func NewLabelAttrType(defaultValue *pg_label.CanonicalLabel) AttrType {
	return &labelAttrType{
		defaultValue: defaultValue,
	}
}

func (labelAttrType) Type() string {
	return "label"
}

type labelKeyedStringDictAttrType struct {
	allowEmpty    bool
	defaultValues map[pg_label.CanonicalLabel]string
}

func NewLabelKeyedStringDictAttrType(allowEmpty bool, defaultValues map[pg_label.CanonicalLabel]string) AttrType {
	return &labelKeyedStringDictAttrType{
		defaultValues: defaultValues,
	}
}

func (labelKeyedStringDictAttrType) Type() string {
	return "label_keyed_string_dict"
}

type labelListAttrType struct {
	allowEmpty    bool
	defaultValues []pg_label.CanonicalLabel
}

func NewLabelListAttrType(allowEmpty bool, defaultValues []pg_label.CanonicalLabel) AttrType {
	return &labelListAttrType{
		defaultValues: defaultValues,
	}
}

func (labelListAttrType) Type() string {
	return "label_list"
}

type outputAttrType struct{}

var OutputAttrType AttrType = outputAttrType{}

func (outputAttrType) Type() string {
	return "output"
}

type stringAttrType struct {
	defaultValue string
}

func NewStringAttrType(defaultValue string) AttrType {
	return &stringAttrType{
		defaultValue: defaultValue,
	}
}

func (stringAttrType) Type() string {
	return "string"
}

type stringDictAttrType struct {
	defaultValues map[string]string
}

func NewStringDictAttrType(defaultValues map[string]string) AttrType {
	return &stringDictAttrType{
		defaultValues: defaultValues,
	}
}

func (stringDictAttrType) Type() string {
	return "string_dict"
}

type stringListAttrType struct {
	defaultValues []string
}

func NewStringListAttrType(defaultValues []string) AttrType {
	return &stringListAttrType{
		defaultValues: defaultValues,
	}
}

func (stringListAttrType) Type() string {
	return "string_list"
}
