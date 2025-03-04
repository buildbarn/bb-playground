package starlark

import (
	"errors"

	pg_label "github.com/buildbarn/bonanza/pkg/label"
	model_core "github.com/buildbarn/bonanza/pkg/model/core"
	model_starlark_pb "github.com/buildbarn/bonanza/pkg/proto/model/starlark"
	"github.com/buildbarn/bonanza/pkg/storage/object"

	"go.starlark.net/starlark"
)

type TagClass struct {
	TagClassDefinition
}

var (
	_ starlark.Value = &TagClass{}
	_ EncodableValue = &rule{}
)

func NewTagClass(definition TagClassDefinition) starlark.Value {
	return &TagClass{
		TagClassDefinition: definition,
	}
}

func (TagClass) String() string {
	return "<tag_class>"
}

func (TagClass) Type() string {
	return "tag_class"
}

func (TagClass) Freeze() {}

func (TagClass) Truth() starlark.Bool {
	return starlark.True
}

func (TagClass) Hash(thread *starlark.Thread) (uint32, error) {
	return 0, errors.New("tag_class cannot be hashed")
}

func (tc *TagClass) EncodeValue(path map[starlark.Value]struct{}, currentIdentifier *pg_label.CanonicalStarlarkIdentifier, options *ValueEncodingOptions) (model_core.PatchedMessage[*model_starlark_pb.Value, model_core.CreatedObjectTree], bool, error) {
	tagClass, needsCode, err := tc.TagClassDefinition.Encode(path, options)
	if err != nil {
		return model_core.PatchedMessage[*model_starlark_pb.Value, model_core.CreatedObjectTree]{}, false, err
	}
	return model_core.NewPatchedMessage(
		&model_starlark_pb.Value{
			Kind: &model_starlark_pb.Value_TagClass{
				TagClass: tagClass.Message,
			},
		},
		tagClass.Patcher,
	), needsCode, nil
}

type TagClassDefinition interface {
	Encode(path map[starlark.Value]struct{}, options *ValueEncodingOptions) (model_core.PatchedMessage[*model_starlark_pb.TagClass, model_core.CreatedObjectTree], bool, error)
}

type starlarkTagClassDefinition struct {
	attrs map[pg_label.StarlarkIdentifier]*Attr
}

func NewStarlarkTagClassDefinition(attrs map[pg_label.StarlarkIdentifier]*Attr) TagClassDefinition {
	return &starlarkTagClassDefinition{
		attrs: attrs,
	}
}

func (tcd *starlarkTagClassDefinition) Encode(path map[starlark.Value]struct{}, options *ValueEncodingOptions) (model_core.PatchedMessage[*model_starlark_pb.TagClass, model_core.CreatedObjectTree], bool, error) {
	encodedAttrs, needsCode, err := encodeNamedAttrs(tcd.attrs, path, options)
	if err != nil {
		return model_core.PatchedMessage[*model_starlark_pb.TagClass, model_core.CreatedObjectTree]{}, false, nil
	}
	return model_core.NewPatchedMessage(
		&model_starlark_pb.TagClass{
			Attrs: encodedAttrs.Message,
		},
		encodedAttrs.Patcher,
	), needsCode, nil
}

type protoTagClassDefinition struct {
	message model_core.Message[*model_starlark_pb.TagClass, object.OutgoingReferences[object.LocalReference]]
}

func NewProtoTagClassDefinition(message model_core.Message[*model_starlark_pb.TagClass, object.OutgoingReferences[object.LocalReference]]) TagClassDefinition {
	return &protoTagClassDefinition{
		message: message,
	}
}

func (tcd *protoTagClassDefinition) Encode(path map[starlark.Value]struct{}, options *ValueEncodingOptions) (model_core.PatchedMessage[*model_starlark_pb.TagClass, model_core.CreatedObjectTree], bool, error) {
	return model_core.NewPatchedMessageFromExisting(
		tcd.message,
		func(index int) model_core.CreatedObjectTree {
			return model_core.ExistingCreatedObjectTree
		},
	), false, nil
}
