package starlark

import (
	"errors"

	pg_label "github.com/buildbarn/bonanza/pkg/label"
	model_core "github.com/buildbarn/bonanza/pkg/model/core"
	model_starlark_pb "github.com/buildbarn/bonanza/pkg/proto/model/starlark"
	"github.com/buildbarn/bonanza/pkg/storage/object"

	"go.starlark.net/starlark"
)

type TagClass[TReference any, TMetadata model_core.CloneableReferenceMetadata] struct {
	TagClassDefinition[TReference, TMetadata]
}

var (
	_ starlark.Value                                                               = (*TagClass[object.LocalReference, model_core.CloneableReferenceMetadata])(nil)
	_ EncodableValue[object.LocalReference, model_core.CloneableReferenceMetadata] = (*TagClass[object.LocalReference, model_core.CloneableReferenceMetadata])(nil)
)

func NewTagClass[TReference any, TMetadata model_core.CloneableReferenceMetadata](definition TagClassDefinition[TReference, TMetadata]) starlark.Value {
	return &TagClass[TReference, TMetadata]{
		TagClassDefinition: definition,
	}
}

func (TagClass[TReference, TMetadata]) String() string {
	return "<tag_class>"
}

func (TagClass[TReference, TMetadata]) Type() string {
	return "tag_class"
}

func (TagClass[TReference, TMetadata]) Freeze() {}

func (TagClass[TReference, TMetadata]) Truth() starlark.Bool {
	return starlark.True
}

func (TagClass[TReference, TMetadata]) Hash(thread *starlark.Thread) (uint32, error) {
	return 0, errors.New("tag_class cannot be hashed")
}

func (tc *TagClass[TReference, TMetadata]) EncodeValue(path map[starlark.Value]struct{}, currentIdentifier *pg_label.CanonicalStarlarkIdentifier, options *ValueEncodingOptions[TReference, TMetadata]) (model_core.PatchedMessage[*model_starlark_pb.Value, TMetadata], bool, error) {
	tagClass, needsCode, err := tc.TagClassDefinition.Encode(path, options)
	if err != nil {
		return model_core.PatchedMessage[*model_starlark_pb.Value, TMetadata]{}, false, err
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

type TagClassDefinition[TReference any, TMetadata model_core.CloneableReferenceMetadata] interface {
	Encode(path map[starlark.Value]struct{}, options *ValueEncodingOptions[TReference, TMetadata]) (model_core.PatchedMessage[*model_starlark_pb.TagClass, TMetadata], bool, error)
}

type starlarkTagClassDefinition[TReference any, TMetadata model_core.CloneableReferenceMetadata] struct {
	attrs map[pg_label.StarlarkIdentifier]*Attr[TReference, TMetadata]
}

func NewStarlarkTagClassDefinition[TReference any, TMetadata model_core.CloneableReferenceMetadata](attrs map[pg_label.StarlarkIdentifier]*Attr[TReference, TMetadata]) TagClassDefinition[TReference, TMetadata] {
	return &starlarkTagClassDefinition[TReference, TMetadata]{
		attrs: attrs,
	}
}

func (tcd *starlarkTagClassDefinition[TReference, TMetadata]) Encode(path map[starlark.Value]struct{}, options *ValueEncodingOptions[TReference, TMetadata]) (model_core.PatchedMessage[*model_starlark_pb.TagClass, TMetadata], bool, error) {
	encodedAttrs, needsCode, err := encodeNamedAttrs(tcd.attrs, path, options)
	if err != nil {
		return model_core.PatchedMessage[*model_starlark_pb.TagClass, TMetadata]{}, false, nil
	}
	return model_core.NewPatchedMessage(
		&model_starlark_pb.TagClass{
			Attrs: encodedAttrs.Message,
		},
		encodedAttrs.Patcher,
	), needsCode, nil
}

type protoTagClassDefinition[TReference object.BasicReference, TMetadata model_core.CloneableReferenceMetadata] struct {
	message model_core.Message[*model_starlark_pb.TagClass, TReference]
}

func NewProtoTagClassDefinition[TReference object.BasicReference, TMetadata model_core.CloneableReferenceMetadata](message model_core.Message[*model_starlark_pb.TagClass, TReference]) TagClassDefinition[TReference, TMetadata] {
	return &protoTagClassDefinition[TReference, TMetadata]{
		message: message,
	}
}

func (tcd *protoTagClassDefinition[TReference, TMetadata]) Encode(path map[starlark.Value]struct{}, options *ValueEncodingOptions[TReference, TMetadata]) (model_core.PatchedMessage[*model_starlark_pb.TagClass, TMetadata], bool, error) {
	return model_core.NewPatchedMessageFromExistingCaptured(options.ObjectCapturer, tcd.message), false, nil
}
