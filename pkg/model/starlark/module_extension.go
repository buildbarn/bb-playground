package starlark

import (
	"errors"
	"maps"
	"slices"
	"strings"

	pg_label "github.com/buildbarn/bonanza/pkg/label"
	model_core "github.com/buildbarn/bonanza/pkg/model/core"
	model_starlark_pb "github.com/buildbarn/bonanza/pkg/proto/model/starlark"
	"github.com/buildbarn/bonanza/pkg/storage/object"

	"go.starlark.net/starlark"
)

type ModuleExtensionDefinition interface {
	EncodableValue
}

type moduleExtension struct {
	ModuleExtensionDefinition
}

var (
	_ starlark.Value = &moduleExtension{}
	_ EncodableValue = &moduleExtension{}
)

func NewModuleExtension(definition ModuleExtensionDefinition) starlark.Value {
	return &moduleExtension{
		ModuleExtensionDefinition: definition,
	}
}

func (me *moduleExtension) String() string {
	return "<module_extension>"
}

func (me *moduleExtension) Type() string {
	return "module_extension"
}

func (me *moduleExtension) Freeze() {}

func (me *moduleExtension) Truth() starlark.Bool {
	return starlark.True
}

func (me *moduleExtension) Hash(thread *starlark.Thread) (uint32, error) {
	return 0, errors.New("module_extension cannot be hashed")
}

type starlarkModuleExtensionDefinition struct {
	implementation NamedFunction
	tagClasses     map[pg_label.StarlarkIdentifier]*TagClass
}

func NewStarlarkModuleExtensionDefinition(implementation NamedFunction, tagClasses map[pg_label.StarlarkIdentifier]*TagClass) ModuleExtensionDefinition {
	return &starlarkModuleExtensionDefinition{
		implementation: implementation,
		tagClasses:     tagClasses,
	}
}

func (med *starlarkModuleExtensionDefinition) EncodeValue(path map[starlark.Value]struct{}, currentIdentifier *pg_label.CanonicalStarlarkIdentifier, options *ValueEncodingOptions) (model_core.PatchedMessage[*model_starlark_pb.Value, model_core.CreatedObjectTree], bool, error) {
	implementation, needsCode, err := med.implementation.Encode(path, options)
	if err != nil {
		return model_core.PatchedMessage[*model_starlark_pb.Value, model_core.CreatedObjectTree]{}, false, err
	}
	patcher := implementation.Patcher

	tagClasses := make([]*model_starlark_pb.ModuleExtension_NamedTagClass, 0, len(med.tagClasses))
	for _, name := range slices.SortedFunc(
		maps.Keys(med.tagClasses),
		func(a, b pg_label.StarlarkIdentifier) int { return strings.Compare(a.String(), b.String()) },
	) {
		encodedTagClass, tagClassNeedsCode, err := med.tagClasses[name].Encode(path, options)
		if err != nil {
			return model_core.PatchedMessage[*model_starlark_pb.Value, model_core.CreatedObjectTree]{}, false, err
		}
		tagClasses = append(tagClasses, &model_starlark_pb.ModuleExtension_NamedTagClass{
			Name:     name.String(),
			TagClass: encodedTagClass.Message,
		})
		patcher.Merge(encodedTagClass.Patcher)
		needsCode = needsCode || tagClassNeedsCode
	}

	return model_core.NewPatchedMessage(
		&model_starlark_pb.Value{
			Kind: &model_starlark_pb.Value_ModuleExtension{
				ModuleExtension: &model_starlark_pb.ModuleExtension{
					Implementation: implementation.Message,
					TagClasses:     tagClasses,
				},
			},
		},
		patcher,
	), needsCode, nil
}

type protoModuleExtensionDefinition struct {
	message model_core.Message[*model_starlark_pb.ModuleExtension, object.OutgoingReferences[object.LocalReference]]
}

func NewProtoModuleExtensionDefinition(message model_core.Message[*model_starlark_pb.ModuleExtension, object.OutgoingReferences[object.LocalReference]]) ModuleExtensionDefinition {
	return &protoModuleExtensionDefinition{
		message: message,
	}
}

func (med *protoModuleExtensionDefinition) EncodeValue(path map[starlark.Value]struct{}, currentIdentifier *pg_label.CanonicalStarlarkIdentifier, options *ValueEncodingOptions) (model_core.PatchedMessage[*model_starlark_pb.Value, model_core.CreatedObjectTree], bool, error) {
	patchedMessage := model_core.NewPatchedMessageFromExisting(
		med.message,
		func(index int) model_core.CreatedObjectTree {
			return model_core.ExistingCreatedObjectTree
		},
	)
	return model_core.NewPatchedMessage(
		&model_starlark_pb.Value{
			Kind: &model_starlark_pb.Value_ModuleExtension{
				ModuleExtension: patchedMessage.Message,
			},
		},
		patchedMessage.Patcher,
	), false, nil
}
