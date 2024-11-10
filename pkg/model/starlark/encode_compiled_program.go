package starlark

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"maps"
	"math/big"
	"slices"
	"strings"

	pg_label "github.com/buildbarn/bb-playground/pkg/label"
	model_core "github.com/buildbarn/bb-playground/pkg/model/core"
	"github.com/buildbarn/bb-playground/pkg/model/core/btree"
	model_encoding "github.com/buildbarn/bb-playground/pkg/model/encoding"
	model_parser "github.com/buildbarn/bb-playground/pkg/model/parser"
	model_starlark_pb "github.com/buildbarn/bb-playground/pkg/proto/model/starlark"
	"github.com/buildbarn/bb-playground/pkg/storage/dag"
	"github.com/buildbarn/bb-playground/pkg/storage/object"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/emptypb"

	"go.starlark.net/starlark"
	"go.starlark.net/starlarkstruct"
)

const ValueEncodingOptionsKey = "value_encoding_options"

type ValueEncodingOptions struct {
	CurrentFilename pg_label.CanonicalLabel

	// Options to use when storing Starlark values in separate objects.
	ObjectEncoder          model_encoding.BinaryEncoder
	ObjectReferenceFormat  object.ReferenceFormat
	ObjectMinimumSizeBytes int
	ObjectMaximumSizeBytes int
}

// newSplitBTreeBuilder creates a B-tree builder that stores a minimium
// of one entry in leaves, and a minimum of two entries in parents. This
// ensures that very large values are stored in separate objects, while
// ensuring that the B-tree converges to a single root.
func newSplitBTreeBuilder[T proto.Message](options *ValueEncodingOptions, parentNodeComputer btree.ParentNodeComputer[T, dag.ObjectContentsWalker]) btree.Builder[T, dag.ObjectContentsWalker] {
	return btree.NewSplitProllyBuilder(
		options.ObjectMinimumSizeBytes,
		options.ObjectMaximumSizeBytes,
		btree.NewObjectCreatingNodeMerger(
			options.ObjectEncoder,
			options.ObjectReferenceFormat,
			parentNodeComputer,
		),
	)
}

type EncodableValue interface {
	EncodeValue(path map[starlark.Value]struct{}, currentIdentifier *pg_label.CanonicalStarlarkIdentifier, options *ValueEncodingOptions) (model_core.PatchedMessage[*model_starlark_pb.Value, dag.ObjectContentsWalker], bool, error)
}

func EncodeCompiledProgram(program *starlark.Program, globals starlark.StringDict, options *ValueEncodingOptions) (model_core.PatchedMessage[*model_starlark_pb.CompiledProgram, dag.ObjectContentsWalker], error) {
	referencedGlobals := make(map[string]model_core.PatchedMessage[*model_starlark_pb.Value, dag.ObjectContentsWalker], len(globals))
	patcher := model_core.NewReferenceMessagePatcher[dag.ObjectContentsWalker]()
	needsCode := false
	for name, value := range globals {
		identifier, err := pg_label.NewStarlarkIdentifier(name)
		if err != nil {
			return model_core.PatchedMessage[*model_starlark_pb.CompiledProgram, dag.ObjectContentsWalker]{}, err
		}
		currentIdentifier := options.CurrentFilename.AppendStarlarkIdentifier(identifier)
		if _, ok := value.(NamedGlobal); ok || identifier.IsPublic() {
			encodedValue, valueNeedsCode, err := EncodeValue(value, map[starlark.Value]struct{}{}, &currentIdentifier, options)
			if err != nil {
				return model_core.PatchedMessage[*model_starlark_pb.CompiledProgram, dag.ObjectContentsWalker]{}, fmt.Errorf("global %#v: %w", name, err)
			}
			referencedGlobals[name] = encodedValue
			needsCode = needsCode || valueNeedsCode
		}
	}

	encodedGlobals := make([]*model_starlark_pb.NamedValue, 0, len(referencedGlobals))
	for _, name := range slices.Sorted(maps.Keys(referencedGlobals)) {
		encodedValue := referencedGlobals[name]
		encodedGlobals = append(encodedGlobals, &model_starlark_pb.NamedValue{
			Name:  name,
			Value: encodedValue.Message,
		})
		patcher.Merge(encodedValue.Patcher)
	}

	var code bytes.Buffer
	if needsCode {
		if err := program.Write(&code); err != nil {
			return model_core.PatchedMessage[*model_starlark_pb.CompiledProgram, dag.ObjectContentsWalker]{}, err
		}
	}

	return model_core.PatchedMessage[*model_starlark_pb.CompiledProgram, dag.ObjectContentsWalker]{
		Message: &model_starlark_pb.CompiledProgram{
			Globals: encodedGlobals,
			Code:    code.Bytes(),
		},
		Patcher: patcher,
	}, nil
}

func EncodeValue(value starlark.Value, path map[starlark.Value]struct{}, currentIdentifier *pg_label.CanonicalStarlarkIdentifier, options *ValueEncodingOptions) (model_core.PatchedMessage[*model_starlark_pb.Value, dag.ObjectContentsWalker], bool, error) {
	if value == starlark.None {
		return model_core.NewSimplePatchedMessage[dag.ObjectContentsWalker](&model_starlark_pb.Value{
			Kind: &model_starlark_pb.Value_None{
				None: &emptypb.Empty{},
			},
		}), false, nil
	}
	switch typedValue := value.(type) {
	case starlark.Bool:
		return model_core.NewSimplePatchedMessage[dag.ObjectContentsWalker](&model_starlark_pb.Value{
			Kind: &model_starlark_pb.Value_Bool{
				Bool: bool(typedValue),
			},
		}), false, nil
	case *starlark.Builtin:
		return model_core.NewSimplePatchedMessage[dag.ObjectContentsWalker](&model_starlark_pb.Value{
			Kind: &model_starlark_pb.Value_Builtin{
				Builtin: typedValue.Name(),
			},
		}), false, nil
	case starlark.Bytes:
		return model_core.NewSimplePatchedMessage[dag.ObjectContentsWalker](&model_starlark_pb.Value{
			Kind: &model_starlark_pb.Value_Bytes{
				Bytes: []byte(typedValue),
			},
		}), false, nil
	case *starlark.Dict:
		if _, ok := path[value]; ok {
			return model_core.PatchedMessage[*model_starlark_pb.Value, dag.ObjectContentsWalker]{}, false, errors.New("value is defined recursively")
		}
		path[value] = struct{}{}
		defer delete(path, value)

		treeBuilder := newSplitBTreeBuilder(
			options,
			/* parentNodeComputer = */ func(contents *object.Contents, childNodes []*model_starlark_pb.Dict_Entry, outgoingReferences object.OutgoingReferences, metadata []dag.ObjectContentsWalker) (model_core.PatchedMessage[*model_starlark_pb.Dict_Entry, dag.ObjectContentsWalker], error) {
				patcher := model_core.NewReferenceMessagePatcher[dag.ObjectContentsWalker]()
				return model_core.PatchedMessage[*model_starlark_pb.Dict_Entry, dag.ObjectContentsWalker]{
					Message: &model_starlark_pb.Dict_Entry{
						Level: &model_starlark_pb.Dict_Entry_Parent_{
							Parent: &model_starlark_pb.Dict_Entry_Parent{
								Reference: patcher.AddReference(contents.GetReference(), dag.NewSimpleObjectContentsWalker(contents, metadata)),
							},
						},
					},
					Patcher: patcher,
				}, nil
			},
		)

		needsCode := false
		for key, value := range starlark.Entries(typedValue) {
			encodedKey, keyNeedsCode, err := EncodeValue(key, path, nil, options)
			if err != nil {
				return model_core.PatchedMessage[*model_starlark_pb.Value, dag.ObjectContentsWalker]{}, false, fmt.Errorf("in key: %w", err)
			}
			encodedValue, valueNeedsCode, err := EncodeValue(value, path, nil, options)
			if err != nil {
				return model_core.PatchedMessage[*model_starlark_pb.Value, dag.ObjectContentsWalker]{}, false, fmt.Errorf("in value: %w", err)
			}
			encodedKey.Patcher.Merge(encodedValue.Patcher)
			needsCode = needsCode || keyNeedsCode || valueNeedsCode
			if err := treeBuilder.PushChild(model_core.PatchedMessage[*model_starlark_pb.Dict_Entry, dag.ObjectContentsWalker]{
				Message: &model_starlark_pb.Dict_Entry{
					Level: &model_starlark_pb.Dict_Entry_Leaf_{
						Leaf: &model_starlark_pb.Dict_Entry_Leaf{
							Key:   encodedKey.Message,
							Value: encodedValue.Message,
						},
					},
				},
				Patcher: encodedKey.Patcher,
			}); err != nil {
				return model_core.PatchedMessage[*model_starlark_pb.Value, dag.ObjectContentsWalker]{}, false, err
			}
		}

		entries, err := treeBuilder.FinalizeList()
		if err != nil {
			return model_core.PatchedMessage[*model_starlark_pb.Value, dag.ObjectContentsWalker]{}, false, err
		}

		// TODO: This should use inlinedtree to ensure the
		// resulting Value object is not too large.
		return model_core.PatchedMessage[*model_starlark_pb.Value, dag.ObjectContentsWalker]{
			Message: &model_starlark_pb.Value{
				Kind: &model_starlark_pb.Value_Dict{
					Dict: &model_starlark_pb.Dict{
						Entries: entries.Message,
					},
				},
			},
			Patcher: entries.Patcher,
		}, needsCode, nil
	case *starlark.Function:
		return NewNamedFunction(NewStarlarkNamedFunctionDefinition(typedValue)).
			EncodeValue(path, currentIdentifier, options)
	case starlark.Int:
		return model_core.NewSimplePatchedMessage[dag.ObjectContentsWalker](&model_starlark_pb.Value{
			Kind: &model_starlark_pb.Value_Int{
				Int: typedValue.BigInt().Bytes(),
			},
		}), false, nil
	case *starlark.List:
		if _, ok := path[value]; ok {
			return model_core.PatchedMessage[*model_starlark_pb.Value, dag.ObjectContentsWalker]{}, false, errors.New("value is defined recursively")
		}
		path[value] = struct{}{}
		defer delete(path, value)

		treeBuilder := newSplitBTreeBuilder(
			options,
			/* parentNodeComputer = */ func(contents *object.Contents, childNodes []*model_starlark_pb.List_Element, outgoingReferences object.OutgoingReferences, metadata []dag.ObjectContentsWalker) (model_core.PatchedMessage[*model_starlark_pb.List_Element, dag.ObjectContentsWalker], error) {
				patcher := model_core.NewReferenceMessagePatcher[dag.ObjectContentsWalker]()
				return model_core.PatchedMessage[*model_starlark_pb.List_Element, dag.ObjectContentsWalker]{
					Message: &model_starlark_pb.List_Element{
						Level: &model_starlark_pb.List_Element_Parent_{
							Parent: &model_starlark_pb.List_Element_Parent{
								Reference: patcher.AddReference(contents.GetReference(), dag.NewSimpleObjectContentsWalker(contents, metadata)),
							},
						},
					},
					Patcher: patcher,
				}, nil
			},
		)

		needsCode := false
		for value := range starlark.Elements(typedValue) {
			encodedValue, valueNeedsCode, err := EncodeValue(value, path, nil, options)
			if err != nil {
				return model_core.PatchedMessage[*model_starlark_pb.Value, dag.ObjectContentsWalker]{}, false, err
			}
			needsCode = needsCode || valueNeedsCode
			if err := treeBuilder.PushChild(model_core.PatchedMessage[*model_starlark_pb.List_Element, dag.ObjectContentsWalker]{
				Message: &model_starlark_pb.List_Element{
					Level: &model_starlark_pb.List_Element_Leaf{
						Leaf: encodedValue.Message,
					},
				},
				Patcher: encodedValue.Patcher,
			}); err != nil {
				return model_core.PatchedMessage[*model_starlark_pb.Value, dag.ObjectContentsWalker]{}, false, err
			}
		}

		elements, err := treeBuilder.FinalizeList()
		if err != nil {
			return model_core.PatchedMessage[*model_starlark_pb.Value, dag.ObjectContentsWalker]{}, false, err
		}

		// TODO: This should use inlinedtree to ensure the
		// resulting Value object is not too large.
		return model_core.PatchedMessage[*model_starlark_pb.Value, dag.ObjectContentsWalker]{
			Message: &model_starlark_pb.Value{
				Kind: &model_starlark_pb.Value_List{
					List: &model_starlark_pb.List{
						Elements: elements.Message,
					},
				},
			},
			Patcher: elements.Patcher,
		}, needsCode, nil
	case starlark.String:
		return model_core.NewSimplePatchedMessage[dag.ObjectContentsWalker](&model_starlark_pb.Value{
			Kind: &model_starlark_pb.Value_Str{
				Str: string(typedValue),
			},
		}), false, nil
	case *starlarkstruct.Struct:
		encodedStruct, needsCode, err := encodeStruct(typedValue, path, "", options)
		if err != nil {
			return model_core.PatchedMessage[*model_starlark_pb.Value, dag.ObjectContentsWalker]{}, false, err
		}
		return model_core.PatchedMessage[*model_starlark_pb.Value, dag.ObjectContentsWalker]{
			Message: &model_starlark_pb.Value{
				Kind: &model_starlark_pb.Value_Struct{
					Struct: encodedStruct.Message,
				},
			},
			Patcher: encodedStruct.Patcher,
		}, needsCode, nil
	case starlark.Tuple:
		encodedValues := make([]*model_starlark_pb.Value, 0, len(typedValue))
		patcher := model_core.NewReferenceMessagePatcher[dag.ObjectContentsWalker]()
		needsCode := false
		for _, value := range typedValue {
			encodedValue, valueNeedsCode, err := EncodeValue(value, path, nil, options)
			if err != nil {
				return model_core.PatchedMessage[*model_starlark_pb.Value, dag.ObjectContentsWalker]{}, false, err
			}
			encodedValues = append(encodedValues, encodedValue.Message)
			patcher.Merge(encodedValue.Patcher)
			needsCode = needsCode || valueNeedsCode
		}
		return model_core.PatchedMessage[*model_starlark_pb.Value, dag.ObjectContentsWalker]{
			Message: &model_starlark_pb.Value{
				Kind: &model_starlark_pb.Value_Tuple{
					Tuple: &model_starlark_pb.Tuple{
						Elements: encodedValues,
					},
				},
			},
			Patcher: patcher,
		}, false, nil
	case EncodableValue:
		return typedValue.EncodeValue(path, currentIdentifier, options)
	case nil:
		return model_core.PatchedMessage[*model_starlark_pb.Value, dag.ObjectContentsWalker]{}, false, errors.New("no value provided")
	default:
		return model_core.PatchedMessage[*model_starlark_pb.Value, dag.ObjectContentsWalker]{}, false, fmt.Errorf("value of type %s cannot be encoded", value.Type())
	}
}

func encodeStruct(strukt *starlarkstruct.Struct, path map[starlark.Value]struct{}, providerIdentifier string, options *ValueEncodingOptions) (model_core.PatchedMessage[*model_starlark_pb.Struct, dag.ObjectContentsWalker], bool, error) {
	keys := strukt.AttrNames()
	fields := make([]*model_starlark_pb.NamedValue, 0, len(keys))
	patcher := model_core.NewReferenceMessagePatcher[dag.ObjectContentsWalker]()
	needsCode := false
	for _, name := range keys {
		value, _ := strukt.Attr(name)
		encodedValue, valueNeedsCode, err := EncodeValue(value, path, nil, options)
		if err != nil {
			return model_core.PatchedMessage[*model_starlark_pb.Struct, dag.ObjectContentsWalker]{}, false, fmt.Errorf("field %#v: %w", name, err)
		}
		fields = append(fields, &model_starlark_pb.NamedValue{
			Name:  name,
			Value: encodedValue.Message,
		})
		patcher.Merge(encodedValue.Patcher)
		needsCode = needsCode || valueNeedsCode
	}

	return model_core.PatchedMessage[*model_starlark_pb.Struct, dag.ObjectContentsWalker]{
		Message: &model_starlark_pb.Struct{
			Fields:             fields,
			ProviderIdentifier: providerIdentifier,
		},
		Patcher: patcher,
	}, needsCode, nil
}

func DecodeGlobals(encodedGlobals model_core.Message[[]*model_starlark_pb.NamedValue], currentFilename pg_label.CanonicalLabel, options *ValueDecodingOptions) (starlark.StringDict, error) {
	globals := make(map[string]starlark.Value, len(encodedGlobals.Message))
	for _, encodedGlobal := range encodedGlobals.Message {
		identifier, err := pg_label.NewStarlarkIdentifier(encodedGlobal.Name)
		if err != nil {
			return nil, err
		}
		currentIdentifier := currentFilename.AppendStarlarkIdentifier(identifier)
		value, err := DecodeValue(model_core.Message[*model_starlark_pb.Value]{
			Message:            encodedGlobal.Value,
			OutgoingReferences: encodedGlobals.OutgoingReferences,
		}, &currentIdentifier, options)
		if err != nil {
			return nil, err
		}
		value.Freeze()
		globals[encodedGlobal.Name] = value
	}
	return globals, nil
}

type ValueDecodingOptions struct {
	Context          context.Context
	ObjectDownloader object.Downloader[object.LocalReference]
	ObjectEncoder    model_encoding.BinaryEncoder
	LabelCreator     func(pg_label.CanonicalLabel) (starlark.Value, error)
}

func DecodeValue(encodedValue model_core.Message[*model_starlark_pb.Value], currentIdentifier *pg_label.CanonicalStarlarkIdentifier, options *ValueDecodingOptions) (starlark.Value, error) {
	switch typedValue := encodedValue.Message.GetKind().(type) {
	case *model_starlark_pb.Value_Aspect:
		switch aspectKind := typedValue.Aspect.Kind.(type) {
		case *model_starlark_pb.Aspect_Reference:
			identifier, err := pg_label.NewCanonicalStarlarkIdentifier(aspectKind.Reference)
			if err != nil {
				return nil, err
			}
			return NewAspect(&identifier, nil), nil
		case *model_starlark_pb.Aspect_Definition_:
			if currentIdentifier == nil {
				return nil, errors.New("encoded aspect does not have a name")
			}
			return NewAspect(currentIdentifier, aspectKind.Definition), nil
		default:
			return nil, errors.New("encoded aspect does not have a reference or definition")
		}
	case *model_starlark_pb.Value_Attr:
		attrType, err := DecodeAttrType(typedValue.Attr)
		if err != nil {
			return nil, err
		}

		var defaultValue starlark.Value
		if d := typedValue.Attr.Default; d != nil {
			// TODO: Should we also canonicalize?
			var err error
			defaultValue, err = DecodeValue(
				model_core.Message[*model_starlark_pb.Value]{
					Message:            d,
					OutgoingReferences: encodedValue.OutgoingReferences,
				},
				nil,
				options,
			)
			if err != nil {
				return nil, err
			}
		}
		return NewAttr(attrType, defaultValue), nil
	case *model_starlark_pb.Value_Bool:
		return starlark.Bool(typedValue.Bool), nil
	case *model_starlark_pb.Value_Builtin:
		parts := strings.Split(typedValue.Builtin, ".")
		value, ok := BzlFileBuiltins[parts[0]]
		if !ok {
			return nil, fmt.Errorf("builtin %#v does not exist", parts[0])
		}
		for i := 1; i < len(parts); i++ {
			hasAttrs, ok := value.(starlark.HasAttrs)
			if !ok {
				return nil, fmt.Errorf("builtin %#v does have attributes", strings.Join(parts[:i], "."))
			}
			var err error
			value, err = hasAttrs.Attr(parts[i])
			if err != nil {
				return nil, fmt.Errorf("builtin %#v does not exist", strings.Join(parts[:i+1], "."))
			}
		}
		return value, nil
	case *model_starlark_pb.Value_Bytes:
		return starlark.Bytes(typedValue.Bytes), nil
	case *model_starlark_pb.Value_Dict:
		dict := starlark.NewDict(len(typedValue.Dict.Entries))
		if err := decodeDictEntries(
			model_core.Message[*model_starlark_pb.Dict]{
				Message:            typedValue.Dict,
				OutgoingReferences: encodedValue.OutgoingReferences,
			},
			&dictEntriesDecodingOptions{
				valueDecodingOptions: options,
				reader: model_parser.NewStorageBackedParsedObjectReader(
					options.ObjectDownloader,
					options.ObjectEncoder,
					model_parser.NewMessageObjectParser[object.LocalReference, model_starlark_pb.Dict](),
				),
				out: dict,
			},
		); err != nil {
			return nil, err
		}
		return dict, nil
	case *model_starlark_pb.Value_Function:
		return NewNamedFunction(NewProtoNamedFunctionDefinition(
			model_core.Message[*model_starlark_pb.Function]{
				Message:            typedValue.Function,
				OutgoingReferences: encodedValue.OutgoingReferences,
			},
		)), nil
	case *model_starlark_pb.Value_Int:
		var i big.Int
		return starlark.MakeBigInt(i.SetBytes(typedValue.Int)), nil
	case *model_starlark_pb.Value_Label:
		canonicalLabel, err := pg_label.NewCanonicalLabel(typedValue.Label)
		if err != nil {
			return nil, err
		}
		return options.LabelCreator(canonicalLabel)
	case *model_starlark_pb.Value_Provider:
		providerIdentifier, err := pg_label.NewCanonicalStarlarkIdentifier(typedValue.Provider.ProviderIdentifier)
		if err != nil {
			return nil, err
		}
		var initFunction *NamedFunction
		if typedValue.Provider.InitFunction != nil {
			f := NewNamedFunction(NewProtoNamedFunctionDefinition(
				model_core.Message[*model_starlark_pb.Function]{
					Message:            typedValue.Provider.InitFunction,
					OutgoingReferences: encodedValue.OutgoingReferences,
				},
			))
			initFunction = &f
		}
		return NewProvider(&providerIdentifier, initFunction), nil
	case *model_starlark_pb.Value_List:
		list := starlark.NewList(nil)
		if err := decodeList_Elements(
			model_core.Message[*model_starlark_pb.List]{
				Message:            typedValue.List,
				OutgoingReferences: encodedValue.OutgoingReferences,
			},
			&listElementsDecodingOptions{
				valueDecodingOptions: options,
				reader: model_parser.NewStorageBackedParsedObjectReader(
					options.ObjectDownloader,
					options.ObjectEncoder,
					model_parser.NewMessageObjectParser[object.LocalReference, model_starlark_pb.List](),
				),
				out: list,
			}); err != nil {
			return nil, err
		}
		return list, nil
	case *model_starlark_pb.Value_ModuleExtension:
		return NewModuleExtension(NewProtoModuleExtensionDefinition(
			model_core.Message[*model_starlark_pb.ModuleExtension]{
				Message:            typedValue.ModuleExtension,
				OutgoingReferences: encodedValue.OutgoingReferences,
			},
		)), nil
	case *model_starlark_pb.Value_None:
		return starlark.None, nil
	case *model_starlark_pb.Value_RepositoryRule:
		switch repositoryRuleKind := typedValue.RepositoryRule.Kind.(type) {
		case *model_starlark_pb.RepositoryRule_Reference:
			identifier, err := pg_label.NewCanonicalStarlarkIdentifier(repositoryRuleKind.Reference)
			if err != nil {
				return nil, err
			}
			return NewRepositoryRule(&identifier, nil), nil
		case *model_starlark_pb.RepositoryRule_Definition_:
			if currentIdentifier == nil {
				return nil, errors.New("encoded repository_rule does not have a name")
			}
			return NewRepositoryRule(currentIdentifier, NewProtoRepositoryRuleDefinition(
				model_core.Message[*model_starlark_pb.RepositoryRule_Definition]{
					Message:            repositoryRuleKind.Definition,
					OutgoingReferences: encodedValue.OutgoingReferences,
				},
			)), nil
		default:
			return nil, errors.New("encoded repository_rule does not have a reference or definition")
		}
	case *model_starlark_pb.Value_Rule:
		switch ruleKind := typedValue.Rule.Kind.(type) {
		case *model_starlark_pb.Rule_Reference:
			identifier, err := pg_label.NewCanonicalStarlarkIdentifier(ruleKind.Reference)
			if err != nil {
				return nil, err
			}
			return NewRule(&identifier, NewReloadingRuleDefinition(identifier)), nil
		case *model_starlark_pb.Rule_Definition_:
			if currentIdentifier == nil {
				return nil, errors.New("encoded rule does not have a name")
			}
			return NewRule(currentIdentifier, NewProtoRuleDefinition(
				model_core.Message[*model_starlark_pb.Rule_Definition]{
					Message:            ruleKind.Definition,
					OutgoingReferences: encodedValue.OutgoingReferences,
				},
			)), nil
		default:
			return nil, errors.New("encoded rule does not have a reference or definition")
		}
	case *model_starlark_pb.Value_Str:
		return starlark.String(typedValue.Str), nil
	case *model_starlark_pb.Value_Struct:
		strukt, err := DecodeStruct(model_core.Message[*model_starlark_pb.Struct]{
			Message:            typedValue.Struct,
			OutgoingReferences: encodedValue.OutgoingReferences,
		}, options)
		if err != nil {
			return nil, err
		}
		if providerIdentifier := typedValue.Struct.ProviderIdentifier; providerIdentifier != "" {
			i, err := pg_label.NewCanonicalStarlarkIdentifier(providerIdentifier)
			if err != nil {
				return nil, err
			}
			return NewProviderInstance(strukt, i), nil
		}
		return strukt, nil
	case *model_starlark_pb.Value_Subrule:
		switch subruleKind := typedValue.Subrule.Kind.(type) {
		case *model_starlark_pb.Subrule_Reference:
			identifier, err := pg_label.NewCanonicalStarlarkIdentifier(subruleKind.Reference)
			if err != nil {
				return nil, err
			}
			return NewSubrule(&identifier, nil), nil
		case *model_starlark_pb.Subrule_Definition_:
			if currentIdentifier == nil {
				return nil, errors.New("encoded subrule does not have a name")
			}
			return NewSubrule(currentIdentifier, subruleKind.Definition), nil
		default:
			return nil, errors.New("encoded subrule does not have a reference or definition")
		}
	case *model_starlark_pb.Value_TagClass:
		return NewTagClass(NewProtoTagClassDefinition(
			model_core.Message[*model_starlark_pb.TagClass]{
				Message:            typedValue.TagClass,
				OutgoingReferences: encodedValue.OutgoingReferences,
			},
		)), nil
	case *model_starlark_pb.Value_ToolchainType:
		toolchainType, err := pg_label.NewCanonicalLabel(typedValue.ToolchainType.ToolchainType)
		if err != nil {
			return nil, err
		}
		return NewToolchainType(toolchainType, typedValue.ToolchainType.Mandatory), nil
	case *model_starlark_pb.Value_Transition:
		switch transitionKind := typedValue.Transition.Kind.(type) {
		case *model_starlark_pb.Transition_Reference:
			identifier, err := pg_label.NewCanonicalStarlarkIdentifier(transitionKind.Reference)
			if err != nil {
				return nil, err
			}
			return NewTransition(&identifier, nil), nil
		case *model_starlark_pb.Transition_Definition_:
			if currentIdentifier == nil {
				return nil, errors.New("encoded transition does not have a name")
			}
			return NewTransition(currentIdentifier, transitionKind.Definition), nil
		default:
			return nil, errors.New("encoded transition does not have a reference or definition")
		}
	case *model_starlark_pb.Value_Tuple:
		encodedElements := typedValue.Tuple.Elements
		tuple := make(starlark.Tuple, 0, len(encodedElements))
		for _, encodedElement := range encodedElements {
			element, err := DecodeValue(model_core.Message[*model_starlark_pb.Value]{
				Message:            encodedElement,
				OutgoingReferences: encodedValue.OutgoingReferences,
			}, nil, options)
			if err != nil {
				return nil, err
			}
			tuple = append(tuple, element)
		}
		return tuple, nil
	default:
		panic("UNKNOWN VALUE: " + protojson.Format(encodedValue.Message))
	}
}

func DecodeAttrType(attr *model_starlark_pb.Attr) (AttrType, error) {
	switch attrTypeInfo := attr.Type.(type) {
	case *model_starlark_pb.Attr_Bool:
		return BoolAttrType, nil
	case *model_starlark_pb.Attr_Int:
		return NewIntAttrType(attrTypeInfo.Int.Values), nil
	case *model_starlark_pb.Attr_IntList:
		return NewIntListAttrType(), nil
	case *model_starlark_pb.Attr_Label:
		return NewLabelAttrType(attrTypeInfo.Label.AllowNone), nil
	case *model_starlark_pb.Attr_LabelKeyedStringDict:
		return NewLabelKeyedStringDictAttrType(), nil
	case *model_starlark_pb.Attr_LabelList:
		return NewLabelListAttrType(), nil
	case *model_starlark_pb.Attr_Output:
		return OutputAttrType, nil
	case *model_starlark_pb.Attr_OutputList:
		return NewOutputListAttrType(), nil
	case *model_starlark_pb.Attr_String_:
		return NewStringAttrType(attrTypeInfo.String_.Values), nil
	case *model_starlark_pb.Attr_StringDict:
		return NewStringDictAttrType(), nil
	case *model_starlark_pb.Attr_StringList:
		return NewStringListAttrType(), nil
	case *model_starlark_pb.Attr_StringListDict:
		return NewStringListDictAttrType(), nil
	default:
		return nil, errors.New("unknown attribute type")
	}
}

func DecodeStruct(m model_core.Message[*model_starlark_pb.Struct], options *ValueDecodingOptions) (*starlarkstruct.Struct, error) {
	encodedFields := m.Message.Fields
	fields := make(map[string]starlark.Value, len(encodedFields))
	for _, encodedField := range encodedFields {
		fieldValue, err := DecodeValue(model_core.Message[*model_starlark_pb.Value]{
			Message:            encodedField.Value,
			OutgoingReferences: m.OutgoingReferences,
		}, nil, options)
		if err != nil {
			return nil, err
		}
		fields[encodedField.Name] = fieldValue
	}
	return starlarkstruct.FromStringDict(starlarkstruct.Default, fields), nil
}

func DecodeProviderInstance(m model_core.Message[*model_starlark_pb.Struct], options *ValueDecodingOptions) (ProviderInstance, error) {
	strukt, err := DecodeStruct(m, options)
	if err != nil {
		return ProviderInstance{}, err
	}
	providerIdentifier, err := pg_label.NewCanonicalStarlarkIdentifier(m.Message.ProviderIdentifier)
	if err != nil {
		return ProviderInstance{}, err
	}
	return NewProviderInstance(strukt, providerIdentifier), nil
}

type dictEntriesDecodingOptions struct {
	valueDecodingOptions *ValueDecodingOptions
	reader               model_parser.ParsedObjectReader[object.LocalReference, model_core.Message[*model_starlark_pb.Dict]]
	out                  *starlark.Dict
}

func decodeDictEntries(in model_core.Message[*model_starlark_pb.Dict], options *dictEntriesDecodingOptions) error {
	for _, entry := range in.Message.Entries {
		switch typedEntry := entry.Level.(type) {
		case *model_starlark_pb.Dict_Entry_Leaf_:
			key, err := DecodeValue(
				model_core.Message[*model_starlark_pb.Value]{
					Message:            typedEntry.Leaf.Key,
					OutgoingReferences: in.OutgoingReferences,
				},
				nil,
				options.valueDecodingOptions,
			)
			if err != nil {
				return err
			}
			value, err := DecodeValue(
				model_core.Message[*model_starlark_pb.Value]{
					Message:            typedEntry.Leaf.Value,
					OutgoingReferences: in.OutgoingReferences,
				},
				nil,
				options.valueDecodingOptions,
			)
			if err != nil {
				return err
			}
			if err := options.out.SetKey(key, value); err != nil {
				return err
			}
		case *model_starlark_pb.Dict_Entry_Parent_:
			index, err := model_core.GetIndexFromReferenceMessage(typedEntry.Parent.Reference, in.OutgoingReferences.GetDegree())
			if err != nil {
				return err
			}
			child, _, err := options.reader.ReadParsedObject(
				options.valueDecodingOptions.Context,
				in.OutgoingReferences.GetOutgoingReference(index),
			)
			if err != nil {
				return err
			}
			if err := decodeDictEntries(child, options); err != nil {
				return err
			}
		default:
			return errors.New("dict entry is of an unknown type")
		}
	}
	return nil
}

type listElementsDecodingOptions struct {
	valueDecodingOptions *ValueDecodingOptions
	reader               model_parser.ParsedObjectReader[object.LocalReference, model_core.Message[*model_starlark_pb.List]]
	out                  *starlark.List
}

func decodeList_Elements(in model_core.Message[*model_starlark_pb.List], options *listElementsDecodingOptions) error {
	for _, element := range in.Message.Elements {
		switch typedEntry := element.Level.(type) {
		case *model_starlark_pb.List_Element_Leaf:
			value, err := DecodeValue(
				model_core.Message[*model_starlark_pb.Value]{
					Message:            typedEntry.Leaf,
					OutgoingReferences: in.OutgoingReferences,
				},
				nil,
				options.valueDecodingOptions,
			)
			if err != nil {
				return err
			}
			if err := options.out.Append(value); err != nil {
				return err
			}
		case *model_starlark_pb.List_Element_Parent_:
			panic("TODO: Recurse into lists")
		default:
			return errors.New("list element is of an unknown type")
		}
	}
	return nil
}
