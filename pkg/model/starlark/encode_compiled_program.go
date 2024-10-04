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

	"go.starlark.net/starlark"
)

type ValueEncodingOptions struct {
	CurrentFilename string

	// Options to use when storing Starlark values in separate objects.
	ObjectEncoder          model_encoding.BinaryEncoder
	ObjectReferenceFormat  object.ReferenceFormat
	ObjectMinimumSizeBytes int
	ObjectMaximumSizeBytes int
}

type EncodableValue interface {
	EncodeValue(path map[starlark.Value]struct{}, options *ValueEncodingOptions) (model_core.PatchedMessage[*model_starlark_pb.Value, dag.ObjectContentsWalker], bool, error)
}

func EncodeCompiledProgram(program *starlark.Program, globals starlark.StringDict, options *ValueEncodingOptions) (model_core.PatchedMessage[*model_starlark_pb.CompiledProgram, dag.ObjectContentsWalker], error) {
	referencedGlobals := make(map[string]model_core.PatchedMessage[*model_starlark_pb.Value, dag.ObjectContentsWalker], len(globals))
	patcher := model_core.NewReferenceMessagePatcher[dag.ObjectContentsWalker]()
	needsCode := false
	for name, value := range globals {
		if !strings.HasPrefix(name, "_") {
			encodedValue, valueNeedsCode, err := EncodeValue(value, map[starlark.Value]struct{}{}, options)
			if err != nil {
				return model_core.PatchedMessage[*model_starlark_pb.CompiledProgram, dag.ObjectContentsWalker]{}, fmt.Errorf("global %#v: %w", name, err)
			}
			referencedGlobals[name] = encodedValue
			needsCode = needsCode || valueNeedsCode
		}
	}

	encodedGlobals := make([]*model_starlark_pb.Global, 0, len(referencedGlobals))
	for _, name := range slices.Sorted(maps.Keys(referencedGlobals)) {
		encodedValue := referencedGlobals[name]
		encodedGlobals = append(encodedGlobals, &model_starlark_pb.Global{
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

func EncodeValue(value starlark.Value, path map[starlark.Value]struct{}, options *ValueEncodingOptions) (model_core.PatchedMessage[*model_starlark_pb.Value, dag.ObjectContentsWalker], bool, error) {
	if value == starlark.None {
		return model_core.PatchedMessage[*model_starlark_pb.Value, dag.ObjectContentsWalker]{
			Patcher: model_core.NewReferenceMessagePatcher[dag.ObjectContentsWalker](),
		}, false, nil
	}
	switch typedValue := value.(type) {
	case starlark.Bool:
		return model_core.PatchedMessage[*model_starlark_pb.Value, dag.ObjectContentsWalker]{
			Message: &model_starlark_pb.Value{
				Kind: &model_starlark_pb.Value_Bool{
					Bool: bool(typedValue),
				},
			},
			Patcher: model_core.NewReferenceMessagePatcher[dag.ObjectContentsWalker](),
		}, false, nil
	case starlark.Bytes:
		return model_core.PatchedMessage[*model_starlark_pb.Value, dag.ObjectContentsWalker]{
			Message: &model_starlark_pb.Value{
				Kind: &model_starlark_pb.Value_Bytes{
					Bytes: []byte(typedValue),
				},
			},
			Patcher: model_core.NewReferenceMessagePatcher[dag.ObjectContentsWalker](),
		}, false, nil
	case *starlark.Dict:
		if _, ok := path[value]; ok {
			return model_core.PatchedMessage[*model_starlark_pb.Value, dag.ObjectContentsWalker]{}, false, errors.New("value is defined recursively")
		}
		path[value] = struct{}{}
		defer delete(path, value)

		// TODO: minimumCount should be 1 for the lowest level.
		treeBuilder := btree.NewBuilder(
			btree.NewProllyChunkerFactory[*model_starlark_pb.DictEntry, dag.ObjectContentsWalker](
				/* minimumCount = */ 2,
				options.ObjectMinimumSizeBytes,
				options.ObjectMaximumSizeBytes,
			),
			btree.NewObjectCreatingNodeMerger(
				options.ObjectEncoder,
				options.ObjectReferenceFormat,
				/* parentNodeComputer = */ func(contents *object.Contents, childNodes []*model_starlark_pb.DictEntry, outgoingReferences object.OutgoingReferences, metadata []dag.ObjectContentsWalker) (model_core.PatchedMessage[*model_starlark_pb.DictEntry, dag.ObjectContentsWalker], error) {
					patcher := model_core.NewReferenceMessagePatcher[dag.ObjectContentsWalker]()
					return model_core.PatchedMessage[*model_starlark_pb.DictEntry, dag.ObjectContentsWalker]{
						Message: &model_starlark_pb.DictEntry{
							Level: &model_starlark_pb.DictEntry_Parent_{
								Parent: &model_starlark_pb.DictEntry_Parent{
									Reference: patcher.AddReference(contents.GetReference(), dag.NewSimpleObjectContentsWalker(contents, metadata)),
								},
							},
						},
						Patcher: patcher,
					}, nil
				},
			),
		)

		needsCode := false
		for key, value := range starlark.Entries(typedValue) {
			encodedKey, keyNeedsCode, err := EncodeValue(key, path, options)
			if err != nil {
				return model_core.PatchedMessage[*model_starlark_pb.Value, dag.ObjectContentsWalker]{}, false, err
			}
			encodedValue, valueNeedsCode, err := EncodeValue(value, path, options)
			if err != nil {
				return model_core.PatchedMessage[*model_starlark_pb.Value, dag.ObjectContentsWalker]{}, false, err
			}
			encodedKey.Patcher.Merge(encodedValue.Patcher)
			needsCode = needsCode || keyNeedsCode || valueNeedsCode
			if err := treeBuilder.PushChild(model_core.PatchedMessage[*model_starlark_pb.DictEntry, dag.ObjectContentsWalker]{
				Message: &model_starlark_pb.DictEntry{
					Level: &model_starlark_pb.DictEntry_Leaf_{
						Leaf: &model_starlark_pb.DictEntry_Leaf{
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
		}, false, nil
	case *starlark.Function:
		return model_core.PatchedMessage[*model_starlark_pb.Value, dag.ObjectContentsWalker]{
			Message: &model_starlark_pb.Value{
				Kind: &model_starlark_pb.Value_Function{
					Function: &model_starlark_pb.Function{
						Filename: options.CurrentFilename,
						Name:     typedValue.Name(),
					},
				},
			},
			Patcher: model_core.NewReferenceMessagePatcher[dag.ObjectContentsWalker](),
		}, typedValue.Position().Filename() == options.CurrentFilename, nil
	case starlark.Int:
		return model_core.PatchedMessage[*model_starlark_pb.Value, dag.ObjectContentsWalker]{
			Message: &model_starlark_pb.Value{
				Kind: &model_starlark_pb.Value_Int{
					Int: typedValue.BigInt().Bytes(),
				},
			},
			Patcher: model_core.NewReferenceMessagePatcher[dag.ObjectContentsWalker](),
		}, false, nil
	case *starlark.List:
		if _, ok := path[value]; ok {
			return model_core.PatchedMessage[*model_starlark_pb.Value, dag.ObjectContentsWalker]{}, false, errors.New("value is defined recursively")
		}
		path[value] = struct{}{}
		defer delete(path, value)

		// TODO: minimumCount should be 1 for the lowest level.
		treeBuilder := btree.NewBuilder(
			btree.NewProllyChunkerFactory[*model_starlark_pb.ListElement, dag.ObjectContentsWalker](
				/* minimumCount = */ 2,
				options.ObjectMinimumSizeBytes,
				options.ObjectMaximumSizeBytes,
			),
			btree.NewObjectCreatingNodeMerger(
				options.ObjectEncoder,
				options.ObjectReferenceFormat,
				/* parentNodeComputer = */ func(contents *object.Contents, childNodes []*model_starlark_pb.ListElement, outgoingReferences object.OutgoingReferences, metadata []dag.ObjectContentsWalker) (model_core.PatchedMessage[*model_starlark_pb.ListElement, dag.ObjectContentsWalker], error) {
					patcher := model_core.NewReferenceMessagePatcher[dag.ObjectContentsWalker]()
					return model_core.PatchedMessage[*model_starlark_pb.ListElement, dag.ObjectContentsWalker]{
						Message: &model_starlark_pb.ListElement{
							Level: &model_starlark_pb.ListElement_Parent_{
								Parent: &model_starlark_pb.ListElement_Parent{
									Reference: patcher.AddReference(contents.GetReference(), dag.NewSimpleObjectContentsWalker(contents, metadata)),
								},
							},
						},
						Patcher: patcher,
					}, nil
				},
			),
		)

		needsCode := false
		for value := range starlark.Elements(typedValue) {
			encodedValue, valueNeedsCode, err := EncodeValue(value, path, options)
			if err != nil {
				return model_core.PatchedMessage[*model_starlark_pb.Value, dag.ObjectContentsWalker]{}, false, err
			}
			needsCode = needsCode || valueNeedsCode
			if err := treeBuilder.PushChild(model_core.PatchedMessage[*model_starlark_pb.ListElement, dag.ObjectContentsWalker]{
				Message: &model_starlark_pb.ListElement{
					Level: &model_starlark_pb.ListElement_Leaf{
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
		}, false, nil
	case starlark.String:
		return model_core.PatchedMessage[*model_starlark_pb.Value, dag.ObjectContentsWalker]{
			Message: &model_starlark_pb.Value{
				Kind: &model_starlark_pb.Value_Str{
					Str: string(typedValue),
				},
			},
			Patcher: model_core.NewReferenceMessagePatcher[dag.ObjectContentsWalker](),
		}, false, nil
	case starlark.Tuple:
		encodedValues := make([]*model_starlark_pb.Value, 0, len(typedValue))
		patcher := model_core.NewReferenceMessagePatcher[dag.ObjectContentsWalker]()
		needsCode := false
		for _, value := range typedValue {
			encodedValue, valueNeedsCode, err := EncodeValue(value, path, options)
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
		return typedValue.EncodeValue(path, options)
	default:
		return model_core.PatchedMessage[*model_starlark_pb.Value, dag.ObjectContentsWalker]{}, false, fmt.Errorf("value of type %s cannot be encoded", value.Type())
	}
}

func DecodeGlobals(encodedGlobals model_core.Message[[]*model_starlark_pb.Global], options *ValueDecodingOptions) (starlark.StringDict, error) {
	globals := make(map[string]starlark.Value, len(encodedGlobals.Message))
	for _, encodedGlobal := range encodedGlobals.Message {
		value, err := DecodeValue(model_core.Message[*model_starlark_pb.Value]{
			Message:            encodedGlobal.Value,
			OutgoingReferences: encodedGlobals.OutgoingReferences,
		}, options)
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
}

func DecodeValue(encodedValue model_core.Message[*model_starlark_pb.Value], options *ValueDecodingOptions) (starlark.Value, error) {
	switch typedValue := encodedValue.Message.GetKind().(type) {
	case nil:
		return starlark.None, nil
	case *model_starlark_pb.Value_Attr:
		return NewAttr(nil, typedValue.Attr.Mandatory), nil
	case *model_starlark_pb.Value_Bool:
		return starlark.Bool(typedValue.Bool), nil
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
			}); err != nil {
			return nil, err
		}
		return dict, nil
	case *model_starlark_pb.Value_Function:
		filenameLabel, err := pg_label.NewCanonicalLabel(typedValue.Function.Filename)
		if err != nil {
			return nil, err
		}
		return NewIndirectFunction(filenameLabel, typedValue.Function.Name), nil
	case *model_starlark_pb.Value_Int:
		var i big.Int
		return starlark.MakeBigInt(i.SetBytes(typedValue.Int)), nil
	case *model_starlark_pb.Value_Label:
		canonicalLabel, err := pg_label.NewCanonicalLabel(typedValue.Label)
		if err != nil {
			return nil, err
		}
		return NewLabel(canonicalLabel), nil
	case *model_starlark_pb.Value_Provider:
		filenameLabel, err := pg_label.NewCanonicalLabel(typedValue.Provider.Filename)
		if err != nil {
			return nil, err
		}
		return NewProviderValue(filenameLabel, typedValue.Provider.Name, nil), nil
	case *model_starlark_pb.Value_List:
		list := starlark.NewList(nil)
		if err := decodeListElements(
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
	case *model_starlark_pb.Value_RepositoryRule:
		filenameLabel, err := pg_label.NewCanonicalLabel(typedValue.RepositoryRule.Filename)
		if err != nil {
			return nil, err
		}
		return NewRepositoryRuleValue(filenameLabel, typedValue.RepositoryRule.Name), nil
	case *model_starlark_pb.Value_Rule:
		filenameLabel, err := pg_label.NewCanonicalLabel(typedValue.Rule.Filename)
		if err != nil {
			return nil, err
		}
		return NewRuleValue(filenameLabel, typedValue.Rule.Name), nil
	case *model_starlark_pb.Value_Str:
		return starlark.String(typedValue.Str), nil
	case *model_starlark_pb.Value_Struct:
		encodedFields := typedValue.Struct.Fields
		fields := make(map[string]starlark.Value, len(encodedFields))
		for _, encodedField := range encodedFields {
			fieldValue, err := DecodeValue(model_core.Message[*model_starlark_pb.Value]{
				Message:            encodedField.Value,
				OutgoingReferences: encodedValue.OutgoingReferences,
			}, options)
			if err != nil {
				return nil, err
			}
			fields[encodedField.Key] = fieldValue
		}
		return NewStruct(fields), nil
	case *model_starlark_pb.Value_Subrule:
		filenameLabel, err := pg_label.NewCanonicalLabel(typedValue.Subrule.Filename)
		if err != nil {
			return nil, err
		}
		return NewSubruleValue(filenameLabel, typedValue.Subrule.Name), nil
	case *model_starlark_pb.Value_Tuple:
		encodedElements := typedValue.Tuple.Elements
		tuple := make(starlark.Tuple, 0, len(encodedElements))
		for _, encodedElement := range encodedElements {
			element, err := DecodeValue(model_core.Message[*model_starlark_pb.Value]{
				Message:            encodedElement,
				OutgoingReferences: encodedValue.OutgoingReferences,
			}, options)
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

type dictEntriesDecodingOptions struct {
	valueDecodingOptions *ValueDecodingOptions
	reader               model_parser.ParsedObjectReader[object.LocalReference, model_core.Message[*model_starlark_pb.Dict]]
	out                  *starlark.Dict
}

func decodeDictEntries(in model_core.Message[*model_starlark_pb.Dict], options *dictEntriesDecodingOptions) error {
	for _, entry := range in.Message.Entries {
		switch typedEntry := entry.Level.(type) {
		case *model_starlark_pb.DictEntry_Leaf_:
			key, err := DecodeValue(
				model_core.Message[*model_starlark_pb.Value]{
					Message:            typedEntry.Leaf.Key,
					OutgoingReferences: in.OutgoingReferences,
				},
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
				options.valueDecodingOptions,
			)
			if err != nil {
				return err
			}
			if err := options.out.SetKey(key, value); err != nil {
				return err
			}
		case *model_starlark_pb.DictEntry_Parent_:
			panic("TODO: Recurse into dicts")
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

func decodeListElements(in model_core.Message[*model_starlark_pb.List], options *listElementsDecodingOptions) error {
	for _, element := range in.Message.Elements {
		switch typedEntry := element.Level.(type) {
		case *model_starlark_pb.ListElement_Leaf:
			value, err := DecodeValue(
				model_core.Message[*model_starlark_pb.Value]{
					Message:            typedEntry.Leaf,
					OutgoingReferences: in.OutgoingReferences,
				},
				options.valueDecodingOptions,
			)
			if err != nil {
				return err
			}
			if err := options.out.Append(value); err != nil {
				return err
			}
		case *model_starlark_pb.ListElement_Parent_:
			panic("TODO: Recurse into lists")
		default:
			return errors.New("list element is of an unknown type")
		}
	}
	return nil
}
