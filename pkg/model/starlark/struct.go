package starlark

import (
	"context"
	"errors"
	"fmt"
	"iter"
	"slices"
	"sort"
	"strings"
	"sync/atomic"

	pg_label "github.com/buildbarn/bonanza/pkg/label"
	model_core "github.com/buildbarn/bonanza/pkg/model/core"
	"github.com/buildbarn/bonanza/pkg/model/core/btree"
	model_parser "github.com/buildbarn/bonanza/pkg/model/parser"
	model_core_pb "github.com/buildbarn/bonanza/pkg/proto/model/core"
	model_starlark_pb "github.com/buildbarn/bonanza/pkg/proto/model/starlark"
	"github.com/buildbarn/bonanza/pkg/storage/object"

	"go.starlark.net/starlark"
	"go.starlark.net/syntax"
)

type Struct struct {
	providerInstanceProperties *ProviderInstanceProperties
	keys                       []string
	values                     []any
	decodedValues              []atomic.Pointer[starlark.Value]
	hash                       uint32
}

var (
	_ EncodableValue      = (*Struct)(nil)
	_ starlark.Comparable = (*Struct)(nil)
	_ starlark.HasAttrs   = (*Struct)(nil)
	_ starlark.Mapping    = (*Struct)(nil)
)

func NewStructFromDict(providerInstanceProperties *ProviderInstanceProperties, entries map[string]any) *Struct {
	keys := make([]string, 0, len(entries))
	for k := range entries {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	values := make([]any, 0, len(entries))
	for _, k := range keys {
		values = append(values, entries[k])
	}
	return newStructFromLists(providerInstanceProperties, keys, values)
}

func newStructFromLists(providerInstanceProperties *ProviderInstanceProperties, keys []string, values []any) *Struct {
	return &Struct{
		providerInstanceProperties: providerInstanceProperties,
		keys:                       keys,
		values:                     values,
		decodedValues:              make([]atomic.Pointer[starlark.Value], len(values)),
	}
}

func (s *Struct) String() string {
	var sb strings.Builder
	sb.WriteString("struct(")
	for i, key := range s.keys {
		if i > 0 {
			sb.WriteString(", ")
		}
		sb.WriteString(key)

		// As we don't have access to the thread, we can't
		// decode any values if needed. Selectively printing
		// values based on whether they are already decoded is
		// not deterministic. For now, don't print any values.
		sb.WriteString(" = ...")
	}
	sb.WriteByte(')')
	return sb.String()
}

func (Struct) Type() string {
	return "struct"
}

func (Struct) Freeze() {}

func (Struct) Truth() starlark.Bool {
	return starlark.True
}

func (s *Struct) Hash(thread *starlark.Thread) (uint32, error) {
	if s.hash == 0 {
		// The same math as performed by starlarkstruct.
		var h, m uint32 = 8731, 9839
		for i, key := range s.keys {
			keyHash, err := starlark.String(key).Hash(thread)
			if err != nil {
				return 0, fmt.Errorf("key of field %#v: %w", key, err)
			}

			value, err := s.fieldAtIndex(thread, i)
			if err != nil {
				return 0, fmt.Errorf("value of field %#v: %w", key, err)
			}
			valueHash, err := value.Hash(thread)
			if err != nil {
				return 0, fmt.Errorf("value of field %#v: %w", key, err)
			}

			h ^= 3 * keyHash
			h ^= m * valueHash
			m += 7349
		}
		if h == 0 {
			h = 1
		}
		s.hash = h
	}
	return s.hash, nil
}

func (s *Struct) fieldAtIndex(thread *starlark.Thread, index int) (starlark.Value, error) {
	switch typedValue := s.values[index].(type) {
	case starlark.Value:
		return typedValue, nil
	case model_core.Message[*model_starlark_pb.Value, object.OutgoingReferences[object.LocalReference]]:
		if decodedValue := s.decodedValues[index].Load(); decodedValue != nil {
			return *decodedValue, nil
		}

		valueDecodingOptions := thread.Local(ValueDecodingOptionsKey)
		if valueDecodingOptions == nil {
			return nil, errors.New("struct fields with encoded values cannot be decoded from within this context")
		}

		decodedValue, err := DecodeValue(
			typedValue,
			nil,
			valueDecodingOptions.(*ValueDecodingOptions),
		)
		if err != nil {
			return nil, err
		}
		s.decodedValues[index].Store(&decodedValue)
		return decodedValue, nil
	default:
		panic("unknown value type")
	}
}

func (s *Struct) Attr(thread *starlark.Thread, name string) (starlark.Value, error) {
	index, ok := sort.Find(
		len(s.keys),
		func(i int) int { return strings.Compare(name, s.keys[i]) },
	)
	if !ok {
		return nil, nil
	}
	return s.fieldAtIndex(thread, index)
}

func (s *Struct) AttrNames() []string {
	return s.keys
}

func (s *Struct) Get(thread *starlark.Thread, key starlark.Value) (starlark.Value, bool, error) {
	if s.providerInstanceProperties == nil || !s.providerInstanceProperties.dictLike {
		return nil, true, errors.New("only structs that were instantiated through a provider that was declared with dict_like=True may be accessed like a dict")
	}

	keyStr, ok := key.(starlark.String)
	if !ok {
		return nil, false, errors.New("keys have to be of type string")
	}
	index, ok := sort.Find(
		len(s.keys),
		func(i int) int { return strings.Compare(string(keyStr), s.keys[i]) },
	)
	if !ok {
		return nil, false, nil
	}

	value, err := s.fieldAtIndex(thread, index)
	return value, true, err
}

func (s *Struct) equals(thread *starlark.Thread, other *Struct, depth int) (bool, error) {
	if s != other {
		// Compare providers.
		if (s.providerInstanceProperties == nil) != (other.providerInstanceProperties == nil) || (s.providerInstanceProperties != nil &&
			!s.providerInstanceProperties.LateNamedValue.equals(&other.providerInstanceProperties.LateNamedValue)) {
			return false, nil
		}

		// Compare keys.
		if !slices.Equal(s.keys, other.keys) {
			return false, nil
		}

		// Compare values.
		//
		// TODO: Do we want to optimize this to prevent unnecessary
		// decoding of values, or do we only perform struct comparisons
		// sparingly?
		for i, key := range s.keys {
			va, err := s.fieldAtIndex(thread, i)
			if err != nil {
				return false, fmt.Errorf("field %#v: %w", key, err)
			}
			vb, err := other.fieldAtIndex(thread, i)
			if err != nil {
				return false, fmt.Errorf("field %#v: %w", key, err)
			}
			if equal, err := starlark.EqualDepth(thread, va, vb, depth-1); err != nil {
				return false, fmt.Errorf("field %#v: %w", key, err)
			} else if !equal {
				return false, nil
			}
		}
	}
	return true, nil
}

func (s *Struct) CompareSameType(thread *starlark.Thread, op syntax.Token, other starlark.Value, depth int) (bool, error) {
	switch op {
	case syntax.EQL:
		return s.equals(thread, other.(*Struct), depth)
	case syntax.NEQ:
		equal, err := s.equals(thread, other.(*Struct), depth)
		return !equal, err
	default:
		return false, errors.New("structs cannot be compared for inequality")
	}
}

func (s *Struct) ToDict() map[string]any {
	dict := make(map[string]any, len(s.keys))
	for i, k := range s.keys {
		dict[k] = s.values[i]
	}
	return dict
}

func (s *Struct) EncodeStructFields(path map[starlark.Value]struct{}, options *ValueEncodingOptions) (model_core.PatchedMessage[*model_starlark_pb.Struct_Fields, model_core.CreatedObjectTree], bool, error) {
	listBuilder := newListBuilder(options)
	needsCode := false
	for i, value := range s.values {
		var encodedValue model_core.PatchedMessage[*model_starlark_pb.Value, model_core.CreatedObjectTree]
		switch typedValue := value.(type) {
		case starlark.Value:
			var fieldNeedsCode bool
			var err error
			encodedValue, fieldNeedsCode, err = EncodeValue(typedValue, path, nil, options)
			if err != nil {
				return model_core.PatchedMessage[*model_starlark_pb.Struct_Fields, model_core.CreatedObjectTree]{}, false, fmt.Errorf("field %#v: %w", s.keys[i], err)
			}
			needsCode = needsCode || fieldNeedsCode
		case model_core.Message[*model_starlark_pb.Value, object.OutgoingReferences[object.LocalReference]]:
			encodedValue = model_core.NewPatchedMessageFromExisting(
				typedValue,
				func(index int) model_core.CreatedObjectTree {
					return model_core.ExistingCreatedObjectTree
				},
			)
		default:
			panic("unknown value type")
		}
		if err := listBuilder.PushChild(model_core.NewPatchedMessage(
			&model_starlark_pb.List_Element{
				Level: &model_starlark_pb.List_Element_Leaf{
					Leaf: encodedValue.Message,
				},
			},
			encodedValue.Patcher,
		)); err != nil {
			return model_core.PatchedMessage[*model_starlark_pb.Struct_Fields, model_core.CreatedObjectTree]{}, false, err
		}
	}

	values, err := listBuilder.FinalizeList()
	if err != nil {
		return model_core.PatchedMessage[*model_starlark_pb.Struct_Fields, model_core.CreatedObjectTree]{}, false, err
	}

	return model_core.NewPatchedMessage(
		&model_starlark_pb.Struct_Fields{
			Keys:   s.keys,
			Values: values.Message,
		},
		values.Patcher,
	), needsCode, nil
}

func (s *Struct) Encode(path map[starlark.Value]struct{}, options *ValueEncodingOptions) (model_core.PatchedMessage[*model_starlark_pb.Struct, model_core.CreatedObjectTree], bool, error) {
	var providerInstanceProperties *model_starlark_pb.Provider_InstanceProperties
	if pip := s.providerInstanceProperties; pip != nil {
		var err error
		providerInstanceProperties, err = pip.Encode()
		if err != nil {
			return model_core.PatchedMessage[*model_starlark_pb.Struct, model_core.CreatedObjectTree]{}, false, err
		}
	}

	fields, needsCode, err := s.EncodeStructFields(path, options)
	if err != nil {
		return model_core.PatchedMessage[*model_starlark_pb.Struct, model_core.CreatedObjectTree]{}, false, err
	}

	return model_core.NewPatchedMessage(
		&model_starlark_pb.Struct{
			Fields:                     fields.Message,
			ProviderInstanceProperties: providerInstanceProperties,
		},
		fields.Patcher,
	), needsCode, nil
}

func (s *Struct) EncodeValue(path map[starlark.Value]struct{}, currentIdentifier *pg_label.CanonicalStarlarkIdentifier, options *ValueEncodingOptions) (model_core.PatchedMessage[*model_starlark_pb.Value, model_core.CreatedObjectTree], bool, error) {
	encodedStruct, needsCode, err := s.Encode(path, options)
	if err != nil {
		return model_core.PatchedMessage[*model_starlark_pb.Value, model_core.CreatedObjectTree]{}, false, err
	}
	return model_core.NewPatchedMessage(
		&model_starlark_pb.Value{
			Kind: &model_starlark_pb.Value_Struct{
				Struct: encodedStruct.Message,
			},
		},
		encodedStruct.Patcher,
	), needsCode, nil
}

func (s *Struct) GetProviderIdentifier() (pg_label.CanonicalStarlarkIdentifier, error) {
	var bad pg_label.CanonicalStarlarkIdentifier
	pip := s.providerInstanceProperties
	if pip == nil {
		return bad, errors.New("struct was not created using a provider")
	}
	if pip.Identifier == nil {
		return bad, errors.New("provider that was used to create the struct does not have a name")
	}
	return *pip.Identifier, nil
}

func AllStructFields(
	ctx context.Context,
	reader model_parser.ParsedObjectReader[object.LocalReference, model_core.Message[[]*model_starlark_pb.List_Element, object.OutgoingReferences[object.LocalReference]]],
	structFields model_core.Message[*model_starlark_pb.Struct_Fields, object.OutgoingReferences[object.LocalReference]],
	errOut *error,
) iter.Seq2[string, model_core.Message[*model_starlark_pb.Value, object.OutgoingReferences[object.LocalReference]]] {
	if structFields.Message == nil {
		*errOut = errors.New("no struct fields provided")
		return func(yield func(string, model_core.Message[*model_starlark_pb.Value, object.OutgoingReferences[object.LocalReference]]) bool) {
		}
	}

	allLeaves := btree.AllLeaves(
		ctx,
		reader,
		model_core.NewNestedMessage(structFields, structFields.Message.Values),
		func(element model_core.Message[*model_starlark_pb.List_Element, object.OutgoingReferences[object.LocalReference]]) (*model_core_pb.Reference, error) {
			if level, ok := element.Message.Level.(*model_starlark_pb.List_Element_Parent_); ok {
				return level.Parent.Reference, nil
			}
			return nil, nil
		},
		errOut,
	)

	keys := structFields.Message.Keys
	return func(yield func(string, model_core.Message[*model_starlark_pb.Value, object.OutgoingReferences[object.LocalReference]]) bool) {
		allLeaves(func(entry model_core.Message[*model_starlark_pb.List_Element, object.OutgoingReferences[object.LocalReference]]) bool {
			leaf, ok := entry.Message.Level.(*model_starlark_pb.List_Element_Leaf)
			if !ok {
				*errOut = errors.New("not a valid leaf entry")
				return false
			}

			if len(keys) == 0 {
				*errOut = errors.New("struct has fewer keys than values")
				return false
			}
			key := keys[0]
			keys = keys[1:]

			return yield(key, model_core.NewNestedMessage(entry, leaf.Leaf))
		})
	}
}

func GetStructFieldValue(
	ctx context.Context,
	reader model_parser.ParsedObjectReader[object.LocalReference, model_core.Message[[]*model_starlark_pb.List_Element, object.OutgoingReferences[object.LocalReference]]],
	structFields model_core.Message[*model_starlark_pb.Struct_Fields, object.OutgoingReferences[object.LocalReference]],
	key string,
) (model_core.Message[*model_starlark_pb.Value, object.OutgoingReferences[object.LocalReference]], error) {
	if structFields.Message == nil {
		return model_core.Message[*model_starlark_pb.Value, object.OutgoingReferences[object.LocalReference]]{}, errors.New("no struct fields provided")
	}

	keys := structFields.Message.Keys
	index, ok := sort.Find(
		len(keys),
		func(i int) int { return strings.Compare(key, keys[i]) },
	)
	if !ok {
		return model_core.Message[*model_starlark_pb.Value, object.OutgoingReferences[object.LocalReference]]{}, errors.New("struct field not found")
	}

	contiguousLength := len(keys)
	list := model_core.NewNestedMessage(structFields, structFields.Message.Values)
	for {
		// List elements may never refer to empty nested lists,
		// meaning that if the length of a list is equal to the
		// expected total number of elements, each list element
		// contains exactly one value. This allows us to jump
		// directly to the right spot.
		if len(list.Message) == contiguousLength {
			list.Message = list.Message[index:]
			index = 0
		}

		for _, element := range list.Message {
			switch level := element.Level.(type) {
			case *model_starlark_pb.List_Element_Parent_:
				panic("TODO")
			case *model_starlark_pb.List_Element_Leaf:
				if index == 0 {
					return model_core.NewNestedMessage(list, level.Leaf), nil
				}
				index--
			}
		}
		return model_core.Message[*model_starlark_pb.Value, object.OutgoingReferences[object.LocalReference]]{}, errors.New("number of keys does not match number of values")
	}
}
