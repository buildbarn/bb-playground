package starlark

import (
	"errors"
	"iter"
	"slices"

	pg_label "github.com/buildbarn/bb-playground/pkg/label"
	model_core "github.com/buildbarn/bb-playground/pkg/model/core"
	"github.com/buildbarn/bb-playground/pkg/model/core/btree"
	model_parser "github.com/buildbarn/bb-playground/pkg/model/parser"
	model_core_pb "github.com/buildbarn/bb-playground/pkg/proto/model/core"
	model_starlark_pb "github.com/buildbarn/bb-playground/pkg/proto/model/starlark"
	"github.com/buildbarn/bb-playground/pkg/storage/dag"
	"github.com/buildbarn/bb-playground/pkg/storage/object"

	"go.starlark.net/starlark"
)

type Depset struct {
	children any
	order    model_starlark_pb.Depset_Order
}

var (
	_ EncodableValue    = (*Depset)(nil)
	_ starlark.HasAttrs = (*Depset)(nil)
)

var EmptyDepset Depset

func deduplicateAndAddDirect(children *[]any, direct iter.Seq2[int, starlark.Value], childrenSeen map[any]struct{}) {
	for _, v := range direct {
		if _, ok := childrenSeen[v]; !ok {
			*children = append(*children, v)
			childrenSeen[v] = struct{}{}
		}
	}
}

func deduplicateAndAddTransitive(children *[]any, transitive iter.Seq2[int, *Depset], childrenSeen map[any]struct{}, order model_starlark_pb.Depset_Order) error {
	for _, d := range transitive {
		switch v := d.children.(type) {
		case nil:
			// Empty child. Ignore it.
		case starlark.Value:
			// Single child that is decoded. Add it directly.
			if _, ok := childrenSeen[v]; !ok {
				*children = append(*children, v)
				childrenSeen[v] = struct{}{}
			}
		case model_core.Message[*model_starlark_pb.List_Element]:
			// Single child that is encoded. Add it directly.
			if _, ok := childrenSeen[v.Message]; !ok {
				*children = append(*children, v)
				childrenSeen[v.Message] = struct{}{}
			}
		case []any:
			// Multiple children. Reference it.
			if order != d.order && order != model_starlark_pb.Depset_DEFAULT && d.order != model_starlark_pb.Depset_DEFAULT {
				return errors.New("depsets have incompatible orders")
			}
			if _, ok := childrenSeen[d]; !ok {
				*children = append(*children, v)
				childrenSeen[d] = struct{}{}
			}
		}
	}
	return nil
}

func NewDepset(direct []starlark.Value, transitive []*Depset, order model_starlark_pb.Depset_Order) (*Depset, error) {
	var directIter iter.Seq2[int, starlark.Value]
	var transitiveIter iter.Seq2[int, *Depset]
	preorder := false
	switch order {
	case model_starlark_pb.Depset_DEFAULT, model_starlark_pb.Depset_POSTORDER:
		directIter = slices.All(direct)
		transitiveIter = slices.All(transitive)
	case model_starlark_pb.Depset_PREORDER:
		directIter = slices.All(direct)
		transitiveIter = slices.All(transitive)
		preorder = true
	case model_starlark_pb.Depset_TOPOLOGICAL:
		// Insert elements in reverse order, because
		// deduplication needs to happen from the back to the
		// front.
		directIter = slices.Backward(direct)
		transitiveIter = slices.Backward(transitive)
	default:
		return nil, errors.New("unknown order")
	}

	estimatedSize := len(direct) + len(transitive)
	childrenSeen := make(map[any]struct{}, estimatedSize)
	children := make([]any, 0, estimatedSize)
	if preorder {
		deduplicateAndAddDirect(&children, directIter, childrenSeen)
		if err := deduplicateAndAddTransitive(&children, transitiveIter, childrenSeen, order); err != nil {
			return nil, err
		}
	} else {
		if err := deduplicateAndAddTransitive(&children, transitiveIter, childrenSeen, order); err != nil {
			return nil, err
		}
		deduplicateAndAddDirect(&children, directIter, childrenSeen)
	}

	return NewDepsetFromList(children, order), nil
}

func NewDepsetFromList(children []any, order model_starlark_pb.Depset_Order) *Depset {
	switch len(children) {
	case 0:
		return &EmptyDepset
	case 1:
		return &Depset{children: children[0]}
	default:
		return &Depset{children: children, order: order}
	}
}

func (Depset) String() string {
	return "<depset>"
}

func (Depset) Type() string {
	return "depset"
}

func (Depset) Freeze() {}

func (Depset) Truth() starlark.Bool {
	return starlark.True
}

func (Depset) Hash() (uint32, error) {
	return 0, errors.New("depset cannot be hashed")
}

type depsetChildrenEncoder struct {
	path        map[starlark.Value]struct{}
	options     *ValueEncodingOptions
	treeBuilder btree.Builder[*model_starlark_pb.List_Element, dag.ObjectContentsWalker]
	needsCode   bool
}

func (e *depsetChildrenEncoder) encode(children any) error {
	switch v := children.(type) {
	case nil:
	case starlark.Value:
		encodedValue, valueNeedsCode, err := EncodeValue(v, e.path, nil, e.options)
		if err != nil {
			return err
		}
		e.needsCode = e.needsCode || valueNeedsCode
		// TODO: Should we also deduplicate elements at this point?
		if err := e.treeBuilder.PushChild(
			model_core.NewPatchedMessage(
				&model_starlark_pb.List_Element{
					Level: &model_starlark_pb.List_Element_Leaf{
						Leaf: encodedValue.Message,
					},
				},
				encodedValue.Patcher,
			),
		); err != nil {
			return err
		}
	case model_core.Message[*model_starlark_pb.List_Element]:
		if err := e.treeBuilder.PushChild(
			model_core.NewPatchedMessageFromExisting(
				v,
				func(index int) dag.ObjectContentsWalker {
					return dag.ExistingObjectContentsWalker
				},
			),
		); err != nil {
			return err
		}
	case []any:
		for _, child := range v {
			if err := e.encode(child); err != nil {
				return err
			}
		}
	default:
		panic("unexpected element type")
	}
	return nil
}

func (d *Depset) Encode(path map[starlark.Value]struct{}, options *ValueEncodingOptions) (model_core.PatchedMessage[*model_starlark_pb.Depset, dag.ObjectContentsWalker], bool, error) {
	treeBuilder := newSplitBTreeBuilder(
		options,
		/* parentNodeComputer = */ func(contents *object.Contents, childNodes []*model_starlark_pb.List_Element, outgoingReferences object.OutgoingReferences, metadata []dag.ObjectContentsWalker) (model_core.PatchedMessage[*model_starlark_pb.List_Element, dag.ObjectContentsWalker], error) {
			patcher := model_core.NewReferenceMessagePatcher[dag.ObjectContentsWalker]()
			return model_core.NewPatchedMessage(
				&model_starlark_pb.List_Element{
					Level: &model_starlark_pb.List_Element_Parent_{
						Parent: &model_starlark_pb.List_Element_Parent{
							Reference: patcher.AddReference(contents.GetReference(), dag.NewSimpleObjectContentsWalker(contents, metadata)),
						},
					},
				},
				patcher,
			), nil
		},
	)

	e := depsetChildrenEncoder{
		path:        path,
		options:     options,
		treeBuilder: treeBuilder,
	}
	if err := e.encode(d.children); err != nil {
		return model_core.PatchedMessage[*model_starlark_pb.Depset, dag.ObjectContentsWalker]{}, false, err
	}

	elements, err := treeBuilder.FinalizeList()
	if err != nil {
		return model_core.PatchedMessage[*model_starlark_pb.Depset, dag.ObjectContentsWalker]{}, false, err
	}

	return model_core.NewSimplePatchedMessage[dag.ObjectContentsWalker](
		&model_starlark_pb.Depset{
			Elements: elements.Message,
			Order:    d.order,
		},
	), e.needsCode, nil
}

func (d *Depset) EncodeValue(path map[starlark.Value]struct{}, currentIdentifier *pg_label.CanonicalStarlarkIdentifier, options *ValueEncodingOptions) (model_core.PatchedMessage[*model_starlark_pb.Value, dag.ObjectContentsWalker], bool, error) {
	encodedDepset, needsCode, err := d.Encode(path, options)
	if err != nil {
		return model_core.PatchedMessage[*model_starlark_pb.Value, dag.ObjectContentsWalker]{}, false, err
	}
	return model_core.NewPatchedMessage(
		&model_starlark_pb.Value{
			Kind: &model_starlark_pb.Value_Depset{
				Depset: encodedDepset.Message,
			},
		},
		encodedDepset.Patcher,
	), needsCode, nil
}

func (d *Depset) Attr(thread *starlark.Thread, name string) (starlark.Value, error) {
	switch name {
	case "to_list":
		return starlark.NewBuiltin("depset.to_list", d.doToList), nil
	default:
		return nil, nil
	}
}

var depsetAttrNames = []string{
	"to_list",
}

func (d *Depset) AttrNames() []string {
	return depsetAttrNames
}

type depsetToListConverter struct {
	thread *starlark.Thread

	valueDecodingOptions *ValueDecodingOptions
	reader               model_parser.ParsedObjectReader[object.LocalReference, model_core.Message[[]*model_starlark_pb.List_Element]]

	list []starlark.Value
	seen map[any]struct{}
}

func (dlc *depsetToListConverter) appendChildren(children any) error {
	switch v := children.(type) {
	case starlark.Value:
		if _, ok := dlc.seen[v]; !ok {
			dlc.list = append(dlc.list, v)
			dlc.seen[v] = struct{}{}
		}
	case model_core.Message[*model_starlark_pb.List_Element]:
		if dlc.valueDecodingOptions == nil {
			valueDecodingOptionsValue := dlc.thread.Local(ValueDecodingOptionsKey)
			if valueDecodingOptionsValue == nil {
				return errors.New("depsets with encoded elements cannot be decoded from within this context")
			}
			dlc.valueDecodingOptions = valueDecodingOptionsValue.(*ValueDecodingOptions)
			dlc.reader = model_parser.NewStorageBackedParsedObjectReader(
				dlc.valueDecodingOptions.ObjectDownloader,
				dlc.valueDecodingOptions.ObjectEncoder,
				model_parser.NewMessageListObjectParser[object.LocalReference, model_starlark_pb.List_Element](),
			)
		}

		var errIter error
		for element := range btree.AllLeaves(
			dlc.valueDecodingOptions.Context,
			dlc.reader,
			model_core.Message[[]*model_starlark_pb.List_Element]{
				Message:            []*model_starlark_pb.List_Element{v.Message},
				OutgoingReferences: v.OutgoingReferences,
			},
			func(element *model_starlark_pb.List_Element) *model_core_pb.Reference {
				// TODO: Skip parents that we've seen before.
				if level, ok := element.Level.(*model_starlark_pb.List_Element_Parent_); ok {
					return level.Parent.Reference
				}
				return nil
			},
			&errIter,
		) {
			level, ok := element.Message.Level.(*model_starlark_pb.List_Element_Leaf)
			if !ok {
				return errors.New("not a valid leaf entry")
			}
			decodedElement, err := DecodeValue(
				model_core.Message[*model_starlark_pb.Value]{
					Message:            level.Leaf,
					OutgoingReferences: element.OutgoingReferences,
				},
				nil,
				dlc.valueDecodingOptions,
			)
			if err != nil {
				return err
			}
			if _, ok := dlc.seen[decodedElement]; !ok {
				dlc.list = append(dlc.list, decodedElement)
				dlc.seen[decodedElement] = struct{}{}
			}
		}
		if errIter != nil {
			return errIter
		}
	case []any:
		for _, child := range v {
			if err := dlc.appendChildren(child); err != nil {
				return err
			}
		}
	default:
		panic("unexpected element type")
	}
	return nil
}

func (d *Depset) doToList(thread *starlark.Thread, b *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
	dlc := depsetToListConverter{
		thread: thread,
		seen:   map[any]struct{}{},
	}
	if d.children != nil {
		if err := dlc.appendChildren(d.children); err != nil {
			return nil, err
		}
		if d.order == model_starlark_pb.Depset_TOPOLOGICAL {
			// Undo reversal caused by insertion in opposite
			// direction.
			slices.Reverse(dlc.list)
		}
	}
	l := starlark.NewList(dlc.list)
	l.Freeze()
	return l, nil
}
