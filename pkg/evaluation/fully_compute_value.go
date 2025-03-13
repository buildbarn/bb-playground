package evaluation

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"slices"

	"github.com/buildbarn/bonanza/pkg/encoding/varint"
	model_core "github.com/buildbarn/bonanza/pkg/model/core"
	"github.com/buildbarn/bonanza/pkg/storage/dag"
	"github.com/buildbarn/bonanza/pkg/storage/object"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
)

func getKeyString[TReference object.BasicReference](key model_core.Message[proto.Message, TReference]) (string, error) {
	// Marshal the outgoing references of the key.
	degree := key.OutgoingReferences.GetDegree()
	marshaledKey := varint.AppendForward(nil, degree)
	for i := 0; i < degree; i++ {
		marshaledKey = append(marshaledKey, key.OutgoingReferences.GetOutgoingReference(i).GetRawReference()...)
	}

	// Append the message of the key.
	anyKey, err := anypb.New(key.Message)
	if err != nil {
		return "", err
	}
	marshaledKey, err = proto.MarshalOptions{Deterministic: true}.MarshalAppend(marshaledKey, anyKey)
	if err != nil {
		return "", err
	}
	return string(marshaledKey), nil
}

func appendFormattedKey[TReference object.BasicReference](out []byte, key model_core.Message[proto.Message, TReference]) []byte {
	out, _ = protojson.MarshalOptions{}.MarshalAppend(out, key.Message)
	if degree := key.OutgoingReferences.GetDegree(); degree > 0 {
		out = append(out, " ["...)
		for i := 0; i < degree; i++ {
			if i > 0 {
				out = append(out, ", "...)
			}
			out = hex.AppendEncode(out, key.OutgoingReferences.GetOutgoingReference(i).GetRawReference())
		}
		out = append(out, ']')
	}
	return out
}

type keyState[TReference object.BasicReference, TMetadata any] struct {
	parent              *keyState[TReference, TMetadata]
	key                 model_core.Message[proto.Message, TReference]
	next                *keyState[TReference, TMetadata]
	value               valueState[TReference, TMetadata]
	missingDependencies []*keyState[TReference, TMetadata]
}

func (ks *keyState[TReference, TMetadata]) getKeyType() string {
	return string(ks.key.Message.ProtoReflect().Descriptor().FullName().Parent().Name())
}

func (ks *keyState[TReference, TMetadata]) getDependencyCycle(cyclePath *[]*keyState[TReference, TMetadata], seen map[*keyState[TReference, TMetadata]]int) int {
	pathLength := len(*cyclePath)
	for _, ksDep := range ks.missingDependencies {
		*cyclePath = append(*cyclePath, ksDep)
		if index, ok := seen[ksDep]; ok {
			return index
		}
		seen[ksDep] = pathLength

		if index := ksDep.getDependencyCycle(cyclePath, seen); index >= 0 {
			return index
		}

		*cyclePath = (*cyclePath)[:pathLength]
		delete(seen, ksDep)
	}
	return -1
}

type valueState[TReference object.BasicReference, TMetadata any] interface {
	compute(ctx context.Context, c Computer[TReference, TMetadata], e *fullyComputingEnvironment[TReference, TMetadata]) error
}

type messageValueState[TReference object.BasicReference, TMetadata any] struct {
	value model_core.Message[proto.Message, TReference]
}

func (vs *messageValueState[TReference, TMetadata]) compute(ctx context.Context, c Computer[TReference, TMetadata], e *fullyComputingEnvironment[TReference, TMetadata]) error {
	value, err := c.ComputeMessageValue(ctx, e.keyState.key, e)
	if err != nil {
		return err
	}

	// Value got computed. Write any objects referenced by the
	// values to storage.
	p := e.pool
	localReferences, objectContentsWalkers := value.Patcher.SortAndSetReferences()
	var storedReferences []TReference
	if len(localReferences) > 0 {
		storedReferences, err = p.storeValueChildren(localReferences, objectContentsWalkers)
		if err != nil {
			return err
		}
	}

	vs.value = model_core.NewMessage(value.Message, object.OutgoingReferencesList[TReference](storedReferences))
	return nil
}

type nativeValueState[TReference object.BasicReference, TMetadata any] struct {
	value any
	isSet bool
}

func (vs *nativeValueState[TReference, TMetadata]) compute(ctx context.Context, c Computer[TReference, TMetadata], e *fullyComputingEnvironment[TReference, TMetadata]) error {
	value, err := c.ComputeNativeValue(ctx, e.keyState.key, e)
	if err != nil {
		return err
	}
	vs.value = value
	vs.isSet = true
	return nil
}

type fullyComputingKeyPool[TReference object.BasicReference, TMetadata any] struct {
	// Constant fields.
	storeValueChildren ValueChildrenStorer[TReference]

	// Variable fields.
	keys                   map[string]*keyState[TReference, TMetadata]
	firstPendingKey        *keyState[TReference, TMetadata]
	lastPendingKey         **keyState[TReference, TMetadata]
	firstMissingDependency *keyState[TReference, TMetadata]
	err                    error
}

func (p *fullyComputingKeyPool[TReference, TMetadata]) setError(err error) {
	if p.err == nil {
		p.err = err
	}
}

func (p *fullyComputingKeyPool[TReference, TMetadata]) enqueue(ks *keyState[TReference, TMetadata]) {
	*p.lastPendingKey = ks
	p.lastPendingKey = &ks.next
}

type fullyComputingEnvironment[TReference object.BasicReference, TMetadata any] struct {
	model_core.ObjectManager[TReference, TMetadata]

	pool                *fullyComputingKeyPool[TReference, TMetadata]
	keyState            *keyState[TReference, TMetadata]
	missingDependencies []*keyState[TReference, TMetadata]
}

func (e *fullyComputingEnvironment[TReference, TMetadata]) getKeyState(patchedKey model_core.PatchedMessage[proto.Message, dag.ObjectContentsWalker], vs valueState[TReference, TMetadata]) *keyState[TReference, TMetadata] {
	// If the key contains outgoing references, ensure children are
	// written, so that they can be reloaded during evaluation.
	p := e.pool
	localReferences, objectContentsWalkers := patchedKey.Patcher.SortAndSetReferences()
	var storedReferences []TReference
	if len(localReferences) > 0 {
		var err error
		storedReferences, err = e.pool.storeValueChildren(localReferences, objectContentsWalkers)
		if err != nil {
			p.setError(err)
			return nil
		}
	}

	key := model_core.NewMessage(patchedKey.Message, object.OutgoingReferencesList[TReference](storedReferences))
	keyStr, err := getKeyString(key)
	if err != nil {
		p.setError(err)
		return nil
	}
	ks, ok := p.keys[keyStr]
	if !ok {
		ks = &keyState[TReference, TMetadata]{
			parent: e.keyState,
			key:    key,
			value:  vs,
		}
		p.keys[keyStr] = ks
		p.enqueue(ks)
		p.firstMissingDependency = nil
	}
	return ks
}

func (e *fullyComputingEnvironment[TReference, TMetadata]) GetMessageValue(patchedKey model_core.PatchedMessage[proto.Message, dag.ObjectContentsWalker]) model_core.Message[proto.Message, TReference] {
	ks := e.getKeyState(patchedKey, &messageValueState[TReference, TMetadata]{})
	if ks == nil {
		return model_core.Message[proto.Message, TReference]{}
	}
	vs := ks.value.(*messageValueState[TReference, TMetadata])
	if !vs.value.IsSet() {
		e.missingDependencies = append(e.missingDependencies, ks)
	}
	return vs.value
}

func (e *fullyComputingEnvironment[TReference, TMetadata]) GetNativeValue(patchedKey model_core.PatchedMessage[proto.Message, dag.ObjectContentsWalker]) (any, bool) {
	ks := e.getKeyState(patchedKey, &nativeValueState[TReference, TMetadata]{})
	if ks == nil {
		return nil, false
	}
	vs := ks.value.(*nativeValueState[TReference, TMetadata])
	if !vs.isSet {
		e.missingDependencies = append(e.missingDependencies, ks)
		return nil, false
	}
	return vs.value, true
}

func (e *fullyComputingEnvironment[TReference, TMetadata]) Store(references []object.LocalReference, walkers []dag.ObjectContentsWalker) object.OutgoingReferences[TReference] {
	panic("TODO")
}

type ValueChildrenStorer[TReference any] func(localReferences []object.LocalReference, objectContentsWalkers []dag.ObjectContentsWalker) ([]TReference, error)

func FullyComputeValue[TReference object.BasicReference, TMetadata any](
	ctx context.Context,
	c Computer[TReference, TMetadata],
	requestedKey model_core.Message[proto.Message, TReference],
	objectManager model_core.ObjectManager[TReference, TMetadata],
	storeValueChildren ValueChildrenStorer[TReference],
) (model_core.Message[proto.Message, TReference], error) {
	requestedKeyStr, err := getKeyString(requestedKey)
	if err != nil {
		return model_core.Message[proto.Message, TReference]{}, err
	}
	requestedValueState := &messageValueState[TReference, TMetadata]{}
	requestedKeyState := &keyState[TReference, TMetadata]{
		key:   requestedKey,
		value: requestedValueState,
	}
	p := fullyComputingKeyPool[TReference, TMetadata]{
		storeValueChildren: storeValueChildren,

		keys: map[string]*keyState[TReference, TMetadata]{
			requestedKeyStr: requestedKeyState,
		},
		firstPendingKey: requestedKeyState,
		lastPendingKey:  &requestedKeyState.next,
	}
	longestKeyType := 0
	for !requestedValueState.value.IsSet() {
		ks := p.firstPendingKey
		if ks == p.firstMissingDependency {
			var stack []*keyState[TReference, TMetadata]
			traceLongestKeyType := 0
			for ksIter := ks; ksIter != nil; ksIter = ksIter.parent {
				stack = append(stack, ksIter)
				if l := len(ksIter.getKeyType()); traceLongestKeyType < l {
					traceLongestKeyType = l
				}
			}
			slices.Reverse(stack)
			seen := make(map[*keyState[TReference, TMetadata]]int, len(stack))
			for index, ksIter := range stack {
				seen[ksIter] = index
			}
			cycleStart := ks.getDependencyCycle(&stack, seen)
			var cycleStr []byte
			for index, ksIter := range stack {
				if index == cycleStart || index == len(stack)-1 {
					cycleStr = append(cycleStr, "\n→ "...)
				} else {
					cycleStr = append(cycleStr, "\n  "...)
				}
				keyType := ksIter.getKeyType()
				cycleStr = append(cycleStr, keyType...)
				for i := len(keyType); i < traceLongestKeyType+2; i++ {
					cycleStr = append(cycleStr, ' ')
				}
				cycleStr = appendFormattedKey(cycleStr, ksIter.key)
			}
			return model_core.Message[proto.Message, TReference]{}, fmt.Errorf("Traceback (most recent key last):%s\nCyclic evaluation dependency detected", string(cycleStr))
		}

		p.firstPendingKey = ks.next
		ks.next = nil
		if p.firstPendingKey == nil {
			p.lastPendingKey = &p.firstPendingKey
		}

		// TODO: Add interface for exposing information on
		// what's going on.
		keyType := ks.getKeyType()
		if l := len(keyType); longestKeyType < l {
			longestKeyType = l
		}
		fmt.Printf("\x1b[?7l%-*s  %s\x1b[?7h", longestKeyType, keyType, string(appendFormattedKey(nil, ks.key)))

		e := fullyComputingEnvironment[TReference, TMetadata]{
			ObjectManager: objectManager,
			pool:          &p,
			keyState:      ks,
		}
		err = ks.value.compute(ctx, c, &e)
		if err != nil {
			if !errors.Is(err, ErrMissingDependency) {
				fmt.Printf("\n")

				var stack []*keyState[TReference, TMetadata]
				traceLongestKeyType := 0
				for ksIter := ks; ksIter != nil; ksIter = ksIter.parent {
					stack = append(stack, ksIter)
					if l := len(ksIter.getKeyType()); traceLongestKeyType < l {
						traceLongestKeyType = l
					}
				}
				var stackStr []byte
				for i := len(stack) - 1; i >= 0; i-- {
					ksIter := stack[i]
					stackStr = append(stackStr, "\n  "...)
					keyType := ksIter.getKeyType()
					stackStr = append(stackStr, keyType...)
					for i := len(keyType); i < traceLongestKeyType+2; i++ {
						stackStr = append(stackStr, ' ')
					}
					stackStr = appendFormattedKey(stackStr, ksIter.key)
				}
				return model_core.Message[proto.Message, TReference]{}, fmt.Errorf("Traceback (most recent key last):%s\n%w", string(stackStr), err)
			}
			// Value could not be computed, because one of
			// its dependencies hasn't been computed yet.
			p.enqueue(ks)
			if len(e.missingDependencies) == 0 {
				panic("function returned ErrMissingDependency, but no missing dependencies were registered")
			}
			ks.missingDependencies = e.missingDependencies
			if p.firstMissingDependency == nil {
				p.firstMissingDependency = ks
			}
			fmt.Printf("\r\x1b[K")
		} else {
			// Successfully computed value.
			ks.missingDependencies = nil
			p.firstMissingDependency = nil
			fmt.Printf("\n")
		}
	}
	return requestedValueState.value, nil
}

type ValueChildrenStorerForTesting ValueChildrenStorer[object.LocalReference]
