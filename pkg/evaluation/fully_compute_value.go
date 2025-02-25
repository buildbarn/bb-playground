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

func getKeyString(key model_core.Message[proto.Message]) (string, error) {
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

func appendFormattedKey(out []byte, key model_core.Message[proto.Message]) []byte {
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

type keyState struct {
	parent              *keyState
	key                 model_core.Message[proto.Message]
	next                *keyState
	value               valueState
	missingDependencies []*keyState
}

func (ks *keyState) getKeyType() string {
	return string(ks.key.Message.ProtoReflect().Descriptor().FullName().Parent().Name())
}

func (ks *keyState) getDependencyCycle(cyclePath *[]*keyState, seen map[*keyState]int) int {
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

type valueState interface {
	compute(ctx context.Context, c Computer, e *fullyComputingEnvironment) error
}

type messageValueState struct {
	value model_core.Message[proto.Message]
}

func (vs *messageValueState) compute(ctx context.Context, c Computer, e *fullyComputingEnvironment) error {
	value, err := c.ComputeMessageValue(ctx, e.keyState.key, e)
	if err != nil {
		return err
	}

	// Value got computed. Write any objects referenced by the
	// values to storage.
	p := e.pool
	references, objectContentsWalkers := value.Patcher.SortAndSetReferences()
	if len(references) > 0 {
		if err := p.storeValueChildren(references, objectContentsWalkers); err != nil {
			return err
		}
	}

	vs.value = model_core.Message[proto.Message]{
		Message:            value.Message,
		OutgoingReferences: references,
	}
	return nil
}

type nativeValueState struct {
	value any
	isSet bool
}

func (vs *nativeValueState) compute(ctx context.Context, c Computer, e *fullyComputingEnvironment) error {
	value, err := c.ComputeNativeValue(ctx, e.keyState.key, e)
	if err != nil {
		return err
	}
	vs.value = value
	vs.isSet = true
	return nil
}

type fullyComputingKeyPool struct {
	// Constant fields.
	storeValueChildren ValueChildrenStorer

	// Variable fields.
	keys                   map[string]*keyState
	firstPendingKey        *keyState
	lastPendingKey         **keyState
	firstMissingDependency *keyState
	err                    error
}

func (p *fullyComputingKeyPool) setError(err error) {
	if p.err == nil {
		p.err = err
	}
}

func (p *fullyComputingKeyPool) enqueue(ks *keyState) {
	*p.lastPendingKey = ks
	p.lastPendingKey = &ks.next
}

type fullyComputingEnvironment struct {
	pool                *fullyComputingKeyPool
	keyState            *keyState
	missingDependencies []*keyState
}

func (e *fullyComputingEnvironment) getKeyState(patchedKey model_core.PatchedMessage[proto.Message, dag.ObjectContentsWalker], vs valueState) *keyState {
	// If the key contains outgoing references, ensure children are
	// written, so that they can be reloaded during evaluation.
	p := e.pool
	references, objectContentsWalkers := patchedKey.Patcher.SortAndSetReferences()
	if len(references) > 0 {
		if err := e.pool.storeValueChildren(references, objectContentsWalkers); err != nil {
			p.setError(err)
			return nil
		}
	}

	key := model_core.Message[proto.Message]{
		Message:            patchedKey.Message,
		OutgoingReferences: references,
	}
	keyStr, err := getKeyString(key)
	if err != nil {
		p.setError(err)
		return nil
	}
	ks, ok := p.keys[keyStr]
	if !ok {
		ks = &keyState{
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

func (e *fullyComputingEnvironment) GetMessageValue(patchedKey model_core.PatchedMessage[proto.Message, dag.ObjectContentsWalker]) model_core.Message[proto.Message] {
	ks := e.getKeyState(patchedKey, &messageValueState{})
	if ks == nil {
		return model_core.Message[proto.Message]{}
	}
	vs := ks.value.(*messageValueState)
	if !vs.value.IsSet() {
		e.missingDependencies = append(e.missingDependencies, ks)
	}
	return vs.value
}

func (e *fullyComputingEnvironment) GetNativeValue(patchedKey model_core.PatchedMessage[proto.Message, dag.ObjectContentsWalker]) (any, bool) {
	ks := e.getKeyState(patchedKey, &nativeValueState{})
	if ks == nil {
		return nil, false
	}
	vs := ks.value.(*nativeValueState)
	if !vs.isSet {
		e.missingDependencies = append(e.missingDependencies, ks)
		return nil, false
	}
	return vs.value, true
}

type ValueChildrenStorer func(references []object.LocalReference, objectContentsWalkers []dag.ObjectContentsWalker) error

func FullyComputeValue(ctx context.Context, c Computer, requestedKey model_core.Message[proto.Message], storeValueChildren ValueChildrenStorer) (model_core.Message[proto.Message], error) {
	requestedKeyStr, err := getKeyString(requestedKey)
	if err != nil {
		return model_core.Message[proto.Message]{}, err
	}
	requestedValueState := &messageValueState{}
	requestedKeyState := &keyState{
		key:   requestedKey,
		value: requestedValueState,
	}
	p := fullyComputingKeyPool{
		storeValueChildren: storeValueChildren,

		keys: map[string]*keyState{
			requestedKeyStr: requestedKeyState,
		},
		firstPendingKey: requestedKeyState,
		lastPendingKey:  &requestedKeyState.next,
	}
	longestKeyType := 0
	for !requestedValueState.value.IsSet() {
		ks := p.firstPendingKey
		if ks == p.firstMissingDependency {
			var stack []*keyState
			traceLongestKeyType := 0
			for ksIter := ks; ksIter != nil; ksIter = ksIter.parent {
				stack = append(stack, ksIter)
				if l := len(ksIter.getKeyType()); traceLongestKeyType < l {
					traceLongestKeyType = l
				}
			}
			slices.Reverse(stack)
			seen := make(map[*keyState]int, len(stack))
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
			return model_core.Message[proto.Message]{}, fmt.Errorf("Traceback (most recent key last):%s\nCyclic evaluation dependency detected", string(cycleStr))
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

		e := fullyComputingEnvironment{
			pool:     &p,
			keyState: ks,
		}
		err = ks.value.compute(ctx, c, &e)
		if err != nil {
			if !errors.Is(err, ErrMissingDependency) {
				fmt.Printf("\n")

				var stack []*keyState
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
				return model_core.Message[proto.Message]{}, fmt.Errorf("Traceback (most recent key last):%s\n%w", string(stackStr), err)
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
