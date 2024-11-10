package evaluation

import (
	"context"
	"errors"
	"fmt"

	model_core "github.com/buildbarn/bb-playground/pkg/model/core"
	"github.com/buildbarn/bb-playground/pkg/storage/dag"
	"github.com/buildbarn/bb-playground/pkg/storage/object"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
)

func getKeyString(key proto.Message) (string, error) {
	anyKey, err := anypb.New(key)
	if err != nil {
		return "", err
	}
	marshaledKey, err := proto.Marshal(anyKey)
	if err != nil {
		return "", err
	}
	return string(marshaledKey), nil
}

type keyState struct {
	parent *keyState
	key    proto.Message
	next   *keyState
	value  valueState
}

func (ks *keyState) getKeyType() string {
	return string(ks.key.ProtoReflect().Descriptor().FullName().Parent().Name())
}

type valueState interface {
	compute(ctx context.Context, c Computer, e *fullyComputingEnvironment, storeValueChildren ValueChildrenStorer) error
}

type messageValueState struct {
	value model_core.Message[proto.Message]
}

func (vs *messageValueState) compute(ctx context.Context, c Computer, e *fullyComputingEnvironment, storeValueChildren ValueChildrenStorer) error {
	value, err := c.ComputeMessageValue(ctx, e.keyState.key, e)
	if err != nil {
		return err
	}

	// Value got computed. Write any objects referenced by the
	// values to storage.
	references, objectContentsWalkers := value.Patcher.SortAndSetReferences()
	if len(references) > 0 {
		if err := storeValueChildren(references, objectContentsWalkers); err != nil {
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

func (vs *nativeValueState) compute(ctx context.Context, c Computer, e *fullyComputingEnvironment, storeValueChildren ValueChildrenStorer) error {
	value, err := c.ComputeNativeValue(ctx, e.keyState.key, e)
	if err != nil {
		return err
	}
	vs.value = value
	vs.isSet = true
	return nil
}

type fullyComputingKeyPool struct {
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
	pool     *fullyComputingKeyPool
	keyState *keyState
}

func (e *fullyComputingEnvironment) GetMessageValue(key proto.Message) model_core.Message[proto.Message] {
	p := e.pool
	keyStr, err := getKeyString(key)
	if err != nil {
		p.setError(err)
		return model_core.Message[proto.Message]{}
	}
	ks, ok := p.keys[keyStr]
	if !ok {
		ks = &keyState{
			parent: e.keyState,
			key:    key,
			value:  &messageValueState{},
		}
		p.keys[keyStr] = ks
		p.enqueue(ks)
		p.firstMissingDependency = nil
	}
	vs := ks.value.(*messageValueState)
	return vs.value
}

func (e *fullyComputingEnvironment) GetNativeValue(key proto.Message) (any, bool) {
	p := e.pool
	keyStr, err := getKeyString(key)
	if err != nil {
		p.setError(err)
		return nil, false
	}
	ks, ok := p.keys[keyStr]
	if !ok {
		ks = &keyState{
			parent: e.keyState,
			key:    key,
			value:  &nativeValueState{},
		}
		p.keys[keyStr] = ks
		p.enqueue(ks)
		p.firstMissingDependency = nil
	}
	vs := ks.value.(*nativeValueState)
	if !vs.isSet {
		return nil, false
	}
	return vs.value, true
}

type ValueChildrenStorer func(references []object.LocalReference, objectContentsWalkers []dag.ObjectContentsWalker) error

func FullyComputeValue(ctx context.Context, c Computer, requestedKey proto.Message, storeValueChildren ValueChildrenStorer) (model_core.Message[proto.Message], error) {
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
			fmt.Println("Evaluation cycle detected:")
			for ; ks != nil; ks = ks.next {
				anyKey, err := anypb.New(ks.key)
				if err != nil {
					panic(err)
				}
				fmt.Println("  ", protojson.MarshalOptions{}.Format(anyKey))
			}
			return model_core.Message[proto.Message]{}, errors.New("evaluation cycle detected")
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
		fmt.Printf("%-*s  %s", longestKeyType, keyType, protojson.MarshalOptions{}.Format(ks.key))

		e := fullyComputingEnvironment{
			pool:     &p,
			keyState: ks,
		}
		err = ks.value.compute(ctx, c, &e, storeValueChildren)
		e = fullyComputingEnvironment{}
		if err != nil {
			if !errors.Is(err, ErrMissingDependency) {
				fmt.Printf("\n")

				var stack []*keyState
				for ksIter := ks; ksIter != nil; ksIter = ksIter.parent {
					stack = append(stack, ksIter)
				}
				var stackStr []byte
				for i := len(stack) - 1; i >= 0; i-- {
					ksIter := stack[i]
					stackStr = append(stackStr, "  "...)
					keyType := ksIter.getKeyType()
					stackStr = append(stackStr, keyType...)
					for i := len(keyType); i < longestKeyType+2; i++ {
						stackStr = append(stackStr, ' ')
					}
					var err error
					stackStr, err = protojson.MarshalOptions{}.MarshalAppend(stackStr, ksIter.key)
					if err != nil {
						panic(err)
					}
					stackStr = append(stackStr, '\n')
				}
				return model_core.Message[proto.Message]{}, fmt.Errorf("Traceback (most recent key last):\n%s%w", string(stackStr), err)
			}
			// Value could not be computed, because one of
			// its dependencies hasn't been computed yet.
			p.enqueue(ks)
			if p.firstMissingDependency == nil {
				p.firstMissingDependency = ks
			}
			fmt.Printf("\r\x1b[K")
		} else {
			// Successfully computed value.
			p.firstMissingDependency = nil
			fmt.Printf("\n")
		}
	}
	return requestedValueState.value, nil
}
