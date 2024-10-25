package evaluation

import (
	"context"
	"errors"
	"fmt"
	"strings"

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
	key   proto.Message
	next  *keyState
	value valueState
}

type valueState interface {
	compute(ctx context.Context, c Computer, key proto.Message, e Environment, storeValueChildren ValueChildrenStorer) error
}

type messageValueState struct {
	value model_core.Message[proto.Message]
}

func (vs *messageValueState) compute(ctx context.Context, c Computer, key proto.Message, e Environment, storeValueChildren ValueChildrenStorer) error {
	value, err := c.ComputeMessageValue(ctx, key, e)
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
	err   error
}

func (vs *nativeValueState) compute(ctx context.Context, c Computer, key proto.Message, e Environment, storeValueChildren ValueChildrenStorer) error {
	value, err := c.ComputeNativeValue(ctx, key, e)
	if errors.Is(err, ErrMissingDependency) {
		return err
	}
	vs.value, vs.err = value, err
	return nil
}

type fullyComputingEnvironment struct {
	keys            map[string]*keyState
	firstPendingKey *keyState
	lastPendingKey  **keyState
	err             error
}

func (e *fullyComputingEnvironment) setError(err error) {
	if e.err == nil {
		e.err = err
	}
}

func (e *fullyComputingEnvironment) enqueue(ks *keyState) {
	*e.lastPendingKey = ks
	e.lastPendingKey = &ks.next
}

func (e *fullyComputingEnvironment) GetMessageValue(key proto.Message) model_core.Message[proto.Message] {
	keyStr, err := getKeyString(key)
	if err != nil {
		e.setError(err)
		return model_core.Message[proto.Message]{}
	}
	ks, ok := e.keys[keyStr]
	if !ok {
		ks = &keyState{
			key:   key,
			value: &messageValueState{},
		}
		e.keys[keyStr] = ks
		e.enqueue(ks)
	}
	vs := ks.value.(*messageValueState)
	return vs.value
}

func (e *fullyComputingEnvironment) GetNativeValue(key proto.Message) (any, error) {
	keyStr, err := getKeyString(key)
	if err != nil {
		return nil, err
	}
	ks, ok := e.keys[keyStr]
	if !ok {
		ks = &keyState{
			key: key,
			value: &nativeValueState{
				err: ErrMissingDependency,
			},
		}
		e.keys[keyStr] = ks
		e.enqueue(ks)
	}
	vs := ks.value.(*nativeValueState)
	return vs.value, vs.err
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
	e := fullyComputingEnvironment{
		keys: map[string]*keyState{
			requestedKeyStr: requestedKeyState,
		},
		firstPendingKey: requestedKeyState,
		lastPendingKey:  &requestedKeyState.next,
	}
	for !requestedValueState.value.IsSet() {
		ks := e.firstPendingKey
		e.firstPendingKey = ks.next
		ks.next = nil
		if e.firstPendingKey == nil {
			e.lastPendingKey = &e.firstPendingKey
		}

		// TODO: Add interface for exposing information on
		// what's going on.
		anyKey, err := anypb.New(ks.key)
		if err != nil {
			panic(err)
		}
		fmt.Printf("%s", strings.Replace(protojson.MarshalOptions{}.Format(anyKey), "type.googleapis.com/buildbarn.playground.model.analysis.", "", -1))

		if err := ks.value.compute(ctx, c, ks.key, &e, storeValueChildren); err != nil {
			if !errors.Is(err, ErrMissingDependency) {
				fmt.Printf("\n")
				return model_core.Message[proto.Message]{}, err
			}
			// Value could not be computed, because one of
			// its dependencies hasn't been computed yet.
			e.enqueue(ks)
			fmt.Printf("\r\x1b[K")
		} else {
			// Successfully computed value.
			fmt.Printf("\n")
		}
	}
	return requestedValueState.value, nil
}
