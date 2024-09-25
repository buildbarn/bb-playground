package evaluation

import (
	"context"

	model_core "github.com/buildbarn/bb-playground/pkg/model/core"

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
	value model_core.Message[proto.Message]
	next  *keyState
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

func (e *fullyComputingEnvironment) GetValue(key proto.Message) model_core.Message[proto.Message] {
	keyStr, err := getKeyString(key)
	if err != nil {
		e.setError(err)
		return model_core.Message[proto.Message]{}
	}
	ks, ok := e.keys[keyStr]
	if !ok {
		ks = &keyState{
			key: key,
		}
		e.keys[keyStr] = ks
		e.enqueue(ks)
	}
	return ks.value
}

func FullyComputeValue(ctx context.Context, c Computer, requestedKey proto.Message) (model_core.Message[proto.Message], error) {
	requestedKeyStr, err := getKeyString(requestedKey)
	if err != nil {
		return model_core.Message[proto.Message]{}, err
	}
	requestedKeyState := &keyState{key: requestedKey}
	e := fullyComputingEnvironment{
		keys: map[string]*keyState{
			requestedKeyStr: requestedKeyState,
		},
		firstPendingKey: requestedKeyState,
		lastPendingKey:  &requestedKeyState.next,
	}
	for !requestedKeyState.value.IsSet() {
		ks := e.firstPendingKey
		e.firstPendingKey = ks.next
		ks.next = nil
		if e.firstPendingKey == nil {
			e.lastPendingKey = &e.firstPendingKey
		}
		value, err := c.ComputeValue(ctx, ks.key, &e)
		if err != nil {
			return model_core.Message[proto.Message]{}, err
		}
		if value.IsSet() {
			// Value got computed.
			// TODO: Write any objects referenced by the
			// message to storage!
			references, _ := value.Patcher.SortAndSetReferences()
			ks.value = model_core.Message[proto.Message]{
				Message:            value.Message,
				OutgoingReferences: references,
			}
		} else {
			// Value could not be computed, because one of
			// its dependencies hasn't been computed yet.
			e.enqueue(ks)
		}
	}
	return requestedKeyState.value, nil
}
