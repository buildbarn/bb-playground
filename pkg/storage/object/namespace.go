package object

import (
	"github.com/buildbarn/bb-playground/pkg/proto/storage/object"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type Namespace struct {
	InstanceName
	ReferenceFormat
}

func NewNamespace(namespaceMessage *object.Namespace) (Namespace, error) {
	if namespaceMessage == nil {
		return Namespace{}, status.Error(codes.InvalidArgument, "No message provided")
	}
	referenceFormat, err := NewReferenceFormat(namespaceMessage.ReferenceFormat)
	if err != nil {
		return Namespace{}, err
	}
	return Namespace{
		InstanceName:    NewInstanceName(namespaceMessage.InstanceName),
		ReferenceFormat: referenceFormat,
	}, nil
}

func MustNewNamespace(namespaceMessage *object.Namespace) Namespace {
	ns, err := NewNamespace(namespaceMessage)
	if err != nil {
		panic(err)
	}
	return ns
}

func (ns Namespace) NewGlobalReference(rawReference []byte) (GlobalReference, error) {
	localReference, err := ns.ReferenceFormat.NewLocalReference(rawReference)
	if err != nil {
		return GlobalReference{}, err
	}
	return GlobalReference{
		InstanceName:   ns.InstanceName,
		LocalReference: localReference,
	}, nil
}

func (ns Namespace) ToProto() *object.Namespace {
	return &object.Namespace{
		InstanceName:    ns.InstanceName.value,
		ReferenceFormat: ns.ReferenceFormat.ToProto(),
	}
}
