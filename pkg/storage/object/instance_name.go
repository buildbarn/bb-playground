package object

type InstanceName struct {
	value string
}

func NewInstanceName(value string) InstanceName {
	return InstanceName{
		value: value,
	}
}

func (in InstanceName) WithLocalReference(localReference LocalReference) GlobalReference {
	return GlobalReference{
		InstanceName:   in,
		LocalReference: localReference,
	}
}
