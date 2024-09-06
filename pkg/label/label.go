package label

type Label struct {
	value string
}

func NewLabel(value string) (Label, error) {
	// TODO: Validate and normalize!
	return Label{
		value: value,
	}, nil
}

func MustNewLabel(value string) Label {
	l, err := NewLabel(value)
	if err != nil {
		panic(err)
	}
	return l
}
