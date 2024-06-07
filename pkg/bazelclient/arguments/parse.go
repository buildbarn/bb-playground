package arguments

type Command interface {
	Reset()
}

func Parse(args []string) (Command, error) {
	_, argsParsed, err := ParseStartupFlags(args)
	if err != nil {
		return nil, err
	}
	args = args[argsParsed:]

	var cmd assignableCommand
	if len(args) == 0 {
		cmd = &HelpCommand{}
	} else {
		var ok bool
		cmd, ok = newCommandByName(args[0])
		if !ok {
			return nil, CommandNotRecognizedError{
				Command: args[0],
			}
		}
		args = args[1:]
	}
	cmd.Reset()

	return cmd, parseCommandArguments(cmd, args)
}

var boolExpectedValues = []string{
	"true",
	"false",
	"yes",
	"no",
	"1",
	"0",
}

func parseBool(hasValue bool, value string, out *bool, flagName string) error {
	if hasValue {
		switch value {
		case "0", "false", "no":
			*out = false
		case "1", "true", "yes":
			*out = true
		default:
			return FlagInvalidEnumValueError{
				Flag:           flagName,
				Value:          value,
				ExpectedValues: boolExpectedValues,
			}
		}
	} else {
		*out = true
	}
	return nil
}
