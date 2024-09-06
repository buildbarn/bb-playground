package arguments

type Command interface {
	Reset()
}

type commandAncestor struct {
	name      string
	mustApply bool
}

func ParseCommandAndArguments(configurationDirectives ConfigurationDirectives, args []string) (Command, error) {
	var cmd assignableCommand
	var ancestors []commandAncestor
	if len(args) == 0 {
		cmd = &HelpCommand{}
		ancestors = helpAncestors
	} else {
		var ok bool
		cmd, ancestors, ok = newCommandByName(args[0])
		if !ok {
			return nil, CommandNotRecognizedError{
				Command: args[0],
			}
		}
		args = args[1:]
	}
	cmd.Reset()

	return cmd, parseArguments(cmd, ancestors, configurationDirectives, args)
}

var boolExpectedValues = []string{
	"true",
	"false",
	"yes",
	"no",
	"1",
	"0",
}

type stackEntry struct {
	remainingArgs []string
	mustApply     bool
	allowFlags    bool
	directiveName string
}

func parseBool(hasValue bool, value string, out *bool, flagName string) error {
	v := true
	if hasValue {
		switch value {
		case "0", "false", "no":
			v = false
		case "1", "true", "yes":
			v = true
		default:
			return FlagInvalidEnumValueError{
				Flag:           flagName,
				Value:          value,
				ExpectedValues: boolExpectedValues,
			}
		}
	}
	if out != nil {
		*out = v
	}
	return nil
}
