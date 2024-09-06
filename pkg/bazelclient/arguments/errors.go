package arguments

import (
	"fmt"
	"strings"
)

type CommandNotRecognizedError struct {
	Command string
}

func (e CommandNotRecognizedError) Error() string {
	return fmt.Sprintf("command %#v not recognized", e.Command)
}

type CommandDoesNotTakeArgumentsError struct {
	Command string
}

func (e CommandDoesNotTakeArgumentsError) Error() string {
	return fmt.Sprintf("command %#v does not take arguments", e.Command)
}

type FlagNotRecognizedError struct {
	Flag string
}

func (e FlagNotRecognizedError) Error() string {
	return fmt.Sprintf("flag %s not recognized", e.Flag)
}

type FlagNotApplicableError struct {
	Flag string
}

func (e FlagNotApplicableError) Error() string {
	return fmt.Sprintf("flag %s does not apply to this command", e.Flag)
}

type FlagMissingValueError struct {
	Flag string
}

func (e FlagMissingValueError) Error() string {
	return fmt.Sprintf("flag %s expects a value", e.Flag)
}

type FlagUnexpectedValueError struct {
	Flag string
}

func (e FlagUnexpectedValueError) Error() string {
	return fmt.Sprintf("flag %s does not take a value", e.Flag)
}

type FlagInvalidEnumValueError struct {
	Flag           string
	Value          string
	ExpectedValues []string
}

func (e FlagInvalidEnumValueError) Error() string {
	var sb strings.Builder
	sb.WriteString("flag ")
	sb.WriteString(e.Flag)
	sb.WriteString(" only accepts ")
	for i, expectedValue := range e.ExpectedValues {
		fmt.Fprintf(&sb, "%#v", expectedValue)
		switch i {
		default:
			sb.WriteString(", ")
		case len(e.ExpectedValues) - 2:
			sb.WriteString(" or ")
		case len(e.ExpectedValues) - 1:
		}
	}
	sb.WriteString(", not ")
	fmt.Fprintf(&sb, "%#v", e.Value)
	return sb.String()
}

type ConfigValueNotRecognizedError struct {
	Config string
}

func (e ConfigValueNotRecognizedError) Error() string {
	return fmt.Sprintf("config value %#v is not defined in any configuration file", e.Config)
}

type ConfigExpansionContainsCycleError struct {
	Directive string
}

func (e ConfigExpansionContainsCycleError) Error() string {
	return fmt.Sprintf("config expansion for configuration directive %#v contains a cycle", e.Directive)
}
