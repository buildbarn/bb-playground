package main

import (
	"fmt"
	"maps"
	"slices"
	"sort"
	"strings"
)

type flagSet struct {
	flags []flag
}

type flag struct {
	longName    string
	shortName   string
	description string
	flagType    flagType
}

type command struct {
	flagSets       []string
	takesArguments bool
}

func toSymbolName(s string, public bool) string {
	fields := strings.FieldsFunc(s, func(c rune) bool {
		return (c < '0' || c > '9') && (c < 'a' || c > 'z')
	})
	start := 1
	if public {
		start = 0
	}
	for i := start; i < len(fields); i++ {
		fields[i] = strings.Title(fields[i])
	}
	return strings.Join(fields, "")
}

func main() {
	commandFlagSets := map[string]struct{}{}
	for _, command := range commands {
		for _, flagSetName := range command.flagSets {
			commandFlagSets[flagSetName] = struct{}{}
		}
	}

	fmt.Println("package arguments")
	fmt.Println("import (")
	fmt.Println("\"strings\"")
	fmt.Println(")")

	for _, enumTypeName := range slices.Sorted(maps.Keys(enumTypes)) {
		fmt.Printf("type %s int\n", enumTypeName)
		enumValueNames := enumTypes[enumTypeName]
		sort.Strings(enumValueNames)
		for i, enumValueName := range enumValueNames {
			fmt.Printf("const %s_%s = %d\n", enumTypeName, toSymbolName(enumValueName, true), i+1)
		}
		fmt.Printf("var expectedValues_%s = %#v\n", enumTypeName, enumValueNames)
		fmt.Printf("func (v *%s) set(flagName string, valueString string) error {\n", enumTypeName)
		fmt.Printf("  var newV %s\n", enumTypeName)
		fmt.Printf("  switch valueString {\n")
		for _, enumValueName := range enumValueNames {
			fmt.Printf("  case %#v:\n", enumValueName)
			fmt.Printf("    newV = %s_%s\n", enumTypeName, toSymbolName(enumValueName, true))
		}
		fmt.Printf("  default:\n")
		fmt.Printf("    return FlagInvalidEnumValueError{\n")
		fmt.Printf("      Flag: flagName,\n")
		fmt.Printf("      Value: valueString,\n")
		fmt.Printf("      ExpectedValues: expectedValues_%s,\n", enumTypeName)
		fmt.Printf("    }\n")
		fmt.Printf("  }\n")
		fmt.Printf("  *v = newV\n")
		fmt.Printf("  return nil\n")
		fmt.Printf("}\n")
	}

	for _, flagSetName := range slices.Sorted(maps.Keys(flagSets)) {
		flagSet := flagSets[flagSetName]
		flagSetSymbolName := toSymbolName(flagSetName, true)
		fmt.Printf("type %sFlags struct{\n", flagSetSymbolName)
		for _, flag := range flagSet.flags {
			flag.flagType.emitStructField(flag.longName)
		}
		fmt.Printf("}\n")

		fmt.Printf("func (f *%sFlags) Reset() {\n", flagSetSymbolName)
		for _, flag := range flagSet.flags {
			flag.flagType.emitDefaultInitializer(flag.longName)
		}
		fmt.Printf("}\n")
	}

	fmt.Printf("type assignableCommand interface {\n")
	fmt.Printf("  Command\n")
	for _, flagSetName := range slices.Sorted(maps.Keys(commandFlagSets)) {
		flagSetSymbolName := toSymbolName(flagSetName, true)
		fmt.Printf("  get%sFlags() *%sFlags\n", flagSetSymbolName, flagSetSymbolName)
	}
	fmt.Printf("  appendArgument(argument string) error\n")
	fmt.Printf("}\n")

	for _, commandName := range slices.Sorted(maps.Keys(commands)) {
		command := commands[commandName]
		commandSymbolName := toSymbolName(commandName, true)
		fmt.Printf("type %sCommand struct {\n", commandSymbolName)
		for _, flagSetName := range slices.Sorted(slices.Values(command.flagSets)) {
			flagSetSymbolName := toSymbolName(flagSetName, true)
			fmt.Printf("%sFlags %sFlags\n", flagSetSymbolName, flagSetSymbolName)
		}
		if command.takesArguments {
			fmt.Printf("Arguments []string\n")
		}
		fmt.Printf("}\n")

		fmt.Printf("func (c *%sCommand) Reset() {\n", commandSymbolName)
		for _, flagSetName := range slices.Sorted(slices.Values(command.flagSets)) {
			fmt.Printf("c.%sFlags.Reset()\n", toSymbolName(flagSetName, true))
		}
		fmt.Printf("}\n")

		applicableFlagSets := map[string]struct{}{}
		for _, flagSetName := range command.flagSets {
			applicableFlagSets[flagSetName] = struct{}{}
		}
		for _, flagSetName := range slices.Sorted(maps.Keys(commandFlagSets)) {
			flagSetSymbolName := toSymbolName(flagSetName, true)
			fmt.Printf("func (c *%sCommand) get%sFlags() *%sFlags {\n", commandSymbolName, flagSetSymbolName, flagSetSymbolName)
			if _, ok := applicableFlagSets[flagSetName]; ok {
				fmt.Printf("  return &c.%sFlags", flagSetSymbolName)
			} else {
				fmt.Printf("  return nil")
			}
			fmt.Printf("}\n")
		}

		fmt.Printf("func (c *%sCommand) appendArgument(argument string) error {\n", commandSymbolName)
		if command.takesArguments {
			fmt.Printf("  c.Arguments = append(c.Arguments, argument)\n")
			fmt.Printf("  return nil\n")
		} else {
			fmt.Printf("  return CommandDoesNotTakeArgumentsError{Command: %#v}\n", commandName)
		}
		fmt.Printf("}\n")
	}

	fmt.Printf("func newCommandByName(name string) (assignableCommand, bool) {\n")
	fmt.Printf("switch name {")
	for _, commandName := range slices.Sorted(maps.Keys(commands)) {
		fmt.Printf("case %#v:\n", commandName)
		fmt.Printf("return &%sCommand{}, true\n", toSymbolName(commandName, true))
	}
	fmt.Printf("default:\n")
	fmt.Printf("return nil, false\n")
	fmt.Printf("}\n")
	fmt.Printf("}\n")

	fmt.Printf("func parseCommandArguments(cmd assignableCommand, args []string) error {\n")
	fmt.Printf("  stack := [][]string{args}\n")
	fmt.Printf("  allowFlags := true\n")
	fmt.Printf("  for len(stack) > 0 {\n")
	fmt.Printf("    currentArgs := &stack[len(stack)-1]\n")
	fmt.Printf("    if len(*currentArgs) == 0 {\n")
	fmt.Printf("      stack = stack[:len(stack)-1]\n")
	fmt.Printf("    } else {\n")
	fmt.Printf("      firstArg := (*currentArgs)[0]\n")
	fmt.Printf("      (*currentArgs) = (*currentArgs)[1:]\n")
	fmt.Printf("      if allowFlags && len(firstArg) > 1 && firstArg[0] == '-' && firstArg[1] == '-' {\n")
	fmt.Printf("        longOptionName := firstArg\n")
	fmt.Printf("        var optionValue string\n")
	fmt.Printf("        assignmentIndex := strings.IndexByte(longOptionName, '=')\n")
	fmt.Printf("        if assignmentIndex >= 0 {\n")
	fmt.Printf("          optionValue = longOptionName[assignmentIndex+1:]\n")
	fmt.Printf("          longOptionName = longOptionName[:assignmentIndex]\n")
	fmt.Printf("        }\n")
	fmt.Printf("        switch longOptionName {\n")
	for _, flagSetName := range slices.Sorted(maps.Keys(commandFlagSets)) {
		flagSet := flagSets[flagSetName]
		for _, flag := range flagSet.flags {
			flag.flagType.emitLongNameParser(flagSetName, flag.longName)
		}
	}
	fmt.Printf("        case \"--\":\n")
	fmt.Printf("          allowFlags = false\n")
	fmt.Printf("        default:\n")
	fmt.Printf("          return FlagNotRecognizedError{Flag: longOptionName}\n")
	fmt.Printf("        }\n")
	fmt.Printf("      } else if allowFlags && len(firstArg) > 0 && firstArg[0] == '-' {\n")
	fmt.Printf("        shortOptionName := firstArg\n")
	fmt.Printf("        switch shortOptionName {\n")
	for _, flagSetName := range slices.Sorted(maps.Keys(commandFlagSets)) {
		flagSet := flagSets[flagSetName]
		for _, flag := range flagSet.flags {
			if flag.shortName != "" {
				flag.flagType.emitShortNameParser(flagSetName, flag.longName, flag.shortName)
			}
		}
	}
	fmt.Printf("        default:\n")
	fmt.Printf("          return FlagNotRecognizedError{Flag: shortOptionName}\n")
	fmt.Printf("        }\n")
	fmt.Printf("      } else {\n")
	fmt.Printf("        if err := cmd.appendArgument(firstArg); err != nil {\n")
	fmt.Printf("          return err\n")
	fmt.Printf("        }\n")
	fmt.Printf("      }\n")
	fmt.Printf("    }\n")
	fmt.Printf("  }\n")
	fmt.Printf("  return nil\n")
	fmt.Printf("}\n")

	fmt.Printf("func ParseStartupFlags(args []string) (*StartupFlags, int, error) {\n")
	fmt.Printf("  var flags StartupFlags\n")
	fmt.Printf("  flags.Reset()\n")
	fmt.Printf("  argsIndex := 0\n")
	fmt.Printf("  for argsIndex < len(args) {\n")
	fmt.Printf("    firstArg := args[argsIndex]\n")
	fmt.Printf("    if len(firstArg) < 2 || firstArg[0] != '-' || firstArg[1] != '-' {\n")
	fmt.Printf("      break\n")
	fmt.Printf("    }\n")
	fmt.Printf("    argsIndex++\n")
	fmt.Printf("    longOptionName := firstArg\n")
	fmt.Printf("    var optionValue string\n")
	fmt.Printf("    assignmentIndex := strings.IndexByte(longOptionName, '=')\n")
	fmt.Printf("    if assignmentIndex >= 0 {\n")
	fmt.Printf("      optionValue = longOptionName[assignmentIndex+1:]\n")
	fmt.Printf("      longOptionName = longOptionName[:assignmentIndex]\n")
	fmt.Printf("    }\n")
	fmt.Printf("    switch longOptionName {\n")
	for _, flag := range flagSets["startup"].flags {
		flag.flagType.emitStartupParser(flag.longName)
	}
	fmt.Printf("    default:\n")
	fmt.Printf("      return nil, 0, FlagNotRecognizedError{Flag: longOptionName}\n")
	fmt.Printf("    }\n")
	fmt.Printf("  }\n")
	fmt.Printf("  return &flags, argsIndex, nil\n")
	fmt.Printf("}\n")
}
