package main

import (
	"fmt"
)

type flagType interface {
	emitStructField(longName string)
	emitDefaultInitializer(longName string)
	emitLongNameParser(flagSetName, longName string)
	emitShortNameParser(flagSetName, longName, shortName string)
	emitStartupParser(longName string)
}

type boolFlagType struct {
	defaultValue bool
}

func (ft boolFlagType) emitStructField(longName string) {
	fmt.Printf("%s bool\n", toSymbolName(longName, true))
}

func (ft boolFlagType) emitDefaultInitializer(longName string) {
	fmt.Printf("f.%s = %#v\n", toSymbolName(longName, true), ft.defaultValue)
}

func (ft boolFlagType) emitLongNameParser(flagSetName, longName string) {
	flagSetSymbolName := toSymbolName(flagSetName, true)
	longSymbolName := toSymbolName(longName, true)

	fmt.Printf("case %#v:\n", "--"+longName)
	fmt.Printf("  flags := cmd.get%sFlags()\n", flagSetSymbolName)
	fmt.Printf("  if flags == nil {\n")
	fmt.Printf("    return FlagNotApplicableError{Flag: longOptionName}\n")
	fmt.Printf("  }\n")
	fmt.Printf("  if err := parseBool(assignmentIndex >= 0, optionValue, &flags.%s, longOptionName); err != nil {\n", longSymbolName)
	fmt.Printf("    return err\n")
	fmt.Printf("  }\n")

	fmt.Printf("case %#v:\n", "--no"+longName)
	fmt.Printf("  flags := cmd.get%sFlags()\n", flagSetSymbolName)
	fmt.Printf("  if flags == nil {\n")
	fmt.Printf("    return FlagNotApplicableError{Flag: longOptionName}\n")
	fmt.Printf("  }\n")
	fmt.Printf("  if assignmentIndex >= 0 {\n")
	fmt.Printf("    return FlagUnexpectedValueError{Flag: longOptionName}\n")
	fmt.Printf("  }\n")
	fmt.Printf("  flags.%s = false\n", longSymbolName)
}

func (ft boolFlagType) emitShortNameParser(flagSetName, longName, shortName string) {
	flagSetSymbolName := toSymbolName(flagSetName, true)
	longSymbolName := toSymbolName(longName, true)

	fmt.Printf("case %#v:\n", "-"+shortName)
	fmt.Printf("  flags := cmd.get%sFlags()\n", flagSetSymbolName)
	fmt.Printf("  if flags == nil {\n")
	fmt.Printf("    return FlagNotApplicableError{Flag: shortOptionName}\n")
	fmt.Printf("  }\n")
	fmt.Printf("  flags.%s = true\n", longSymbolName)

	fmt.Printf("case %#v:\n", "-"+shortName+"-")
	fmt.Printf("  flags := cmd.get%sFlags()\n", flagSetSymbolName)
	fmt.Printf("  if flags == nil {\n")
	fmt.Printf("    return FlagNotApplicableError{Flag: shortOptionName}\n")
	fmt.Printf("  }\n")
	fmt.Printf("  flags.%s = false\n", longSymbolName)
}

func (ft boolFlagType) emitStartupParser(longName string) {
	longSymbolName := toSymbolName(longName, true)

	fmt.Printf("case %#v:\n", "--"+longName)
	fmt.Printf("  if err := parseBool(assignmentIndex >= 0, optionValue, &flags.%s, longOptionName); err != nil {\n", longSymbolName)
	fmt.Printf("    return nil, 0, err\n")
	fmt.Printf("  }\n")

	fmt.Printf("case %#v:\n", "--no"+longName)
	fmt.Printf("  if assignmentIndex >= 0 {\n")
	fmt.Printf("    return nil, 0, FlagUnexpectedValueError{Flag: longOptionName}\n")
	fmt.Printf("  }\n")
	fmt.Printf("  flags.%s = false\n", longSymbolName)
}

type enumFlagType struct {
	enumType     string
	defaultValue string
}

func (ft enumFlagType) emitStructField(longName string) {
	fmt.Printf("%s %s\n", toSymbolName(longName, true), ft.enumType)
}

func (ft enumFlagType) emitDefaultInitializer(longName string) {
	fmt.Printf("f.%s = %s_%s\n", toSymbolName(longName, true), ft.enumType, toSymbolName(ft.defaultValue, true))
}

func (ft enumFlagType) emitLongNameParser(flagSetName, longName string) {
	fmt.Printf("case %#v:\n", "--"+longName)
	fmt.Printf("  flags := cmd.get%sFlags()\n", toSymbolName(flagSetName, true))
	fmt.Printf("  if flags == nil {\n")
	fmt.Printf("    return FlagNotApplicableError{Flag: longOptionName}\n")
	fmt.Printf("  }\n")
	fmt.Printf("  if assignmentIndex < 0 {\n")
	fmt.Printf("    if len(*currentArgs) == 0 {\n")
	fmt.Printf("      return FlagMissingValueError{Flag: longOptionName}\n")
	fmt.Printf("    }\n")
	fmt.Printf("    optionValue = (*currentArgs)[0]\n")
	fmt.Printf("    (*currentArgs) = (*currentArgs)[1:]\n")
	fmt.Printf("  }\n")
	fmt.Printf("  if err := flags.%s.set(longOptionName, optionValue); err != nil {\n", toSymbolName(longName, true))
	fmt.Printf("    return err\n")
	fmt.Printf("  }\n")
}

func (ft enumFlagType) emitShortNameParser(flagSetName, longName, shortName string) {
	panic("TODO")
}

func (ft enumFlagType) emitStartupParser(longName string) {
	panic("TODO")
}

type expansionFlagType struct {
	expandsTo []string
}

func (ft expansionFlagType) emitStructField(longName string) {}

func (ft expansionFlagType) emitDefaultInitializer(longName string) {}

func (ft expansionFlagType) emitLongNameParser(flagSetName, longName string) {
	fmt.Printf("case %#v:\n", "--"+longName)
	fmt.Printf("  if cmd.get%sFlags() == nil {\n", toSymbolName(flagSetName, true))
	fmt.Printf("    return FlagNotApplicableError{Flag: longOptionName}\n")
	fmt.Printf("  }\n")
	fmt.Printf("  if assignmentIndex >= 0 {\n")
	fmt.Printf("    return FlagUnexpectedValueError{Flag: longOptionName}\n")
	fmt.Printf("  }\n")
	fmt.Printf("  stack = append(stack, %#v)\n", ft.expandsTo)
}

func (ft expansionFlagType) emitShortNameParser(flagSetName, longName, shortName string) {
	fmt.Printf("case %#v:\n", "-"+shortName)
	fmt.Printf("  stack = append(stack, %#v)\n", ft.expandsTo)
}

func (ft expansionFlagType) emitStartupParser(longName string) {
	panic("TODO")
}

type stringListFlagType struct{}

func (ft stringListFlagType) emitStructField(longName string) {
	fmt.Printf("%s []string\n", toSymbolName(longName, true))
}

func (ft stringListFlagType) emitDefaultInitializer(longName string) {
	fmt.Printf("f.%s = nil\n", toSymbolName(longName, true))
}

func (ft stringListFlagType) emitLongNameParser(flagSetName, longName string) {
	panic("TODO")
}

func (ft stringListFlagType) emitShortNameParser(flagSetName, longName, shortName string) {
	panic("TODO")
}

func (ft stringListFlagType) emitStartupParser(longName string) {
	longSymbolName := toSymbolName(longName, true)

	fmt.Printf("case %#v:\n", "--"+longName)
	fmt.Printf("  if assignmentIndex < 0 {\n")
	fmt.Printf("    if argsIndex == len(args) {\n")
	fmt.Printf("      return nil, 0, FlagMissingValueError{Flag: longOptionName}\n")
	fmt.Printf("    }\n")
	fmt.Printf("    optionValue = args[argsIndex]\n")
	fmt.Printf("    argsIndex++\n")
	fmt.Printf("  }\n")
	fmt.Printf("  flags.%s = append(flags.%s, optionValue)\n", longSymbolName, longSymbolName)
}
