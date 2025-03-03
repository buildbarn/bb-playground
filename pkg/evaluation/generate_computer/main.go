package main

import (
	"encoding/json"
	"fmt"
	"log"
	"maps"
	"os"
	"slices"
)

type computerDefinition struct {
	Functions    map[string]functionDefinition `json:"functions"`
	GoPackage    string                        `json:"goPackage"`
	ProtoPackage string                        `json:"protoPackage"`
}

type functionDefinition struct {
	KeyContainsReferences bool
	DependsOn             []string `json:"dependsOn"`
	NativeValueType       *nativeValueTypeDefinition
}

func (fd functionDefinition) getKeyType(functionName string, isPatched bool) string {
	if !fd.KeyContainsReferences {
		return fmt.Sprintf("*pb.%s_Key", functionName)
	} else if isPatched {
		return fmt.Sprintf("model_core.PatchedMessage[*pb.%s_Key, dag.ObjectContentsWalker]", functionName)
	} else {
		return fmt.Sprintf("model_core.Message[*pb.%s_Key, object.OutgoingReferences]", functionName)
	}
}

func (fd functionDefinition) keyToPatchedMessage() string {
	if fd.KeyContainsReferences {
		return "model_core.PatchedMessage[proto.Message, dag.ObjectContentsWalker]{Message: key.Message, Patcher: key.Patcher}"
	} else {
		return "model_core.NewSimplePatchedMessage[dag.ObjectContentsWalker, proto.Message](key)"
	}
}

func (fd functionDefinition) typedKeyToArgument(functionName string) string {
	if fd.KeyContainsReferences {
		return fmt.Sprintf("model_core.Message[*pb.%s_Key, object.OutgoingReferences]{Message: typedKey, OutgoingReferences: key.OutgoingReferences}", functionName)
	} else {
		return "typedKey"
	}
}

type nativeValueTypeDefinition struct {
	Imports map[string]string `json:"imports"`
	Type    string            `json:"type"`
}

func main() {
	computerDefinitionData, err := os.ReadFile(os.Args[1])
	if err != nil {
		log.Fatal("Failed to read computer definition: ", err)
	}
	var computerDefinition computerDefinition
	if err := json.Unmarshal(computerDefinitionData, &computerDefinition); err != nil {
		log.Fatal("Failed to unmarshal computer definition: ", err)
	}

	fmt.Printf("package %s\n", computerDefinition.GoPackage)

	imports := map[string]string{}
	for _, functionDefinition := range computerDefinition.Functions {
		if nativeValueType := functionDefinition.NativeValueType; nativeValueType != nil {
			for shortName, importPath := range nativeValueType.Imports {
				imports[shortName] = importPath
			}
		}
	}
	fmt.Printf("import (\n")
	fmt.Printf("\t\"context\"\n")
	fmt.Printf("\t\"github.com/buildbarn/bonanza/pkg/evaluation\"\n")
	fmt.Printf("\t\"github.com/buildbarn/bonanza/pkg/storage/dag\"\n")
	fmt.Printf("\t\"github.com/buildbarn/bonanza/pkg/storage/object\"\n")
	fmt.Printf("\tmodel_core \"github.com/buildbarn/bonanza/pkg/model/core\"\n")
	fmt.Printf("\t\"google.golang.org/protobuf/proto\"\n")
	fmt.Printf("\tpb %#v\n", computerDefinition.ProtoPackage)
	for _, shortName := range slices.Sorted(maps.Keys(imports)) {
		fmt.Printf("\t%s %#v\n", shortName, imports[shortName])
	}
	fmt.Printf(")\n")

	for _, functionName := range slices.Sorted(maps.Keys(computerDefinition.Functions)) {
		functionDefinition := computerDefinition.Functions[functionName]
		if functionDefinition.KeyContainsReferences {
			fmt.Printf(
				"type Patched%sKey = model_core.PatchedMessage[*pb.%s_Key, dag.ObjectContentsWalker]\n",
				functionName,
				functionName,
			)
		}
		if functionDefinition.NativeValueType == nil {
			fmt.Printf(
				"type Patched%sValue = model_core.PatchedMessage[*pb.%s_Value, dag.ObjectContentsWalker]\n",
				functionName,
				functionName,
			)
		}
	}

	fmt.Printf("type Computer interface {\n")
	for _, functionName := range slices.Sorted(maps.Keys(computerDefinition.Functions)) {
		// TODO: This should return a patched message? What about capturing/metadata?
		functionDefinition := computerDefinition.Functions[functionName]
		if nativeValueType := functionDefinition.NativeValueType; nativeValueType == nil {
			fmt.Printf(
				"\tCompute%sValue(context.Context, %s, %sEnvironment) (Patched%sValue, error)\n",
				functionName,
				functionDefinition.getKeyType(functionName, false),
				functionName,
				functionName,
			)
		} else {
			fmt.Printf(
				"\tCompute%sValue(context.Context, %s, %sEnvironment) (%s, error)\n",
				functionName,
				functionDefinition.getKeyType(functionName, false),
				functionName,
				nativeValueType.Type,
			)
		}
	}
	fmt.Printf("}\n")

	for _, functionName := range slices.Sorted(maps.Keys(computerDefinition.Functions)) {
		fmt.Printf("type %sEnvironment interface{\n", functionName)
		functionDefinition := computerDefinition.Functions[functionName]
		for _, dependencyName := range slices.Sorted(slices.Values(functionDefinition.DependsOn)) {
			dependencyDefinition := computerDefinition.Functions[dependencyName]
			if nativeValueType := dependencyDefinition.NativeValueType; nativeValueType == nil {
				fmt.Printf(
					"\tGet%sValue(key %s) model_core.Message[*pb.%s_Value, object.OutgoingReferences]\n",
					dependencyName,
					dependencyDefinition.getKeyType(dependencyName, true),
					dependencyName,
				)
			} else {
				fmt.Printf(
					"\tGet%sValue(key %s) (%s, bool)\n",
					dependencyName,
					dependencyDefinition.getKeyType(dependencyName, true),
					nativeValueType.Type,
				)
			}
		}
		fmt.Printf("}\n")
	}

	fmt.Printf("type typedEnvironment struct{\n")
	fmt.Printf("\tbase evaluation.Environment\n")
	fmt.Printf("}\n")
	for _, functionName := range slices.Sorted(maps.Keys(computerDefinition.Functions)) {
		functionDefinition := computerDefinition.Functions[functionName]
		if nativeValueType := functionDefinition.NativeValueType; nativeValueType == nil {
			fmt.Printf(
				"func (e *typedEnvironment) Get%sValue(key %s) model_core.Message[*pb.%s_Value, object.OutgoingReferences] {\n",
				functionName,
				functionDefinition.getKeyType(functionName, true),
				functionName,
			)
			fmt.Printf("\tm := e.base.GetMessageValue(%s)\n", functionDefinition.keyToPatchedMessage())
			fmt.Printf("\tif !m.IsSet() {\n")
			fmt.Printf("\t\treturn model_core.Message[*pb.%s_Value, object.OutgoingReferences]{}\n", functionName)
			fmt.Printf("\t}\n")
			fmt.Printf("\treturn model_core.Message[*pb.%s_Value, object.OutgoingReferences]{\n", functionName)
			fmt.Printf("\t\tMessage: m.Message.(*pb.%s_Value),\n", functionName)
			fmt.Printf("\t\tOutgoingReferences: m.OutgoingReferences,\n")
			fmt.Printf("\t}\n")
			fmt.Printf("}\n")
		} else {
			fmt.Printf(
				"func (e *typedEnvironment) Get%sValue(key %s) (%s, bool) {\n",
				functionName,
				functionDefinition.getKeyType(functionName, true),
				nativeValueType.Type,
			)
			fmt.Printf("\tv, ok := e.base.GetNativeValue(%s)\n", functionDefinition.keyToPatchedMessage())
			fmt.Printf("\tif !ok {\n")
			fmt.Printf("\t\treturn nil, false\n")
			fmt.Printf("\t}\n")
			fmt.Printf("\t\treturn v.(%s), true\n", nativeValueType.Type)
			fmt.Printf("}\n")
		}
	}

	fmt.Printf("type typedComputer struct{\n")
	fmt.Printf("\tbase Computer\n")
	fmt.Printf("}\n")
	fmt.Printf("func NewTypedComputer(base Computer) evaluation.Computer {\n")
	fmt.Printf("\treturn &typedComputer{base: base}\n")
	fmt.Printf("}\n")

	fmt.Printf("func (c *typedComputer) ComputeMessageValue(ctx context.Context, key model_core.Message[proto.Message, object.OutgoingReferences], e evaluation.Environment) (model_core.PatchedMessage[proto.Message, dag.ObjectContentsWalker], error) {\n")
	fmt.Printf("\ttypedE := typedEnvironment{base: e}\n")
	fmt.Printf("\tswitch typedKey := key.Message.(type) {\n")
	for _, functionName := range slices.Sorted(maps.Keys(computerDefinition.Functions)) {
		functionDefinition := computerDefinition.Functions[functionName]
		if functionDefinition.NativeValueType == nil {
			fmt.Printf("\tcase *pb.%s_Key:\n", functionName)
			fmt.Printf("\t\tm, err := c.base.Compute%sValue(ctx, %s, &typedE)\n", functionName, functionDefinition.typedKeyToArgument(functionName))
			fmt.Printf("\t\treturn model_core.PatchedMessage[proto.Message, dag.ObjectContentsWalker]{\n")
			fmt.Printf("\t\t\tMessage: m.Message,\n")
			fmt.Printf("\t\t\tPatcher: m.Patcher,\n")
			fmt.Printf("\t\t}, err\n")
		}
	}
	fmt.Printf("\tdefault:\n")
	fmt.Printf("\t\tpanic(\"unrecognized key type\")\n")
	fmt.Printf("\t}\n")
	fmt.Printf("}\n")

	fmt.Printf("func (c *typedComputer) ComputeNativeValue(ctx context.Context, key model_core.Message[proto.Message, object.OutgoingReferences], e evaluation.Environment) (any, error) {\n")
	fmt.Printf("\ttypedE := typedEnvironment{base: e}\n")
	fmt.Printf("\tswitch typedKey := key.Message.(type) {\n")
	for _, functionName := range slices.Sorted(maps.Keys(computerDefinition.Functions)) {
		functionDefinition := computerDefinition.Functions[functionName]
		if functionDefinition.NativeValueType != nil {
			fmt.Printf("\tcase *pb.%s_Key:\n", functionName)
			fmt.Printf("\t\treturn c.base.Compute%sValue(ctx, %s, &typedE)\n", functionName, functionDefinition.typedKeyToArgument(functionName))
		}
	}
	fmt.Printf("\tdefault:\n")
	fmt.Printf("\t\tpanic(\"unrecognized key type\")\n")
	fmt.Printf("\t}\n")
	fmt.Printf("}\n")
}
