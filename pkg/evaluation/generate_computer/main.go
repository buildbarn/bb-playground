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
	DependsOn []string `json:"dependsOn"`
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
	fmt.Printf("import (\n")
	fmt.Printf("\t\"context\"\n")
	fmt.Printf("\t\"github.com/buildbarn/bb-playground/pkg/evaluation\"\n")
	fmt.Printf("\t\"github.com/buildbarn/bb-playground/pkg/storage/dag\"\n")
	fmt.Printf("\tmodel_core \"github.com/buildbarn/bb-playground/pkg/model/core\"\n")
	fmt.Printf("\t\"google.golang.org/protobuf/proto\"\n")
	fmt.Printf("\tpb %#v\n", computerDefinition.ProtoPackage)
	fmt.Printf(")\n")
	fmt.Printf("type Computer interface {\n")
	for _, functionName := range slices.Sorted(maps.Keys(computerDefinition.Functions)) {
		// TODO: This should return a patched message? What about capturing/metadata?
		fmt.Printf(
			"\tCompute%sValue(context.Context, *pb.%s_Key, %sEnvironment) (model_core.PatchedMessage[*pb.%s_Value, dag.ObjectContentsWalker], error)\n",
			functionName,
			functionName,
			functionName,
			functionName,
		)
	}
	fmt.Printf("}\n")

	for _, functionName := range slices.Sorted(maps.Keys(computerDefinition.Functions)) {
		fmt.Printf("type %sEnvironment interface{\n", functionName)
		functionDefinition := computerDefinition.Functions[functionName]
		for _, dependencyName := range slices.Sorted(slices.Values(functionDefinition.DependsOn)) {
			fmt.Printf(
				"\tGet%sValue(key *pb.%s_Key) model_core.Message[*pb.%s_Value]\n",
				dependencyName,
				dependencyName,
				dependencyName,
			)
		}
		fmt.Printf("}\n")
	}

	fmt.Printf("type typedEnvironment struct{\n")
	fmt.Printf("\tbase evaluation.Environment\n")
	fmt.Printf("}\n")
	for _, functionName := range slices.Sorted(maps.Keys(computerDefinition.Functions)) {
		fmt.Printf(
			"func (e *typedEnvironment) Get%sValue(key *pb.%s_Key) model_core.Message[*pb.%s_Value] {\n",
			functionName,
			functionName,
			functionName,
		)
		fmt.Printf("\tm := e.base.GetValue(key)\n")
		fmt.Printf("\tif !m.IsSet() {\n")
		fmt.Printf("\t\treturn model_core.Message[*pb.%s_Value]{}\n", functionName)
		fmt.Printf("\t}\n")
		fmt.Printf("\treturn model_core.Message[*pb.%s_Value] {\n", functionName)
		fmt.Printf("\t\tMessage: m.Message.(*pb.%s_Value),\n", functionName)
		fmt.Printf("\t\tOutgoingReferences: m.OutgoingReferences,\n")
		fmt.Printf("\t}\n")
		fmt.Printf("}\n")
	}

	fmt.Printf("type typedComputer struct{\n")
	fmt.Printf("\tbase Computer\n")
	fmt.Printf("}\n")
	fmt.Printf("func NewTypedComputer(base Computer) evaluation.Computer {\n")
	fmt.Printf("\treturn &typedComputer{base: base}\n")
	fmt.Printf("}\n")
	fmt.Printf("func (c *typedComputer) ComputeValue(ctx context.Context, key proto.Message, e evaluation.Environment) (model_core.PatchedMessage[proto.Message, dag.ObjectContentsWalker], error) {\n")
	fmt.Printf("\ttypedE := typedEnvironment{base: e}\n")
	fmt.Printf("\tswitch typedKey := key.(type) {\n")
	for _, functionName := range slices.Sorted(maps.Keys(computerDefinition.Functions)) {
		fmt.Printf("\tcase *pb.%s_Key:\n", functionName)
		fmt.Printf("\t\tm, err := c.base.Compute%sValue(ctx, typedKey, &typedE)\n", functionName)
		fmt.Printf("\t\treturn model_core.PatchedMessage[proto.Message, dag.ObjectContentsWalker]{\n")
		fmt.Printf("\t\t\tMessage: m.Message,\n")
		fmt.Printf("\t\t\tPatcher: m.Patcher,\n")
		fmt.Printf("\t\t}, err\n")
	}
	fmt.Printf("\tdefault:\n")
	fmt.Printf("\t\tpanic(\"unrecognized key type\")\n")
	fmt.Printf("\t}\n")
	fmt.Printf("}\n")
}
