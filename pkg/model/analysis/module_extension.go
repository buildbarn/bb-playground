package analysis

import (
	"context"
	"errors"
	"fmt"

	"github.com/buildbarn/bb-playground/pkg/evaluation"
	"github.com/buildbarn/bb-playground/pkg/label"
	model_analysis_pb "github.com/buildbarn/bb-playground/pkg/proto/model/analysis"
	model_starlark_pb "github.com/buildbarn/bb-playground/pkg/proto/model/starlark"
)

type ModuleExtension struct {
	CanonicalIdentifier label.CanonicalStarlarkIdentifier
}

func (c *baseComputer) ComputeModuleExtensionValue(ctx context.Context, key *model_analysis_pb.ModuleExtension_Key, e ModuleExtensionEnvironment) (*ModuleExtension, error) {
	moduleExtensionGlobalValue := e.GetCompiledBzlFileGlobalValue(&model_analysis_pb.CompiledBzlFileGlobal_Key{
		Identifier: key.Identifier,
	})
	if !moduleExtensionGlobalValue.IsSet() {
		return nil, evaluation.ErrMissingDependency
	}
	// var moduleExtensionDefinition *model_starlark_pb.ModuleExtension_Definition
	switch moduleExtensionResult := moduleExtensionGlobalValue.Message.Result.(type) {
	case *model_analysis_pb.CompiledBzlFileGlobal_Value_Success:
		v, ok := moduleExtensionResult.Success.Kind.(*model_starlark_pb.Value_ModuleExtension)
		if !ok {
			return nil, errors.New("not a module extension")
		}
		switch moduleExtensionKind := v.ModuleExtension.Kind.(type) {
		case *model_starlark_pb.ModuleExtension_Definition_:
			// moduleExtensionDefinition = moduleExtensionKind.Definition
		case *model_starlark_pb.ModuleExtension_Reference:
			return e.GetModuleExtensionValue(&model_analysis_pb.ModuleExtension_Key{
				Identifier: moduleExtensionKind.Reference,
			})
		default:
			return nil, errors.New("unknown module extension kind")
		}
	case *model_analysis_pb.CompiledBzlFileGlobal_Value_Failure:
		return nil, fmt.Errorf("failed to obtain value: %s", moduleExtensionResult.Failure)
	default:
		return nil, errors.New("compiled .bzl file global value has an unknown result type")
	}

	canonicalIdentifier, err := label.NewCanonicalStarlarkIdentifier(key.Identifier)
	if err != nil {
		return nil, fmt.Errorf("invalid canonical Starlark identifier: %w", err)
	}
	return &ModuleExtension{
		CanonicalIdentifier: canonicalIdentifier,
	}, nil
}
