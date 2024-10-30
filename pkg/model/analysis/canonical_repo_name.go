package analysis

import (
	"context"
	"fmt"
	"sort"

	"github.com/buildbarn/bb-playground/pkg/evaluation"
	"github.com/buildbarn/bb-playground/pkg/label"
	model_core "github.com/buildbarn/bb-playground/pkg/model/core"
	model_analysis_pb "github.com/buildbarn/bb-playground/pkg/proto/model/analysis"
	model_starlark_pb "github.com/buildbarn/bb-playground/pkg/proto/model/starlark"
	"github.com/buildbarn/bb-playground/pkg/storage/dag"
)

func (c *baseComputer) ComputeCanonicalRepoNameValue(ctx context.Context, key *model_analysis_pb.CanonicalRepoName_Key, e CanonicalRepoNameEnvironment) (PatchedCanonicalRepoNameValue, error) {
	fromCanonicalRepo, err := label.NewCanonicalRepo(key.FromCanonicalRepo)
	if err != nil {
		return model_core.NewSimplePatchedMessage[dag.ObjectContentsWalker](&model_analysis_pb.CanonicalRepoName_Value{
			Result: &model_analysis_pb.CanonicalRepoName_Value_Failure{
				Failure: fmt.Sprintf("Invalid canonical repo %#v: %s", key.FromCanonicalRepo, err),
			},
		}), nil
	}
	moduleRepoMappingValue := e.GetModuleRepoMappingValue(&model_analysis_pb.ModuleRepoMapping_Key{
		ModuleInstance: fromCanonicalRepo.GetModuleInstance().String(),
	})
	if !moduleRepoMappingValue.IsSet() {
		return PatchedCanonicalRepoNameValue{}, evaluation.ErrMissingDependency
	}
	switch moduleRepoMappingResult := moduleRepoMappingValue.Message.Result.(type) {
	case *model_analysis_pb.ModuleRepoMapping_Value_Success_:
		toApparentRepo := key.ToApparentRepo
		mappings := moduleRepoMappingResult.Success.Mappings
		mappingIndex := sort.Search(
			len(mappings),
			func(i int) bool { return mappings[i].FromApparentRepo >= toApparentRepo },
		)
		if mappingIndex >= len(mappings) || mappings[mappingIndex].FromApparentRepo != toApparentRepo {
			return model_core.NewSimplePatchedMessage[dag.ObjectContentsWalker](&model_analysis_pb.CanonicalRepoName_Value{
				Result: &model_analysis_pb.CanonicalRepoName_Value_Failure{
					Failure: "Unknown repo",
				},
			}), nil
		}
		switch target := mappings[mappingIndex].Target.(type) {
		case *model_analysis_pb.ModuleRepoMapping_Value_Success_Mapping_CanonicalRepo:
			// Canonical name of the repo is known. Return it.
			return model_core.NewSimplePatchedMessage[dag.ObjectContentsWalker](&model_analysis_pb.CanonicalRepoName_Value{
				Result: &model_analysis_pb.CanonicalRepoName_Value_ToCanonicalRepo{
					ToCanonicalRepo: target.CanonicalRepo,
				},
			}), nil
		case *model_analysis_pb.ModuleRepoMapping_Value_Success_Mapping_ModuleExtensionRepo_:
			// Repo is part of a module extension. To obtain
			// the repo's canonical name, we must first
			// canonicalize the name of the module extension.
			moduleExtensionIdentifierStr := target.ModuleExtensionRepo.ExtensionIdentifier
			moduleExtensionValue := e.GetCompiledBzlFileGlobalValue(&model_analysis_pb.CompiledBzlFileGlobal_Key{
				Identifier: moduleExtensionIdentifierStr,
			})
			if !moduleExtensionValue.IsSet() {
				return PatchedCanonicalRepoNameValue{}, evaluation.ErrMissingDependency
			}
			switch moduleExtensionResult := moduleExtensionValue.Message.Result.(type) {
			case *model_analysis_pb.CompiledBzlFileGlobal_Value_Success:
				v, ok := moduleExtensionResult.Success.Kind.(*model_starlark_pb.Value_ModuleExtension)
				if !ok {
					return model_core.NewSimplePatchedMessage[dag.ObjectContentsWalker](&model_analysis_pb.CanonicalRepoName_Value{
						Result: &model_analysis_pb.CanonicalRepoName_Value_Failure{
							Failure: fmt.Sprintf("%#v is not a module extension", moduleExtensionIdentifierStr),
						},
					}), nil
				}
				switch moduleExtensionKind := v.ModuleExtension.Kind.(type) {
				case *model_starlark_pb.ModuleExtension_Definition_:
					// Identifier was already canonical.
				case *model_starlark_pb.ModuleExtension_Reference:
					// Identifier points to a module
					// extension using one or more
					// levels of indirection.
					moduleExtensionIdentifierStr = moduleExtensionKind.Reference
				default:
					return model_core.NewSimplePatchedMessage[dag.ObjectContentsWalker](&model_analysis_pb.CanonicalRepoName_Value{
						Result: &model_analysis_pb.CanonicalRepoName_Value_Failure{
							Failure: fmt.Sprintf("%#v has an unknown module extension kind", moduleExtensionIdentifierStr),
						},
					}), nil
				}
			case *model_analysis_pb.CompiledBzlFileGlobal_Value_Failure:
				return model_core.NewSimplePatchedMessage[dag.ObjectContentsWalker](&model_analysis_pb.CanonicalRepoName_Value{
					Result: &model_analysis_pb.CanonicalRepoName_Value_Failure{
						Failure: fmt.Sprintf("Failed to obtain value for %#v: %s", moduleExtensionIdentifierStr, moduleExtensionResult.Failure),
					},
				}), nil
			default:
				return model_core.NewSimplePatchedMessage[dag.ObjectContentsWalker](&model_analysis_pb.CanonicalRepoName_Value{
					Result: &model_analysis_pb.CanonicalRepoName_Value_Failure{
						Failure: "Compiled .bzl file global value has an unknown result type",
					},
				}), nil
			}

			moduleExtensionIdentifier, err := label.NewCanonicalStarlarkIdentifier(moduleExtensionIdentifierStr)
			if err != nil {
				return model_core.NewSimplePatchedMessage[dag.ObjectContentsWalker](&model_analysis_pb.CanonicalRepoName_Value{
					Result: &model_analysis_pb.CanonicalRepoName_Value_Failure{
						Failure: fmt.Sprintf("Invalid canonical Starlak identifier %#v: %s", moduleExtensionIdentifierStr, err),
					},
				}), nil
			}
			toApparentRepo, err := label.NewApparentRepo(target.ModuleExtensionRepo.ToApparentRepo)
			if err != nil {
				return model_core.NewSimplePatchedMessage[dag.ObjectContentsWalker](&model_analysis_pb.CanonicalRepoName_Value{
					Result: &model_analysis_pb.CanonicalRepoName_Value_Failure{
						Failure: fmt.Sprintf("Invalid apparent repo identifier %#v: %s", target.ModuleExtensionRepo.ToApparentRepo, err),
					},
				}), nil
			}
			return model_core.NewSimplePatchedMessage[dag.ObjectContentsWalker](&model_analysis_pb.CanonicalRepoName_Value{
				Result: &model_analysis_pb.CanonicalRepoName_Value_ToCanonicalRepo{
					ToCanonicalRepo: moduleExtensionIdentifier.
						ToModuleExtension().
						GetCanonicalRepoWithModuleExtension(toApparentRepo).
						String(),
				},
			}), nil
		default:
			return model_core.NewSimplePatchedMessage[dag.ObjectContentsWalker](&model_analysis_pb.CanonicalRepoName_Value{
				Result: &model_analysis_pb.CanonicalRepoName_Value_Failure{
					Failure: "Unknown repo",
				},
			}), nil
		}
	case *model_analysis_pb.ModuleRepoMapping_Value_Failure:
		return model_core.NewSimplePatchedMessage[dag.ObjectContentsWalker](&model_analysis_pb.CanonicalRepoName_Value{
			Result: &model_analysis_pb.CanonicalRepoName_Value_Failure{
				Failure: moduleRepoMappingResult.Failure,
			},
		}), nil
	default:
		return model_core.NewSimplePatchedMessage[dag.ObjectContentsWalker](&model_analysis_pb.CanonicalRepoName_Value{
			Result: &model_analysis_pb.CanonicalRepoName_Value_Failure{
				Failure: "Module repo mapping value has an unknown result type",
			},
		}), nil
	}
}
