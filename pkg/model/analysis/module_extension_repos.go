package analysis

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	"sort"
	"strings"

	"github.com/buildbarn/bb-playground/pkg/evaluation"
	"github.com/buildbarn/bb-playground/pkg/label"
	model_core "github.com/buildbarn/bb-playground/pkg/model/core"
	"github.com/buildbarn/bb-playground/pkg/model/core/btree"
	model_encoding "github.com/buildbarn/bb-playground/pkg/model/encoding"
	model_starlark "github.com/buildbarn/bb-playground/pkg/model/starlark"
	model_analysis_pb "github.com/buildbarn/bb-playground/pkg/proto/model/analysis"
	model_starlark_pb "github.com/buildbarn/bb-playground/pkg/proto/model/starlark"
	"github.com/buildbarn/bb-playground/pkg/starlark/unpack"
	"github.com/buildbarn/bb-playground/pkg/storage/dag"
	"github.com/buildbarn/bb-playground/pkg/storage/object"
	"github.com/buildbarn/bb-storage/pkg/filesystem/path"

	"go.starlark.net/starlark"
	"go.starlark.net/starlarkstruct"
)

type bazelModuleTag struct {
	tagClass        *model_starlark_pb.TagClass
	isDevDependency bool
	attrs           []starlark.Value
}

var (
	_ starlark.HasAttrs = bazelModuleTag{}
	_ starlark.Value    = bazelModuleTag{}
)

func (bazelModuleTag) String() string {
	return "<bazel_module_tag>"
}

func (bazelModuleTag) Type() string {
	return "bazel_module_tag"
}

func (bazelModuleTag) Freeze() {
}

func (bazelModuleTag) Truth() starlark.Bool {
	return starlark.True
}

func (bazelModuleTag) Hash() (uint32, error) {
	return 0, nil
}

func (t bazelModuleTag) Attr(name string) (starlark.Value, error) {
	attrs := t.tagClass.GetAttrs()
	if index, ok := sort.Find(
		len(attrs),
		func(i int) int { return strings.Compare(name, attrs[i].Name) },
	); ok {
		return t.attrs[index], nil
	}
	return nil, nil
}

func (t bazelModuleTag) AttrNames() []string {
	attrs := t.tagClass.GetAttrs()
	attrNames := make([]string, 0, len(attrs))
	for _, attr := range attrs {
		attrNames = append(attrNames, attr.Name)
	}
	return attrNames
}

func (c *baseComputer) ComputeModuleExtensionReposValue(ctx context.Context, key *model_analysis_pb.ModuleExtensionRepos_Key, e ModuleExtensionReposEnvironment) (PatchedModuleExtensionReposValue, error) {
	allBuiltinsModulesNames := e.GetBuiltinsModuleNamesValue(&model_analysis_pb.BuiltinsModuleNames_Key{})
	repoPlatform := e.GetRegisteredRepoPlatformValue(&model_analysis_pb.RegisteredRepoPlatform_Key{})
	if !allBuiltinsModulesNames.IsSet() || !repoPlatform.IsSet() {
		return PatchedModuleExtensionReposValue{}, evaluation.ErrMissingDependency
	}

	// Resolve the module extension object that was declared within
	// Starlark code.
	moduleExtensionName, err := label.NewModuleExtension(key.ModuleExtension)
	if err != nil {
		return PatchedModuleExtensionReposValue{}, fmt.Errorf("invalid module extension: %w", err)
	}

	usedModuleExtensionValue := e.GetUsedModuleExtensionValue(&model_analysis_pb.UsedModuleExtension_Key{
		ModuleExtension: moduleExtensionName.String(),
	})
	if !usedModuleExtensionValue.IsSet() {
		return PatchedModuleExtensionReposValue{}, evaluation.ErrMissingDependency
	}
	usedModuleExtension := usedModuleExtensionValue.Message.ModuleExtension

	moduleExtensionIdentifierStr := usedModuleExtension.GetIdentifier()
	moduleExtensionIdentifier, err := label.NewCanonicalStarlarkIdentifier(moduleExtensionIdentifierStr)
	if err != nil {
		return PatchedModuleExtensionReposValue{}, fmt.Errorf("invalid module extension identifier %#v: %w", moduleExtensionIdentifierStr, err)
	}
	moduleExtensionDefinitionValue := e.GetCompiledBzlFileGlobalValue(&model_analysis_pb.CompiledBzlFileGlobal_Key{
		Identifier: moduleExtensionIdentifier.String(),
	})
	if !moduleExtensionDefinitionValue.IsSet() {
		return PatchedModuleExtensionReposValue{}, evaluation.ErrMissingDependency
	}
	v, ok := moduleExtensionDefinitionValue.Message.Global.GetKind().(*model_starlark_pb.Value_ModuleExtension)
	if !ok {
		return PatchedModuleExtensionReposValue{}, fmt.Errorf("%#v is not a module extension", moduleExtensionIdentifier.String())
	}
	moduleExtensionDefinition := v.ModuleExtension

	// Decode tags declared in all MODULE.bazel files belonging to
	// this module extension.
	moduleExtensionUsers := usedModuleExtension.GetUsers()
	modules := make([]starlark.Value, 0, len(moduleExtensionUsers))
	valueDecodingOptions := c.getValueDecodingOptions(ctx, func(canonicalLabel label.CanonicalLabel) (starlark.Value, error) {
		return model_starlark.NewLabel(canonicalLabel), nil
	})
	tagClassAttrTypes := make([][]model_starlark.AttrType, len(moduleExtensionDefinition.TagClasses))
	tagClassAttrDefaults := make([][]starlark.Value, len(moduleExtensionDefinition.TagClasses))
	for _, user := range moduleExtensionUsers {
		moduleInstance, err := label.NewModuleInstance(user.ModuleInstance)
		if err != nil {
			return PatchedModuleExtensionReposValue{}, fmt.Errorf("invalid module instance %#v: %w", user.ModuleInstance, err)
		}
		versionStr := ""
		if v, ok := moduleInstance.GetModuleVersion(); ok {
			versionStr = v.String()
		}

		usedTagClasses := user.TagClasses
		tagClasses := starlark.StringDict{}
		for tagClassIndex, tagClass := range moduleExtensionDefinition.TagClasses {
			tagClassDefinition := tagClass.TagClass
			tagClassAttrs := tagClassDefinition.GetAttrs()

			var tags []starlark.Value
			if len(usedTagClasses) > 0 && usedTagClasses[0].Name == tagClass.Name {
				declaredTags := usedTagClasses[0].Tags
				tags = make([]starlark.Value, 0, len(declaredTags))
				for _, declaredTag := range declaredTags {
					attrs := make([]starlark.Value, 0, len(tagClassAttrs))
					declaredAttrs := declaredTag.Attrs
					for attrIndex, attr := range tagClassAttrs {
						if len(declaredAttrs) > 0 && declaredAttrs[0].Name == attr.Name {
							value, err := model_starlark.DecodeValue(
								model_core.Message[*model_starlark_pb.Value]{
									Message:            declaredAttrs[0].Value,
									OutgoingReferences: usedModuleExtensionValue.OutgoingReferences,
								},
								/* currentIdentifier = */ nil,
								valueDecodingOptions,
							)
							if err != nil {
								return PatchedModuleExtensionReposValue{}, fmt.Errorf("failed to decode value of attribute %#v of tag class %#v declared by module instance %#v", attr.Name, tagClass.Name, moduleInstance.String())
							}

							if len(tagClassAttrTypes[tagClassIndex]) != len(tagClassAttrs) {
								tagClassAttrTypes[tagClassIndex] = make([]model_starlark.AttrType, len(tagClassAttrs))
							}
							attrType := &tagClassAttrTypes[tagClassIndex][attrIndex]
							if *attrType == nil {
								// First time we see this tag class be
								// invoked with a value for this
								// attribute. Determine the attribute
								// type, so that the provided value can be
								// canonicalized.
								*attrType, err = model_starlark.DecodeAttrType(attr.Attr)
								if err != nil {
									return PatchedModuleExtensionReposValue{}, fmt.Errorf("failed to decode type of attribute %#v of tag class %#v", attr.Name, tagClass.Name)
								}
							}

							// TODO: We're passing in a nil thread!
							canonicalValue, err := (*attrType).GetCanonicalizer(
								moduleInstance.GetBareCanonicalRepo().GetRootPackage(),
							).Canonicalize(nil, value)
							if err != nil {
								return PatchedModuleExtensionReposValue{}, fmt.Errorf("failed to canonicalize value of attribute %#v of tag class %#v declared by module instance %#v: %w", attr.Name, tagClass.Name, moduleInstance.String(), err)
							}
							attrs = append(attrs, canonicalValue)

							declaredAttrs = declaredAttrs[1:]
						} else if encodedDefaultValue := attr.Attr.GetDefault(); encodedDefaultValue != nil {
							// Tag didn't provide the attribute.
							// Use the default value.
							if len(tagClassAttrDefaults[tagClassIndex]) != len(tagClassAttrs) {
								tagClassAttrDefaults[tagClassIndex] = make([]starlark.Value, len(tagClassAttrs))
							}
							defaultValue := &tagClassAttrDefaults[tagClassIndex][attrIndex]
							if *defaultValue == nil {
								// First time we see this tag class be
								// invoked without a value for this
								// attribute. Decode the default value.
								*defaultValue, err = model_starlark.DecodeValue(
									model_core.Message[*model_starlark_pb.Value]{
										Message:            encodedDefaultValue,
										OutgoingReferences: moduleExtensionDefinitionValue.OutgoingReferences,
									},
									/* currentIdentifier = */ nil,
									valueDecodingOptions,
								)
								if err != nil {
									return PatchedModuleExtensionReposValue{}, fmt.Errorf("failed to decode default value of attribute %#v of tag class %#v", attr.Name, tagClass.Name)
								}
							}
							attrs = append(attrs, *defaultValue)
						} else {
							return PatchedModuleExtensionReposValue{}, fmt.Errorf("module instance %#v declares tag of class %#v with missing attribute %#v", moduleInstance.String(), tagClass.Name, attr.Name)
						}
					}
					if len(declaredAttrs) > 0 {
						return PatchedModuleExtensionReposValue{}, fmt.Errorf("module instance %#v declares tag of class %#v with unknown attribute %#v", moduleInstance.String(), tagClass.Name, declaredAttrs[0].Name)
					}

					tags = append(tags, bazelModuleTag{
						tagClass:        tagClassDefinition,
						isDevDependency: declaredTag.IsDevDependency,
						attrs:           attrs,
					})
				}

				usedTagClasses = usedTagClasses[1:]
			}
			tagClasses[tagClass.Name] = starlark.NewList(tags)
		}
		if len(usedTagClasses) > 0 {
			return PatchedModuleExtensionReposValue{}, fmt.Errorf("module instance %#v uses unknown tag class %#v", moduleInstance.String(), usedTagClasses[0].Name)
		}

		modules = append(modules, starlarkstruct.FromStringDict(starlarkstruct.Default, starlark.StringDict{
			"is_root": starlark.Bool(user.IsRoot),
			"name":    starlark.String(moduleInstance.GetModule().String()),
			"tags":    starlarkstruct.FromStringDict(starlarkstruct.Default, tagClasses),
			"version": starlark.String(versionStr),
		}))
	}

	// Call into the implementation function to obtain a set of
	// repos declared by this module extension.
	thread := c.newStarlarkThread(ctx, e, allBuiltinsModulesNames.Message.BuiltinsModuleNames)
	thread.SetLocal(model_starlark.CanonicalPackageKey, moduleExtensionName.GetModuleInstance().GetBareCanonicalRepo().GetRootPackage())
	thread.SetLocal(model_starlark.ValueEncodingOptionsKey, c.getValueEncodingOptions(moduleExtensionIdentifier.GetCanonicalLabel()))

	repoRegistrar := model_starlark.NewRepoRegistrar()
	thread.SetLocal(model_starlark.RepoRegistrarKey, repoRegistrar)

	moduleContext, err := c.newModuleOrRepositoryContext(ctx, e, []path.Component{
		path.MustNewComponent("modextwd"),
		path.MustNewComponent(moduleExtensionName.String()),
	})
	if err != nil {
		return PatchedModuleExtensionReposValue{}, err
	}
	defer moduleContext.release()

	moduleCtx := starlarkstruct.FromStringDict(starlarkstruct.Default, starlark.StringDict{
		// Fields shared with repository_ctx.
		"download":             starlark.NewBuiltin("module_ctx.download", moduleContext.doDownload),
		"download_and_extract": starlark.NewBuiltin("module_ctx.download_and_extract", moduleContext.doDownloadAndExtract),
		"execute":              starlark.NewBuiltin("module_ctx.execute", moduleContext.doExecute),
		"extract":              starlark.NewBuiltin("module_ctx.extract", moduleContext.doExtract),
		"file":                 starlark.NewBuiltin("module_ctx.file", moduleContext.doFile),
		"getenv":               starlark.NewBuiltin("module_ctx.getenv", moduleContext.doGetenv),
		"os":                   newRepositoryOS(repoPlatform.Message),
		"path":                 starlark.NewBuiltin("module_ctx.path", moduleContext.doPath),
		"read":                 starlark.NewBuiltin("module_ctx.read", moduleContext.doRead),
		"report_progress":      starlark.NewBuiltin("module_ctx.report_progress", moduleContext.doReportProgress),
		"watch":                starlark.NewBuiltin("module_ctx.watch", moduleContext.doWatch),
		"which":                starlark.NewBuiltin("module_ctx.which", moduleContext.doWhich),

		// Fields specific to module_ctx.
		"extension_metadata": starlark.NewBuiltin(
			"module_ctx.extension_metadata",
			func(thread *starlark.Thread, b *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
				// TODO: Properly implement this function.
				return starlark.None, nil
			},
		),
		"is_dev_dependency": starlark.NewBuiltin(
			"module_ctx.is_dev_dependency",
			func(thread *starlark.Thread, b *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
				var tag bazelModuleTag
				if err := starlark.UnpackArgs(
					b.Name(), args, kwargs,
					"tag", unpack.Bind(thread, &tag, unpack.Type[bazelModuleTag]("bazel_module_tag")),
				); err != nil {
					return nil, err
				}
				return starlark.Bool(tag.isDevDependency), nil
			},
		),
		"modules": starlark.NewList(modules),
	})
	moduleCtx.Freeze()

	// TODO: Capture extension_metadata.
	_, err = starlark.Call(
		thread,
		model_starlark.NewNamedFunction(model_starlark.NewProtoNamedFunctionDefinition(
			model_core.Message[*model_starlark_pb.Function]{
				Message:            moduleExtensionDefinition.Implementation,
				OutgoingReferences: moduleExtensionDefinitionValue.OutgoingReferences,
			},
		)),
		/* args = */ starlark.Tuple{moduleCtx},
		/* kwargs = */ nil,
	)
	if err != nil {
		var evalErr *starlark.EvalError
		if !errors.Is(err, evaluation.ErrMissingDependency) && errors.As(err, &evalErr) {
			return PatchedModuleExtensionReposValue{}, errors.New(evalErr.Backtrace())
		}
		return PatchedModuleExtensionReposValue{}, err
	}

	// Store all repos in a B-tree.
	// TODO: Use a proper encoder!
	treeBuilder := btree.NewSplitProllyBuilder(
		/* minimumSizeBytes = */ 32*1024,
		/* maximumSizeBytes = */ 128*1024,
		btree.NewObjectCreatingNodeMerger(
			model_encoding.NewChainedBinaryEncoder(nil),
			c.buildSpecificationReference.GetReferenceFormat(),
			/* parentNodeComputer = */ func(contents *object.Contents, childNodes []*model_analysis_pb.ModuleExtensionRepos_Value_RepoList_Element, outgoingReferences object.OutgoingReferences, metadata []dag.ObjectContentsWalker) (model_core.PatchedMessage[*model_analysis_pb.ModuleExtensionRepos_Value_RepoList_Element, dag.ObjectContentsWalker], error) {
				var firstName string
				switch firstElement := childNodes[0].Level.(type) {
				case *model_analysis_pb.ModuleExtensionRepos_Value_RepoList_Element_Leaf:
					firstName = firstElement.Leaf.Name
				case *model_analysis_pb.ModuleExtensionRepos_Value_RepoList_Element_Parent_:
					firstName = firstElement.Parent.FirstName
				}
				patcher := model_core.NewReferenceMessagePatcher[dag.ObjectContentsWalker]()
				return model_core.NewPatchedMessage(
					&model_analysis_pb.ModuleExtensionRepos_Value_RepoList_Element{
						Level: &model_analysis_pb.ModuleExtensionRepos_Value_RepoList_Element_Parent_{
							Parent: &model_analysis_pb.ModuleExtensionRepos_Value_RepoList_Element_Parent{
								Reference: patcher.AddReference(contents.GetReference(), dag.NewSimpleObjectContentsWalker(contents, metadata)),
								FirstName: firstName,
							},
						},
					},
					patcher,
				), nil
			},
		),
	)

	repos := repoRegistrar.GetRepos()
	for _, name := range slices.Sorted(maps.Keys(repos)) {
		repo := repos[name]
		if err := treeBuilder.PushChild(model_core.NewPatchedMessage(
			&model_analysis_pb.ModuleExtensionRepos_Value_RepoList_Element{
				Level: &model_analysis_pb.ModuleExtensionRepos_Value_RepoList_Element_Leaf{
					Leaf: repo.Message,
				},
			},
			repo.Patcher,
		)); err != nil {
			return PatchedModuleExtensionReposValue{}, err
		}
	}

	reposList, err := treeBuilder.FinalizeList()
	if err != nil {
		return PatchedModuleExtensionReposValue{}, err
	}

	return PatchedModuleExtensionReposValue{
		Message: &model_analysis_pb.ModuleExtensionRepos_Value{
			Repos: reposList.Message,
		},
		Patcher: reposList.Patcher,
	}, nil
}
