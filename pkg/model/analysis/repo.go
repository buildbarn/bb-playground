package analysis

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"sort"
	"strings"

	"github.com/buildbarn/bb-playground/pkg/evaluation"
	"github.com/buildbarn/bb-playground/pkg/label"
	model_core "github.com/buildbarn/bb-playground/pkg/model/core"
	model_filesystem "github.com/buildbarn/bb-playground/pkg/model/filesystem"
	model_parser "github.com/buildbarn/bb-playground/pkg/model/parser"
	model_analysis_pb "github.com/buildbarn/bb-playground/pkg/proto/model/analysis"
	model_core_pb "github.com/buildbarn/bb-playground/pkg/proto/model/core"
	model_filesystem_pb "github.com/buildbarn/bb-playground/pkg/proto/model/filesystem"
	"github.com/buildbarn/bb-playground/pkg/storage/dag"
	"github.com/buildbarn/bb-playground/pkg/storage/object"
	"github.com/buildbarn/bb-storage/pkg/filesystem/path"
	"github.com/buildbarn/bb-storage/pkg/util"
)

type sourceJSON struct {
	Integrity   string `json:"integrity"`
	StripPrefix string `json:"strip_prefix"`
	URL         string `json:"url"`
}

type directoryResolver struct {
	// Immutable fields.
	context         context.Context
	directoryReader model_parser.ParsedObjectReader[object.LocalReference, model_core.Message[*model_filesystem_pb.Directory]]

	// Mutable fields.
	currentReference model_core.Message[*model_core_pb.Reference]
	currentDirectory model_core.Message[*model_filesystem_pb.Directory]
}

func (r *directoryResolver) OnDirectory(name path.Component) (path.GotDirectoryOrSymlink, error) {
	d := r.currentDirectory
	if !d.IsSet() {
		reference := r.currentReference
		index, err := model_core.GetIndexFromReferenceMessage(reference.Message, reference.OutgoingReferences.GetDegree())
		if err != nil {
			return nil, err
		}
		d, _, err = r.directoryReader.ReadParsedObject(r.context, reference.OutgoingReferences.GetOutgoingReference(index))
		if err != nil {
			return nil, err
		}
	}

	n := name.String()
	directories := d.Message.Directories
	if i := sort.Search(
		len(directories),
		func(i int) bool { return directories[i].Name >= n },
	); i < len(directories) && directories[i].Name == n {
		directoryNode := directories[i]
		switch contents := directoryNode.Contents.(type) {
		case *model_filesystem_pb.DirectoryNode_ContentsExternal:
			r.currentReference = model_core.Message[*model_core_pb.Reference]{
				Message:            contents.ContentsExternal.Reference,
				OutgoingReferences: d.OutgoingReferences,
			}
			r.currentDirectory.Clear()
		case *model_filesystem_pb.DirectoryNode_ContentsInline:
			r.currentReference.Clear()
			r.currentDirectory = model_core.Message[*model_filesystem_pb.Directory]{
				Message:            contents.ContentsInline,
				OutgoingReferences: d.OutgoingReferences,
			}
		default:
			return nil, errors.New("unknown directory node contents type")
		}
		return path.GotDirectory{
			Child:        r,
			IsReversible: true,
		}, nil
	}

	return nil, errors.New("directory does not exist")
}

func (r *directoryResolver) OnTerminal(name path.Component) (*path.GotSymlink, error) {
	return path.OnTerminalViaOnDirectory(r, name)
}

func (r *directoryResolver) OnUp() (path.ComponentWalker, error) {
	return nil, errors.New("path cannot go up")
}

func (c *baseComputer) fetchModuleFromRegistry(ctx context.Context, module *model_analysis_pb.BuildList_Module, e RepoEnvironment) (PatchedRepoValue, error) {
	fileReader, err := e.GetFileReaderValue(&model_analysis_pb.FileReader_Key{})
	if err != nil {
		if !errors.Is(err, evaluation.ErrMissingDependency) {
			return model_core.NewSimplePatchedMessage[dag.ObjectContentsWalker](&model_analysis_pb.Repo_Value{
				Result: &model_analysis_pb.Repo_Value_Failure{
					Failure: err.Error(),
				},
			}), nil
		}
		return PatchedRepoValue{}, err
	}

	sourceJSONURL, err := url.JoinPath(
		module.RegistryUrl,
		"modules",
		module.Name,
		module.Version,
		"source.json",
	)
	if err != nil {
		return model_core.NewSimplePatchedMessage[dag.ObjectContentsWalker](&model_analysis_pb.Repo_Value{
			Result: &model_analysis_pb.Repo_Value_Failure{
				Failure: fmt.Sprintf("Failed to construct URL for module %s with version %s in registry %#v: %s", module.Name, module.Version, module.RegistryUrl),
			},
		}), nil
	}

	sourceJSONContentsValue := e.GetHttpFileContentsValue(&model_analysis_pb.HttpFileContents_Key{Url: sourceJSONURL})
	if !sourceJSONContentsValue.IsSet() {
		return PatchedRepoValue{}, evaluation.ErrMissingDependency
	}
	var sourceJSONContents model_core.Message[*model_filesystem_pb.FileContents]
	switch httpFileContentsResult := sourceJSONContentsValue.Message.Result.(type) {
	case *model_analysis_pb.HttpFileContents_Value_Exists_:
		sourceJSONContents = model_core.Message[*model_filesystem_pb.FileContents]{
			Message:            httpFileContentsResult.Exists.Contents,
			OutgoingReferences: sourceJSONContentsValue.OutgoingReferences,
		}
	case *model_analysis_pb.HttpFileContents_Value_DoesNotExist:
		return model_core.NewSimplePatchedMessage[dag.ObjectContentsWalker](&model_analysis_pb.Repo_Value{
			Result: &model_analysis_pb.Repo_Value_Failure{
				Failure: fmt.Sprintf("Failed to fetch %#v, as the file does not exist", sourceJSONURL),
			},
		}), nil
	case *model_analysis_pb.HttpFileContents_Value_Failure:
		return model_core.NewSimplePatchedMessage[dag.ObjectContentsWalker](&model_analysis_pb.Repo_Value{
			Result: &model_analysis_pb.Repo_Value_Failure{
				Failure: fmt.Sprintf("Failed to fetch %#v: %s", sourceJSONURL, httpFileContentsResult.Failure),
			},
		}), nil
	default:
		return model_core.NewSimplePatchedMessage[dag.ObjectContentsWalker](&model_analysis_pb.Repo_Value{
			Result: &model_analysis_pb.Repo_Value_Failure{
				Failure: "HTTP file contents value has an unknown result type",
			},
		}), nil
	}

	sourceJSONContentsEntry, err := model_filesystem.NewFileContentsEntryFromProto(
		sourceJSONContents,
		c.buildSpecificationReference.GetReferenceFormat(),
	)
	if err != nil {
		return model_core.NewSimplePatchedMessage[dag.ObjectContentsWalker](&model_analysis_pb.Repo_Value{
			Result: &model_analysis_pb.Repo_Value_Failure{
				Failure: util.StatusWrap(err, "Invalid file contents").Error(),
			},
		}), nil
	}

	sourceJSONData, err := fileReader.FileReadAll(ctx, sourceJSONContentsEntry, 1<<20)
	if err != nil {
		return PatchedRepoValue{}, err
	}
	var sourceJSON sourceJSON
	if err := json.Unmarshal(sourceJSONData, &sourceJSON); err != nil {
		return model_core.NewSimplePatchedMessage[dag.ObjectContentsWalker](&model_analysis_pb.Repo_Value{
			Result: &model_analysis_pb.Repo_Value_Failure{
				Failure: fmt.Sprintf("Invalid JSON contents for %#v: %s", sourceJSONURL, err),
			},
		}), nil
	}

	var archiveFormat model_analysis_pb.HttpArchiveContents_Key_Format
	if strings.HasSuffix(sourceJSON.URL, ".tar.gz") {
		archiveFormat = model_analysis_pb.HttpArchiveContents_Key_TAR_GZ
	} else if strings.HasSuffix(sourceJSON.URL, ".zip") {
		archiveFormat = model_analysis_pb.HttpArchiveContents_Key_ZIP
	} else {
		return model_core.NewSimplePatchedMessage[dag.ObjectContentsWalker](&model_analysis_pb.Repo_Value{
			Result: &model_analysis_pb.Repo_Value_Failure{
				Failure: fmt.Sprintf("Cannot derive archive format from file extension of URL %#v", sourceJSONURL),
			},
		}), nil
	}

	archiveContentsValue := e.GetHttpArchiveContentsValue(&model_analysis_pb.HttpArchiveContents_Key{
		Url:    sourceJSON.URL,
		Format: archiveFormat,
	})
	if !archiveContentsValue.IsSet() {
		return PatchedRepoValue{}, evaluation.ErrMissingDependency
	}
	switch archiveContentsResult := archiveContentsValue.Message.Result.(type) {
	case *model_analysis_pb.HttpArchiveContents_Value_Exists:
		directoryCreationParameters, err := e.GetDirectoryCreationParametersObjectValue(&model_analysis_pb.DirectoryCreationParametersObject_Key{})
		if err != nil {
			return PatchedRepoValue{}, err
		}
		directoryReader := model_parser.NewStorageBackedParsedObjectReader(
			c.objectDownloader,
			directoryCreationParameters.GetEncoder(),
			model_parser.NewMessageObjectParser[object.LocalReference, model_filesystem_pb.Directory](),
		)
		rootDirectoryResolver := directoryResolver{
			context:         ctx,
			directoryReader: directoryReader,

			currentReference: model_core.Message[*model_core_pb.Reference]{
				Message:            archiveContentsResult.Exists,
				OutgoingReferences: archiveContentsValue.OutgoingReferences,
			},
		}

		if err := path.Resolve(
			path.UNIXFormat.NewParser(sourceJSON.StripPrefix),
			path.NewRelativeScopeWalker(&rootDirectoryResolver),
		); err != nil {
			return model_core.NewSimplePatchedMessage[dag.ObjectContentsWalker](&model_analysis_pb.Repo_Value{
				Result: &model_analysis_pb.Repo_Value_Failure{
					Failure: fmt.Sprintf("Failed to strip prefix %#v from %#v: %s", sourceJSON.StripPrefix, sourceJSON.URL, err),
				},
			}), nil
		}

		// TODO: Apply patches!

		if r := rootDirectoryResolver.currentReference; r.IsSet() {
			rootDirectoryReference := model_core.NewPatchedMessageFromExisting(
				r,
				func(index int) dag.ObjectContentsWalker {
					return dag.ExistingObjectContentsWalker
				},
			)
			return PatchedRepoValue{
				Message: &model_analysis_pb.Repo_Value{
					Result: &model_analysis_pb.Repo_Value_RootDirectoryReference{
						RootDirectoryReference: rootDirectoryReference.Message,
					},
				},
				Patcher: rootDirectoryReference.Patcher,
			}, nil
		}

		// We had to strip a prefix and ended up inside of a
		// Directory message.
		rootDirectoryMessage := model_core.NewPatchedMessageFromExisting(
			rootDirectoryResolver.currentDirectory,
			func(index int) dag.ObjectContentsWalker {
				return dag.ExistingObjectContentsWalker
			},
		)
		references, children := rootDirectoryMessage.Patcher.SortAndSetReferences()
		rootDirectoryContents, err := directoryCreationParameters.EncodeDirectory(references, rootDirectoryMessage.Message)
		if err != nil {
			return model_core.NewSimplePatchedMessage[dag.ObjectContentsWalker](&model_analysis_pb.Repo_Value{
				Result: &model_analysis_pb.Repo_Value_Failure{
					Failure: err.Error(),
				},
			}), nil
		}

		patcher := model_core.NewReferenceMessagePatcher[dag.ObjectContentsWalker]()
		return PatchedRepoValue{
			Message: &model_analysis_pb.Repo_Value{
				Result: &model_analysis_pb.Repo_Value_RootDirectoryReference{
					RootDirectoryReference: patcher.AddReference(
						rootDirectoryContents.GetReference(),
						dag.NewSimpleObjectContentsWalker(rootDirectoryContents, children),
					),
				},
			},
			Patcher: patcher,
		}, nil
	case *model_analysis_pb.HttpArchiveContents_Value_DoesNotExist:
		return model_core.NewSimplePatchedMessage[dag.ObjectContentsWalker](&model_analysis_pb.Repo_Value{
			Result: &model_analysis_pb.Repo_Value_Failure{
				Failure: "HTTP archive does not exist",
			},
		}), nil
	case *model_analysis_pb.HttpArchiveContents_Value_Failure:
		return model_core.NewSimplePatchedMessage[dag.ObjectContentsWalker](&model_analysis_pb.Repo_Value{
			Result: &model_analysis_pb.Repo_Value_Failure{
				Failure: fmt.Sprintf("Failed to download %#v: %s", sourceJSON.URL, archiveContentsResult.Failure),
			},
		}), nil
	default:
		return model_core.NewSimplePatchedMessage[dag.ObjectContentsWalker](&model_analysis_pb.Repo_Value{
			Result: &model_analysis_pb.Repo_Value_Failure{
				Failure: "HTTP archive contents value has an unknown result type",
			},
		}), nil
	}
}

func (c *baseComputer) ComputeRepoValue(ctx context.Context, key *model_analysis_pb.Repo_Key, e RepoEnvironment) (PatchedRepoValue, error) {
	canonicalRepo, err := label.NewCanonicalRepo(key.CanonicalRepo)
	if err != nil {
		return model_core.NewSimplePatchedMessage[dag.ObjectContentsWalker](&model_analysis_pb.Repo_Value{
			Result: &model_analysis_pb.Repo_Value_Failure{
				Failure: util.StatusWrapf(err, "Invalid canonical repo %#v", key.CanonicalRepo).Error(),
			},
		}), nil
	}

	if !canonicalRepo.HasModuleExtension() {
		moduleInstance := canonicalRepo.GetModuleInstance()
		if _, ok := moduleInstance.GetModuleVersion(); !ok {
			// See if this is one of the modules for which sources
			// are provided. If so, return a repo value immediately.
			// This allows any files contained within to be accessed
			// without processing MODULE.bazel. This prevents cyclic
			// dependencies.
			buildSpecification := e.GetBuildSpecificationValue(&model_analysis_pb.BuildSpecification_Key{})
			if !buildSpecification.IsSet() {
				return PatchedRepoValue{}, evaluation.ErrMissingDependency
			}

			moduleName := moduleInstance.GetModule().String()
			modules := buildSpecification.Message.BuildSpecification.GetModules()
			if i := sort.Search(
				len(modules),
				func(i int) bool { return modules[i].Name >= moduleName },
			); i < len(modules) && modules[i].Name == moduleName {
				// Found matching module.
				rootDirectoryReference := model_core.NewPatchedMessageFromExisting(
					model_core.Message[*model_core_pb.Reference]{
						Message:            modules[i].RootDirectoryReference,
						OutgoingReferences: buildSpecification.OutgoingReferences,
					},
					func(index int) dag.ObjectContentsWalker {
						return dag.ExistingObjectContentsWalker
					},
				)
				return PatchedRepoValue{
					Message: &model_analysis_pb.Repo_Value{
						Result: &model_analysis_pb.Repo_Value_RootDirectoryReference{
							RootDirectoryReference: rootDirectoryReference.Message,
						},
					},
					Patcher: rootDirectoryReference.Patcher,
				}, nil
			}

			// If a version of the module is selected as
			// part of the final build list, we can download
			// that exact version.
			buildListValue := e.GetModuleFinalBuildListValue(&model_analysis_pb.ModuleFinalBuildList_Key{})
			if !buildListValue.IsSet() {
				return PatchedRepoValue{}, evaluation.ErrMissingDependency
			}
			switch buildListResult := buildListValue.Message.Result.(type) {
			case *model_analysis_pb.ModuleFinalBuildList_Value_Success:
				buildList := buildListResult.Success.Modules
				if i := sort.Search(
					len(buildList),
					func(i int) bool { return buildList[i].Name >= moduleName },
				); i < len(buildList) && buildList[i].Name == moduleName {
					return c.fetchModuleFromRegistry(ctx, buildList[i], e)
				}
			case *model_analysis_pb.ModuleFinalBuildList_Value_Failure:
				return model_core.NewSimplePatchedMessage[dag.ObjectContentsWalker](&model_analysis_pb.Repo_Value{
					Result: &model_analysis_pb.Repo_Value_Failure{
						Failure: buildListResult.Failure,
					},
				}), nil
			default:
				return model_core.NewSimplePatchedMessage[dag.ObjectContentsWalker](&model_analysis_pb.Repo_Value{
					Result: &model_analysis_pb.Repo_Value_Failure{
						Failure: "Final build list value has an unknown result type",
					},
				}), nil
			}
		}
	} else {
		// TODO: Look up module extension.
	}

	return model_core.NewSimplePatchedMessage[dag.ObjectContentsWalker](&model_analysis_pb.Repo_Value{
		Result: &model_analysis_pb.Repo_Value_Failure{
			Failure: fmt.Sprintf("Repo %#v not found", canonicalRepo.String()),
		},
	}), nil
}
