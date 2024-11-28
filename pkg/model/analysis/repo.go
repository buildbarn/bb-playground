package analysis

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"net/url"
	"slices"
	"sort"
	"strings"

	"github.com/bluekeyes/go-gitdiff/gitdiff"
	"github.com/buildbarn/bb-playground/pkg/diff"
	"github.com/buildbarn/bb-playground/pkg/evaluation"
	"github.com/buildbarn/bb-playground/pkg/label"
	model_core "github.com/buildbarn/bb-playground/pkg/model/core"
	model_filesystem "github.com/buildbarn/bb-playground/pkg/model/filesystem"
	model_parser "github.com/buildbarn/bb-playground/pkg/model/parser"
	model_starlark "github.com/buildbarn/bb-playground/pkg/model/starlark"
	model_analysis_pb "github.com/buildbarn/bb-playground/pkg/proto/model/analysis"
	model_core_pb "github.com/buildbarn/bb-playground/pkg/proto/model/core"
	model_filesystem_pb "github.com/buildbarn/bb-playground/pkg/proto/model/filesystem"
	model_starlark_pb "github.com/buildbarn/bb-playground/pkg/proto/model/starlark"
	"github.com/buildbarn/bb-playground/pkg/search"
	"github.com/buildbarn/bb-playground/pkg/starlark/unpack"
	"github.com/buildbarn/bb-playground/pkg/storage/dag"
	"github.com/buildbarn/bb-playground/pkg/storage/object"
	"github.com/buildbarn/bb-storage/pkg/filesystem"
	"github.com/buildbarn/bb-storage/pkg/filesystem/path"
	"github.com/buildbarn/bb-storage/pkg/util"

	"golang.org/x/sync/errgroup"
	"golang.org/x/sync/semaphore"

	"go.starlark.net/starlark"
	"go.starlark.net/starlarkstruct"
)

// sourceJSON corresponds to the format of source.json files that are
// served by Bazel Central Registry (BCR).
type sourceJSON struct {
	Integrity   string            `json:"integrity"`
	PatchStrip  int               `json:"patch_strip"`
	Patches     map[string]string `json:"patches"`
	StripPrefix string            `json:"strip_prefix"`
	URL         string            `json:"url"`
}

type changeTrackingDirectory struct {
	// If set, the directory has not been accessed, and its contents
	// are still identical to the original version. If not set, the
	// directory has been accessed and potentially modified,
	// requiring it to be recomputed and uploaded once again.
	currentReference model_core.Message[*model_core_pb.Reference]

	directories map[path.Component]*changeTrackingDirectory
	files       map[path.Component]*changeTrackingFile
	symlinks    map[path.Component]string
}

func (d *changeTrackingDirectory) setContents(contents model_core.Message[*model_filesystem_pb.Directory], options *changeTrackingDirectoryLoadOptions) error {
	var leaves model_core.Message[*model_filesystem_pb.Leaves]
	switch leavesType := contents.Message.Leaves.(type) {
	case *model_filesystem_pb.Directory_LeavesExternal:
		index, err := model_core.GetIndexFromReferenceMessage(leavesType.LeavesExternal, contents.OutgoingReferences.GetDegree())
		if err != nil {
			return err
		}
		leaves, _, err = options.leavesReader.ReadParsedObject(
			options.context,
			contents.OutgoingReferences.GetOutgoingReference(index),
		)
		if err != nil {
			return err
		}
	case *model_filesystem_pb.Directory_LeavesInline:
		leaves = model_core.Message[*model_filesystem_pb.Leaves]{
			Message:            leavesType.LeavesInline,
			OutgoingReferences: contents.OutgoingReferences,
		}
	default:
		return errors.New("unknown leaves contents type")
	}

	d.files = make(map[path.Component]*changeTrackingFile, len(leaves.Message.Files))
	for _, file := range leaves.Message.Files {
		name, ok := path.NewComponent(file.Name)
		if !ok {
			return fmt.Errorf("file %#v has an invalid name", file.Name)
		}
		properties := file.Properties
		if properties == nil {
			return fmt.Errorf("file %#v has no properties", file.Name)
		}
		d.files[name] = &changeTrackingFile{
			isExecutable: properties.IsExecutable,
			contents: unmodifiedFileContents{
				contents: model_core.Message[*model_filesystem_pb.FileContents]{
					Message:            properties.Contents,
					OutgoingReferences: leaves.OutgoingReferences,
				},
			},
		}
	}
	d.symlinks = make(map[path.Component]string, len(leaves.Message.Symlinks))
	for _, symlink := range leaves.Message.Symlinks {
		name, ok := path.NewComponent(symlink.Name)
		if !ok {
			return fmt.Errorf("symbolic link %#v has an invalid name", symlink.Name)
		}
		d.symlinks[name] = symlink.Target
	}

	d.directories = make(map[path.Component]*changeTrackingDirectory, len(contents.Message.Directories))
	for _, directory := range contents.Message.Directories {
		name, ok := path.NewComponent(directory.Name)
		if !ok {
			return fmt.Errorf("directory %#v has an invalid name", directory.Name)
		}
		switch childContents := directory.Contents.(type) {
		case *model_filesystem_pb.DirectoryNode_ContentsExternal:
			d.directories[name] = &changeTrackingDirectory{
				currentReference: model_core.Message[*model_core_pb.Reference]{
					Message:            childContents.ContentsExternal.Reference,
					OutgoingReferences: contents.OutgoingReferences,
				},
			}
		case *model_filesystem_pb.DirectoryNode_ContentsInline:
			dChild := &changeTrackingDirectory{}
			if err := dChild.setContents(
				model_core.Message[*model_filesystem_pb.Directory]{
					Message:            childContents.ContentsInline,
					OutgoingReferences: contents.OutgoingReferences,
				},
				options,
			); err != nil {
				return err
			}
			d.directories[name] = dChild
		default:
			return errors.New("unknown directory contents type")
		}
	}
	return nil
}

func (d *changeTrackingDirectory) setFile(name path.Component, f *changeTrackingFile) {
	if d.files == nil {
		d.files = map[path.Component]*changeTrackingFile{}
	}
	d.files[name] = f
}

type changeTrackingDirectoryLoadOptions struct {
	context         context.Context
	directoryReader model_parser.ParsedObjectReader[object.LocalReference, model_core.Message[*model_filesystem_pb.Directory]]
	leavesReader    model_parser.ParsedObjectReader[object.LocalReference, model_core.Message[*model_filesystem_pb.Leaves]]
}

func (d *changeTrackingDirectory) maybeLoadContents(options *changeTrackingDirectoryLoadOptions) error {
	if reference := d.currentReference; reference.IsSet() {
		// Directory has not been accessed before. Load it from
		// storage and ingest its contents.
		index, err := model_core.GetIndexFromReferenceMessage(reference.Message, reference.OutgoingReferences.GetDegree())
		if err != nil {
			return err
		}
		directoryMessage, _, err := options.directoryReader.ReadParsedObject(options.
			context,
			reference.OutgoingReferences.GetOutgoingReference(index),
		)
		if err != nil {
			return err
		}
		d.currentReference.Clear()
		if err := d.setContents(directoryMessage, options); err != nil {
			return err
		}
	}
	return nil
}

type changeTrackingFile struct {
	isExecutable bool
	contents     changeTrackingFileContents
}

type changeTrackingFileContents interface {
	createFileMerkleTree(ctx context.Context, options *capturableChangeTrackingDirectoryOptions) (model_core.PatchedMessage[*model_filesystem_pb.FileContents, model_core.FileBackedObjectLocation], error)
	openRead(ctx context.Context, referenceFormat object.ReferenceFormat, fileReader *model_filesystem.FileReader, patchedFiles io.ReaderAt) (io.Reader, error)
}

type unmodifiedFileContents struct {
	contents model_core.Message[*model_filesystem_pb.FileContents]
}

func (fc unmodifiedFileContents) createFileMerkleTree(ctx context.Context, options *capturableChangeTrackingDirectoryOptions) (model_core.PatchedMessage[*model_filesystem_pb.FileContents, model_core.FileBackedObjectLocation], error) {
	return model_core.NewPatchedMessageFromExisting(
		fc.contents,
		func(index int) model_core.FileBackedObjectLocation {
			return model_core.ExistingFileBackedObjectLocation
		},
	), nil
}

func (fc unmodifiedFileContents) openRead(ctx context.Context, referenceFormat object.ReferenceFormat, fileReader *model_filesystem.FileReader, patchedFiles io.ReaderAt) (io.Reader, error) {
	entry, err := model_filesystem.NewFileContentsEntryFromProto(fc.contents, referenceFormat)
	if err != nil {
		return nil, err
	}
	return fileReader.FileOpenRead(ctx, entry, 0), nil
}

type patchedFileContents struct {
	offsetBytes int64
	sizeBytes   int64
}

func (fc patchedFileContents) createFileMerkleTree(ctx context.Context, options *capturableChangeTrackingDirectoryOptions) (model_core.PatchedMessage[*model_filesystem_pb.FileContents, model_core.FileBackedObjectLocation], error) {
	return model_filesystem.CreateFileMerkleTree(
		ctx,
		options.fileCreationParameters,
		io.NewSectionReader(options.patchedFiles, fc.offsetBytes, fc.sizeBytes),
		options.fileMerkleTreeCapturer,
	)
}

func (fc patchedFileContents) openRead(ctx context.Context, referenceFormat object.ReferenceFormat, fileReader *model_filesystem.FileReader, patchedFiles io.ReaderAt) (io.Reader, error) {
	return io.NewSectionReader(patchedFiles, fc.offsetBytes, fc.sizeBytes), nil
}

type changeTrackingDirectoryResolver struct {
	loadOptions      *changeTrackingDirectoryLoadOptions
	currentDirectory *changeTrackingDirectory
}

func (r *changeTrackingDirectoryResolver) OnDirectory(name path.Component) (path.GotDirectoryOrSymlink, error) {
	d := r.currentDirectory
	if err := d.maybeLoadContents(r.loadOptions); err != nil {
		return nil, err
	}

	if dChild, ok := d.directories[name]; ok {
		r.currentDirectory = dChild
		return path.GotDirectory{
			Child:        r,
			IsReversible: true,
		}, nil
	}

	return nil, errors.New("directory does not exist")
}

func (r *changeTrackingDirectoryResolver) OnTerminal(name path.Component) (*path.GotSymlink, error) {
	return path.OnTerminalViaOnDirectory(r, name)
}

func (r *changeTrackingDirectoryResolver) OnUp() (path.ComponentWalker, error) {
	return nil, errors.New("path cannot go up")
}

type capturableChangeTrackingDirectoryOptions struct {
	context                context.Context
	directoryReader        model_parser.ParsedObjectReader[object.LocalReference, model_core.Message[*model_filesystem_pb.Directory]]
	fileCreationParameters *model_filesystem.FileCreationParameters
	fileMerkleTreeCapturer model_filesystem.FileMerkleTreeCapturer[model_core.FileBackedObjectLocation]
	patchedFiles           io.ReaderAt
}

type capturableChangeTrackingDirectory struct {
	options   *capturableChangeTrackingDirectoryOptions
	directory *changeTrackingDirectory
}

func (cd *capturableChangeTrackingDirectory) Close() error {
	return nil
}

func (cd *capturableChangeTrackingDirectory) EnterCapturableDirectory(name path.Component) (model_core.PatchedMessage[*model_filesystem_pb.Directory, model_core.FileBackedObjectLocation], model_filesystem.CapturableDirectory[model_core.FileBackedObjectLocation, model_core.FileBackedObjectLocation], error) {
	dChild, ok := cd.directory.directories[name]
	if !ok {
		panic("attempted to enter non-existent directory")
	}
	if reference := dChild.currentReference; reference.IsSet() {
		// Directory has not been modified. Load the copy from
		// storage, so that it may potentially be inlined into
		// the parent directory.
		index, err := model_core.GetIndexFromReferenceMessage(reference.Message, reference.OutgoingReferences.GetDegree())
		if err != nil {
			return model_core.PatchedMessage[*model_filesystem_pb.Directory, model_core.FileBackedObjectLocation]{}, nil, err
		}
		directoryMessage, _, err := cd.options.directoryReader.ReadParsedObject(cd.options.context, reference.OutgoingReferences.GetOutgoingReference(index))
		if err != nil {
			return model_core.PatchedMessage[*model_filesystem_pb.Directory, model_core.FileBackedObjectLocation]{}, nil, err
		}
		return model_core.NewPatchedMessageFromExisting(
			directoryMessage,
			func(index int) model_core.FileBackedObjectLocation {
				return model_core.ExistingFileBackedObjectLocation
			},
		), nil, nil
	}

	// Directory contains one or more changes. Recurse into it.
	return model_core.PatchedMessage[*model_filesystem_pb.Directory, model_core.FileBackedObjectLocation]{},
		&capturableChangeTrackingDirectory{
			options:   cd.options,
			directory: dChild,
		},
		nil
}

func (cd *capturableChangeTrackingDirectory) OpenForFileMerkleTreeCreation(name path.Component) (model_filesystem.CapturableFile[model_core.FileBackedObjectLocation], error) {
	file, ok := cd.directory.files[name]
	if !ok {
		panic("attempted to enter non-existent file")
	}
	return &capturableChangeTrackingFile{
		options:  cd.options,
		contents: file.contents,
	}, nil
}

func (cd *capturableChangeTrackingDirectory) ReadDir() ([]filesystem.FileInfo, error) {
	d := cd.directory
	infos := make(filesystem.FileInfoList, 0, len(d.directories)+len(d.files)+len(d.symlinks))
	for name := range d.directories {
		infos = append(infos, filesystem.NewFileInfo(name, filesystem.FileTypeDirectory, false))
	}
	for name, file := range d.files {
		infos = append(infos, filesystem.NewFileInfo(name, filesystem.FileTypeRegularFile, file.isExecutable))
	}
	for name := range d.symlinks {
		infos = append(infos, filesystem.NewFileInfo(name, filesystem.FileTypeSymlink, false))
	}
	sort.Sort(infos)
	return infos, nil
}

func (cd *capturableChangeTrackingDirectory) Readlink(name path.Component) (path.Parser, error) {
	target, ok := cd.directory.symlinks[name]
	if !ok {
		panic("attempted to read non-existent symbolic link")
	}
	return path.UNIXFormat.NewParser(target), nil
}

type capturableChangeTrackingFile struct {
	options  *capturableChangeTrackingDirectoryOptions
	contents changeTrackingFileContents
}

func (cf *capturableChangeTrackingFile) CreateFileMerkleTree(ctx context.Context) (model_core.PatchedMessage[*model_filesystem_pb.FileContents, model_core.FileBackedObjectLocation], error) {
	return cf.contents.createFileMerkleTree(ctx, cf.options)
}

func (cf *capturableChangeTrackingFile) Discard() {}

type strippingComponentWalker struct {
	remainder            path.ComponentWalker
	additionalStripCount int
}

func newStrippingComponentWalker(remainder path.ComponentWalker, stripCount int) path.ComponentWalker {
	return strippingComponentWalker{
		remainder:            remainder,
		additionalStripCount: stripCount,
	}.stripComponent()
}

func (cw strippingComponentWalker) stripComponent() path.ComponentWalker {
	if cw.additionalStripCount > 0 {
		return strippingComponentWalker{
			remainder:            cw.remainder,
			additionalStripCount: cw.additionalStripCount - 1,
		}
	}
	return cw.remainder
}

func (cw strippingComponentWalker) OnDirectory(name path.Component) (path.GotDirectoryOrSymlink, error) {
	return path.GotDirectory{
		Child:        cw.stripComponent(),
		IsReversible: false,
	}, nil
}

func (cw strippingComponentWalker) OnTerminal(name path.Component) (*path.GotSymlink, error) {
	return nil, nil
}

func (cw strippingComponentWalker) OnUp() (path.ComponentWalker, error) {
	return cw.stripComponent(), nil
}

type changeTrackingDirectoryExistingFileResolver struct {
	loadOptions *changeTrackingDirectoryLoadOptions
	stack       util.NonEmptyStack[*changeTrackingDirectory]
	file        *changeTrackingFile
}

func (cw *changeTrackingDirectoryExistingFileResolver) OnDirectory(name path.Component) (path.GotDirectoryOrSymlink, error) {
	d := cw.stack.Peek()
	if err := d.maybeLoadContents(cw.loadOptions); err != nil {
		return nil, err
	}
	if dChild, ok := d.directories[name]; ok {
		cw.stack.Push(dChild)
		return path.GotDirectory{
			Child:        cw,
			IsReversible: true,
		}, nil
	}
	panic("TODO")
}

func (cw *changeTrackingDirectoryExistingFileResolver) OnTerminal(name path.Component) (*path.GotSymlink, error) {
	d := cw.stack.Peek()
	if err := d.maybeLoadContents(cw.loadOptions); err != nil {
		return nil, err
	}
	f, ok := d.files[name]
	if !ok {
		return nil, errors.New("file does not exist")
	}
	cw.file = f
	return nil, nil
}

func (cw *changeTrackingDirectoryExistingFileResolver) OnUp() (path.ComponentWalker, error) {
	panic("TODO")
}

type changeTrackingDirectoryNewFileResolver struct {
	loadOptions      *changeTrackingDirectoryLoadOptions
	currentDirectory *changeTrackingDirectory

	name *path.Component
}

func (r *changeTrackingDirectoryNewFileResolver) OnDirectory(name path.Component) (path.GotDirectoryOrSymlink, error) {
	d := r.currentDirectory
	if err := d.maybeLoadContents(r.loadOptions); err != nil {
		return nil, err
	}

	if dChild, ok := d.directories[name]; ok {
		r.currentDirectory = dChild
		return path.GotDirectory{
			Child:        r,
			IsReversible: true,
		}, nil
	}

	return nil, errors.New("directory does not exist")
}

func (r *changeTrackingDirectoryNewFileResolver) OnTerminal(name path.Component) (*path.GotSymlink, error) {
	d := r.currentDirectory
	if _, ok := d.directories[name]; ok {
		return nil, errors.New("path resolves to a directory")
	}
	if _, ok := d.symlinks[name]; ok {
		return nil, errors.New("path resolves to a symbolic link")
	}
	r.name = &name
	return nil, nil
}

func (r *changeTrackingDirectoryNewFileResolver) OnUp() (path.ComponentWalker, error) {
	return nil, errors.New("path cannot go up")
}

func (c *baseComputer) fetchModuleFromRegistry(ctx context.Context, module *model_analysis_pb.BuildListModule, e RepoEnvironment) (PatchedRepoValue, error) {
	fileReader, gotFileReader := e.GetFileReaderValue(&model_analysis_pb.FileReader_Key{})
	directoryCreationParameters, gotDirectoryCreationParameters := e.GetDirectoryCreationParametersObjectValue(&model_analysis_pb.DirectoryCreationParametersObject_Key{})
	fileCreationParameters, gotFileCreationParameters := e.GetFileCreationParametersObjectValue(&model_analysis_pb.FileCreationParametersObject_Key{})
	if !gotFileReader || !gotDirectoryCreationParameters || !gotFileCreationParameters {
		return PatchedRepoValue{}, evaluation.ErrMissingDependency
	}

	sourceJSONURL, err := url.JoinPath(
		module.RegistryUrl,
		"modules",
		module.Name,
		module.Version,
		"source.json",
	)
	if err != nil {
		return PatchedRepoValue{}, fmt.Errorf("failed to construct URL for module %s with version %s in registry %#v: %w", module.Name, module.Version, module.RegistryUrl, err)
	}

	sourceJSONContentsValue := e.GetHttpFileContentsValue(&model_analysis_pb.HttpFileContents_Key{Url: sourceJSONURL})
	if !sourceJSONContentsValue.IsSet() {
		return PatchedRepoValue{}, evaluation.ErrMissingDependency
	}
	if sourceJSONContentsValue.Message.Exists == nil {
		return PatchedRepoValue{}, fmt.Errorf("file at URL %#v does not exist", sourceJSONURL)
	}
	sourceJSONContentsEntry, err := model_filesystem.NewFileContentsEntryFromProto(
		model_core.Message[*model_filesystem_pb.FileContents]{
			Message:            sourceJSONContentsValue.Message.Exists.Contents,
			OutgoingReferences: sourceJSONContentsValue.OutgoingReferences,
		},
		c.buildSpecificationReference.GetReferenceFormat(),
	)
	if err != nil {
		return PatchedRepoValue{}, fmt.Errorf("invalid file contents: %w", err)
	}

	sourceJSONData, err := fileReader.FileReadAll(ctx, sourceJSONContentsEntry, 1<<20)
	if err != nil {
		return PatchedRepoValue{}, err
	}
	var sourceJSON sourceJSON
	if err := json.Unmarshal(sourceJSONData, &sourceJSON); err != nil {
		return PatchedRepoValue{}, fmt.Errorf("invalid JSON contents for %#v: %w", sourceJSONURL, err)
	}

	var archiveFormat model_analysis_pb.HttpArchiveContents_Key_Format
	if strings.HasSuffix(sourceJSON.URL, ".tar.gz") {
		archiveFormat = model_analysis_pb.HttpArchiveContents_Key_TAR_GZ
	} else if strings.HasSuffix(sourceJSON.URL, ".zip") {
		archiveFormat = model_analysis_pb.HttpArchiveContents_Key_ZIP
	} else {
		return PatchedRepoValue{}, fmt.Errorf("cannot derive archive format from file extension of URL %#v", sourceJSONURL)
	}

	// Download source archive and all patches that need to be applied.
	missingDependencies := false
	archiveContentsValue := e.GetHttpArchiveContentsValue(&model_analysis_pb.HttpArchiveContents_Key{
		Url:       sourceJSON.URL,
		Integrity: sourceJSON.Integrity,
		Format:    archiveFormat,
	})
	if !archiveContentsValue.IsSet() {
		missingDependencies = true
	}

	patchFilenames := slices.Sorted(maps.Keys(sourceJSON.Patches))
	patchContentsValues := make([]model_core.Message[*model_analysis_pb.HttpFileContents_Value], 0, len(patchFilenames))
	for _, filename := range patchFilenames {
		patchURL, err := url.JoinPath(
			module.RegistryUrl,
			"modules",
			module.Name,
			module.Version,
			"patches",
			filename,
		)
		if err != nil {
			return PatchedRepoValue{}, fmt.Errorf("failed to construct URL for patch %s of module %s with version %s in registry %#v: %s", filename, module.Name, module.Version, module.RegistryUrl, err)
		}
		patchContentsValue := e.GetHttpFileContentsValue(&model_analysis_pb.HttpFileContents_Key{
			Url:       patchURL,
			Integrity: sourceJSON.Patches[filename],
		})
		if !patchContentsValue.IsSet() {
			missingDependencies = true
		}
		patchContentsValues = append(patchContentsValues, patchContentsValue)
	}

	if missingDependencies {
		return PatchedRepoValue{}, evaluation.ErrMissingDependency
	}

	if archiveContentsValue.Message.Exists == nil {
		return PatchedRepoValue{}, fmt.Errorf("file at URL %#v does not exist", sourceJSON.URL)
	}
	rootDirectory := &changeTrackingDirectory{
		currentReference: model_core.Message[*model_core_pb.Reference]{
			Message:            archiveContentsValue.Message.Exists,
			OutgoingReferences: archiveContentsValue.OutgoingReferences,
		},
	}

	// Strip the provided directory prefix.
	directoryReader := model_parser.NewStorageBackedParsedObjectReader(
		c.objectDownloader,
		directoryCreationParameters.GetEncoder(),
		model_parser.NewMessageObjectParser[object.LocalReference, model_filesystem_pb.Directory](),
	)
	leavesReader := model_parser.NewStorageBackedParsedObjectReader(
		c.objectDownloader,
		directoryCreationParameters.GetEncoder(),
		model_parser.NewMessageObjectParser[object.LocalReference, model_filesystem_pb.Leaves](),
	)
	loadOptions := &changeTrackingDirectoryLoadOptions{
		context:         ctx,
		directoryReader: directoryReader,
		leavesReader:    leavesReader,
	}
	rootDirectoryResolver := changeTrackingDirectoryResolver{
		loadOptions:      loadOptions,
		currentDirectory: rootDirectory,
	}
	if err := path.Resolve(
		path.UNIXFormat.NewParser(sourceJSON.StripPrefix),
		path.NewRelativeScopeWalker(&rootDirectoryResolver),
	); err != nil {
		return PatchedRepoValue{}, fmt.Errorf("failed to strip prefix %#v from contents of %#v: %s", sourceJSON.StripPrefix, sourceJSON.URL, err)
	}
	rootDirectory = rootDirectoryResolver.currentDirectory

	patchedFiles, err := c.filePool.NewFile()
	if err != nil {
		return PatchedRepoValue{}, err
	}
	defer patchedFiles.Close()
	patchedFilesWriter := &sectionWriter{w: patchedFiles}

	// TODO: Apply patches!
	for patchIndex, patchContentsValue := range patchContentsValues {
		if patchContentsValue.Message.Exists == nil {
			return PatchedRepoValue{}, fmt.Errorf("patch at URL %#v does not exist", patchFilenames[patchIndex])
		}
		patchContentsEntry, err := model_filesystem.NewFileContentsEntryFromProto(
			model_core.Message[*model_filesystem_pb.FileContents]{
				Message:            patchContentsValue.Message.Exists.Contents,
				OutgoingReferences: patchContentsValue.OutgoingReferences,
			},
			c.buildSpecificationReference.GetReferenceFormat(),
		)
		if err != nil {
			return PatchedRepoValue{}, fmt.Errorf("invalid file contents for patch %#v: %s", patchFilenames[patchIndex], err)
		}

		files, _, err := gitdiff.Parse(fileReader.FileOpenRead(ctx, patchContentsEntry, 0))
		if err != nil {
			return PatchedRepoValue{}, fmt.Errorf("invalid patch %#v: %s", patchFilenames[patchIndex], err)
		}

		for _, file := range files {
			var fileContents changeTrackingFileContents
			isExecutable := false
			if !file.IsNew {
				r := &changeTrackingDirectoryExistingFileResolver{
					loadOptions: loadOptions,
					stack:       util.NewNonEmptyStack(rootDirectory),
				}
				if err := path.Resolve(
					path.UNIXFormat.NewParser(file.OldName),
					path.NewRelativeScopeWalker(
						newStrippingComponentWalker(r, sourceJSON.PatchStrip),
					),
				); err != nil {
					return PatchedRepoValue{}, fmt.Errorf("cannot resolve path %#v: %w", file.OldName, err)
				}
				if r.file == nil {
					return PatchedRepoValue{}, fmt.Errorf("path %#v does not resolve to a file", file.OldName)
				}
				fileContents = r.file.contents
				isExecutable = r.file.isExecutable
			}

			// Compute the offsets at which changes need to
			// be made to the file.
			var srcScan io.Reader
			if fileContents == nil {
				srcScan = bytes.NewBuffer(nil)
			} else {
				srcScan, err = fileContents.openRead(ctx, c.buildSpecificationReference.GetReferenceFormat(), fileReader, patchedFiles)
				if err != nil {
					return PatchedRepoValue{}, fmt.Errorf("failed to open file %#v: %s", file.OldName, err)
				}
			}
			fragmentsOffsetsBytes, err := diff.FindTextFragmentOffsetsBytes(file.TextFragments, bufio.NewReader(srcScan))
			if err != nil {
				return PatchedRepoValue{}, fmt.Errorf("failed to apply patch %#v to file %#v: %w", patchFilenames[patchIndex], file.OldName, err)
			}

			var srcReplace io.Reader
			if fileContents == nil {
				srcReplace = bytes.NewBuffer(nil)
			} else {
				srcReplace, err = fileContents.openRead(ctx, c.buildSpecificationReference.GetReferenceFormat(), fileReader, patchedFiles)
				if err != nil {
					return PatchedRepoValue{}, fmt.Errorf("failed to open file %#v: %w", file.OldName, err)
				}
			}

			patchedFileOffsetBytes := patchedFilesWriter.offsetBytes
			if err := diff.ReplaceTextFragments(patchedFilesWriter, srcReplace, file.TextFragments, fragmentsOffsetsBytes); err != nil {
				return PatchedRepoValue{}, fmt.Errorf("failed to replace text fragments for patch %#v to %#v: %w", patchFilenames[patchIndex], file.OldName, err)
			}

			r := &changeTrackingDirectoryNewFileResolver{
				loadOptions:      loadOptions,
				currentDirectory: rootDirectory,
			}
			if err := path.Resolve(
				path.UNIXFormat.NewParser(file.NewName),
				path.NewRelativeScopeWalker(
					newStrippingComponentWalker(r, sourceJSON.PatchStrip),
				),
			); err != nil {
				return PatchedRepoValue{}, fmt.Errorf("cannot resolve path %#v: %w", file.NewName, err)
			}
			if r.name == nil {
				return PatchedRepoValue{}, fmt.Errorf("path %#v does not resolve to a file", file.NewName)
			}

			if file.NewMode != 0 {
				isExecutable = file.NewMode&0o111 != 0
			}
			r.currentDirectory.setFile(*r.name, &changeTrackingFile{
				isExecutable: isExecutable,
				contents: patchedFileContents{
					offsetBytes: patchedFileOffsetBytes,
					sizeBytes:   patchedFilesWriter.offsetBytes - patchedFileOffsetBytes,
				},
			})
		}
	}

	return c.returnRepoMerkleTree(
		ctx,
		rootDirectory,
		directoryCreationParameters,
		fileCreationParameters,
		patchedFiles,
	)
}

// newRepositoryOS creates a repository_os object that can be embedded
// into module_ctx and repository_ctx objects.
func newRepositoryOS(repoPlatform *model_analysis_pb.RegisteredRepoPlatform_Value) starlark.Value {
	environ := starlark.NewDict(len(repoPlatform.RepositoryOsEnviron))
	for _, entry := range repoPlatform.RepositoryOsEnviron {
		environ.SetKey(starlark.String(entry.Name), starlark.String(entry.Value))
	}
	s := starlarkstruct.FromStringDict(starlarkstruct.Default, starlark.StringDict{
		"arch":    starlark.String(repoPlatform.RepositoryOsArch),
		"environ": environ,
		"name":    starlark.String(repoPlatform.RepositoryOsName),
	})
	s.Freeze()
	return s
}

func (c *baseComputer) fetchModuleExtensionRepo(ctx context.Context, canonicalRepo label.CanonicalRepo, e RepoEnvironment) (PatchedRepoValue, error) {
	// Obtain the definition of the declared repo.
	repoValue := e.GetModuleExtensionRepoValue(&model_analysis_pb.ModuleExtensionRepo_Key{
		CanonicalRepo: canonicalRepo.String(),
	})
	allBuiltinsModulesNames := e.GetBuiltinsModuleNamesValue(&model_analysis_pb.BuiltinsModuleNames_Key{})
	directoryCreationParameters, gotDirectoryCreationParameters := e.GetDirectoryCreationParametersObjectValue(&model_analysis_pb.DirectoryCreationParametersObject_Key{})
	fileCreationParameters, gotFileCreationParameters := e.GetFileCreationParametersObjectValue(&model_analysis_pb.FileCreationParametersObject_Key{})
	fileReader, gotFileReader := e.GetFileReaderValue(&model_analysis_pb.FileReader_Key{})
	repoPlatform := e.GetRegisteredRepoPlatformValue(&model_analysis_pb.RegisteredRepoPlatform_Key{})
	if !repoValue.IsSet() || !allBuiltinsModulesNames.IsSet() || !gotDirectoryCreationParameters || !gotFileCreationParameters || !gotFileReader || !repoPlatform.IsSet() {
		return PatchedRepoValue{}, evaluation.ErrMissingDependency
	}
	repo := repoValue.Message.Definition
	if repo == nil {
		return PatchedRepoValue{}, errors.New("no repo definition present")
	}

	// Obtain the definition of the repository rule used by the repo.
	repositoryRule, gotRepositoryRule := e.GetRepositoryRuleObjectValue(&model_analysis_pb.RepositoryRuleObject_Key{
		Identifier: repo.RepositoryRuleIdentifier,
	})
	if !gotRepositoryRule {
		return PatchedRepoValue{}, evaluation.ErrMissingDependency
	}

	attrs := starlark.StringDict{}
	attrValues := repo.AttrValues
	for _, publicAttr := range repositoryRule.Attrs.Public {
		if len(attrValues) > 0 && attrValues[0].Name == publicAttr.Name {
			decodedValue, err := model_starlark.DecodeValue(
				model_core.Message[*model_starlark_pb.Value]{
					Message:            attrValues[0].Value,
					OutgoingReferences: repoValue.OutgoingReferences,
				},
				/* currentIdentifier = */ nil,
				c.getValueDecodingOptions(ctx, func(canonicalLabel label.CanonicalLabel) (starlark.Value, error) {
					panic("TODO")
				}),
			)
			if err != nil {
				return PatchedRepoValue{}, err
			}
			attrs[publicAttr.Name] = decodedValue
			attrValues = attrValues[1:]
		} else if d := publicAttr.Default; d != nil {
			attrs[publicAttr.Name] = d
		} else {
			return PatchedRepoValue{}, fmt.Errorf("missing value for mandatory attribute %#v", publicAttr.Name)
		}
	}
	if len(attrValues) > 0 {
		return PatchedRepoValue{}, fmt.Errorf("unknown attribute %#v", attrValues[0].Name)
	}
	for name, value := range repositoryRule.Attrs.Private {
		attrs[name] = value
	}

	rootDirectory := &changeTrackingDirectory{}
	loadOptions := &changeTrackingDirectoryLoadOptions{
		context: ctx,
		// TODO: Add directoryReader and leavesReader to support
		// repository_ctx.execute.
	}

	patchedFiles, err := c.filePool.NewFile()
	if err != nil {
		return PatchedRepoValue{}, err
	}
	defer patchedFiles.Close()
	patchedFilesWriter := &sectionWriter{w: patchedFiles}

	// Invoke the implementation function.
	thread := c.newStarlarkThread(ctx, e, allBuiltinsModulesNames.Message.BuiltinsModuleNames)

	_, err = starlark.Call(
		thread,
		repositoryRule.Implementation,
		/* args = */ starlark.Tuple{
			starlarkstruct.FromStringDict(starlarkstruct.Default, starlark.StringDict{
				"attr": starlarkstruct.FromStringDict(starlarkstruct.Default, attrs),
				"file": starlark.NewBuiltin(
					"repository_ctx.file",
					func(thread *starlark.Thread, b *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
						content := ""
						executable := true
						legacyUTF8 := true
						var filePath label.CanonicalLabel
						if err := starlark.UnpackArgs(
							b.Name(), args, kwargs,
							// TODO: Permit arguments of type path.
							"path", unpack.Bind(thread, &filePath, model_starlark.NewLabelOrStringUnpackerInto(canonicalRepo.GetRootPackage())),
							"content?", unpack.Bind(thread, &content, unpack.String),
							"executable?", unpack.Bind(thread, &executable, unpack.Bool),
							"legacy_utf8?", unpack.Bind(thread, &legacyUTF8, unpack.Bool),
						); err != nil {
							return nil, err
						}
						if filePathRepo := filePath.GetCanonicalRepo(); filePathRepo != canonicalRepo {
							return nil, fmt.Errorf("path resolves to file in repo %#v, while repo %#v was expected", filePathRepo.String(), canonicalRepo.String())
						}

						r := &changeTrackingDirectoryNewFileResolver{
							loadOptions:      loadOptions,
							currentDirectory: rootDirectory,
						}
						if err := path.Resolve(
							path.UNIXFormat.NewParser(filePath.GetRepoRelativePath()),
							path.NewRelativeScopeWalker(r),
						); err != nil {
							return nil, fmt.Errorf("cannot resolve %#v: %w", filePath.String(), err)
						}
						if r.name == nil {
							return nil, fmt.Errorf("%#v does not resolve to a file", filePath.String())
						}

						// TODO: Do UTF-8 -> ISO-8859-1
						// conversion if legacy_utf8=False.
						patchedFileOffsetBytes := patchedFilesWriter.offsetBytes
						if _, err := patchedFilesWriter.WriteString(content); err != nil {
							return nil, fmt.Errorf("failed to write to file at %#v: %w", filePath.String(), err)
						}

						r.currentDirectory.setFile(*r.name, &changeTrackingFile{
							isExecutable: executable,
							contents: patchedFileContents{
								offsetBytes: patchedFileOffsetBytes,
								sizeBytes:   patchedFilesWriter.offsetBytes - patchedFileOffsetBytes,
							},
						})
						return starlark.None, nil
					},
				),
				"os": newRepositoryOS(repoPlatform.Message),
				"path": starlark.NewBuiltin(
					"repository_ctx.path",
					func(thread *starlark.Thread, b *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
						var filePath model_starlark.Path
						if err := starlark.UnpackArgs(
							b.Name(), args, kwargs,
							"path", unpack.Bind(thread, &filePath, model_starlark.NewPathOrLabelOrStringUnpackerInto()),
						); err != nil {
							return nil, err
						}
						return filePath, nil
					},
				),
				"template": starlark.NewBuiltin(
					"repository_ctx.template",
					func(thread *starlark.Thread, b *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
						if len(args) > 4 {
							return nil, fmt.Errorf("%s: got %d positional arguments, want at most 4", b.Name(), len(args))
						}
						var filePath label.CanonicalLabel
						var templatePath label.CanonicalLabel
						var substitutions map[string]string
						executable := true
						var watchTemplate string
						if err := starlark.UnpackArgs(
							b.Name(), args, kwargs,
							// TODO: Permit arguments of type path.
							"path", unpack.Bind(thread, &filePath, model_starlark.NewLabelOrStringUnpackerInto(canonicalRepo.GetRootPackage())),
							"template", unpack.Bind(thread, &templatePath, model_starlark.NewLabelOrStringUnpackerInto(canonicalRepo.GetRootPackage())),
							"substitutions?", unpack.Bind(thread, &substitutions, unpack.Dict(unpack.String, unpack.String)),
							"executable?", unpack.Bind(thread, &executable, unpack.Bool),
							"watch_template?", unpack.Bind(thread, &watchTemplate, unpack.String),
						); err != nil {
							return nil, err
						}

						needles := make([][]byte, 0, len(substitutions))
						replacements := make([][]byte, 0, len(substitutions))
						for _, needle := range slices.Sorted(maps.Keys(substitutions)) {
							needles = append(needles, []byte(needle))
							replacements = append(replacements, []byte(substitutions[needle]))
						}
						searchAndReplacer, err := search.NewMultiSearchAndReplacer(needles)
						if err != nil {
							return nil, fmt.Errorf("invalid substitution keys: %w", err)
						}

						// Load the template file.
						templateProperties := e.GetFilePropertiesValue(&model_analysis_pb.FileProperties_Key{
							CanonicalRepo: templatePath.GetCanonicalRepo().String(),
							Path:          templatePath.GetRepoRelativePath(),
						})
						if !templateProperties.IsSet() {
							return nil, evaluation.ErrMissingDependency
						}
						if templateProperties.Message.Exists == nil {
							return nil, fmt.Errorf("template %#v does not exist", templatePath.String())
						}
						templateContents, err := model_filesystem.NewFileContentsEntryFromProto(
							model_core.Message[*model_filesystem_pb.FileContents]{
								Message:            templateProperties.Message.Exists.Contents,
								OutgoingReferences: templateProperties.OutgoingReferences,
							},
							c.buildSpecificationReference.GetReferenceFormat(),
						)
						if err != nil {
							return nil, fmt.Errorf("invalid file contents for template %#v: %s", templatePath.String(), err)
						}
						templateFile := fileReader.FileOpenRead(ctx, templateContents, 0)

						r := &changeTrackingDirectoryNewFileResolver{
							loadOptions:      loadOptions,
							currentDirectory: rootDirectory,
						}
						if err := path.Resolve(
							path.UNIXFormat.NewParser(filePath.GetRepoRelativePath()),
							path.NewRelativeScopeWalker(r),
						); err != nil {
							return nil, fmt.Errorf("cannot resolve %#v: %w", filePath.String(), err)
						}
						if r.name == nil {
							return nil, fmt.Errorf("%#v does not resolve to a file", filePath.String())
						}

						// Perform substitutions.
						patchedFileOffsetBytes := patchedFilesWriter.offsetBytes
						if err := searchAndReplacer.SearchAndReplace(patchedFilesWriter, templateFile, replacements); err != nil {
							return nil, fmt.Errorf("failed to write to file at %#v: %w", filePath.String(), err)
						}

						r.currentDirectory.setFile(*r.name, &changeTrackingFile{
							isExecutable: executable,
							contents: patchedFileContents{
								offsetBytes: patchedFileOffsetBytes,
								sizeBytes:   patchedFilesWriter.offsetBytes - patchedFileOffsetBytes,
							},
						})
						return starlark.None, nil
					},
				),
			}),
		},
		/* kwargs = */ nil,
	)
	if err != nil {
		var evalErr *starlark.EvalError
		if !errors.Is(err, evaluation.ErrMissingDependency) && errors.As(err, &evalErr) {
			return PatchedRepoValue{}, errors.New(evalErr.Backtrace())
		}
		return PatchedRepoValue{}, err
	}

	return c.returnRepoMerkleTree(
		ctx,
		rootDirectory,
		directoryCreationParameters,
		fileCreationParameters,
		patchedFiles,
	)
}

func (c *baseComputer) returnRepoMerkleTree(ctx context.Context, rootDirectory *changeTrackingDirectory, directoryCreationParameters *model_filesystem.DirectoryCreationParameters, fileCreationParameters *model_filesystem.FileCreationParameters, patchedFiles io.ReaderAt) (PatchedRepoValue, error) {
	if r := rootDirectory.currentReference; r.IsSet() {
		// Directory remained completely unmodified. Simply
		// return the original directory.
		rootDirectoryReference := model_core.NewPatchedMessageFromExisting(
			r,
			func(index int) dag.ObjectContentsWalker {
				return dag.ExistingObjectContentsWalker
			},
		)
		return PatchedRepoValue{
			Message: &model_analysis_pb.Repo_Value{
				RootDirectoryReference: rootDirectoryReference.Message,
			},
			Patcher: rootDirectoryReference.Patcher,
		}, nil
	}

	// We had to strip a path prefix or apply one or more patches.
	// This means we need to create a new Merkle tree.
	merkleTreeNodes, err := c.filePool.NewFile()
	if err != nil {
		return PatchedRepoValue{}, err
	}
	defer func() {
		if merkleTreeNodes != nil {
			merkleTreeNodes.Close()
		}
	}()

	group, groupCtx := errgroup.WithContext(ctx)
	var rootDirectoryMessage model_core.PatchedMessage[*model_filesystem_pb.Directory, model_core.FileBackedObjectLocation]
	directoryReader := model_parser.NewStorageBackedParsedObjectReader(
		c.objectDownloader,
		directoryCreationParameters.GetEncoder(),
		model_parser.NewMessageObjectParser[object.LocalReference, model_filesystem_pb.Directory](),
	)
	fileWritingMerkleTreeCapturer := model_core.NewFileWritingMerkleTreeCapturer(&sectionWriter{w: merkleTreeNodes})
	group.Go(func() error {
		return model_filesystem.CreateDirectoryMerkleTree(
			groupCtx,
			semaphore.NewWeighted(1),
			group,
			directoryCreationParameters,
			&capturableChangeTrackingDirectory{
				options: &capturableChangeTrackingDirectoryOptions{
					context:                groupCtx,
					directoryReader:        directoryReader,
					fileCreationParameters: fileCreationParameters,
					fileMerkleTreeCapturer: model_filesystem.NewFileWritingFileMerkleTreeCapturer(fileWritingMerkleTreeCapturer),
					patchedFiles:           patchedFiles,
				},
				directory: rootDirectory,
			},
			model_filesystem.NewFileWritingDirectoryMerkleTreeCapturer(fileWritingMerkleTreeCapturer),
			&rootDirectoryMessage,
		)
	})
	if err := group.Wait(); err != nil {
		return PatchedRepoValue{}, err
	}

	// Store the root directory itself. We don't embed it into the
	// response, as that prevents it from being accessed separately.
	contents, children, err := model_core.MarshalAndEncodePatchedMessage(
		rootDirectoryMessage,
		c.buildSpecificationReference.GetReferenceFormat(),
		directoryCreationParameters.GetEncoder(),
	)
	if err != nil {
		return PatchedRepoValue{}, err
	}
	capturedRootDirectory := fileWritingMerkleTreeCapturer.CaptureObject(contents, children)

	// Finalize writing of Merkle tree nodes to disk, and provide
	// read access to the nodes, so that they can be uploaded.
	if err := fileWritingMerkleTreeCapturer.Flush(); err != nil {
		return PatchedRepoValue{}, err
	}
	objectContentsWalkerFactory := model_core.NewFileReadingObjectContentsWalkerFactory(merkleTreeNodes)
	defer objectContentsWalkerFactory.Release()
	merkleTreeNodes = nil

	patcher := model_core.NewReferenceMessagePatcher[dag.ObjectContentsWalker]()
	rootReference := contents.GetReference()
	return PatchedRepoValue{
		Message: &model_analysis_pb.Repo_Value{
			RootDirectoryReference: patcher.AddReference(
				rootReference,
				objectContentsWalkerFactory.CreateObjectContentsWalker(rootReference, capturedRootDirectory),
			),
		},
		Patcher: patcher,
	}, nil
}

func (c *baseComputer) ComputeRepoValue(ctx context.Context, key *model_analysis_pb.Repo_Key, e RepoEnvironment) (PatchedRepoValue, error) {
	canonicalRepo, err := label.NewCanonicalRepo(key.CanonicalRepo)
	if err != nil {
		return PatchedRepoValue{}, fmt.Errorf("invalid canonical repo: %w", err)
	}

	if _, _, ok := canonicalRepo.GetModuleExtension(); ok {
		return c.fetchModuleExtensionRepo(ctx, canonicalRepo, e)
	} else {
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
						RootDirectoryReference: rootDirectoryReference.Message,
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
			buildList := buildListValue.Message.BuildList
			if i := sort.Search(
				len(buildList),
				func(i int) bool { return buildList[i].Name >= moduleName },
			); i < len(buildList) && buildList[i].Name == moduleName {
				return c.fetchModuleFromRegistry(ctx, buildList[i], e)
			}
		}
	}

	return PatchedRepoValue{}, errors.New("repo not found")
}
