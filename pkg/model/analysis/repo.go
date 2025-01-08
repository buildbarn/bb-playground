package analysis

import (
	"bufio"
	"bytes"
	"context"
	"encoding/base64"
	"encoding/hex"
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
	"github.com/buildbarn/bb-playground/pkg/model/core/btree"
	model_encoding "github.com/buildbarn/bb-playground/pkg/model/encoding"
	model_filesystem "github.com/buildbarn/bb-playground/pkg/model/filesystem"
	model_parser "github.com/buildbarn/bb-playground/pkg/model/parser"
	model_starlark "github.com/buildbarn/bb-playground/pkg/model/starlark"
	model_analysis_pb "github.com/buildbarn/bb-playground/pkg/proto/model/analysis"
	model_command_pb "github.com/buildbarn/bb-playground/pkg/proto/model/command"
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
	"google.golang.org/protobuf/types/known/durationpb"

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

func (d *changeTrackingDirectory) getOrCreateDirectory(name path.Component) (*changeTrackingDirectory, error) {
	dChild, ok := d.directories[name]
	if !ok {
		if _, ok := d.files[name]; ok {
			return nil, errors.New("a file with this name already exists")
		}
		if _, ok := d.symlinks[name]; ok {
			return nil, errors.New("a symbolic link with this name already exists")
		}
		if d.directories == nil {
			d.directories = map[path.Component]*changeTrackingDirectory{}
		}
		dChild = &changeTrackingDirectory{}
		d.directories[name] = dChild
	}
	return dChild, nil
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
	loadOptions *changeTrackingDirectoryLoadOptions

	stack util.NonEmptyStack[*changeTrackingDirectory]
	name  *path.Component
}

func (r *changeTrackingDirectoryNewFileResolver) OnAbsolute() (path.ComponentWalker, error) {
	r.stack.PopAll()
	return r, nil
}

func (r *changeTrackingDirectoryNewFileResolver) OnRelative() (path.ComponentWalker, error) {
	return r, nil
}

func (r *changeTrackingDirectoryNewFileResolver) OnDriveLetter(driveLetter rune) (path.ComponentWalker, error) {
	return nil, errors.New("drive letters are not supported")
}

func (r *changeTrackingDirectoryNewFileResolver) OnDirectory(name path.Component) (path.GotDirectoryOrSymlink, error) {
	d := r.stack.Peek()
	if err := d.maybeLoadContents(r.loadOptions); err != nil {
		return nil, err
	}

	dChild, err := d.getOrCreateDirectory(name)
	if err != nil {
		return nil, fmt.Errorf("failed to create directory %#v: %w", name.String(), err)
	}

	r.stack.Push(dChild)
	return path.GotDirectory{
		Child:        r,
		IsReversible: true,
	}, nil
}

func (r *changeTrackingDirectoryNewFileResolver) OnTerminal(name path.Component) (*path.GotSymlink, error) {
	d := r.stack.Peek()
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
	if _, ok := r.stack.PopSingle(); !ok {
		return nil, errors.New("path resolves to a location above the root directory")
	}
	return r, nil
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

	sourceJSONContentsValue := e.GetHttpFileContentsValue(&model_analysis_pb.HttpFileContents_Key{Urls: []string{sourceJSONURL}})
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
		Urls:      []string{sourceJSON.URL},
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
			Urls:      []string{patchURL},
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
	patchedFilesWriter := model_filesystem.NewSectionWriter(patchedFiles)

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

			patchedFileOffsetBytes := patchedFilesWriter.GetOffsetBytes()
			if err := diff.ReplaceTextFragments(patchedFilesWriter, srcReplace, file.TextFragments, fragmentsOffsetsBytes); err != nil {
				return PatchedRepoValue{}, fmt.Errorf("failed to replace text fragments for patch %#v to %#v: %w", patchFilenames[patchIndex], file.OldName, err)
			}

			r := &changeTrackingDirectoryNewFileResolver{
				loadOptions: loadOptions,
				stack:       util.NewNonEmptyStack(rootDirectory),
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
			r.stack.Peek().setFile(*r.name, &changeTrackingFile{
				isExecutable: isExecutable,
				contents: patchedFileContents{
					offsetBytes: patchedFileOffsetBytes,
					sizeBytes:   patchedFilesWriter.GetOffsetBytes() - patchedFileOffsetBytes,
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

type moduleOrRepositoryContextEnvironment interface {
	GetActionResultValue(model_core.PatchedMessage[*model_analysis_pb.ActionResult_Key, dag.ObjectContentsWalker]) model_core.Message[*model_analysis_pb.ActionResult_Value]
	GetCommandEncoderObjectValue(*model_analysis_pb.CommandEncoderObject_Key) (model_encoding.BinaryEncoder, bool)
	GetDirectoryCreationParametersObjectValue(*model_analysis_pb.DirectoryCreationParametersObject_Key) (*model_filesystem.DirectoryCreationParameters, bool)
	GetDirectoryCreationParametersValue(*model_analysis_pb.DirectoryCreationParameters_Key) model_core.Message[*model_analysis_pb.DirectoryCreationParameters_Value]
	GetFileCreationParametersObjectValue(*model_analysis_pb.FileCreationParametersObject_Key) (*model_filesystem.FileCreationParameters, bool)
	GetFileCreationParametersValue(*model_analysis_pb.FileCreationParameters_Key) model_core.Message[*model_analysis_pb.FileCreationParameters_Value]
	GetFileReaderValue(*model_analysis_pb.FileReader_Key) (*model_filesystem.FileReader, bool)
	GetHttpFileContentsValue(*model_analysis_pb.HttpFileContents_Key) model_core.Message[*model_analysis_pb.HttpFileContents_Value]
	GetRegisteredRepoPlatformValue(*model_analysis_pb.RegisteredRepoPlatform_Key) model_core.Message[*model_analysis_pb.RegisteredRepoPlatform_Value]
	GetRepoValue(*model_analysis_pb.Repo_Key) model_core.Message[*model_analysis_pb.Repo_Value]
	GetStableInputRootPathObjectValue(*model_analysis_pb.StableInputRootPathObject_Key) (*model_starlark.Path, bool)
}

type moduleOrRepositoryContext struct {
	computer               *baseComputer
	context                context.Context
	environment            moduleOrRepositoryContextEnvironment
	subdirectoryComponents []path.Component

	commandEncoder                     model_encoding.BinaryEncoder
	defaultWorkingDirectoryBuilder     *path.Builder
	directoryCreationParameters        *model_filesystem.DirectoryCreationParameters
	directoryCreationParametersMessage *model_filesystem_pb.DirectoryCreationParameters
	directoryLoadOptions               *changeTrackingDirectoryLoadOptions
	fileCreationParameters             *model_filesystem.FileCreationParameters
	fileCreationParametersMessage      *model_filesystem_pb.FileCreationParameters
	fileReader                         *model_filesystem.FileReader
	pathUnpackerInto                   unpack.UnpackerInto[*model_starlark.Path]
	repoPlatform                       model_core.Message[*model_analysis_pb.RegisteredRepoPlatform_Value]
	virtualRootScopeWalkerFactory      *path.VirtualRootScopeWalkerFactory

	inputRootDirectory *changeTrackingDirectory
	patchedFiles       filesystem.FileReader
	patchedFilesWriter *model_filesystem.SectionWriter
}

func (c *baseComputer) newModuleOrRepositoryContext(ctx context.Context, e moduleOrRepositoryContextEnvironment, subdirectoryComponents []path.Component) (*moduleOrRepositoryContext, error) {
	inputRootDirectory := &changeTrackingDirectory{}
	currentDirectory := inputRootDirectory
	for _, component := range subdirectoryComponents {
		subdirectory := &changeTrackingDirectory{}
		currentDirectory.directories = map[path.Component]*changeTrackingDirectory{
			component: subdirectory,
		}
		currentDirectory = subdirectory
	}
	return &moduleOrRepositoryContext{
		computer:               c,
		context:                ctx,
		environment:            e,
		subdirectoryComponents: subdirectoryComponents,

		inputRootDirectory: inputRootDirectory,
	}, nil
}

func (mrc *moduleOrRepositoryContext) release() {
	if mrc.patchedFiles != nil {
		mrc.patchedFiles.Close()
	}
}

func (mrc *moduleOrRepositoryContext) maybeGetDirectoryCreationParameters() {
	if mrc.directoryCreationParameters == nil {
		if v, ok := mrc.environment.GetDirectoryCreationParametersObjectValue(&model_analysis_pb.DirectoryCreationParametersObject_Key{}); ok {
			directoryReader := model_parser.NewStorageBackedParsedObjectReader(
				mrc.computer.objectDownloader,
				v.GetEncoder(),
				model_parser.NewMessageObjectParser[object.LocalReference, model_filesystem_pb.Directory](),
			)
			leavesReader := model_parser.NewStorageBackedParsedObjectReader(
				mrc.computer.objectDownloader,
				v.GetEncoder(),
				model_parser.NewMessageObjectParser[object.LocalReference, model_filesystem_pb.Leaves](),
			)

			mrc.directoryCreationParameters = v
			mrc.directoryLoadOptions = &changeTrackingDirectoryLoadOptions{
				context:         mrc.context,
				directoryReader: directoryReader,
				leavesReader:    leavesReader,
			}
		}
	}
}

func (mrc *moduleOrRepositoryContext) maybeGetFileCreationParameters() {
	if mrc.fileCreationParameters == nil {
		if v, ok := mrc.environment.GetFileCreationParametersObjectValue(&model_analysis_pb.FileCreationParametersObject_Key{}); ok {
			mrc.fileCreationParameters = v
		}
	}
}

func (mrc *moduleOrRepositoryContext) maybeGetFileReader() {
	if mrc.fileReader == nil {
		if v, ok := mrc.environment.GetFileReaderValue(&model_analysis_pb.FileReader_Key{}); ok {
			mrc.fileReader = v
		}
	}
}

func (mrc *moduleOrRepositoryContext) maybeInitializePatchedFiles() error {
	if mrc.patchedFiles == nil {
		patchedFiles, err := mrc.computer.filePool.NewFile()
		if err != nil {
			return err
		}
		mrc.patchedFiles = patchedFiles
		mrc.patchedFilesWriter = model_filesystem.NewSectionWriter(patchedFiles)
	}
	return nil
}

func (mrc *moduleOrRepositoryContext) maybeGetStableInputRootPath() error {
	if mrc.defaultWorkingDirectoryBuilder == nil {
		stableInputRootPath, ok := mrc.environment.GetStableInputRootPathObjectValue(&model_analysis_pb.StableInputRootPathObject_Key{})
		if !ok {
			return evaluation.ErrMissingDependency
		}

		defaultWorkingDirectoryPath := stableInputRootPath
		for _, component := range mrc.subdirectoryComponents {
			defaultWorkingDirectoryPath = defaultWorkingDirectoryPath.Append(component)
		}
		defaultWorkingDirectoryBuilder, scopeWalker := path.EmptyBuilder.Join(path.VoidScopeWalker)
		if err := path.Resolve(defaultWorkingDirectoryPath, scopeWalker); err != nil {
			return fmt.Errorf("failed to resolve default working directory path: %w", err)
		}

		virtualRootScopeWalkerFactory, err := path.NewVirtualRootScopeWalkerFactory(stableInputRootPath, nil)
		if err != nil {
			return err
		}

		mrc.defaultWorkingDirectoryBuilder = defaultWorkingDirectoryBuilder
		mrc.pathUnpackerInto = model_starlark.NewPathOrLabelOrStringUnpackerInto(
			func(canonicalRepo label.CanonicalRepo) (*model_starlark.Path, error) {
				// If ctx.path() is called with a label, that
				// should force a download of the referenced
				// repository.
				mrc.maybeGetDirectoryCreationParameters()
				if mrc.directoryLoadOptions == nil {
					return nil, evaluation.ErrMissingDependency
				}
				if err := mrc.inputRootDirectory.maybeLoadContents(mrc.directoryLoadOptions); err != nil {
					return nil, fmt.Errorf("failed to load contents of input root directory")
				}

				externalDirectoryName := path.MustNewComponent("external")
				externalDirectory, err := mrc.inputRootDirectory.getOrCreateDirectory(externalDirectoryName)
				if err != nil {
					return nil, fmt.Errorf("Failed to create directory %#v: %w", externalDirectoryName.String(), err)
				}
				if err := externalDirectory.maybeLoadContents(mrc.directoryLoadOptions); err != nil {
					return nil, fmt.Errorf("failed to load contents of %#v directory: %w", err)
				}

				canonicalRepoComponent := path.MustNewComponent(canonicalRepo.String())
				if _, ok := externalDirectory.directories[canonicalRepoComponent]; !ok {
					repo := mrc.environment.GetRepoValue(&model_analysis_pb.Repo_Key{
						CanonicalRepo: canonicalRepo.String(),
					})
					if !repo.IsSet() {
						return nil, evaluation.ErrMissingDependency
					}
					repoDirectory, err := externalDirectory.getOrCreateDirectory(canonicalRepoComponent)
					if err != nil {
						return nil, fmt.Errorf("failed to create directory for repo: %w", err)
					}
					repoDirectory.currentReference = model_core.Message[*model_core_pb.Reference]{
						Message:            repo.Message.RootDirectoryReference,
						OutgoingReferences: repo.OutgoingReferences,
					}
				}

				return stableInputRootPath.Append(externalDirectoryName).Append(canonicalRepoComponent), nil
			},
			defaultWorkingDirectoryPath,
		)
		mrc.virtualRootScopeWalkerFactory = virtualRootScopeWalkerFactory
	}
	return nil
}

func (mrc *moduleOrRepositoryContext) doDownload(thread *starlark.Thread, b *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
	mrc.maybeGetDirectoryCreationParameters()
	if err := mrc.maybeGetStableInputRootPath(); err != nil {
		return nil, err
	}
	if mrc.directoryLoadOptions == nil {
		return nil, evaluation.ErrMissingDependency
	}

	if len(args) > 8 {
		return nil, fmt.Errorf("%s: got %d positional arguments, want at most 8", b.Name(), len(args))
	}
	var urls []string
	var output *model_starlark.Path
	sha256 := ""
	executable := false
	allowFail := false
	canonicalID := ""
	var auth map[string]map[string]string
	var headers map[string]string
	integrity := ""
	block := true
	if err := starlark.UnpackArgs(
		b.Name(), args, kwargs,
		"url", unpack.Bind(thread, &urls, unpack.Or([]unpack.UnpackerInto[[]string]{
			unpack.List(unpack.Stringer(unpack.URL)),
			unpack.Singleton(unpack.Stringer(unpack.URL)),
		})),
		"output?", unpack.Bind(thread, &output, mrc.pathUnpackerInto),
		"sha256?", unpack.Bind(thread, &sha256, unpack.String),
		"executable?", unpack.Bind(thread, &executable, unpack.Bool),
		"allow_fail?", unpack.Bind(thread, &allowFail, unpack.Bool),
		"canonical_id?", unpack.Bind(thread, &canonicalID, unpack.String),
		"auth?", unpack.Bind(thread, &auth, unpack.Dict(unpack.Stringer(unpack.URL), unpack.Dict(unpack.String, unpack.String))),
		"headers?", unpack.Bind(thread, &headers, unpack.Dict(unpack.String, unpack.String)),
		"integrity?", unpack.Bind(thread, &integrity, unpack.String),
		"block?", unpack.Bind(thread, &block, unpack.Bool),
	); err != nil {
		return nil, err
	}

	if integrity == "" && sha256 != "" {
		sha256Bytes, err := hex.DecodeString(sha256)
		if err != nil {
			return nil, fmt.Errorf("invalid sha256: %w", err)
		}
		integrity = "sha256-" + base64.StdEncoding.EncodeToString(sha256Bytes)
	}

	fileContentsValue := mrc.environment.GetHttpFileContentsValue(&model_analysis_pb.HttpFileContents_Key{
		Urls:      urls,
		Integrity: integrity,
		AllowFail: allowFail,
		// TODO: Set auth and headers!
	})
	if !block {
		return nil, errors.New("non-blocking downloads are not implemented yet")
	}

	if !fileContentsValue.IsSet() {
		return nil, evaluation.ErrMissingDependency
	}
	if fileContentsValue.Message.Exists == nil {
		// File does not exist, or allow_fail was set and an
		// error occurred.
		return starlarkstruct.FromStringDict(starlarkstruct.Default, starlark.StringDict{
			"success": starlark.Bool(false),
		}), nil
	}

	// Insert the downloaded file into the file system.
	r := &changeTrackingDirectoryNewFileResolver{
		loadOptions: mrc.directoryLoadOptions,
		stack:       util.NewNonEmptyStack(mrc.inputRootDirectory),
	}
	if err := path.Resolve(output, mrc.virtualRootScopeWalkerFactory.New(r)); err != nil {
		return nil, fmt.Errorf("cannot resolve %#v: %w", output.String(), err)
	}
	if r.name == nil {
		return nil, fmt.Errorf("%#v does not resolve to a file", output.String())
	}

	r.stack.Peek().setFile(*r.name, &changeTrackingFile{
		isExecutable: executable,
		contents: unmodifiedFileContents{
			contents: model_core.Message[*model_filesystem_pb.FileContents]{
				Message:            fileContentsValue.Message.Exists.Contents,
				OutgoingReferences: fileContentsValue.OutgoingReferences,
			},
		},
	})

	return starlarkstruct.FromStringDict(starlarkstruct.Default, starlark.StringDict{
		"success": starlark.Bool(true),
		// TODO: Add "sha256" and "integrity" fields.
	}), nil
}

func (moduleOrRepositoryContext) doDownloadAndExtract(thread *starlark.Thread, b *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
	return nil, errors.New("TODO: Implement!")
}

func (mrc *moduleOrRepositoryContext) doExecute(thread *starlark.Thread, b *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
	if mrc.commandEncoder == nil {
		if v, ok := mrc.environment.GetCommandEncoderObjectValue(&model_analysis_pb.CommandEncoderObject_Key{}); ok {
			mrc.commandEncoder = v
		}
	}
	mrc.maybeGetDirectoryCreationParameters()
	if mrc.directoryCreationParametersMessage == nil {
		if v := mrc.environment.GetDirectoryCreationParametersValue(&model_analysis_pb.DirectoryCreationParameters_Key{}); v.IsSet() {
			mrc.directoryCreationParametersMessage = v.Message.DirectoryCreationParameters
		}
	}
	mrc.maybeGetFileCreationParameters()
	if mrc.fileCreationParametersMessage == nil {
		if v := mrc.environment.GetFileCreationParametersValue(&model_analysis_pb.FileCreationParameters_Key{}); v.IsSet() {
			mrc.fileCreationParametersMessage = v.Message.FileCreationParameters
		}
	}
	mrc.maybeGetFileReader()
	if !mrc.repoPlatform.IsSet() {
		mrc.repoPlatform = mrc.environment.GetRegisteredRepoPlatformValue(&model_analysis_pb.RegisteredRepoPlatform_Key{})
	}
	stableInputRootPathError := mrc.maybeGetStableInputRootPath()
	if mrc.commandEncoder == nil ||
		mrc.directoryCreationParameters == nil ||
		mrc.directoryCreationParametersMessage == nil ||
		mrc.fileCreationParameters == nil ||
		mrc.fileCreationParametersMessage == nil ||
		mrc.fileReader == nil ||
		!mrc.repoPlatform.IsSet() {
		return nil, evaluation.ErrMissingDependency
	}
	if stableInputRootPathError != nil {
		return nil, stableInputRootPathError
	}

	var arguments []string
	timeout := int64(600)
	environment := map[string]string{}
	quiet := true
	var workingDirectory path.Parser = &path.EmptyBuilder
	if err := starlark.UnpackArgs(
		b.Name(), args, kwargs,
		"arguments", unpack.Bind(thread, &arguments, unpack.List(unpack.String)),
		"timeout?", unpack.Bind(thread, &timeout, unpack.Int[int64]()),
		"environment?", unpack.Bind(thread, &environment, unpack.Dict(unpack.String, unpack.String)),
		"quiet?", unpack.Bind(thread, &quiet, unpack.Bool),
		"working_directory?", unpack.Bind(thread, &workingDirectory, unpack.PathParser(path.UNIXFormat)),
	); err != nil {
		return nil, err
	}

	// Inherit environment variables from
	// the repo platform.
	for _, environmentVariable := range mrc.repoPlatform.Message.RepositoryOsEnviron {
		if _, ok := environment[environmentVariable.Name]; !ok {
			environment[environmentVariable.Name] = environmentVariable.Value
		}
	}

	// Convert arguments and environment
	// variables to B-trees, so that they can
	// be attached to the Command message.
	referenceFormat := mrc.computer.buildSpecificationReference.GetReferenceFormat()
	argumentsBuilder := btree.NewSplitProllyBuilder(
		1<<16,
		1<<18,
		btree.NewObjectCreatingNodeMerger(
			mrc.commandEncoder,
			referenceFormat,
			/* parentNodeComputer = */ func(contents *object.Contents, childNodes []*model_command_pb.ArgumentList_Element, outgoingReferences object.OutgoingReferences, metadata []dag.ObjectContentsWalker) (model_core.PatchedMessage[*model_command_pb.ArgumentList_Element, dag.ObjectContentsWalker], error) {
				patcher := model_core.NewReferenceMessagePatcher[dag.ObjectContentsWalker]()
				return model_core.NewPatchedMessage(
					&model_command_pb.ArgumentList_Element{
						Level: &model_command_pb.ArgumentList_Element_Parent{
							Parent: patcher.AddReference(contents.GetReference(), dag.NewSimpleObjectContentsWalker(contents, metadata)),
						},
					},
					patcher,
				), nil
			},
		),
	)
	for _, argument := range arguments {
		if err := argumentsBuilder.PushChild(
			model_core.NewSimplePatchedMessage[dag.ObjectContentsWalker](&model_command_pb.ArgumentList_Element{
				Level: &model_command_pb.ArgumentList_Element_Leaf{
					Leaf: argument,
				},
			}),
		); err != nil {
			return nil, err
		}
	}
	argumentList, err := argumentsBuilder.FinalizeList()
	if err != nil {
		return nil, err
	}

	environmentVariableList, err := mrc.computer.convertDictToEnvironmentVariableList(environment, mrc.commandEncoder)
	if err != nil {
		return nil, err
	}

	workingDirectoryBuilder, scopeWalker := mrc.defaultWorkingDirectoryBuilder.Join(path.NewRelativeScopeWalker(path.VoidComponentWalker))
	if err := path.Resolve(workingDirectory, scopeWalker); err != nil {
		return nil, fmt.Errorf("failed to resolve working directory: %w", err)
	}

	commandPatcher := argumentList.Patcher
	commandPatcher.Merge(environmentVariableList.Patcher)
	commandContents, commandMetadata, err := model_core.MarshalAndEncodePatchedMessage(
		model_core.NewPatchedMessage(
			&model_command_pb.Command{
				Arguments:                   argumentList.Message,
				EnvironmentVariables:        environmentVariableList.Message,
				DirectoryCreationParameters: mrc.directoryCreationParametersMessage,
				FileCreationParameters:      mrc.fileCreationParametersMessage,
				WorkingDirectory:            workingDirectoryBuilder.GetUNIXString(),
				NeedsStableInputRootPath:    true,
			},
			commandPatcher,
		),
		referenceFormat,
		mrc.commandEncoder,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create command: %w", err)
	}

	inputRootReference, err := mrc.computer.createMerkleTreeFromChangeTrackingDirectory(mrc.context, mrc.inputRootDirectory, mrc.directoryCreationParameters, mrc.fileCreationParameters, mrc.patchedFiles)
	if err != nil {
		return nil, fmt.Errorf("failed to create Merkle tree of root directory: %w", err)
	}

	// Execute the command.
	keyPatcher := inputRootReference.Patcher
	actionResult := mrc.environment.GetActionResultValue(PatchedActionResultKey{
		Message: &model_analysis_pb.ActionResult_Key{
			PlatformPkixPublicKey: mrc.repoPlatform.Message.ExecPkixPublicKey,
			CommandReference: keyPatcher.AddReference(
				commandContents.GetReference(),
				dag.NewSimpleObjectContentsWalker(commandContents, commandMetadata),
			),
			InputRootReference: inputRootReference.Message,
			ExecutionTimeout:   &durationpb.Duration{Seconds: timeout},
		},
		Patcher: keyPatcher,
	})
	if !actionResult.IsSet() {
		return nil, evaluation.ErrMissingDependency
	}

	// Extract standard output and standard
	// error from the results.
	outputs, err := mrc.computer.getOutputsFromActionResult(mrc.context, actionResult, mrc.directoryCreationParameters.DirectoryAccessParameters)
	if err != nil {
		return nil, fmt.Errorf("failed to obtain outputs from action result: %w", err)
	}

	stdoutEntry, err := model_filesystem.NewFileContentsEntryFromProto(
		model_core.Message[*model_filesystem_pb.FileContents]{
			Message:            outputs.Message.Stdout,
			OutgoingReferences: outputs.OutgoingReferences,
		},
		referenceFormat,
	)
	if err != nil {
		return nil, fmt.Errorf("invalid standard output entry: %w", err)
	}
	stdout, err := mrc.fileReader.FileReadAll(mrc.context, stdoutEntry, 1<<20)
	if err != nil {
		return nil, fmt.Errorf("failed to read standard output: %w", err)
	}

	stderrEntry, err := model_filesystem.NewFileContentsEntryFromProto(
		model_core.Message[*model_filesystem_pb.FileContents]{
			Message:            outputs.Message.Stderr,
			OutgoingReferences: outputs.OutgoingReferences,
		},
		referenceFormat,
	)
	if err != nil {
		return nil, fmt.Errorf("invalid standard error entry: %w", err)
	}
	stderr, err := mrc.fileReader.FileReadAll(mrc.context, stderrEntry, 1<<20)
	if err != nil {
		return nil, fmt.Errorf("failed to read standard error: %w", err)
	}

	return starlarkstruct.FromStringDict(starlarkstruct.Default, starlark.StringDict{
		"return_code": starlark.MakeInt64(actionResult.Message.ExitCode),
		"stderr":      starlark.String(stderr),
		"stdout":      starlark.String(stdout),
	}), nil
}

func (moduleOrRepositoryContext) doExtract(thread *starlark.Thread, b *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
	return nil, errors.New("TODO: Implement!")
}

func (mrc *moduleOrRepositoryContext) doFile(thread *starlark.Thread, b *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
	mrc.maybeGetDirectoryCreationParameters()
	stableInputRootPathError := mrc.maybeGetStableInputRootPath()
	if mrc.directoryCreationParameters == nil {
		return nil, evaluation.ErrMissingDependency
	}
	if stableInputRootPathError != nil {
		return nil, stableInputRootPathError
	}

	var filePath *model_starlark.Path
	content := ""
	executable := true
	legacyUTF8 := true
	if err := starlark.UnpackArgs(
		b.Name(), args, kwargs,
		"path", unpack.Bind(thread, &filePath, mrc.pathUnpackerInto),
		"content?", unpack.Bind(thread, &content, unpack.String),
		"executable?", unpack.Bind(thread, &executable, unpack.Bool),
		"legacy_utf8?", unpack.Bind(thread, &legacyUTF8, unpack.Bool),
	); err != nil {
		return nil, err
	}

	r := &changeTrackingDirectoryNewFileResolver{
		loadOptions: mrc.directoryLoadOptions,
		stack:       util.NewNonEmptyStack(mrc.inputRootDirectory),
	}
	if err := path.Resolve(filePath, mrc.virtualRootScopeWalkerFactory.New(r)); err != nil {
		return nil, fmt.Errorf("cannot resolve %#v: %w", filePath.String(), err)
	}
	if r.name == nil {
		return nil, fmt.Errorf("%#v does not resolve to a file", filePath.String())
	}

	if err := mrc.maybeInitializePatchedFiles(); err != nil {
		return nil, err
	}

	// TODO: Do UTF-8 -> ISO-8859-1
	// conversion if legacy_utf8=False.
	patchedFileOffsetBytes := mrc.patchedFilesWriter.GetOffsetBytes()
	if _, err := mrc.patchedFilesWriter.WriteString(content); err != nil {
		return nil, fmt.Errorf("failed to write to file at %#v: %w", filePath.String(), err)
	}

	r.stack.Peek().setFile(*r.name, &changeTrackingFile{
		isExecutable: executable,
		contents: patchedFileContents{
			offsetBytes: patchedFileOffsetBytes,
			sizeBytes:   mrc.patchedFilesWriter.GetOffsetBytes() - patchedFileOffsetBytes,
		},
	})
	return starlark.None, nil
}

func (moduleOrRepositoryContext) doGetenv(thread *starlark.Thread, b *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
	return nil, errors.New("TODO: Implement!")
}

func (mrc *moduleOrRepositoryContext) doPath(thread *starlark.Thread, b *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
	if err := mrc.maybeGetStableInputRootPath(); err != nil {
		return nil, err
	}

	var filePath *model_starlark.Path
	if err := starlark.UnpackArgs(
		b.Name(), args, kwargs,
		"path", unpack.Bind(thread, &filePath, mrc.pathUnpackerInto),
	); err != nil {
		return nil, err
	}
	return filePath, nil
}

func (moduleOrRepositoryContext) doRead(thread *starlark.Thread, b *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
	return nil, errors.New("TODO: Implement!")
}

func (moduleOrRepositoryContext) doReportProgress(thread *starlark.Thread, b *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
	var status string
	if err := starlark.UnpackArgs(
		b.Name(), args, kwargs,
		"status", unpack.Bind(thread, &status, unpack.String),
	); err != nil {
		return nil, err
	}
	return starlark.None, nil
}

func (moduleOrRepositoryContext) doWatch(thread *starlark.Thread, b *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
	return nil, errors.New("TODO: Implement!")
}

func (moduleOrRepositoryContext) doWhich(thread *starlark.Thread, b *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
	return nil, errors.New("TODO: Implement!")
}

func (c *baseComputer) fetchModuleExtensionRepo(ctx context.Context, canonicalRepo label.CanonicalRepo, e RepoEnvironment) (PatchedRepoValue, error) {
	// Obtain the definition of the declared repo.
	repoValue := e.GetModuleExtensionRepoValue(&model_analysis_pb.ModuleExtensionRepo_Key{
		CanonicalRepo: canonicalRepo.String(),
	})
	allBuiltinsModulesNames := e.GetBuiltinsModuleNamesValue(&model_analysis_pb.BuiltinsModuleNames_Key{})
	repoPlatform := e.GetRegisteredRepoPlatformValue(&model_analysis_pb.RegisteredRepoPlatform_Key{})
	if !repoValue.IsSet() || !allBuiltinsModulesNames.IsSet() || !repoPlatform.IsSet() {
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
					return model_starlark.NewLabel(canonicalLabel), nil
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

	// Invoke the implementation function.
	thread := c.newStarlarkThread(ctx, e, allBuiltinsModulesNames.Message.BuiltinsModuleNames)

	subdirectoryComponents := []path.Component{
		path.MustNewComponent("external"),
		path.MustNewComponent(canonicalRepo.String()),
	}
	repositoryContext, err := c.newModuleOrRepositoryContext(ctx, e, subdirectoryComponents)
	if err != nil {
		return PatchedRepoValue{}, err
	}
	defer repositoryContext.release()

	// These are needed at the end to create the directory Merkle tree.
	repositoryContext.maybeGetDirectoryCreationParameters()
	repositoryContext.maybeGetFileCreationParameters()

	repositoryCtx := starlarkstruct.FromStringDict(starlarkstruct.Default, starlark.StringDict{
		// Fields shared with module_ctx.
		"download":             starlark.NewBuiltin("repository_ctx.download", repositoryContext.doDownload),
		"download_and_extract": starlark.NewBuiltin("repository_ctx.download_and_extract", repositoryContext.doDownloadAndExtract),
		"execute":              starlark.NewBuiltin("repository_ctx.execute", repositoryContext.doExecute),
		"extract":              starlark.NewBuiltin("repository_ctx.extract", repositoryContext.doExtract),
		"file":                 starlark.NewBuiltin("repository_ctx.file", repositoryContext.doFile),
		"getenv":               starlark.NewBuiltin("repository_ctx.getenv", repositoryContext.doGetenv),
		"os":                   newRepositoryOS(repoPlatform.Message),
		"path":                 starlark.NewBuiltin("repository_ctx.path", repositoryContext.doPath),
		"read":                 starlark.NewBuiltin("repository_ctx.read", repositoryContext.doRead),
		"report_progress":      starlark.NewBuiltin("repository_ctx.report_progress", repositoryContext.doReportProgress),
		"watch":                starlark.NewBuiltin("repository_ctx.watch", repositoryContext.doWatch),
		"which":                starlark.NewBuiltin("repository_ctx.which", repositoryContext.doWatch),

		// Fields specific to repository_ctx.
		"attr": starlarkstruct.FromStringDict(starlarkstruct.Default, attrs),
		"name": starlark.String(canonicalRepo.String()),
		"template": starlark.NewBuiltin(
			"repository_ctx.template",
			func(thread *starlark.Thread, b *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
				repositoryContext.maybeGetDirectoryCreationParameters()
				repositoryContext.maybeGetFileReader()
				stableInputRootPathError := repositoryContext.maybeGetStableInputRootPath()
				if repositoryContext.directoryCreationParameters == nil || repositoryContext.fileReader == nil {
					return nil, evaluation.ErrMissingDependency
				}
				if stableInputRootPathError != nil {
					return nil, stableInputRootPathError
				}

				if len(args) > 4 {
					return nil, fmt.Errorf("%s: got %d positional arguments, want at most 4", b.Name(), len(args))
				}
				var filePath *model_starlark.Path
				var templatePath label.CanonicalLabel
				var substitutions map[string]string
				executable := true
				var watchTemplate string
				if err := starlark.UnpackArgs(
					b.Name(), args, kwargs,
					"path", unpack.Bind(thread, &filePath, repositoryContext.pathUnpackerInto),
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
				templateFile := repositoryContext.fileReader.FileOpenRead(repositoryContext.context, templateContents, 0)

				r := &changeTrackingDirectoryNewFileResolver{
					loadOptions: repositoryContext.directoryLoadOptions,
					stack:       util.NewNonEmptyStack(repositoryContext.inputRootDirectory),
				}
				if err := path.Resolve(filePath, repositoryContext.virtualRootScopeWalkerFactory.New(r)); err != nil {
					return nil, fmt.Errorf("cannot resolve %#v: %w", filePath.String(), err)
				}
				if r.name == nil {
					return nil, fmt.Errorf("%#v does not resolve to a file", filePath.String())
				}

				if err := repositoryContext.maybeInitializePatchedFiles(); err != nil {
					return nil, err
				}

				// Perform substitutions.
				patchedFileOffsetBytes := repositoryContext.patchedFilesWriter.GetOffsetBytes()
				if err := searchAndReplacer.SearchAndReplace(repositoryContext.patchedFilesWriter, templateFile, replacements); err != nil {
					return nil, fmt.Errorf("failed to write to file at %#v: %w", filePath.String(), err)
				}

				r.stack.Peek().setFile(*r.name, &changeTrackingFile{
					isExecutable: executable,
					contents: patchedFileContents{
						offsetBytes: patchedFileOffsetBytes,
						sizeBytes:   repositoryContext.patchedFilesWriter.GetOffsetBytes() - patchedFileOffsetBytes,
					},
				})
				return starlark.None, nil
			},
		),
	})
	repositoryCtx.Freeze()

	_, err = starlark.Call(
		thread,
		repositoryRule.Implementation,
		/* args = */ starlark.Tuple{repositoryCtx},
		/* kwargs = */ nil,
	)
	if err != nil {
		var evalErr *starlark.EvalError
		if !errors.Is(err, evaluation.ErrMissingDependency) && errors.As(err, &evalErr) {
			return PatchedRepoValue{}, errors.New(evalErr.Backtrace())
		}
		return PatchedRepoValue{}, err
	}

	// Capture the resulting external/${repo} directory.
	repoDirectory := repositoryContext.inputRootDirectory
	for _, component := range subdirectoryComponents {
		childDirectory, ok := repoDirectory.directories[component]
		if !ok {
			return PatchedRepoValue{}, errors.New("repository rule removed its own repository directory")
		}
		repoDirectory = childDirectory
	}

	if repositoryContext.directoryCreationParameters == nil || repositoryContext.fileCreationParameters == nil {
		return PatchedRepoValue{}, evaluation.ErrMissingDependency
	}
	return c.returnRepoMerkleTree(
		ctx,
		repoDirectory,
		repositoryContext.directoryCreationParameters,
		repositoryContext.fileCreationParameters,
		repositoryContext.patchedFiles,
	)
}

func (c *baseComputer) createMerkleTreeFromChangeTrackingDirectory(ctx context.Context, rootDirectory *changeTrackingDirectory, directoryCreationParameters *model_filesystem.DirectoryCreationParameters, fileCreationParameters *model_filesystem.FileCreationParameters, patchedFiles io.ReaderAt) (model_core.PatchedMessage[*model_core_pb.Reference, dag.ObjectContentsWalker], error) {
	if r := rootDirectory.currentReference; r.IsSet() {
		// Directory remained completely unmodified. Simply
		// return the original directory.
		return model_core.NewPatchedMessageFromExisting(
			r,
			func(index int) dag.ObjectContentsWalker {
				return dag.ExistingObjectContentsWalker
			},
		), nil
	}

	// We had to strip a path prefix or apply one or more patches.
	// This means we need to create a new Merkle tree.
	merkleTreeNodes, err := c.filePool.NewFile()
	if err != nil {
		return model_core.PatchedMessage[*model_core_pb.Reference, dag.ObjectContentsWalker]{}, err
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
	fileWritingMerkleTreeCapturer := model_core.NewFileWritingMerkleTreeCapturer(model_filesystem.NewSectionWriter(merkleTreeNodes))
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
		return model_core.PatchedMessage[*model_core_pb.Reference, dag.ObjectContentsWalker]{}, err
	}

	// Store the root directory itself. We don't embed it into the
	// response, as that prevents it from being accessed separately.
	contents, children, err := model_core.MarshalAndEncodePatchedMessage(
		rootDirectoryMessage,
		c.buildSpecificationReference.GetReferenceFormat(),
		directoryCreationParameters.GetEncoder(),
	)
	if err != nil {
		return model_core.PatchedMessage[*model_core_pb.Reference, dag.ObjectContentsWalker]{}, err
	}
	capturedRootDirectory := fileWritingMerkleTreeCapturer.CaptureObject(contents, children)

	// Finalize writing of Merkle tree nodes to disk, and provide
	// read access to the nodes, so that they can be uploaded.
	if err := fileWritingMerkleTreeCapturer.Flush(); err != nil {
		return model_core.PatchedMessage[*model_core_pb.Reference, dag.ObjectContentsWalker]{}, err
	}
	objectContentsWalkerFactory := model_core.NewFileReadingObjectContentsWalkerFactory(merkleTreeNodes)
	defer objectContentsWalkerFactory.Release()
	merkleTreeNodes = nil

	patcher := model_core.NewReferenceMessagePatcher[dag.ObjectContentsWalker]()
	rootReference := contents.GetReference()
	return model_core.NewPatchedMessage(
		patcher.AddReference(
			rootReference,
			objectContentsWalkerFactory.CreateObjectContentsWalker(rootReference, capturedRootDirectory),
		),
		patcher,
	), nil
}

func (c *baseComputer) returnRepoMerkleTree(ctx context.Context, rootDirectory *changeTrackingDirectory, directoryCreationParameters *model_filesystem.DirectoryCreationParameters, fileCreationParameters *model_filesystem.FileCreationParameters, patchedFiles io.ReaderAt) (PatchedRepoValue, error) {
	rootDirectoryReference, err := c.createMerkleTreeFromChangeTrackingDirectory(ctx, rootDirectory, directoryCreationParameters, fileCreationParameters, patchedFiles)
	if err != nil {
		return PatchedRepoValue{}, err
	}
	return PatchedRepoValue{
		Message: &model_analysis_pb.Repo_Value{
			RootDirectoryReference: rootDirectoryReference.Message,
		},
		Patcher: rootDirectoryReference.Patcher,
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
