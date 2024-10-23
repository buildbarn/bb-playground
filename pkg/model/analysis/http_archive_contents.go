package analysis

import (
	"archive/tar"
	"archive/zip"
	"compress/gzip"
	"context"
	"errors"
	"fmt"
	"io"
	"sort"
	"strings"
	"sync/atomic"

	"github.com/buildbarn/bb-playground/pkg/evaluation"
	model_core "github.com/buildbarn/bb-playground/pkg/model/core"
	model_filesystem "github.com/buildbarn/bb-playground/pkg/model/filesystem"
	model_analysis_pb "github.com/buildbarn/bb-playground/pkg/proto/model/analysis"
	model_filesystem_pb "github.com/buildbarn/bb-playground/pkg/proto/model/filesystem"
	"github.com/buildbarn/bb-playground/pkg/storage/dag"
	"github.com/buildbarn/bb-storage/pkg/filesystem"
	"github.com/buildbarn/bb-storage/pkg/filesystem/path"
	"github.com/buildbarn/bb-storage/pkg/util"

	"golang.org/x/sync/errgroup"
	"golang.org/x/sync/semaphore"
	"google.golang.org/protobuf/types/known/emptypb"
)

type archiveFile struct {
	isExecutable bool
	offsetBytes  int64
	sizeBytes    int64
}

type archiveDirectory struct {
	directories map[path.Component]*archiveDirectory
	files       map[path.Component]archiveFile
	symlinks    map[path.Component]path.Parser
}

func (d *archiveDirectory) resolveNewDirectory(filePath string) error {
	return path.Resolve(
		path.UNIXFormat.NewParser(filePath),
		path.NewLoopDetectingScopeWalker(
			path.NewRelativeScopeWalker(&archiveDirectoryCreatingResolver{
				baseArchiveDirectoryCreatingResolver: baseArchiveDirectoryCreatingResolver{
					stack: util.NewNonEmptyStack(d),
				},
			}),
		),
	)
}

func (d *archiveDirectory) resolveNewFile(filePath string) (*archiveDirectory, path.Component, error) {
	r := archiveFileCreatingResolver{
		baseArchiveDirectoryCreatingResolver: baseArchiveDirectoryCreatingResolver{
			stack: util.NewNonEmptyStack(d),
		},
	}
	var badComponent path.Component
	if err := path.Resolve(
		path.UNIXFormat.NewParser(filePath),
		path.NewLoopDetectingScopeWalker(path.NewRelativeScopeWalker(&r)),
	); err != nil {
		return nil, badComponent, err
	}
	if r.TerminalName == nil {
		return nil, badComponent, errors.New("path resolves to a directory")
	}

	dChild := r.stack.Peek()
	if _, ok := dChild.files[*r.TerminalName]; ok {
		return nil, badComponent, errors.New("path resolves to an already existing file")
	}
	if _, ok := dChild.symlinks[*r.TerminalName]; ok {
		return nil, badComponent, errors.New("path resolves to an already existing symbolic link")
	}
	return dChild, *r.TerminalName, nil
}

func (d *archiveDirectory) addFile(name path.Component, extractedFilesWriter *sectionWriter, f io.Reader, isExecutable bool) error {
	fileOffsetBytes := extractedFilesWriter.offsetBytes
	fileSizeBytes, err := io.Copy(extractedFilesWriter, f)
	if err != nil {
		return err
	}

	if d.files == nil {
		d.files = map[path.Component]archiveFile{}
	}
	d.files[name] = archiveFile{
		isExecutable: isExecutable,
		offsetBytes:  fileOffsetBytes,
		sizeBytes:    fileSizeBytes,
	}
	return nil
}

type baseArchiveDirectoryCreatingResolver struct {
	stack util.NonEmptyStack[*archiveDirectory]
}

func (r *baseArchiveDirectoryCreatingResolver) onDirectory(self path.ComponentWalker, name path.Component) (path.GotDirectoryOrSymlink, error) {
	d := r.stack.Peek()
	dChild, ok := d.directories[name]
	if !ok {
		if _, ok := d.files[name]; ok {
			return nil, errors.New("path resolves to an existing file")
		}
		if target, ok := d.symlinks[name]; ok {
			return path.GotSymlink{
				Parent: path.NewRelativeScopeWalker(self),
				Target: target,
			}, nil
		}

		// Create new directory.
		if d.directories == nil {
			d.directories = map[path.Component]*archiveDirectory{}
		}
		dChild = &archiveDirectory{}
		d.directories[name] = dChild
	}
	r.stack.Push(dChild)
	return path.GotDirectory{
		Child:        self,
		IsReversible: true,
	}, nil
}

func (r *baseArchiveDirectoryCreatingResolver) onUp(self path.ComponentWalker) (path.ComponentWalker, error) {
	if _, ok := r.stack.PopSingle(); ok {
		return self, nil
	}
	return nil, errors.New("path resolves to a location outside the build directory")
}

type archiveDirectoryCreatingResolver struct {
	baseArchiveDirectoryCreatingResolver
}

func (r *archiveDirectoryCreatingResolver) OnDirectory(name path.Component) (path.GotDirectoryOrSymlink, error) {
	return r.onDirectory(r, name)
}

func (r *archiveDirectoryCreatingResolver) OnTerminal(name path.Component) (*path.GotSymlink, error) {
	return path.OnTerminalViaOnDirectory(r, name)
}

func (r *archiveDirectoryCreatingResolver) OnUp() (path.ComponentWalker, error) {
	return r.onUp(r)
}

type archiveFileCreatingResolver struct {
	baseArchiveDirectoryCreatingResolver
	path.TerminalNameTrackingComponentWalker
}

func (r *archiveFileCreatingResolver) OnDirectory(name path.Component) (path.GotDirectoryOrSymlink, error) {
	return r.onDirectory(r, name)
}

func (r *archiveFileCreatingResolver) OnUp() (path.ComponentWalker, error) {
	return r.onUp(r)
}

type archiveContentsFile struct {
	underlyingFile filesystem.FileReader
	referenceCount atomic.Uint64
}

func (acf *archiveContentsFile) acquire() {
	if acf.referenceCount.Add(1) == 0 {
		panic("invalid reference count")
	}
}

func (acf *archiveContentsFile) release() {
	// TODO: Figure out why reference counting is broken.
	/*
		if acf.referenceCount.Add(^uint64(0)) == 1 {
			acf.underlyingFile.Close()
			acf.underlyingFile = nil
		}
	*/
}

type openedArchiveDirectory struct {
	contentsFile *archiveContentsFile
	directory    *archiveDirectory
}

func (od *openedArchiveDirectory) EnterCapturableDirectory(name path.Component) (model_filesystem.CapturableDirectoryCloser, error) {
	dChild, ok := od.directory.directories[name]
	if !ok {
		panic("attempted to enter non-existent directory")
	}
	od.contentsFile.acquire()
	return &openedArchiveDirectory{
		contentsFile: od.contentsFile,
		directory:    dChild,
	}, nil
}

func (od *openedArchiveDirectory) OpenRead(name path.Component) (filesystem.FileReader, error) {
	info, ok := od.directory.files[name]
	if !ok {
		panic("attempted to enter non-existent file")
	}
	od.contentsFile.acquire()
	return &openedArchiveFile{
		contentsFile: od.contentsFile,
		info:         info,
	}, nil
}

func (od *openedArchiveDirectory) ReadDir() ([]filesystem.FileInfo, error) {
	d := od.directory
	children := make(filesystem.FileInfoList, 0, len(d.directories)+len(d.files)+len(d.symlinks))
	for name := range d.directories {
		children = append(children, filesystem.NewFileInfo(name, filesystem.FileTypeDirectory, false))
	}
	for name, info := range d.files {
		children = append(children, filesystem.NewFileInfo(name, filesystem.FileTypeRegularFile, info.isExecutable))
	}
	for name := range d.symlinks {
		children = append(children, filesystem.NewFileInfo(name, filesystem.FileTypeSymlink, false))
	}
	sort.Sort(children)
	return children, nil
}

func (od *openedArchiveDirectory) Readlink(name path.Component) (path.Parser, error) {
	target, ok := od.directory.symlinks[name]
	if !ok {
		panic("attempted to read non-existent symbolic link")
	}
	return target, nil
}

func (od *openedArchiveDirectory) Close() error {
	od.contentsFile.release()
	od.contentsFile = nil
	return nil
}

type openedArchiveFile struct {
	contentsFile *archiveContentsFile
	info         archiveFile
}

func (of *openedArchiveFile) Close() error {
	of.contentsFile.release()
	of.contentsFile = nil
	return nil
}

func (of *openedArchiveFile) ReadAt(p []byte, off int64) (int, error) {
	if off < 0 || off >= of.info.sizeBytes {
		return 0, io.EOF
	}
	remainingBytes := of.info.sizeBytes - off
	if int64(len(p)) > remainingBytes {
		p = p[:remainingBytes]
	}
	return of.contentsFile.underlyingFile.ReadAt(p, of.info.offsetBytes+off)
}

func (of *openedArchiveFile) GetNextRegionOffset(offset int64, regionType filesystem.RegionType) (int64, error) {
	panic("TODO")
}

func (c *baseComputer) ComputeHttpArchiveContentsValue(ctx context.Context, key *model_analysis_pb.HttpArchiveContents_Key, e HttpArchiveContentsEnvironment) (PatchedHttpArchiveContentsValue, error) {
	fileReader, err := e.GetFileReaderValue(&model_analysis_pb.FileReader_Key{})
	if err != nil {
		if !errors.Is(err, evaluation.ErrMissingDependency) {
			return model_core.NewSimplePatchedMessage[dag.ObjectContentsWalker](&model_analysis_pb.HttpArchiveContents_Value{
				Result: &model_analysis_pb.HttpArchiveContents_Value_Failure{
					Failure: err.Error(),
				},
			}), nil
		}
		return PatchedHttpArchiveContentsValue{}, err
	}

	directoryCreationParameters, err := e.GetDirectoryCreationParametersObjectValue(&model_analysis_pb.DirectoryCreationParametersObject_Key{})
	if err != nil {
		if !errors.Is(err, evaluation.ErrMissingDependency) {
			return model_core.NewSimplePatchedMessage[dag.ObjectContentsWalker](&model_analysis_pb.HttpArchiveContents_Value{
				Result: &model_analysis_pb.HttpArchiveContents_Value_Failure{
					Failure: err.Error(),
				},
			}), nil
		}
		return PatchedHttpArchiveContentsValue{}, err
	}
	fileCreationParameters, err := e.GetFileCreationParametersObjectValue(&model_analysis_pb.FileCreationParametersObject_Key{})
	if err != nil {
		if !errors.Is(err, evaluation.ErrMissingDependency) {
			return model_core.NewSimplePatchedMessage[dag.ObjectContentsWalker](&model_analysis_pb.HttpArchiveContents_Value{
				Result: &model_analysis_pb.HttpArchiveContents_Value_Failure{
					Failure: err.Error(),
				},
			}), nil
		}
		return PatchedHttpArchiveContentsValue{}, err
	}

	httpFileContentsValue := e.GetHttpFileContentsValue(&model_analysis_pb.HttpFileContents_Key{
		Url: key.Url,
	})
	if !httpFileContentsValue.IsSet() {
		return PatchedHttpArchiveContentsValue{}, evaluation.ErrMissingDependency
	}

	var httpFileContents model_core.Message[*model_filesystem_pb.FileContents]
	switch httpFileContentsResult := httpFileContentsValue.Message.Result.(type) {
	case *model_analysis_pb.HttpFileContents_Value_Exists_:
		httpFileContents = model_core.Message[*model_filesystem_pb.FileContents]{
			Message:            httpFileContentsResult.Exists.Contents,
			OutgoingReferences: httpFileContentsValue.OutgoingReferences,
		}
	case *model_analysis_pb.HttpFileContents_Value_DoesNotExist:
		return model_core.NewSimplePatchedMessage[dag.ObjectContentsWalker](&model_analysis_pb.HttpArchiveContents_Value{
			Result: &model_analysis_pb.HttpArchiveContents_Value_DoesNotExist{
				DoesNotExist: &emptypb.Empty{},
			},
		}), nil
	case *model_analysis_pb.HttpFileContents_Value_Failure:
		return model_core.NewSimplePatchedMessage[dag.ObjectContentsWalker](&model_analysis_pb.HttpArchiveContents_Value{
			Result: &model_analysis_pb.HttpArchiveContents_Value_Failure{
				Failure: httpFileContentsResult.Failure,
			},
		}), nil
	default:
		return model_core.NewSimplePatchedMessage[dag.ObjectContentsWalker](&model_analysis_pb.HttpArchiveContents_Value{
			Result: &model_analysis_pb.HttpArchiveContents_Value_Failure{
				Failure: "HTTP file contents value has an unknown result type",
			},
		}), nil
	}

	httpFileContentsEntry, err := model_filesystem.NewFileContentsEntryFromProto(
		httpFileContents,
		c.buildSpecificationReference.GetReferenceFormat(),
	)
	if err != nil {
		return model_core.NewSimplePatchedMessage[dag.ObjectContentsWalker](&model_analysis_pb.HttpArchiveContents_Value{
			Result: &model_analysis_pb.HttpArchiveContents_Value_Failure{
				Failure: fmt.Sprintf("Invalid file contents: %s", err),
			},
		}), nil
	}

	f, err := c.filePool.NewFile()
	if err != nil {
		return model_core.NewSimplePatchedMessage[dag.ObjectContentsWalker](&model_analysis_pb.HttpArchiveContents_Value{
			Result: &model_analysis_pb.HttpArchiveContents_Value_Failure{
				Failure: err.Error(),
			},
		}), nil
	}
	extractedFiles := archiveContentsFile{
		underlyingFile: f,
	}
	extractedFiles.referenceCount.Store(1)
	defer extractedFiles.release()
	extractedFilesWriter := &sectionWriter{w: f}

	var rootDirectory archiveDirectory

	switch key.Format {
	case model_analysis_pb.HttpArchiveContents_Key_TAR_GZ:
		gzipReader, err := gzip.NewReader(fileReader.FileOpenRead(ctx, httpFileContentsEntry, 0))
		if err != nil {
			return PatchedHttpArchiveContentsValue{}, err
		}
		tarReader := tar.NewReader(gzipReader)
		for {
			header, err := tarReader.Next()
			if err != nil {
				if err == io.EOF {
					break
				}
				return PatchedHttpArchiveContentsValue{}, err
			}

			switch header.Typeflag {
			case tar.TypeDir:
				if err := rootDirectory.resolveNewDirectory(header.Name); err != nil {
					return model_core.NewSimplePatchedMessage[dag.ObjectContentsWalker](&model_analysis_pb.HttpArchiveContents_Value{
						Result: &model_analysis_pb.HttpArchiveContents_Value_Failure{
							Failure: fmt.Sprintf("Invalid path %#v: %s", header.Name, err),
						},
					}), nil
				}
			case tar.TypeLink:
				panic("TODO")
			case tar.TypeReg, tar.TypeSymlink:
				d, name, err := rootDirectory.resolveNewFile(header.Name)
				if err != nil {
					return model_core.NewSimplePatchedMessage[dag.ObjectContentsWalker](&model_analysis_pb.HttpArchiveContents_Value{
						Result: &model_analysis_pb.HttpArchiveContents_Value_Failure{
							Failure: fmt.Sprintf("Invalid path %#v: %s", header.Name, err),
						},
					}), nil
				}

				switch header.Typeflag {
				case tar.TypeReg:
					if err := d.addFile(name, extractedFilesWriter, tarReader, header.Mode&0o111 != 0); err != nil {
						return PatchedHttpArchiveContentsValue{}, err
					}
				case tar.TypeSymlink:
					if d.symlinks == nil {
						d.symlinks = map[path.Component]path.Parser{}
					}
					d.symlinks[name] = path.UNIXFormat.NewParser(header.Linkname)
				default:
					panic("switch statement should have matched one of the cases above")
				}
			}
		}
	case model_analysis_pb.HttpArchiveContents_Key_ZIP:
		zipReader, err := zip.NewReader(fileReader.FileOpenReadAt(ctx, httpFileContentsEntry), int64(httpFileContentsEntry.EndBytes))
		if err != nil {
			return PatchedHttpArchiveContentsValue{}, err
		}
		for _, file := range zipReader.File {
			if strings.HasSuffix(file.Name, "/") {
				if err := rootDirectory.resolveNewDirectory(file.Name); err != nil {
					return model_core.NewSimplePatchedMessage[dag.ObjectContentsWalker](&model_analysis_pb.HttpArchiveContents_Value{
						Result: &model_analysis_pb.HttpArchiveContents_Value_Failure{
							Failure: fmt.Sprintf("Invalid path %#v: %s", file.Name, err),
						},
					}), nil
				}
			} else {
				d, name, err := rootDirectory.resolveNewFile(file.Name)
				if err != nil {
					return model_core.NewSimplePatchedMessage[dag.ObjectContentsWalker](&model_analysis_pb.HttpArchiveContents_Value{
						Result: &model_analysis_pb.HttpArchiveContents_Value_Failure{
							Failure: fmt.Sprintf("Invalid path %#v: %s", file.Name, err),
						},
					}), nil
				}

				f, err := file.Open()
				if err != nil {
					return PatchedHttpArchiveContentsValue{}, err
				}
				errAdd := d.addFile(name, extractedFilesWriter, f, true)
				f.Close()
				if errAdd != nil {
					return PatchedHttpArchiveContentsValue{}, errAdd
				}
			}
		}
	default:
		return model_core.NewSimplePatchedMessage[dag.ObjectContentsWalker](&model_analysis_pb.HttpArchiveContents_Value{
			Result: &model_analysis_pb.HttpArchiveContents_Value_Failure{
				Failure: "Unknown archive format",
			},
		}), nil
	}

	group, groupCtx := errgroup.WithContext(ctx)
	var rootDirectoryMessage model_core.PatchedMessage[*model_filesystem_pb.Directory, model_filesystem.CapturedObject]
	group.Go(func() error {
		return model_filesystem.CreateDirectoryMerkleTree(
			groupCtx,
			semaphore.NewWeighted(1),
			group,
			directoryCreationParameters,
			fileCreationParameters,
			&openedArchiveDirectory{
				contentsFile: &extractedFiles,
				directory:    &rootDirectory,
			},
			model_filesystem.FileDiscardingDirectoryMerkleTreeCapturer,
			&rootDirectoryMessage,
		)
	})
	if err := group.Wait(); err != nil {
		return model_core.NewSimplePatchedMessage[dag.ObjectContentsWalker](&model_analysis_pb.HttpArchiveContents_Value{
			Result: &model_analysis_pb.HttpArchiveContents_Value_Failure{
				Failure: err.Error(),
			},
		}), nil
	}

	references, children := rootDirectoryMessage.Patcher.SortAndSetReferences()
	contents, err := directoryCreationParameters.EncodeDirectory(references, rootDirectoryMessage.Message)
	if err != nil {
		return model_core.NewSimplePatchedMessage[dag.ObjectContentsWalker](&model_analysis_pb.HttpArchiveContents_Value{
			Result: &model_analysis_pb.HttpArchiveContents_Value_Failure{
				Failure: err.Error(),
			},
		}), nil
	}

	patcher := model_core.NewReferenceMessagePatcher[dag.ObjectContentsWalker]()
	extractedFiles.acquire()
	return PatchedHttpArchiveContentsValue{
		Message: &model_analysis_pb.HttpArchiveContents_Value{
			Result: &model_analysis_pb.HttpArchiveContents_Value_Exists{
				Exists: patcher.AddReference(
					contents.GetReference(),
					model_filesystem.NewCapturedDirectoryWalker(
						directoryCreationParameters.DirectoryAccessParameters,
						fileCreationParameters,
						&openedArchiveDirectory{
							contentsFile: &extractedFiles,
							directory:    &rootDirectory,
						},
						&model_filesystem.CapturedObject{
							Contents: contents,
							Children: children,
						},
					),
				),
			},
		},
		Patcher: patcher,
	}, nil
}
