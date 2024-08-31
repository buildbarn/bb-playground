package main

import (
	"context"
	"crypto/aes"
	"encoding/base64"
	"io"
	"log"
	"math"
	"os"
	"sync/atomic"

	"github.com/buildbarn/bb-playground/pkg/encoding"
	model_core "github.com/buildbarn/bb-playground/pkg/model/core"
	"github.com/buildbarn/bb-playground/pkg/model/core/btree"
	"github.com/buildbarn/bb-playground/pkg/model/core/inlinedtree"
	"github.com/buildbarn/bb-playground/pkg/proto/configuration/upload_directory"
	model_filesystem_pb "github.com/buildbarn/bb-playground/pkg/proto/model/filesystem"
	dag_pb "github.com/buildbarn/bb-playground/pkg/proto/storage/dag"
	"github.com/buildbarn/bb-playground/pkg/storage/dag"
	"github.com/buildbarn/bb-playground/pkg/storage/object"
	"github.com/buildbarn/bb-storage/pkg/filesystem"
	"github.com/buildbarn/bb-storage/pkg/filesystem/path"
	"github.com/buildbarn/bb-storage/pkg/grpc"
	"github.com/buildbarn/bb-storage/pkg/program"
	"github.com/buildbarn/bb-storage/pkg/util"
	cdc "github.com/buildbarn/go-cdc"

	"golang.org/x/sync/semaphore"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

func main() {
	program.RunMain(func(ctx context.Context, siblingsGroup, dependenciesGroup program.Group) error {
		if len(os.Args) != 2 {
			return status.Error(codes.InvalidArgument, "Usage: upload_directory upload_directory.jsonnet")
		}
		var configuration upload_directory.ApplicationConfiguration
		if err := util.UnmarshalConfigurationFromFile(os.Args[1], &configuration); err != nil {
			return util.StatusWrapf(err, "Failed to read configuration from %s", os.Args[1])
		}

		directory, err := filesystem.NewLocalDirectory(path.LocalFormat.NewParser(configuration.Path))
		if err != nil {
			return util.StatusWrap(err, "Failed to open directory to upload")
		}

		namespace, err := object.NewNamespace(configuration.Namespace)
		if err != nil {
			return util.StatusWrap(err, "Invalid namespace")
		}

		encryptionKey, err := aes.NewCipher(configuration.EncryptionKey)
		if err != nil {
			return util.StatusWrap(err, "Invalid encryption key")
		}
		encryptingEncoder := encoding.NewDeterministicEncryptingBinaryEncoder(encryptionKey)

		directoryEncoder := encryptingEncoder
		smallFileEncoder := encoding.NewChainedBinaryEncoder([]encoding.BinaryEncoder{
			encoding.NewLZWCompressingBinaryEncoder(
				uint32(namespace.ReferenceFormat.GetMaximumObjectSizeBytes()),
			),
			encryptingEncoder,
		})
		concatenatedFileEncoder := encryptingEncoder

		directoryMerkleTreeBuilder := directoryMerkleTreeBuilder{
			context:                 ctx,
			referenceFormat:         namespace.ReferenceFormat,
			directoryEncoder:        directoryEncoder,
			smallFileEncoder:        smallFileEncoder,
			concatenatedFileEncoder: concatenatedFileEncoder,
		}
		rootDirectory, err := directoryMerkleTreeBuilder.constructDirectoryMerkleTree(directory, nil)
		if err != nil {
			return util.StatusWrap(err, "Failed to construct directory Merkle tree")
		}

		rootDirectoryObject, err := newComputedDirectoryObject(namespace.ReferenceFormat, directoryEncoder, rootDirectory)
		if err != nil {
			return util.StatusWrap(err, "Failed to get computed directory object for root directory")
		}

		rootDirectoryReference := rootDirectoryObject.contents.GetReference()
		log.Printf("Starting upload of directory with reference %s / %s", rootDirectoryReference, base64.StdEncoding.EncodeToString(rootDirectoryReference.GetRawReference()))

		grpcClientFactory := grpc.NewBaseClientFactory(grpc.BaseClientDialer, nil, nil)
		grpcClient, err := grpcClientFactory.NewClientFromConfiguration(configuration.GrpcClient)
		if err != nil {
			return util.StatusWrap(err, "Failed to create gRPC client")
		}

		if configuration.MaximumUnfinalizedParentsLimit == nil {
			return status.Error(codes.InvalidArgument, "No maximum unfinalized parents limit provided")
		}
		maximumUnfinalizedParentsLimit := object.NewLimit(configuration.MaximumUnfinalizedParentsLimit)

		if err := dag.UploadDAG(
			ctx,
			dag_pb.NewUploaderClient(grpcClient),
			object.GlobalReference{
				InstanceName:   namespace.InstanceName,
				LocalReference: rootDirectoryObject.contents.GetReference(),
			},
			&directoryObjectWalker{
				options: &objectWalkerOptions{
					rootDirectory:           directory,
					directoryEncoder:        directoryEncoder,
					smallFileEncoder:        smallFileEncoder,
					concatenatedFileEncoder: concatenatedFileEncoder,
				},
				object: &rootDirectoryObject,
			},
			semaphore.NewWeighted(10),
			maximumUnfinalizedParentsLimit,
		); err != nil {
			return util.StatusWrap(err, "Failed to upload directory")
		}
		return nil
	})
}

type computedDirectoryObject struct {
	contents *object.Contents
	children []computedDirectoryObject
}

func newComputedDirectoryObject(referenceFormat object.ReferenceFormat, directoryEncoder encoding.BinaryEncoder, directory model_core.MessageWithReferences[*model_filesystem_pb.Directory, computedDirectoryObject]) (computedDirectoryObject, error) {
	references, children := directory.Patcher.SortAndSetReferences()
	data, err := marshalOptions.Marshal(directory.Message)
	if err != nil {
		return computedDirectoryObject{}, err
	}
	encodedData, err := directoryEncoder.EncodeBinary(data)
	if err != nil {
		return computedDirectoryObject{}, err
	}
	contents, err := referenceFormat.NewContents(references, encodedData)
	if err != nil {
		return computedDirectoryObject{}, err
	}
	return computedDirectoryObject{
		contents: contents,
		children: children,
	}, nil
}

type objectWalkerOptions struct {
	rootDirectory           filesystem.Directory
	directoryEncoder        encoding.BinaryEncoder
	smallFileEncoder        encoding.BinaryEncoder
	concatenatedFileEncoder encoding.BinaryEncoder
}

func (o *objectWalkerOptions) openFile(pathTrace *path.Trace) (filesystem.FileReader, error) {
	var dPathTrace *path.Trace
	d := filesystem.NopDirectoryCloser(o.rootDirectory)
	defer d.Close()

	components := pathTrace.ToList()
	for _, component := range components[:len(components)-1] {
		dPathTrace = dPathTrace.Append(component)
		dChild, err := d.EnterDirectory(component)
		if err != nil {
			return nil, util.StatusWrapf(err, "Failed to enter directory %#v", dPathTrace.GetUNIXString())
		}
		d.Close()
		d = dChild
	}

	return d.OpenRead(components[len(components)-1])
}

type directoryObjectWalker struct {
	options   *objectWalkerOptions
	object    *computedDirectoryObject
	pathTrace *path.Trace
}

func (w *directoryObjectWalker) GetContents() (*object.Contents, []dag.ObjectContentsWalker, error) {
	log.Printf("Uploading directory %#v with reference %s", w.pathTrace.GetUNIXString(), w.object.contents.GetReference())

	decodedData, err := w.options.directoryEncoder.DecodeBinary(w.object.contents.GetPayload())
	if err != nil {
		panic(err)
	}
	var directory model_filesystem_pb.Directory
	if err := proto.Unmarshal(decodedData, &directory); err != nil {
		panic(err)
	}

	walkers := make([]dag.ObjectContentsWalker, w.object.contents.GetDegree())
	w.gatherWalkers(&directory, w.pathTrace, walkers)
	return w.object.contents, walkers, nil
}

func (w *directoryObjectWalker) gatherWalkers(directory *model_filesystem_pb.Directory, pathTrace *path.Trace, walkers []dag.ObjectContentsWalker) {
	switch leaves := directory.Leaves.(type) {
	case *model_filesystem_pb.Directory_LeavesExternal:
		index, err := model_core.GetIndexFromReferenceMessage(leaves.LeavesExternal, len(walkers))
		if err != nil {
			panic(err)
		}
		walkers[index] = &leavesObjectWalker{
			options:   w.options,
			object:    &w.object.children[index],
			pathTrace: pathTrace,
		}
	case *model_filesystem_pb.Directory_LeavesInline:
		for _, childFile := range leaves.LeavesInline.Files {
			if contents := childFile.Contents; contents != nil {
				childPathTrace := pathTrace.Append(path.MustNewComponent(childFile.Name))
				index, err := model_core.GetIndexFromReferenceMessage(contents.Reference, len(walkers))
				if err != nil {
					panic(err)
				}
				childReference := w.object.contents.GetOutgoingReference(index)
				if childReference.GetHeight() == 0 {
					walkers[index] = &smallFileObjectWalker{
						options:   w.options,
						reference: childReference,
						pathTrace: childPathTrace,
						sizeBytes: uint32(contents.TotalSizeBytes),
					}
				} else {
					walkers[index] = &recomputingConcatenatedFileObjectWalker{
						options:   w.options,
						reference: childReference,
						pathTrace: childPathTrace,
					}
				}
			}
		}
	default:
		panic("unknown leaves type")
	}

	for _, childDirectory := range directory.Directories {
		childPathTrace := pathTrace.Append(path.MustNewComponent(childDirectory.Name))
		switch contents := childDirectory.Contents.(type) {
		case *model_filesystem_pb.DirectoryNode_ContentsExternal:
			index, err := model_core.GetIndexFromReferenceMessage(contents.ContentsExternal.Reference, len(walkers))
			if err != nil {
				panic(err)
			}
			walkers[index] = &directoryObjectWalker{
				options:   w.options,
				object:    &w.object.children[index],
				pathTrace: childPathTrace,
			}
		case *model_filesystem_pb.DirectoryNode_ContentsInline:
			w.gatherWalkers(contents.ContentsInline, childPathTrace, walkers)
		default:
			panic("unknown directory contents type")
		}
	}
}

func (w *directoryObjectWalker) Discard() {
	log.Print("Skipping directory ", w.pathTrace.GetUNIXString())
}

type leavesObjectWalker struct {
	options   *objectWalkerOptions
	object    *computedDirectoryObject
	pathTrace *path.Trace
}

func (w *leavesObjectWalker) GetContents() (*object.Contents, []dag.ObjectContentsWalker, error) {
	log.Printf("Uploading leaves %#v with reference %s", w.pathTrace.GetUNIXString(), w.object.contents.GetReference())

	decodedData, err := w.options.directoryEncoder.DecodeBinary(w.object.contents.GetPayload())
	if err != nil {
		panic(err)
	}
	var leaves model_filesystem_pb.Leaves
	if err := proto.Unmarshal(decodedData, &leaves); err != nil {
		panic(err)
	}

	walkers := make([]dag.ObjectContentsWalker, w.object.contents.GetDegree())
	for _, childFile := range leaves.Files {
		if contents := childFile.Contents; contents != nil {
			childPathTrace := w.pathTrace.Append(path.MustNewComponent(childFile.Name))
			index, err := model_core.GetIndexFromReferenceMessage(contents.Reference, len(walkers))
			if err != nil {
				panic(err)
			}
			childReference := w.object.contents.GetOutgoingReference(index)
			if childReference.GetHeight() == 0 {
				walkers[index] = &smallFileObjectWalker{
					options:   w.options,
					reference: childReference,
					pathTrace: childPathTrace,
					sizeBytes: uint32(contents.TotalSizeBytes),
				}
			} else {
				walkers[index] = &recomputingConcatenatedFileObjectWalker{
					options:   w.options,
					reference: childReference,
					pathTrace: childPathTrace,
				}
			}
		}
	}
	return w.object.contents, walkers, nil
}

func (w *leavesObjectWalker) Discard() {
	log.Print("Skipping leaves ", w.pathTrace.GetUNIXString())
}

type smallFileObjectWalker struct {
	options   *objectWalkerOptions
	reference object.LocalReference
	pathTrace *path.Trace
	sizeBytes uint32
}

func (w *smallFileObjectWalker) GetContents() (*object.Contents, []dag.ObjectContentsWalker, error) {
	log.Printf("Uploading small file %#v", w.pathTrace.GetUNIXString())

	r, err := w.options.openFile(w.pathTrace)
	if err != nil {
		return nil, nil, util.StatusWrapf(err, "Failed to open %#v", w.pathTrace.GetUNIXString())
	}
	defer r.Close()

	data := make([]byte, w.sizeBytes)
	if n, err := r.ReadAt(data, 0); n != len(data) {
		return nil, nil, util.StatusWrapf(err, "Failed to read %#v", w.pathTrace.GetUNIXString())
	}
	encodedData, err := w.options.smallFileEncoder.EncodeBinary(data)
	if err != nil {
		return nil, nil, util.StatusWrapf(err, "Failed to encode %#v", w.pathTrace.GetUNIXString())
	}
	contents, err := w.reference.GetReferenceFormat().NewContents(nil, encodedData)
	if err != nil {
		panic(err)
	}
	if actualReference := contents.GetReference(); actualReference != w.reference {
		return nil, nil, status.Errorf(codes.InvalidArgument, "File %#v has reference %s, while %s was expected", w.pathTrace.GetUNIXString(), actualReference, w.reference)
	}
	return contents, nil, nil
}

func (w *smallFileObjectWalker) Discard() {
	log.Print("Skipping small file ", w.pathTrace.GetUNIXString())
}

type recomputingConcatenatedFileObjectWalker struct {
	options   *objectWalkerOptions
	reference object.LocalReference
	pathTrace *path.Trace
}

type computedConcatenatedFileObject struct {
	contents *object.Contents
	children []computedConcatenatedFileObject
}

type recordingFileMerkleTreeCapturer struct{}

func (recordingFileMerkleTreeCapturer) CaptureSmallFile(contents *object.Contents) computedConcatenatedFileObject {
	return computedConcatenatedFileObject{}
}

func (recordingFileMerkleTreeCapturer) CaptureConcatenatedFile(contents *object.Contents, children []computedConcatenatedFileObject) computedConcatenatedFileObject {
	o := computedConcatenatedFileObject{
		contents: contents,
	}
	if contents.GetReference().GetHeight() > 1 {
		o.children = children
	}
	return o
}

func (w *recomputingConcatenatedFileObjectWalker) GetContents() (*object.Contents, []dag.ObjectContentsWalker, error) {
	log.Printf("Uploading concatenated file %#v with reference %s", w.pathTrace.GetUNIXString(), w.reference)

	r, err := w.options.openFile(w.pathTrace)
	if err != nil {
		return nil, nil, util.StatusWrapf(err, "Failed to open %#v", w.pathTrace.GetUNIXString())
	}
	fileContents, err := constructFileMerkleTree(w.reference.GetReferenceFormat(), w.options.smallFileEncoder, w.options.concatenatedFileEncoder, r, recordingFileMerkleTreeCapturer{})
	if err != nil {
		r.Close()
		return nil, nil, err
	}

	if fileContents == nil {
		return nil, nil, status.Errorf(codes.InvalidArgument, "File %#v no longer has any contents", w.pathTrace.GetUNIXString())
	}
	references, objects := fileContents.Patcher.SortAndSetReferences()
	if references[0] != w.reference {
		r.Close()
		return nil, nil, status.Errorf(codes.InvalidArgument, "File %#v has reference %s, while %s was expected", w.pathTrace.GetUNIXString(), references[0], w.reference)
	}

	options := &computedConcatenatedFileObjectOptions{
		smallFileEncoder:        w.options.smallFileEncoder,
		concatenatedFileEncoder: w.options.concatenatedFileEncoder,
		pathTrace:               w.pathTrace,
		file:                    r,
	}
	options.referenceCount.Store(1)
	wComputed := &computedConcatenatedFileObjectWalker{
		options: options,
		object:  &objects[0],
	}
	return wComputed.GetContents()
}

func (w *recomputingConcatenatedFileObjectWalker) Discard() {
	log.Print("Skipping concatenated file ", w.pathTrace.GetUNIXString())
}

type computedConcatenatedFileObjectOptions struct {
	smallFileEncoder        encoding.BinaryEncoder
	concatenatedFileEncoder encoding.BinaryEncoder
	pathTrace               *path.Trace
	referenceCount          atomic.Uint64
	file                    filesystem.FileReader
}

type computedConcatenatedFileObjectWalker struct {
	options     *computedConcatenatedFileObjectOptions
	object      *computedConcatenatedFileObject
	offsetBytes uint64
}

func (w *computedConcatenatedFileObjectWalker) GetContents() (*object.Contents, []dag.ObjectContentsWalker, error) {
	var fileContentsList model_filesystem_pb.FileContentsList
	decodedData, err := w.options.concatenatedFileEncoder.DecodeBinary(w.object.contents.GetPayload())
	if err != nil {
		panic(err)
	}
	if err := proto.Unmarshal(decodedData, &fileContentsList); err != nil {
		panic(err)
	}
	reference := w.object.contents.GetReference()
	walkers := make([]dag.ObjectContentsWalker, reference.GetDegree())
	offsetBytes := w.offsetBytes
	for _, part := range fileContentsList.Parts {
		index, err := model_core.GetIndexFromReferenceMessage(part.Reference, len(walkers))
		if err != nil {
			panic(err)
		}
		partReference := w.object.contents.GetOutgoingReference(index)
		if partReference.GetHeight() == 0 {
			walkers[index] = &concatenatedFileChunkObjectWalker{
				options:     w.options,
				reference:   partReference,
				offsetBytes: offsetBytes,
				sizeBytes:   uint32(part.TotalSizeBytes),
			}
		} else {
			walkers[index] = &computedConcatenatedFileObjectWalker{
				options:     w.options,
				object:      &w.object.children[index],
				offsetBytes: offsetBytes,
			}
		}
		offsetBytes += part.TotalSizeBytes
	}
	w.options.referenceCount.Add(uint64(len(walkers) - 1))
	return w.object.contents, walkers, nil
}

func (w *computedConcatenatedFileObjectWalker) Discard() {
	if w.options.referenceCount.Add(^uint64(0)) == 0 {
		w.options.file.Close()
	}
}

type concatenatedFileChunkObjectWalker struct {
	options     *computedConcatenatedFileObjectOptions
	reference   object.LocalReference
	offsetBytes uint64
	sizeBytes   uint32
}

func (w *concatenatedFileChunkObjectWalker) GetContents() (*object.Contents, []dag.ObjectContentsWalker, error) {
	defer w.Discard()

	data := make([]byte, w.sizeBytes)
	if n, err := w.options.file.ReadAt(data, int64(w.offsetBytes)); n != len(data) {
		return nil, nil, util.StatusWrapf(err, "Failed to read %#v at offset %d", w.options.pathTrace.GetUNIXString(), w.offsetBytes)
	}
	encodedData, err := w.options.smallFileEncoder.EncodeBinary(data)
	if err != nil {
		return nil, nil, util.StatusWrapf(err, "Failed to encode %#v", w.options.pathTrace.GetUNIXString())
	}
	contents, err := w.reference.GetReferenceFormat().NewContents(nil, encodedData)
	if err != nil {
		panic(err)
	}
	if actualReference := contents.GetReference(); actualReference != w.reference {
		return nil, nil, status.Errorf(codes.InvalidArgument, "Chunk at offset %d in file %#v has reference %s, while %s was expected", w.offsetBytes, w.options.pathTrace.GetUNIXString(), actualReference, w.reference)
	}
	return contents, nil, nil
}

func (w *concatenatedFileChunkObjectWalker) Discard() {
	if w.options.referenceCount.Add(^uint64(0)) == 0 {
		w.options.file.Close()
	}
}

var marshalOptions = proto.MarshalOptions{UseCachedSize: true}

type noopFileMerkleTreeCapturer struct{}

func (noopFileMerkleTreeCapturer) CaptureSmallFile(contents *object.Contents) computedDirectoryObject {
	return computedDirectoryObject{}
}

func (noopFileMerkleTreeCapturer) CaptureConcatenatedFile(contents *object.Contents, children []computedDirectoryObject) computedDirectoryObject {
	return computedDirectoryObject{}
}

type directoryMerkleTreeBuilder struct {
	context                 context.Context
	referenceFormat         object.ReferenceFormat
	directoryEncoder        encoding.BinaryEncoder
	smallFileEncoder        encoding.BinaryEncoder
	concatenatedFileEncoder encoding.BinaryEncoder
}

func (b *directoryMerkleTreeBuilder) constructDirectoryMerkleTree(directory filesystem.Directory, directoryPath *path.Trace) (model_core.MessageWithReferences[*model_filesystem_pb.Directory, computedDirectoryObject], error) {
	if b.context.Err() != nil {
		return model_core.MessageWithReferences[*model_filesystem_pb.Directory, computedDirectoryObject]{}, util.StatusFromContext(b.context)
	}

	entries, err := directory.ReadDir()
	if err != nil {
		return model_core.MessageWithReferences[*model_filesystem_pb.Directory, computedDirectoryObject]{}, util.StatusWrap(err, "Failed to read directory contents")
	}

	// Construct a Directory message that only holds the leaf nodes
	// contained in the current directory.
	var directoryNames []path.Component
	var leaves model_filesystem_pb.Leaves
	leavesPatcher := model_core.NewReferenceMessagePatcher[computedDirectoryObject]()
	for _, entry := range entries {
		name := entry.Name()
		switch entry.Type() {
		case filesystem.FileTypeDirectory:
			directoryNames = append(directoryNames, name)
		case filesystem.FileTypeRegularFile:
			f, err := directory.OpenRead(name)
			if err != nil {
				return model_core.MessageWithReferences[*model_filesystem_pb.Directory, computedDirectoryObject]{}, util.StatusWrapf(err, "Failed to open file %#v", directoryPath.Append(name).GetUNIXString())
			}
			fileContents, err := constructFileMerkleTree(b.referenceFormat, b.smallFileEncoder, b.concatenatedFileEncoder, f, noopFileMerkleTreeCapturer{})
			f.Close()
			if err != nil {
				return model_core.MessageWithReferences[*model_filesystem_pb.Directory, computedDirectoryObject]{}, err
			}
			var fileContentsMessage *model_filesystem_pb.FileContents
			if fileContents != nil {
				fileContentsMessage = fileContents.Message
				leavesPatcher.Merge(fileContents.Patcher)
			}
			leaves.Files = append(
				leaves.Files,
				&model_filesystem_pb.FileNode{
					Name:         name.String(),
					Contents:     fileContentsMessage,
					IsExecutable: entry.IsExecutable(),
				},
			)
		case filesystem.FileTypeSymlink:
			targetParser, err := directory.Readlink(name)
			if err != nil {
				return model_core.MessageWithReferences[*model_filesystem_pb.Directory, computedDirectoryObject]{}, util.StatusWrapf(err, "Failed to read target of symbolic link %#v", directoryPath.Append(name).GetUNIXString())
			}
			targetBuilder, scopeWalker := path.EmptyBuilder.Join(path.VoidScopeWalker)
			if err := path.Resolve(targetParser, scopeWalker); err != nil {
				return model_core.MessageWithReferences[*model_filesystem_pb.Directory, computedDirectoryObject]{}, util.StatusWrapf(err, "Failed to resolve target of symbolic link %#v", directoryPath.Append(name).GetUNIXString())
			}
			leaves.Symlinks = append(
				leaves.Symlinks,
				&model_filesystem_pb.SymlinkNode{
					Name:   name.String(),
					Target: targetBuilder.GetUNIXString(),
				},
			)
		}
	}

	inlineCandidates := make([]inlinedtree.Candidate[*model_filesystem_pb.Directory, computedDirectoryObject], 0, 1+len(directoryNames))
	leavesInline := &model_filesystem_pb.Directory_LeavesInline{
		LeavesInline: &leaves,
	}
	inlineCandidates = append(
		inlineCandidates,
		inlinedtree.Candidate[*model_filesystem_pb.Directory, computedDirectoryObject]{
			ExternalMessage: model_core.MessageWithReferences[proto.Message, computedDirectoryObject]{
				Message: &leaves,
				Patcher: leavesPatcher,
			},
			ParentAppender: func(
				directory model_core.MessageWithReferences[*model_filesystem_pb.Directory, computedDirectoryObject],
				externalContents *object.Contents,
				externalChildren []computedDirectoryObject,
			) {
				if externalContents == nil {
					directory.Message.Leaves = leavesInline
				} else {
					directory.Message.Leaves = &model_filesystem_pb.Directory_LeavesExternal{
						LeavesExternal: directory.Patcher.AddReference(
							externalContents.GetReference(),
							computedDirectoryObject{
								contents: externalContents,
								children: externalChildren,
							},
						),
					}
				}
			},
		},
	)

	for _, name := range directoryNames {
		childPath := directoryPath.Append(name)
		child, err := directory.EnterDirectory(name)
		if err != nil {
			return model_core.MessageWithReferences[*model_filesystem_pb.Directory, computedDirectoryObject]{}, util.StatusWrapf(err, "Failed to open directory %#v", childPath.GetUNIXString())
		}
		childDirectory, err := b.constructDirectoryMerkleTree(child, childPath)
		if err != nil {
			return model_core.MessageWithReferences[*model_filesystem_pb.Directory, computedDirectoryObject]{}, err
		}

		nameStr := name.String()
		directoriesCount := uint32(len(childDirectory.Message.Directories))
		inlineDirectoryNode := model_filesystem_pb.DirectoryNode{
			Name: nameStr,
			Contents: &model_filesystem_pb.DirectoryNode_ContentsInline{
				ContentsInline: childDirectory.Message,
			},
		}
		inlineCandidates = append(
			inlineCandidates,
			inlinedtree.Candidate[*model_filesystem_pb.Directory, computedDirectoryObject]{
				ExternalMessage: model_core.MessageWithReferences[proto.Message, computedDirectoryObject]{
					Message: childDirectory.Message,
					Patcher: childDirectory.Patcher,
				},
				ParentAppender: func(
					directory model_core.MessageWithReferences[*model_filesystem_pb.Directory, computedDirectoryObject],
					externalContents *object.Contents,
					externalChildren []computedDirectoryObject,
				) {
					if externalContents == nil {
						directory.Message.Directories = append(directory.Message.Directories, &inlineDirectoryNode)
					} else {
						directory.Message.Directories = append(directory.Message.Directories, &model_filesystem_pb.DirectoryNode{
							Name: nameStr,
							Contents: &model_filesystem_pb.DirectoryNode_ContentsExternal{
								ContentsExternal: &model_filesystem_pb.DirectoryReference{
									Reference: directory.Patcher.AddReference(
										externalContents.GetReference(),
										computedDirectoryObject{
											contents: externalContents,
											children: externalChildren,
										},
									),
									DirectoriesCount: directoriesCount,
								},
							},
						})
					}
				},
			},
		)
	}

	return inlinedtree.Build(b.referenceFormat, b.directoryEncoder, inlineCandidates, 16*1024)
}

type FileMerkleTreeCapturer[T any] interface {
	CaptureSmallFile(contents *object.Contents) T
	CaptureConcatenatedFile(contents *object.Contents, children []T) T
}

func constructFileMerkleTree[T any](referenceFormat object.ReferenceFormat, smallFileEncoder, concatenatedFileEncoder encoding.BinaryEncoder, file io.ReaderAt, capturer FileMerkleTreeCapturer[T]) (*model_core.MessageWithReferences[*model_filesystem_pb.FileContents, T], error) {
	r := cdc.NewMaxContentDefinedChunker(
		io.NewSectionReader(file, 0, math.MaxInt64),
		/* bufferSizeBytes = */ 2*1024*1024,
		/* minSizeBytes = */ 64*1024,
		/* maxSizeBytes = */ 256*1024,
	)

	treeBuilder := btree.NewBuilder(
		btree.NewProllyLevelBuilderFactory(
			/* minimumCount = */ 2,
			/* minimumSizeBytes = */ 1024,
			/* maximumSizeBytes = */ 4*1024,
			concatenatedFileEncoder,
			referenceFormat,
			/* parentNodeComputer = */ func(contents *object.Contents, childNodes []*model_filesystem_pb.FileContents, outgoingReferences object.OutgoingReferences, metadata []T) (model_core.MessageWithReferences[*model_filesystem_pb.FileContents, T], error) {
				// Compute the total file size to store
				// in the parent FileContents node.
				var totalSizeBytes uint64
				for _, childNode := range childNodes {
					totalSizeBytes += childNode.TotalSizeBytes
				}

				patcher := model_core.NewReferenceMessagePatcher[T]()
				return model_core.MessageWithReferences[*model_filesystem_pb.FileContents, T]{
					Message: &model_filesystem_pb.FileContents{
						TotalSizeBytes: totalSizeBytes,
						Reference:      patcher.AddReference(contents.GetReference(), capturer.CaptureConcatenatedFile(contents, metadata)),
					},
					Patcher: patcher,
				}, nil
			},
		),
	)

	for {
		// Obtain the next chunk of data from the file and
		// create a FileContents message that can be used to
		// refer to it.
		chunk, err := r.ReadNextChunk()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		encodedChunk, err := smallFileEncoder.EncodeBinary(chunk)
		if err != nil {
			return nil, err
		}
		chunkObjectContents, err := referenceFormat.NewContents(nil, encodedChunk)
		if err != nil {
			return nil, err
		}

		patcher := model_core.NewReferenceMessagePatcher[T]()
		if err := treeBuilder.PushChild(model_core.MessageWithReferences[*model_filesystem_pb.FileContents, T]{
			Message: &model_filesystem_pb.FileContents{
				Reference:      patcher.AddReference(chunkObjectContents.GetReference(), capturer.CaptureSmallFile(chunkObjectContents)),
				TotalSizeBytes: uint64(len(chunk)),
			},
			Patcher: patcher,
		}); err != nil {
			return nil, err
		}
	}

	return treeBuilder.Finalize()
}
