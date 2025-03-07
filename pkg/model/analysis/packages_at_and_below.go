package analysis

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/buildbarn/bb-storage/pkg/filesystem/path"
	"github.com/buildbarn/bonanza/pkg/evaluation"
	"github.com/buildbarn/bonanza/pkg/label"
	model_core "github.com/buildbarn/bonanza/pkg/model/core"
	model_filesystem "github.com/buildbarn/bonanza/pkg/model/filesystem"
	model_parser "github.com/buildbarn/bonanza/pkg/model/parser"
	model_analysis_pb "github.com/buildbarn/bonanza/pkg/proto/model/analysis"
	model_filesystem_pb "github.com/buildbarn/bonanza/pkg/proto/model/filesystem"
	"github.com/buildbarn/bonanza/pkg/storage/dag"
)

func (c *baseComputer[TReference, TMetadata]) ComputePackagesAtAndBelowValue(ctx context.Context, key *model_analysis_pb.PackagesAtAndBelow_Key, e PackagesAtAndBelowEnvironment[TReference]) (PatchedPackagesAtAndBelowValue, error) {
	basePackage, err := label.NewCanonicalPackage(key.BasePackage)
	if err != nil {
		return PatchedPackagesAtAndBelowValue{}, errors.New("invalid base package")
	}

	directoryReaders, gotDirectoryReaders := e.GetDirectoryReadersValue(
		&model_analysis_pb.DirectoryReaders_Key{},
	)
	repoValue := e.GetRepoValue(&model_analysis_pb.Repo_Key{
		CanonicalRepo: basePackage.GetCanonicalRepo().String(),
	})
	if !gotDirectoryReaders || !repoValue.IsSet() {
		return PatchedPackagesAtAndBelowValue{}, evaluation.ErrMissingDependency
	}

	// Obtain the root directory of the repo.
	baseDirectory, err := model_parser.Dereference(ctx, directoryReaders.Directory, model_core.NewNestedMessage(repoValue, repoValue.Message.RootDirectoryReference.GetReference()))
	if err != nil {
		return PatchedPackagesAtAndBelowValue{}, err
	}

	// Traverse into the directory belonging to the package path.
	for component := range strings.FieldsFuncSeq(
		basePackage.GetPackagePath(),
		func(r rune) bool { return r == '/' },
	) {
		directories := baseDirectory.Message.Directories
		directoryIndex, ok := sort.Find(
			len(directories),
			func(i int) int { return strings.Compare(component, directories[i].Name) },
		)
		if !ok {
			// Base package does not exist.
			return model_core.NewSimplePatchedMessage[dag.ObjectContentsWalker](&model_analysis_pb.PackagesAtAndBelow_Value{}), nil
		}
		switch contents := directories[directoryIndex].Contents.(type) {
		case *model_filesystem_pb.DirectoryNode_ContentsExternal:
			baseDirectory, err = model_parser.Dereference(ctx, directoryReaders.Directory, model_core.NewNestedMessage(baseDirectory, contents.ContentsExternal.Reference))
			if err != nil {
				return PatchedPackagesAtAndBelowValue{}, err
			}
		case *model_filesystem_pb.DirectoryNode_ContentsInline:
			baseDirectory = model_core.NewNestedMessage(baseDirectory, contents.ContentsInline)
		default:
			return PatchedPackagesAtAndBelowValue{}, errors.New("invalid directory contents type")
		}
	}

	// Find packages at and below the base package.
	checker := packageExistenceChecker[TReference]{
		context:         ctx,
		directoryReader: directoryReaders.Directory,
		leavesReader:    directoryReaders.Leaves,
	}
	packageAtBasePackage, err := checker.directoryIsPackage(baseDirectory)
	if err != nil {
		return PatchedPackagesAtAndBelowValue{}, err
	}
	if err := checker.findPackagesBelow(baseDirectory, nil); err != nil {
		return PatchedPackagesAtAndBelowValue{}, err
	}

	return model_core.NewSimplePatchedMessage[dag.ObjectContentsWalker](&model_analysis_pb.PackagesAtAndBelow_Value{
		PackageAtBasePackage:     packageAtBasePackage,
		PackagesBelowBasePackage: checker.packagesBelowBasePackage,
	}), nil
}

type packageExistenceChecker[TReference any] struct {
	context                  context.Context
	directoryReader          model_parser.ParsedObjectReader[TReference, model_core.Message[*model_filesystem_pb.Directory, TReference]]
	leavesReader             model_parser.ParsedObjectReader[TReference, model_core.Message[*model_filesystem_pb.Leaves, TReference]]
	packagesBelowBasePackage []string
}

func (pec *packageExistenceChecker[TReference]) directoryIsPackage(d model_core.Message[*model_filesystem_pb.Directory, TReference]) (bool, error) {
	leaves, err := model_filesystem.DirectoryGetLeaves(pec.context, pec.leavesReader, d)
	if err != nil {
		return false, err
	}

	// TODO: Should we also consider symlinks having such names?
	files := leaves.Message.Files
	for _, filename := range buildDotBazelTargetNames {
		filenameStr := filename.String()
		if _, ok := sort.Find(
			len(files),
			func(i int) int { return strings.Compare(filenameStr, files[i].Name) },
		); ok {
			// Current directory is a package.
			return true, nil
		}
	}
	return false, nil
}

func (pec *packageExistenceChecker[TReference]) findPackagesBelow(d model_core.Message[*model_filesystem_pb.Directory, TReference], dTrace *path.Trace) error {
	for _, entry := range d.Message.Directories {
		name, ok := path.NewComponent(entry.Name)
		if !ok {
			return fmt.Errorf("invalid directory name %#v in directory %#v", entry.Name, dTrace.GetUNIXString())
		}
		childTrace := dTrace.Append(name)

		var childDirectory model_core.Message[*model_filesystem_pb.Directory, TReference]
		switch contents := entry.Contents.(type) {
		case *model_filesystem_pb.DirectoryNode_ContentsExternal:
			var err error
			childDirectory, err = model_parser.Dereference(pec.context, pec.directoryReader, model_core.NewNestedMessage(d, contents.ContentsExternal.Reference))
			if err != nil {
				return err
			}
		case *model_filesystem_pb.DirectoryNode_ContentsInline:
			childDirectory = model_core.NewNestedMessage(d, contents.ContentsInline)
		default:
			return fmt.Errorf("invalid contents type for directory %#v", childTrace.GetUNIXString())
		}

		directoryIsPackage, err := pec.directoryIsPackage(childDirectory)
		if err != nil {
			return fmt.Errorf("failed to determine whether directory %#v is a package: %w", dTrace.GetUNIXString(), err)
		}
		if directoryIsPackage {
			pec.packagesBelowBasePackage = append(pec.packagesBelowBasePackage, childTrace.GetUNIXString())
		} else {
			// Not a package. Find packages below.
			if err := pec.findPackagesBelow(childDirectory, childTrace); err != nil {
				return err
			}
		}
	}
	return nil
}
