package starlark

import (
	"errors"
	"fmt"

	pg_label "github.com/buildbarn/bonanza/pkg/label"
	model_core "github.com/buildbarn/bonanza/pkg/model/core"
	model_starlark_pb "github.com/buildbarn/bonanza/pkg/proto/model/starlark"
	"github.com/buildbarn/bonanza/pkg/starlark/unpack"
	"github.com/buildbarn/bonanza/pkg/storage/object"

	"go.starlark.net/starlark"
	"go.starlark.net/syntax"
)

type Runfiles[TReference object.BasicReference, TMetadata model_core.CloneableReferenceMetadata] struct {
	files        *Depset[TReference, TMetadata]
	rootSymlinks *Depset[TReference, TMetadata]
	symlinks     *Depset[TReference, TMetadata]
	hash         uint32
}

var (
	_ EncodableValue[object.LocalReference, model_core.CloneableReferenceMetadata] = (*Runfiles[object.LocalReference, model_core.CloneableReferenceMetadata])(nil)
	_ starlark.Comparable                                                          = (*Runfiles[object.LocalReference, model_core.CloneableReferenceMetadata])(nil)
	_ starlark.HasAttrs                                                            = (*Runfiles[object.LocalReference, model_core.CloneableReferenceMetadata])(nil)
)

func NewRunfiles[TReference object.BasicReference, TMetadata model_core.CloneableReferenceMetadata](files, rootSymlinks, symlinks *Depset[TReference, TMetadata]) *Runfiles[TReference, TMetadata] {
	return &Runfiles[TReference, TMetadata]{
		files:        files,
		rootSymlinks: rootSymlinks,
		symlinks:     symlinks,
	}
}

func (Runfiles[TReference, TMetadata]) String() string {
	return "<runfiles>"
}

func (Runfiles[TReference, TMetadata]) Type() string {
	return "runfiles"
}

func (Runfiles[TReference, TMetadata]) Freeze() {
}

func (Runfiles[TReference, TMetadata]) Truth() starlark.Bool {
	return starlark.True
}

func (r *Runfiles[TReference, TMetadata]) Hash(thread *starlark.Thread) (uint32, error) {
	if r.hash == 0 {
		filesHash, err := r.files.Hash(thread)
		if err != nil {
			return 0, fmt.Errorf("files: %w", err)
		}
		rootSymlinksHash, err := r.rootSymlinks.Hash(thread)
		if err != nil {
			return 0, fmt.Errorf("root_symlinks: %w", err)
		}
		symlinksHash, err := r.symlinks.Hash(thread)
		if err != nil {
			return 0, fmt.Errorf("symlinks: %w", err)
		}
		h := filesHash*2911474037 + rootSymlinksHash*3974532307 + symlinksHash*2991729701
		if h == 0 {
			h = 1
		}
		r.hash = h
	}
	return r.hash, nil
}

func (r *Runfiles[TReference, TMetadata]) equals(thread *starlark.Thread, other *Runfiles[TReference, TMetadata], depth int) (bool, error) {
	if r != other {
		equal, err := starlark.EqualDepth(thread, r.files, other.files, depth-1)
		if err != nil || !equal {
			return false, err
		}
		equal, err = starlark.EqualDepth(thread, r.rootSymlinks, other.rootSymlinks, depth-1)
		if err != nil || !equal {
			return false, err
		}
		equal, err = starlark.EqualDepth(thread, r.symlinks, other.symlinks, depth-1)
		if err != nil || !equal {
			return false, err
		}
	}
	return true, nil
}

func (r *Runfiles[TReference, TMetadata]) CompareSameType(thread *starlark.Thread, op syntax.Token, other starlark.Value, depth int) (bool, error) {
	switch op {
	case syntax.EQL:
		return r.equals(thread, other.(*Runfiles[TReference, TMetadata]), depth)
	case syntax.NEQ:
		equal, err := r.equals(thread, other.(*Runfiles[TReference, TMetadata]), depth)
		return !equal, err
	default:
		return false, errors.New("runfiles cannot be compared for inequality")
	}
}

func (r *Runfiles[TReference, TMetadata]) Attr(thread *starlark.Thread, name string) (starlark.Value, error) {
	switch name {
	case "empty_filenames":
		return nil, errors.New("TODO: Implement runfiles.empty_filenames")
	case "files":
		return r.files, nil
	case "merge":
		return starlark.NewBuiltin("runfiles.merge", r.doMerge), nil
	case "merge_all":
		return starlark.NewBuiltin("runfiles.merge_all", r.doMergeAll), nil
	case "root_symlinks":
		return r.rootSymlinks, nil
	case "symlinks":
		return r.symlinks, nil
	default:
		return nil, nil
	}
}

var runfilesAttrNames = []string{
	"empty_filenames",
	"files",
	"merge",
	"merge_all",
	"root_symlinks",
	"symlinks",
}

func (Runfiles[TReference, TMetadata]) AttrNames() []string {
	return runfilesAttrNames
}

func mergeRunfiles[TReference object.BasicReference, TMetadata model_core.CloneableReferenceMetadata](thread *starlark.Thread, allFiles, allRootSymlinks, allSymlinks []*Depset[TReference, TMetadata]) (starlark.Value, error) {
	files, err := NewDepset(thread, nil, allFiles, model_starlark_pb.Depset_DEFAULT)
	if err != nil {
		return nil, fmt.Errorf("failed to merge files: %w", err)
	}
	rootSymlinks, err := NewDepset(thread, nil, allRootSymlinks, model_starlark_pb.Depset_DEFAULT)
	if err != nil {
		return nil, fmt.Errorf("failed to merge root symlinks: %w", err)
	}
	symlinks, err := NewDepset(thread, nil, allSymlinks, model_starlark_pb.Depset_DEFAULT)
	if err != nil {
		return nil, fmt.Errorf("failed to merge symlinks: %w", err)
	}
	return NewRunfiles[TReference, TMetadata](files, rootSymlinks, symlinks), nil
}

func (r *Runfiles[TReference, TMetadata]) doMerge(thread *starlark.Thread, b *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
	var other *Runfiles[TReference, TMetadata]
	if err := starlark.UnpackArgs(
		b.Name(), args, kwargs,
		"other", unpack.Bind(thread, &other, unpack.Type[*Runfiles[TReference, TMetadata]]("runfiles")),
	); err != nil {
		return nil, err
	}

	return mergeRunfiles(
		thread,
		[]*Depset[TReference, TMetadata]{r.files, other.files},
		[]*Depset[TReference, TMetadata]{r.rootSymlinks, other.rootSymlinks},
		[]*Depset[TReference, TMetadata]{r.symlinks, other.symlinks},
	)
}

func (r *Runfiles[TReference, TMetadata]) doMergeAll(thread *starlark.Thread, b *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
	var other []*Runfiles[TReference, TMetadata]
	if err := starlark.UnpackArgs(
		b.Name(), args, kwargs,
		"other", unpack.Bind(thread, &other, unpack.List(unpack.Type[*Runfiles[TReference, TMetadata]]("runfiles"))),
	); err != nil {
		return nil, err
	}

	transitiveFiles := append(make([]*Depset[TReference, TMetadata], 0, len(other)+1), r.files)
	transitiveRootSymlinks := append(make([]*Depset[TReference, TMetadata], 0, len(other)+1), r.rootSymlinks)
	transitiveSymlinks := append(make([]*Depset[TReference, TMetadata], 0, len(other)+1), r.symlinks)
	for _, o := range other {
		transitiveFiles = append(transitiveFiles, o.files)
		transitiveRootSymlinks = append(transitiveRootSymlinks, o.rootSymlinks)
		transitiveSymlinks = append(transitiveSymlinks, o.symlinks)
	}
	return mergeRunfiles(thread, transitiveFiles, transitiveRootSymlinks, transitiveSymlinks)
}

func (r *Runfiles[TReference, TMetadata]) EncodeValue(path map[starlark.Value]struct{}, currentIdentifier *pg_label.CanonicalStarlarkIdentifier, options *ValueEncodingOptions[TReference, TMetadata]) (model_core.PatchedMessage[*model_starlark_pb.Value, TMetadata], bool, error) {
	files, filesNeedsCode, err := r.files.Encode(path, options)
	if err != nil {
		return model_core.PatchedMessage[*model_starlark_pb.Value, TMetadata]{}, false, fmt.Errorf("files: %w", err)
	}
	rootSymlinks, rootSymlinksNeedsCode, err := r.rootSymlinks.Encode(path, options)
	if err != nil {
		return model_core.PatchedMessage[*model_starlark_pb.Value, TMetadata]{}, false, fmt.Errorf("root symlinks: %w", err)
	}
	symlinks, symlinksNeedsCode, err := r.symlinks.Encode(path, options)
	if err != nil {
		return model_core.PatchedMessage[*model_starlark_pb.Value, TMetadata]{}, false, fmt.Errorf("symlinks: %w", err)
	}

	files.Patcher.Merge(rootSymlinks.Patcher)
	files.Patcher.Merge(symlinks.Patcher)
	return model_core.NewPatchedMessage(
		&model_starlark_pb.Value{
			Kind: &model_starlark_pb.Value_Runfiles{
				Runfiles: &model_starlark_pb.Runfiles{
					Files:        files.Message,
					RootSymlinks: rootSymlinks.Message,
					Symlinks:     symlinks.Message,
				},
			},
		},
		files.Patcher,
	), filesNeedsCode || rootSymlinksNeedsCode || symlinksNeedsCode, nil
}
