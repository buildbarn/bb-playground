package starlark

import (
	"errors"
	"fmt"

	pg_label "github.com/buildbarn/bonanza/pkg/label"
	model_core "github.com/buildbarn/bonanza/pkg/model/core"
	model_starlark_pb "github.com/buildbarn/bonanza/pkg/proto/model/starlark"
	"github.com/buildbarn/bonanza/pkg/starlark/unpack"
	"github.com/buildbarn/bonanza/pkg/storage/dag"

	"go.starlark.net/starlark"
	"go.starlark.net/syntax"
)

type Runfiles struct {
	files        *Depset
	rootSymlinks *Depset
	symlinks     *Depset
	hash         uint32
}

var (
	_ EncodableValue      = (*Runfiles)(nil)
	_ starlark.Comparable = (*Struct)(nil)
	_ starlark.HasAttrs   = File{}
)

func NewRunfiles(files, rootSymlinks, symlinks *Depset) *Runfiles {
	return &Runfiles{
		files:        files,
		rootSymlinks: rootSymlinks,
		symlinks:     symlinks,
	}
}

func (Runfiles) String() string {
	return "<runfiles>"
}

func (Runfiles) Type() string {
	return "runfiles"
}

func (Runfiles) Freeze() {
}

func (Runfiles) Truth() starlark.Bool {
	return starlark.True
}

func (r *Runfiles) Hash(thread *starlark.Thread) (uint32, error) {
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

func (r *Runfiles) equals(thread *starlark.Thread, other *Runfiles, depth int) (bool, error) {
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

func (r *Runfiles) CompareSameType(thread *starlark.Thread, op syntax.Token, other starlark.Value, depth int) (bool, error) {
	switch op {
	case syntax.EQL:
		return r.equals(thread, other.(*Runfiles), depth)
	case syntax.NEQ:
		equal, err := r.equals(thread, other.(*Runfiles), depth)
		return !equal, err
	default:
		return false, errors.New("runfiles cannot be compared for inequality")
	}
}

func (r *Runfiles) Attr(thread *starlark.Thread, name string) (starlark.Value, error) {
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

func (Runfiles) AttrNames() []string {
	return runfilesAttrNames
}

func mergeRunfiles(thread *starlark.Thread, allFiles, allRootSymlinks, allSymlinks []*Depset) (starlark.Value, error) {
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
	return NewRunfiles(files, rootSymlinks, symlinks), nil
}

func (r *Runfiles) doMerge(thread *starlark.Thread, b *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
	var other *Runfiles
	if err := starlark.UnpackArgs(
		b.Name(), args, kwargs,
		"other", unpack.Bind(thread, &other, unpack.Type[*Runfiles]("runfiles")),
	); err != nil {
		return nil, err
	}

	return mergeRunfiles(
		thread,
		[]*Depset{r.files, other.files},
		[]*Depset{r.rootSymlinks, other.rootSymlinks},
		[]*Depset{r.symlinks, other.symlinks},
	)
}

func (r *Runfiles) doMergeAll(thread *starlark.Thread, b *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
	var other []*Runfiles
	if err := starlark.UnpackArgs(
		b.Name(), args, kwargs,
		"other", unpack.Bind(thread, &other, unpack.List(unpack.Type[*Runfiles]("runfiles"))),
	); err != nil {
		return nil, err
	}

	transitiveFiles := append(make([]*Depset, 0, len(other)+1), r.files)
	transitiveRootSymlinks := append(make([]*Depset, 0, len(other)+1), r.rootSymlinks)
	transitiveSymlinks := append(make([]*Depset, 0, len(other)+1), r.symlinks)
	for _, o := range other {
		transitiveFiles = append(transitiveFiles, o.files)
		transitiveRootSymlinks = append(transitiveRootSymlinks, o.rootSymlinks)
		transitiveSymlinks = append(transitiveSymlinks, o.symlinks)
	}
	return mergeRunfiles(thread, transitiveFiles, transitiveRootSymlinks, transitiveSymlinks)
}

func (r *Runfiles) EncodeValue(path map[starlark.Value]struct{}, currentIdentifier *pg_label.CanonicalStarlarkIdentifier, options *ValueEncodingOptions) (model_core.PatchedMessage[*model_starlark_pb.Value, dag.ObjectContentsWalker], bool, error) {
	files, filesNeedsCode, err := r.files.Encode(path, options)
	if err != nil {
		return model_core.PatchedMessage[*model_starlark_pb.Value, dag.ObjectContentsWalker]{}, false, fmt.Errorf("files: %w", err)
	}
	rootSymlinks, rootSymlinksNeedsCode, err := r.rootSymlinks.Encode(path, options)
	if err != nil {
		return model_core.PatchedMessage[*model_starlark_pb.Value, dag.ObjectContentsWalker]{}, false, fmt.Errorf("root symlinks: %w", err)
	}
	symlinks, symlinksNeedsCode, err := r.symlinks.Encode(path, options)
	if err != nil {
		return model_core.PatchedMessage[*model_starlark_pb.Value, dag.ObjectContentsWalker]{}, false, fmt.Errorf("symlinks: %w", err)
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
