package starlark

import (
	"errors"
	"fmt"

	pg_label "github.com/buildbarn/bb-playground/pkg/label"
	model_core "github.com/buildbarn/bb-playground/pkg/model/core"
	model_starlark_pb "github.com/buildbarn/bb-playground/pkg/proto/model/starlark"
	"github.com/buildbarn/bb-playground/pkg/starlark/unpack"
	"github.com/buildbarn/bb-playground/pkg/storage/dag"

	"go.starlark.net/starlark"
)

type Runfiles struct {
	files        *Depset
	rootSymlinks *Depset
	symlinks     *Depset
}

var (
	_ EncodableValue    = (*Runfiles)(nil)
	_ starlark.HasAttrs = File{}
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

func (Runfiles) Hash() (uint32, error) {
	return 0, errors.New("runfiles cannot be hashed")
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

func mergeRunfiles(allFiles, allRootSymlinks, allSymlinks []*Depset) (starlark.Value, error) {
	files, err := NewDepset(nil, allFiles, model_starlark_pb.Depset_DEFAULT)
	if err != nil {
		return nil, fmt.Errorf("failed to merge files: %w", err)
	}
	rootSymlinks, err := NewDepset(nil, allRootSymlinks, model_starlark_pb.Depset_DEFAULT)
	if err != nil {
		return nil, fmt.Errorf("failed to merge root symlinks: %w", err)
	}
	symlinks, err := NewDepset(nil, allSymlinks, model_starlark_pb.Depset_DEFAULT)
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
	return mergeRunfiles(transitiveFiles, transitiveRootSymlinks, transitiveSymlinks)
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
