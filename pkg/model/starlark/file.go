package starlark

import (
	"encoding/hex"
	"errors"
	"fmt"
	"hash/fnv"
	go_path "path"

	pg_label "github.com/buildbarn/bonanza/pkg/label"
	model_core "github.com/buildbarn/bonanza/pkg/model/core"
	model_starlark_pb "github.com/buildbarn/bonanza/pkg/proto/model/starlark"
	"github.com/buildbarn/bonanza/pkg/storage/object"

	"google.golang.org/protobuf/proto"

	"go.starlark.net/starlark"
	"go.starlark.net/syntax"
)

const externalDirectoryName = "external"

type File[TReference object.BasicReference, TMetadata model_core.CloneableReferenceMetadata] struct {
	definition *model_starlark_pb.File
}

var (
	_ EncodableValue[object.LocalReference, model_core.CloneableReferenceMetadata] = File[object.LocalReference, model_core.CloneableReferenceMetadata]{}
	_ starlark.Comparable                                                          = File[object.LocalReference, model_core.CloneableReferenceMetadata]{}
	_ starlark.HasAttrs                                                            = File[object.LocalReference, model_core.CloneableReferenceMetadata]{}
)

func NewFile[TReference object.BasicReference, TMetadata model_core.CloneableReferenceMetadata](definition *model_starlark_pb.File) File[TReference, TMetadata] {
	return File[TReference, TMetadata]{
		definition: definition,
	}
}

func (File[TReference, TMetadata]) String() string {
	return "<File>"
}

func (File[TReference, TMetadata]) Type() string {
	return "File"
}

func (File[TReference, TMetadata]) Freeze() {
}

func (File[TReference, TMetadata]) Truth() starlark.Bool {
	return starlark.True
}

func (f File[TReference, TMetadata]) Hash(thread *starlark.Thread) (uint32, error) {
	data, err := proto.Marshal(f.definition)
	if err != nil {
		return 0, err
	}
	h := fnv.New32a()
	h.Write(data)
	return h.Sum32(), nil
}

func (f File[TReference, TMetadata]) CompareSameType(thread *starlark.Thread, op syntax.Token, other starlark.Value, depth int) (bool, error) {
	switch op {
	case syntax.EQL:
		return proto.Equal(f.definition, other.(File[TReference, TMetadata]).definition), nil
	case syntax.NEQ:
		return !proto.Equal(f.definition, other.(File[TReference, TMetadata]).definition), nil
	default:
		return false, errors.New("File can only be compared for equality")
	}
}

func (f File[TReference, TMetadata]) appendOwner(parts []string) []string {
	if o := f.definition.Owner; o != nil {
		parts = append(
			parts,
			"bazel-out",
			hex.EncodeToString(o.Cfg),
			"bin",
		)
	}
	return parts
}

func (f File[TReference, TMetadata]) getPath() (string, error) {
	canonicalPackage, err := pg_label.NewCanonicalPackage(f.definition.Package)
	if err != nil {
		return "", fmt.Errorf("invalid canonical package %#v: %w", f.definition.Package, err)
	}
	parts := f.appendOwner(make([]string, 0, 7))
	return go_path.Join(
		append(
			parts,
			externalDirectoryName,
			canonicalPackage.GetCanonicalRepo().String(),
			canonicalPackage.GetPackagePath(),
			f.definition.PackageRelativePath,
		)...,
	), nil
}

func (f File[TReference, TMetadata]) Attr(thread *starlark.Thread, name string) (starlark.Value, error) {
	switch name {
	case "basename":
		return starlark.String(go_path.Base(f.definition.PackageRelativePath)), nil
	case "dirname":
		p, err := f.getPath()
		if err != nil {
			return nil, err
		}
		return starlark.String(go_path.Dir(p)), nil
	case "extension":
		p := f.definition.PackageRelativePath
		for i := len(p) - 1; i >= 0 && p[i] != '/'; i-- {
			if p[i] == '.' {
				return starlark.String(p[i+1:]), nil
			}
		}
		return starlark.String(""), nil
	case "is_directory":
		return starlark.Bool(f.definition.Type == model_starlark_pb.File_DIRECTORY), nil
	case "is_source":
		return starlark.Bool(f.definition.Owner == nil), nil
	case "is_symlink":
		return starlark.Bool(f.definition.Type == model_starlark_pb.File_SYMLINK), nil
	case "owner":
		canonicalPackage, err := pg_label.NewCanonicalPackage(f.definition.Package)
		if err != nil {
			return nil, fmt.Errorf("invalid canonical package %#v: %w", f.definition.Package, err)
		}

		// If the file is an output file, return the label of
		// the target that generates it. If it is a source file,
		// return a label of the file itself.
		targetNameStr := f.definition.PackageRelativePath
		if o := f.definition.Owner; o != nil {
			targetNameStr = o.TargetName
		}
		targetName, err := pg_label.NewTargetName(targetNameStr)
		if err != nil {
			return nil, fmt.Errorf("invalid target name %#v: %w", targetNameStr, err)
		}

		return NewLabel[TReference, TMetadata](canonicalPackage.AppendTargetName(targetName).AsResolved()), nil
	case "path":
		p, err := f.getPath()
		if err != nil {
			return nil, err
		}
		return starlark.String(p), nil
	case "root":
		canonicalPackage, err := pg_label.NewCanonicalPackage(f.definition.Package)
		if err != nil {
			return nil, fmt.Errorf("invalid canonical package %#v: %w", f.definition.Package, err)
		}
		parts := f.appendOwner(make([]string, 0, 6))
		return newStructFromLists[TReference, TMetadata](
			nil,
			[]string{"path"},
			[]any{
				starlark.String(go_path.Join(
					append(
						parts,
						externalDirectoryName,
						canonicalPackage.GetCanonicalRepo().String(),
					)...,
				)),
			},
		), nil
	case "short_path":
		canonicalPackage, err := pg_label.NewCanonicalPackage(f.definition.Package)
		if err != nil {
			return nil, fmt.Errorf("invalid canonical package %#v: %w", f.definition.Package, err)
		}
		return starlark.String(go_path.Join(
			"..",
			canonicalPackage.GetCanonicalRepo().String(),
			canonicalPackage.GetPackagePath(),
			f.definition.PackageRelativePath,
		)), nil
	default:
		return nil, nil
	}
}

var fileAttrNames = []string{
	"basename",
	"dirname",
	"extension",
	"is_directory",
	"is_source",
	"is_symlink",
	"owner",
	"path",
	"root",
	"short_path",
}

func (File[TReference, TMetadata]) AttrNames() []string {
	return fileAttrNames
}

func (f File[TReference, TMetadata]) EncodeValue(path map[starlark.Value]struct{}, currentIdentifier *pg_label.CanonicalStarlarkIdentifier, options *ValueEncodingOptions[TReference, TMetadata]) (model_core.PatchedMessage[*model_starlark_pb.Value, TMetadata], bool, error) {
	return model_core.NewSimplePatchedMessage[TMetadata](
		&model_starlark_pb.Value{
			Kind: &model_starlark_pb.Value_File{
				File: f.definition,
			},
		},
	), false, nil
}
