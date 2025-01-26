package starlark

import (
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"

	pg_label "github.com/buildbarn/bb-playground/pkg/label"
	model_core "github.com/buildbarn/bb-playground/pkg/model/core"
	model_starlark_pb "github.com/buildbarn/bb-playground/pkg/proto/model/starlark"
	"github.com/buildbarn/bb-playground/pkg/starlark/unpack"
	"github.com/buildbarn/bb-playground/pkg/storage/dag"

	"go.starlark.net/starlark"
)

type repositoryRule struct {
	LateNamedValue
	definition RepositoryRuleDefinition
}

var (
	_ starlark.Callable = &rule{}
	_ EncodableValue    = &repositoryRule{}
	_ NamedGlobal       = &repositoryRule{}
)

func NewRepositoryRule(identifier *pg_label.CanonicalStarlarkIdentifier, definition RepositoryRuleDefinition) starlark.Value {
	return &repositoryRule{
		LateNamedValue: LateNamedValue{
			Identifier: identifier,
		},
		definition: definition,
	}
}

func (rr *repositoryRule) String() string {
	return "<repository_rule>"
}

func (rr *repositoryRule) Type() string {
	return "repository_rule"
}

func (rr *repositoryRule) Freeze() {}

func (rr *repositoryRule) Truth() starlark.Bool {
	return starlark.True
}

func (rr *repositoryRule) Hash() (uint32, error) {
	// TODO
	return 0, nil
}

func (rr *repositoryRule) Name() string {
	if rr.Identifier == nil {
		return "repository_rule"
	}
	return rr.Identifier.GetStarlarkIdentifier().String()
}

const RepoRegistrarKey = "repo_registrar"

func (rr *repositoryRule) CallInternal(thread *starlark.Thread, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
	if rr.Identifier == nil {
		return nil, errors.New("repository rule does not have a name")
	}
	repoRegistrarValue := thread.Local(RepoRegistrarKey)
	if repoRegistrarValue == nil {
		return nil, fmt.Errorf("repository rule cannot be invoked from within this context")
	}
	repoRegistrar := repoRegistrarValue.(*RepoRegistrar)

	attrs, err := rr.definition.GetAttrsCheap(thread)
	if err != nil {
		return nil, err
	}

	var unpackers []any
	attrNames := make([]string, 0, len(attrs))
	values := make([]starlark.Value, len(attrs))
	currentPackage := thread.Local(CanonicalPackageKey).(pg_label.CanonicalPackage)
	for _, name := range slices.SortedFunc(
		maps.Keys(attrs),
		func(a, b pg_label.StarlarkIdentifier) int { return strings.Compare(a.String(), b.String()) },
	) {
		switch nameStr := name.String(); nameStr {
		case "name":
			return nil, fmt.Errorf("repository rule uses attribute with reserved name %#v", nameStr)
		}
		if name.IsPublic() {
			attr := attrs[name]
			nameStr := name.String()
			unpacker := unpack.Canonicalize(attr.attrType.GetCanonicalizer(currentPackage))
			if attr.defaultValue == nil {
				// Attribute is mandatory.
				unpackers = append(unpackers, nameStr)
			} else {
				// Attribute is optional. Bazel allows
				// None to be used to select the default
				// value.
				unpackers = append(unpackers, nameStr+"?")
				unpacker = unpack.IfNotNone(unpacker)
			}
			unpackers = append(
				unpackers,
				unpack.Bind(thread, &values[len(attrNames)], unpacker),
			)
			attrNames = append(attrNames, nameStr)
		}
	}

	var name string
	unpackers = append(
		unpackers,
		"name", unpack.Bind(thread, &name, unpack.Stringer(unpack.ApparentRepo)),
	)

	if err := starlark.UnpackArgs(
		rr.Identifier.GetStarlarkIdentifier().String(), args, kwargs,
		unpackers...,
	); err != nil {
		return nil, err
	}

	var attrValues []*model_starlark_pb.NamedValue
	patcher := model_core.NewReferenceMessagePatcher[dag.ObjectContentsWalker]()
	valueEncodingOptions := thread.Local(ValueEncodingOptionsKey).(*ValueEncodingOptions)
	for i, attrName := range attrNames {
		if value := values[i]; value != nil {
			encodedValue, _, err := EncodeValue(
				value,
				/* path = */ map[starlark.Value]struct{}{},
				/* currentIdentifier = */ nil,
				valueEncodingOptions,
			)
			if err != nil {
				return nil, err
			}
			attrValues = append(attrValues,
				&model_starlark_pb.NamedValue{
					Name:  attrName,
					Value: encodedValue.Message,
				},
			)
			patcher.Merge(encodedValue.Patcher)
		}
	}

	return starlark.None, repoRegistrar.registerRepo(
		name,
		model_core.NewPatchedMessage(
			&model_starlark_pb.Repo{
				Name: name,
				Definition: &model_starlark_pb.Repo_Definition{
					RepositoryRuleIdentifier: rr.Identifier.String(),
					AttrValues:               attrValues,
				},
			},
			patcher,
		),
	)
}

func (rr *repositoryRule) EncodeValue(path map[starlark.Value]struct{}, currentIdentifier *pg_label.CanonicalStarlarkIdentifier, options *ValueEncodingOptions) (model_core.PatchedMessage[*model_starlark_pb.Value, dag.ObjectContentsWalker], bool, error) {
	if rr.Identifier == nil {
		return model_core.PatchedMessage[*model_starlark_pb.Value, dag.ObjectContentsWalker]{}, false, errors.New("repository_rule does not have a name")
	}
	if currentIdentifier == nil || *currentIdentifier != *rr.Identifier {
		// Not the canonical identifier under which this
		// repository rule is known. Emit a reference.
		return model_core.NewSimplePatchedMessage[dag.ObjectContentsWalker](
			&model_starlark_pb.Value{
				Kind: &model_starlark_pb.Value_RepositoryRule{
					RepositoryRule: &model_starlark_pb.RepositoryRule{
						Kind: &model_starlark_pb.RepositoryRule_Reference{
							Reference: rr.Identifier.String(),
						},
					},
				},
			},
		), false, nil
	}

	definition, needsCode, err := rr.definition.Encode(path, options)
	if err != nil {
		return model_core.PatchedMessage[*model_starlark_pb.Value, dag.ObjectContentsWalker]{}, false, err
	}
	return model_core.NewPatchedMessage(
		&model_starlark_pb.Value{
			Kind: &model_starlark_pb.Value_RepositoryRule{
				RepositoryRule: &model_starlark_pb.RepositoryRule{
					Kind: &model_starlark_pb.RepositoryRule_Definition_{
						Definition: definition.Message,
					},
				},
			},
		},
		definition.Patcher,
	), needsCode, nil
}

type RepositoryRuleDefinition interface {
	Encode(path map[starlark.Value]struct{}, options *ValueEncodingOptions) (model_core.PatchedMessage[*model_starlark_pb.RepositoryRule_Definition, dag.ObjectContentsWalker], bool, error)
	GetAttrsCheap(thread *starlark.Thread) (map[pg_label.StarlarkIdentifier]*Attr, error)
}

type starlarkRepositoryRuleDefinition struct {
	implementation NamedFunction
	attrs          map[pg_label.StarlarkIdentifier]*Attr
}

func NewStarlarkRepositoryRuleDefinition(implementation NamedFunction, attrs map[pg_label.StarlarkIdentifier]*Attr) RepositoryRuleDefinition {
	return &starlarkRepositoryRuleDefinition{
		implementation: implementation,
		attrs:          attrs,
	}
}

func (rrd *starlarkRepositoryRuleDefinition) Encode(path map[starlark.Value]struct{}, options *ValueEncodingOptions) (model_core.PatchedMessage[*model_starlark_pb.RepositoryRule_Definition, dag.ObjectContentsWalker], bool, error) {
	implementation, implementationNeedsCode, err := rrd.implementation.Encode(path, options)
	if err != nil {
		return model_core.PatchedMessage[*model_starlark_pb.RepositoryRule_Definition, dag.ObjectContentsWalker]{}, false, err
	}

	namedAttrs, namedAttrsNeedCode, err := encodeNamedAttrs(rrd.attrs, path, options)
	if err != nil {
		return model_core.PatchedMessage[*model_starlark_pb.RepositoryRule_Definition, dag.ObjectContentsWalker]{}, false, err
	}

	return model_core.NewPatchedMessage(
		&model_starlark_pb.RepositoryRule_Definition{
			Implementation: implementation.Message,
			Attrs:          namedAttrs.Message,
		},
		namedAttrs.Patcher,
	), implementationNeedsCode || namedAttrsNeedCode, nil
}

func (rrd *starlarkRepositoryRuleDefinition) GetAttrsCheap(thread *starlark.Thread) (map[pg_label.StarlarkIdentifier]*Attr, error) {
	return rrd.attrs, nil
}

type protoRepositoryRuleDefinition struct {
	message         model_core.Message[*model_starlark_pb.RepositoryRule_Definition]
	protoAttrsCache protoAttrsCache
}

func NewProtoRepositoryRuleDefinition(message model_core.Message[*model_starlark_pb.RepositoryRule_Definition]) RepositoryRuleDefinition {
	return &protoRepositoryRuleDefinition{
		message: message,
	}
}

func (rrd *protoRepositoryRuleDefinition) Encode(path map[starlark.Value]struct{}, options *ValueEncodingOptions) (model_core.PatchedMessage[*model_starlark_pb.RepositoryRule_Definition, dag.ObjectContentsWalker], bool, error) {
	panic("rule definition was already encoded previously")
}

func (rrd *protoRepositoryRuleDefinition) GetAttrsCheap(thread *starlark.Thread) (map[pg_label.StarlarkIdentifier]*Attr, error) {
	return rrd.protoAttrsCache.getAttrsCheap(thread, rrd.message.Message.Attrs)
}
