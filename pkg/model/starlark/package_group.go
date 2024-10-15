package starlark

import (
	"errors"
	"maps"
	"slices"
	"sort"
	"strings"

	pg_label "github.com/buildbarn/bb-playground/pkg/label"
	model_core "github.com/buildbarn/bb-playground/pkg/model/core"
	"github.com/buildbarn/bb-playground/pkg/model/core/inlinedtree"
	model_starlark_pb "github.com/buildbarn/bb-playground/pkg/proto/model/starlark"
	"github.com/buildbarn/bb-playground/pkg/storage/dag"
	"github.com/buildbarn/bb-playground/pkg/storage/object"

	"google.golang.org/protobuf/proto"
)

// packageGroupNode is a simplified representation of the
// PackageGroup.Package and PackageGroup.Subpackages messages. It is
// used to construct new trees contained in PackageGroup.
type packageGroupNode struct {
	includePackage     bool
	includeSubpackages bool
	subpackages        map[string]*packageGroupNode
}

func (n *packageGroupNode) getOrCreate(name string) *packageGroupNode {
	if n.includeSubpackages {
		panic("attempted to look up node under node that already includes all children")
	}
	nSub, ok := n.subpackages[name]
	if !ok {
		nSub = &packageGroupNode{
			subpackages: map[string]*packageGroupNode{},
		}
		n.subpackages[name] = nSub
	}
	return nSub
}

// lookupPackage looks up the node that corresponds to a given canonical
// package name.
func (n *packageGroupNode) lookupPackage(canonicalPackage pg_label.CanonicalPackage) *packageGroupNode {
	nWalk := n.getOrCreate(canonicalPackage.GetCanonicalRepo().String())
	packagePath := canonicalPackage.GetPackagePath()
	for {
		if nWalk.includeSubpackages {
			return nil
		}
		if packagePath == "" {
			return nWalk
		}
		if split := strings.IndexByte(packagePath, '/'); split < 0 {
			nWalk = nWalk.getOrCreate(packagePath)
			packagePath = ""
		} else {
			nWalk = nWalk.getOrCreate(packagePath[:split])
			packagePath = packagePath[split+1:]
		}
	}
}

// toProto converts the data contained in a tree of packageGroupNode to
// its Protobuf message counterpart.
func (n *packageGroupNode) toProto(inlinedTreeOptions *inlinedtree.Options) (model_core.PatchedMessage[*model_starlark_pb.PackageGroup_Subpackages, dag.ObjectContentsWalker], error) {
	inlineCandidates := make([]inlinedtree.Candidate[*model_starlark_pb.PackageGroup_Subpackages, dag.ObjectContentsWalker], 0, 2)

	// Set the IncludeSubpackages field.
	inlineCandidates = append(inlineCandidates, inlinedtree.Candidate[*model_starlark_pb.PackageGroup_Subpackages, dag.ObjectContentsWalker]{
		ExternalMessage: model_core.NewSimplePatchedMessage[dag.ObjectContentsWalker](proto.Message(nil)),
		ParentAppender: func(
			subpackages model_core.PatchedMessage[*model_starlark_pb.PackageGroup_Subpackages, dag.ObjectContentsWalker],
			externalContents *object.Contents,
			externalChildren []dag.ObjectContentsWalker,
		) {
			subpackages.Message.IncludeSubpackages = n.includeSubpackages
		},
	})

	// If one or more subpackages are present, set the overrides field.
	if len(n.subpackages) > 0 {
		overrides := model_starlark_pb.PackageGroup_Subpackages_Overrides{
			Packages: make([]*model_starlark_pb.PackageGroup_Package, 0, len(n.subpackages)),
		}
		patcher := model_core.NewReferenceMessagePatcher[dag.ObjectContentsWalker]()
		for _, component := range slices.Sorted(maps.Keys(n.subpackages)) {
			nChild := n.subpackages[component]
			subpackages, err := nChild.toProto(inlinedTreeOptions)
			if err != nil {
				return model_core.PatchedMessage[*model_starlark_pb.PackageGroup_Subpackages, dag.ObjectContentsWalker]{}, err
			}
			overrides.Packages = append(overrides.Packages, &model_starlark_pb.PackageGroup_Package{
				Component:      component,
				IncludePackage: nChild.includePackage,
				Subpackages:    subpackages.Message,
			})
			patcher.Merge(subpackages.Patcher)
		}

		inlineCandidates = append(inlineCandidates, inlinedtree.Candidate[*model_starlark_pb.PackageGroup_Subpackages, dag.ObjectContentsWalker]{
			ExternalMessage: model_core.PatchedMessage[proto.Message, dag.ObjectContentsWalker]{
				Message: &overrides,
				Patcher: patcher,
			},
			ParentAppender: func(
				subpackages model_core.PatchedMessage[*model_starlark_pb.PackageGroup_Subpackages, dag.ObjectContentsWalker],
				externalContents *object.Contents,
				externalChildren []dag.ObjectContentsWalker,
			) {
				if externalContents == nil {
					subpackages.Message.Overrides = &model_starlark_pb.PackageGroup_Subpackages_OverridesInline{
						OverridesInline: &overrides,
					}
				} else {
					subpackages.Message.Overrides = &model_starlark_pb.PackageGroup_Subpackages_OverridesExternal{
						OverridesExternal: subpackages.Patcher.AddReference(
							externalContents.GetReference(),
							dag.NewSimpleObjectContentsWalker(externalContents, externalChildren),
						),
					}
				}
			},
		})
	}

	return inlinedtree.Build(inlineCandidates, inlinedTreeOptions)
}

// NewPackageGroupFromVisibility generates a PackageGroup message based
// on a sequence of "visibility" labels provided to repo(), package(),
// or rule targets.
func NewPackageGroupFromVisibility(visibility []pg_label.CanonicalLabel, inlinedTreeOptions *inlinedtree.Options) (model_core.PatchedMessage[*model_starlark_pb.PackageGroup, dag.ObjectContentsWalker], error) {
	tree := packageGroupNode{
		subpackages: map[string]*packageGroupNode{},
	}
	var includePackageGroups []string

	for _, label := range visibility {
		canonicalPackage := label.GetCanonicalPackage()
		packagePath := canonicalPackage.GetPackagePath()
		targetName := label.GetTargetName().String()
		if packagePath == "visibility" {
			// Special labels under //visibility:*.
			switch targetName {
			case "private":
				if len(visibility) > 1 {
					return model_core.PatchedMessage[*model_starlark_pb.PackageGroup, dag.ObjectContentsWalker]{}, errors.New("//visibility:private may not be combined with other labels")
				}
				return model_core.NewSimplePatchedMessage[dag.ObjectContentsWalker](&model_starlark_pb.PackageGroup{
					Tree: &model_starlark_pb.PackageGroup_Subpackages{},
				}), nil
			case "public":
				if len(visibility) > 1 {
					return model_core.PatchedMessage[*model_starlark_pb.PackageGroup, dag.ObjectContentsWalker]{}, errors.New("//visibility:public may not be combined with other labels")
				}
				return model_core.NewSimplePatchedMessage[dag.ObjectContentsWalker](&model_starlark_pb.PackageGroup{
					Tree: &model_starlark_pb.PackageGroup_Subpackages{
						IncludeSubpackages: true,
					},
				}), nil
			}
		}

		switch targetName {
		case "__pkg__":
			// Include a single package.
			if n := tree.lookupPackage(canonicalPackage); n != nil {
				n.includePackage = true
			}
		case "__subpackages__":
			// Include a package and all of its children.
			if n := tree.lookupPackage(canonicalPackage); n != nil {
				*n = packageGroupNode{
					includePackage:     true,
					includeSubpackages: true,
				}
			}
		default:
			// Reference to another package group that
			// should be merged into this set of packages.
			includePackageGroups = append(includePackageGroups, label.String())
		}
	}

	treeProto, err := tree.toProto(inlinedTreeOptions)
	if err != nil {
		return model_core.PatchedMessage[*model_starlark_pb.PackageGroup, dag.ObjectContentsWalker]{}, err
	}

	sort.Strings(includePackageGroups)
	return model_core.PatchedMessage[*model_starlark_pb.PackageGroup, dag.ObjectContentsWalker]{
		Message: &model_starlark_pb.PackageGroup{
			Tree:                 treeProto.Message,
			IncludePackageGroups: slices.Compact(includePackageGroups),
		},
		Patcher: treeProto.Patcher,
	}, nil
}