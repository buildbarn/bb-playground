package starlark

import (
	"fmt"

	model_core "github.com/buildbarn/bb-playground/pkg/model/core"
	model_starlark_pb "github.com/buildbarn/bb-playground/pkg/proto/model/starlark"
	"github.com/buildbarn/bb-playground/pkg/storage/dag"
)

type RepoRegistrar struct {
	repos map[string]model_core.PatchedMessage[*model_starlark_pb.Repo, dag.ObjectContentsWalker]
}

func NewRepoRegistrar() *RepoRegistrar {
	return &RepoRegistrar{
		repos: map[string]model_core.PatchedMessage[*model_starlark_pb.Repo, dag.ObjectContentsWalker]{},
	}
}

func (rr *RepoRegistrar) GetRepos() map[string]model_core.PatchedMessage[*model_starlark_pb.Repo, dag.ObjectContentsWalker] {
	return rr.repos
}

func (rr *RepoRegistrar) registerRepo(name string, repo model_core.PatchedMessage[*model_starlark_pb.Repo, dag.ObjectContentsWalker]) error {
	if _, ok := rr.repos[name]; ok {
		return fmt.Errorf("module extension contains multiple repos with name %#v", name)
	}
	rr.repos[name] = repo
	return nil
}
