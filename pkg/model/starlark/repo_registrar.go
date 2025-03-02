package starlark

import (
	"fmt"

	model_core "github.com/buildbarn/bonanza/pkg/model/core"
	model_starlark_pb "github.com/buildbarn/bonanza/pkg/proto/model/starlark"
)

type RepoRegistrar struct {
	repos map[string]model_core.PatchedMessage[*model_starlark_pb.Repo, model_core.CreatedObjectTree]
}

func NewRepoRegistrar() *RepoRegistrar {
	return &RepoRegistrar{
		repos: map[string]model_core.PatchedMessage[*model_starlark_pb.Repo, model_core.CreatedObjectTree]{},
	}
}

func (rr *RepoRegistrar) GetRepos() map[string]model_core.PatchedMessage[*model_starlark_pb.Repo, model_core.CreatedObjectTree] {
	return rr.repos
}

func (rr *RepoRegistrar) registerRepo(name string, repo model_core.PatchedMessage[*model_starlark_pb.Repo, model_core.CreatedObjectTree]) error {
	if _, ok := rr.repos[name]; ok {
		return fmt.Errorf("module extension contains multiple repos with name %#v", name)
	}
	rr.repos[name] = repo
	return nil
}
