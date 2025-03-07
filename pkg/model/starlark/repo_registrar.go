package starlark

import (
	"fmt"

	model_core "github.com/buildbarn/bonanza/pkg/model/core"
	model_starlark_pb "github.com/buildbarn/bonanza/pkg/proto/model/starlark"
)

type RepoRegistrar[TMetadata model_core.ReferenceMetadata] struct {
	repos map[string]model_core.PatchedMessage[*model_starlark_pb.Repo, TMetadata]
}

func NewRepoRegistrar[TMetadata model_core.ReferenceMetadata]() *RepoRegistrar[TMetadata] {
	return &RepoRegistrar[TMetadata]{
		repos: map[string]model_core.PatchedMessage[*model_starlark_pb.Repo, TMetadata]{},
	}
}

func (rr *RepoRegistrar[TMetadata]) GetRepos() map[string]model_core.PatchedMessage[*model_starlark_pb.Repo, TMetadata] {
	return rr.repos
}

func (rr *RepoRegistrar[TMetadata]) registerRepo(name string, repo model_core.PatchedMessage[*model_starlark_pb.Repo, TMetadata]) error {
	if _, ok := rr.repos[name]; ok {
		return fmt.Errorf("module extension contains multiple repos with name %#v", name)
	}
	rr.repos[name] = repo
	return nil
}
