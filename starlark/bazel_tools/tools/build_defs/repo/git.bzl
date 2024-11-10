def _git_repository_impl(ctx):
    pass

git_repository = repository_rule(
    _git_repository_impl,
)
