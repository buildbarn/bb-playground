def maybe(repo_rule, name, **kwargs):
    # TODO: Do we really need to create repositories conditionally,
    # based on whether they already exist? This macro is likely dead
    # code with bzlmod.
    repo_rule(name = name, **kwargs)
