def maybe(repo_rule, name, **kwargs):
    # TODO: Do we really need to create repositories conditionally,
    # based on whether they already exist? This macro is likely dead
    # code with bzlmod.
    repo_rule(name = name, **kwargs)

def read_user_netrc(ctx):
    pass

def use_netrc(netrc, urls, patterns):
    pass

def patch(ctx, patches = None, patch_cmds = None, patch_cmds_win = None, patch_tool = None, patch_args = None, auth = None):
    pass
