def _http_archive_impl(ctx):
    pass

http_archive = repository_rule(
    _http_archive_impl,
)

def _http_file_impl(ctx):
    pass

http_file = repository_rule(
    _http_file_impl,
)

def _http_jar_impl(ctx):
    pass

http_jar = repository_rule(
    _http_jar_impl,
)
