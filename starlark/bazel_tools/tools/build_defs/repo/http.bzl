def _http_archive_impl(ctx):
    pass

http_archive = repository_rule(
    _http_archive_impl,
)
