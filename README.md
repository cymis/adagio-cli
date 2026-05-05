# Adagio CLI

`adagio-cli` is the Python command-line interface for Adagio pipeline execution.

For user-facing documentation and product guides, please reference the docs:

- [Adagio Docs](https://docs.adagiodata.com)

The adagio frontend is used to build pipelines that can be run with this package on the command line
It can be found here:

- [Adagio](https://adagio.run)

## Development

Set up the project and run the test suite with:

```bash
uv sync --group dev
uv run pytest
```

## Catalog pipelines

Run a pipeline from the Adagio pipeline catalog:

```bash
adagio pipeline show @adagio/microbial-diversity
adagio run @adagio/microbial-diversity --cache-dir /path/to/cache --arguments run-arguments.json
```

`@adagio/<slug>` first resolves against a nearby local `adagio-pipelines`
checkout when one is available. If no local catalog is found, Adagio fetches
`pipeline.adg` from `cymis/adagio-pipelines` on GitHub, checking `official`
before `community`.

During `adagio run`, remote catalog pipelines are downloaded under the selected
`--cache-dir` and reused by source name and slug on later runs. `adagio pipeline
show` uses a temporary download when it fetches from GitHub because it does not
take a cache directory.

Private GitHub access is explicit: set `GITHUB_TOKEN` or `GH_TOKEN` to a token
that can read `cymis/adagio-pipelines`; with a token, the CLI fetches through
the GitHub contents API. The CLI does not read browser, git, or `gh` credentials
automatically.
