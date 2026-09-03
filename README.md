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

## Runtime environments

Every plugin action requires an explicit execution environment. A runtime config
passed with `--config` can define one default and override it per plugin or task;
the CLI never guesses an image from a plugin name.

Conda environments are supported with `kind = "conda"`:

```toml
version = 1

[defaults]
kind = "conda"
prefix = "/opt/conda/envs/qiime2-2026.1"

[plugins]
dada2 = { kind = "conda", prefix = "/opt/conda/envs/q2-dada2" }
```

The environment must already exist and contain QIIME 2 plus the plugins needed
by the pipeline. Adagio enters it with `conda run`; it does not create or manage
the environment.

## Task resource requests

A runtime config can also declare the requested shape of each task execution:

```toml
[resources.tasks."denoise-node"]
cpus = 4
memory = "8 GiB"
```

`cpus` is a positive whole number and `memory` is a positive, unit-bearing
quantity. Each entry describes one independently schedulable execution of that
task, not an aggregate pipeline allocation. If a task is later expanded into
parallel subtasks, each subtask requests this shape and the scheduler determines
the total concurrent allocation.

The current serial executor parses and validates these fields but intentionally
does not apply them yet. Omitting either field leaves that resource at the
executor's default.

## Plugin submission defaults

QAPI submissions can persist the environment that Adagio should use for the
submitted plugins:

```bash
adagio qapi build --plugin my-plugin \
  --default-conda-prefix /opt/conda/envs/my-plugin

adagio qapi build --plugin my-plugin \
  --default-docker-image registry.example.org/my-plugin:2026.1
```

These options are explicit and mutually exclusive. Omitting both leaves the
plugin without a default; the Adagio app will flag that plugin until an
environment is selected for a run.

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
