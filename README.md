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

By default, `adagio run` resolves plugin actions to Docker images. A runtime
config passed with `--config` can override that per default, plugin, or task.

Conda environments are supported with `kind = "conda"`:

```toml
version = 1

[defaults]
kind = "conda"
environment = "qiime2-2026.1"

[plugins]
dada2 = { kind = "conda", prefix = "/opt/conda/envs/q2-dada2" }
```

The environment must already exist and contain QIIME 2 plus the plugins needed
by the pipeline. Adagio enters it with `conda run`; it does not create or manage
the environment.
