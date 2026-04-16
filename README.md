# Adagio CLI

`adagio-cli` is the Python command-line interface for Adagio pipeline execution.

This README is intentionally brief. For user-facing documentation and product
guides, use:

- [Adagio Docs](https://docs.adagiodata.com)
- [Adagio](https://adagio.run)

## Scope

This package is mainly for developers working on:

- local CLI development and testing
- Adagio service integrations that invoke the CLI
- QAPI submission from a QIIME environment

For command reference, pipeline authoring guides, and product workflows, prefer
the docs site instead of duplicating that material here.

## Development

Set up the project and run the test suite with:

```bash
uv sync --group dev
uv run pytest
```

## Monorepo

`adagio-cli` lives inside the Adagio monorepo. For local stack setup and the
surrounding services, see the repository root README.
