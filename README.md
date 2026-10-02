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

## Cache progress and diagnostics

A task reports **Preparing cache lookup** while resolving and loading inputs,
then **Checking cache**. A hit reports **Using cached result** while restoring
and saving that result, and finishes as **Reused cached result**. Only a miss
reports **Running action**. Tasks with reuse disabled report **Preparing task**
then **Running action**. Environment startup remains visible separately.

Each successful task records monotonic timings in the `task_finished` event's
`timings` metadata and in its node log (persisted by `--log-dir`). The app's node
log viewer exposes the same breakdown. Useful fields, in seconds:

| Field | Work measured |
| --- | --- |
| `input_signature_seconds` | Host-side file hashing for telemetry; does not determine reuse |
| `pull_seconds` | Docker image availability check/pull |
| `environment_overhead_seconds` | Launcher's process wall time minus worker time; includes startup and shutdown |
| `plugin_setup_seconds` | Framework imports, plugin discovery, action resolution and cache construction |
| `input_loading_seconds` | Loading/importing inputs, metadata, parameter coercion and defaults |
| `cache_pool_seconds` | Creating/reopening the recycle pool |
| `cache_index_seconds` | Building the framework cache index |
| `cache_match_seconds` | Constructing the resolved invocation identity |
| `cache_load_seconds` | Loading matching cached artifacts |
| `cache_lookup_seconds` | Total pool setup, indexing, matching and cached-result loading |
| `action_seconds` | Scientific action execution; zero on a cache hit |
| `output_save_seconds` | Saving worker artifacts and requested metadata views |
| `output_publish_seconds` | Publishing pipeline outputs after the task finishes |
| `worker_seconds` / `run_seconds` | Worker total / encompassing environment process wall time |

Totals overlap their component stages: do not add every field together. Missing
fields mean the stage was not measured (including older worker manifests).
These are timings of successful tasks, not a profiler for failed actions.
No cache identity, validation, or reuse policy is changed.

To profile a representative pipeline, run it twice against a new isolated cache
with identical inputs, parameters and environment. Preserve both log directories:

```sh
profile_dir=$(mktemp -d)
adagio runtime --spec pipeline.adg --arguments arguments.json --config runtime.toml \
  --cache-dir "$profile_dir/cache" --output-dir "$profile_dir/first/outputs" \
  --log-dir "$profile_dir/first/logs"
adagio runtime --spec pipeline.adg --arguments arguments.json --config runtime.toml \
  --cache-dir "$profile_dir/cache" --output-dir "$profile_dir/repeat/outputs" \
  --log-dir "$profile_dir/repeat/logs"
```

Confirm `Reused cached result: true` before interpreting the repeat as a cache
hit; mutable plugins, changing inputs, or random defaults can still require an
action. Compare repeated runs with the same cache size and input sizes as the
reported slow workload before deciding which stage to optimize.

An opt-in Docker integration test exercises a two-step pipeline with synthetic
feature tables, three cache-hit repeats, changed parameters, changed inputs,
and reuse disabled. Use an installed QIIME image containing `feature-table`:

```sh
ADAGIO_DOCKER_TEST_IMAGE=ghcr.io/cymis/qiime2-plugin-feature-table:2026.1 \
  PYTHONPATH=src python -m pytest -q -s tests/test_docker_cache_integration.py
```

The test uses an isolated temporary cache and prints the path to
`cache-profile.json`, containing the image ID, phases, timings, and output
artifact IDs for each run. Cache hits must preserve artifact IDs and never
report `running`; misses must execute the actions.

Connected deployments require the backend and runtime server with
`adapter-schemas` 0.5.0. Deploy backend support for the new phases before the
runtime update. Progress travels through a mounted JSON-lines file; workers do
not receive runtime credentials or need network access to report phases.
