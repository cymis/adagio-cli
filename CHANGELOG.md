# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project follows [Semantic Versioning](https://semver.org/spec/v2.0.0.html)
using [PEP 440](https://packaging.python.org/en/latest/specifications/version-specifiers/) version strings for Python releases.

## 0.1.3 - 2026-10-17

### Added

- Applies the variable values and input paths saved with a pipeline (`configs` in the `.adg`) as the lowest argument layer, under `--arguments` and command-line flags. `--help` shows a variable's value as the parameter's default.
- Runs each plugin and task in the execution environment saved with the pipeline (`configs[0].execution` in the `.adg`, the shape of a `--config` file), so a downloaded pipeline runs without a separate run-config file. A `--config` file overrides the saved environments. The run header lists each plugin's environment and whether it came from the pipeline file or `--config`.
- Treats a per-output path ending in `/` as a directory: the output is written into it as `<output name>.qza` (`.qzv` for a visualization), matching `--output-dir`.

### Changed

- Reads variable values from the values saved with the pipeline rather than the pipeline signature; signatures written by older releases still supply their defaults.

### Fixed

- Uses the same framework type checking for URL and local artifact inputs,
  avoiding parse failures on polymorphic type display labels.

## 0.1.2 - 2026-10-11

### Added

- Supports HTTPS artifact and metadata inputs in task workers, with TLS validation and bounded downloads.

### Changed

- Imports pipeline runner modules only when running a pipeline, reducing startup time for plugin inspection and QAPI builds.

## 0.1.1 - 2026-10-04

### Added

- Reports cache preparation and reuse phases with task timing breakdowns.

### Fixed

- Preserves task logs for failed and reused steps.
- Drains final progress writes when a worker exits.

- Preserves artifact-to-metadata capability metadata on polymorphic QAPI
  outputs when every concrete semantic type supports the conversion.

## 0.1.0 - 2026-09-10

### Added

- Publishes the first non-prerelease build of the Adagio command-line runner,
  including the task-environment executor and connected Runtime Server support
  validated during the alpha series.

### Changed

- Marks the package as beta while the command-line interface continues to
  mature.

## 0.1.0a10 - 2026-09-02

### Added

- Materializes implicit artifact-to-metadata views across task environments.
- Accepts and validates optional per-task CPU and memory requests in runtime
  configuration. The current serial executor retains but does not yet apply
  these requests.
- Adds the self-hosted Runtime Server execution mode, which runs task containers
  with the server process's UID/GID and supplementary groups, drops Linux
  capabilities, and enables `no-new-privileges` when
  `ADAGIO_ENFORCE_HOST_USER=1`.

### Fixed

- Validates dependencies against the selected execution subgraph so unrelated
  pipeline inputs do not block selected-node runs.
- Rejects unknown execution targets rather than silently ignoring them.

## 0.1.0a6 - 2026-07-18

### Added

- Adds per-task cache exclusions for connected runs. Nodes listed through
  `--no-reuse-nodes` execute without a recycle pool while all other tasks keep
  normal cache eligibility.

### Changed

- Keeps explicit run-wide `--no-reuse` authoritative: it still disables reuse
  for every task, including tasks not listed in `--no-reuse-nodes`.

## 0.1.0a5 - 2026-07-16

### Added

- Adds catalog pipeline resolution with `@adagio/<slug>` sources, including
  authenticated GitHub access and local caching for remote pipeline specs.
- Adds Docker, Apptainer, and existing Conda-environment execution controls,
  including per-platform image selection and per-task runtime overrides.
- Adds manifest data imports, target-aware partial runs, structured connected
  execution events, resource reporting, cancellation, and reproducibility data.
- Adds optional secondary publish paths for pipeline outputs and user-authored
  element descriptions in exported pipeline specifications.

### Changed

- Replaces the legacy Parsl path with the task-environment executor and removes
  Parsl from the package dependencies.
- Expands automated coverage and runs the release test suite through Nox.

### Fixed

- Treats an empty output mapping as a request to use `--output-dir` defaults.
- Preserves node logs and reports complete task tracebacks for failed runs.
- Defaults unspecified Docker platforms to the host architecture and validates
  partial-run arguments against the selected target subgraph.

## 0.1.0a4 - 2026-05-01

### Added

- Adds generated qapi metadata transformer actions so compatible artifacts can
  be converted to metadata inside exported Adagio pipelines.
- Adds pipeline/runtime support for built-in metadata conversion steps and
  archive collection bindings.

### Fixed

- Fixes optional pipeline inputs so omitted optional values are not treated as
  required at runtime.
- Fixes dynamic run options so `--show-params` only controls help display and
  does not affect which CLI options can be passed.

## 0.1.0a3 - 2026-05-01

- Adds support for collections. Adagio pipelines with collections are now handled
- Improves terminal formatting
- Adds semantic types to pipeline descriptions in terminal

## [0.1.0a2] - 2026-04-23

### Added

- qAPI generation skips private QIIME actions and reports skipped actions in CLI output.
- Tests covering private QIIME action filtering in qAPI payload generation.

### Changed

- Reduced README content to a shorter quick-start oriented guide.

## [0.1.0a1] - 2026-04-15

### Added

- GitHub Actions CI for linting, tests, and build verification.
- Trusted Publishing workflows for manual TestPyPI validation and tagged PyPI releases.
- A release playbook covering changelog, tags, and publish steps.
