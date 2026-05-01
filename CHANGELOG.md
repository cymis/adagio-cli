# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project follows [Semantic Versioning](https://semver.org/spec/v2.0.0.html)
using [PEP 440](https://packaging.python.org/en/latest/specifications/version-specifiers/) version strings for Python releases.

## [Unreleased]

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
