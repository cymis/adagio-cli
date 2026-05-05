# Releasing `adagio-cli`

This repository publishes the Python distribution as `adagio-cli` while keeping the installed CLI command as `adagio`.

## One-time setup

1. Make the GitHub repository public before the first Trusted Publishing run, or confirm your GitHub plan supports protected environments for private repositories.
2. Create a PyPI account at <https://pypi.org/account/register/>.
3. Create a separate TestPyPI account at <https://test.pypi.org/account/register/>.
4. Enable two-factor authentication on both accounts.
5. In GitHub, create two repository environments named `testpypi` and `pypi`.
6. Add a required reviewer to the `pypi` environment so production publishes need manual approval.
7. In PyPI Trusted Publishers, register this workflow:
   - Project name: `adagio-cli`
   - Owner: `cymis`
   - Repository: `adagio-cli`
   - Workflow file: `publish.yml`
   - Environment: `pypi`
8. In TestPyPI Trusted Publishers, register this workflow:
   - Project name: `adagio-cli`
   - Owner: `cymis`
   - Repository: `adagio-cli`
   - Workflow file: `publish-testpypi.yml`
   - Environment: `testpypi`

## Before a release

1. Update `CHANGELOG.md`.
2. Update the version in `pyproject.toml`.
3. Run the local checks:

```bash
uv sync --group dev
uv run pytest
uv run python -m build
uv run python -m twine check dist/*
```

4. Commit the changelog and version bump to `dev`.
5. Push the branch and confirm the `CI` workflow passes.

## TestPyPI validation

Run the `Publish to TestPyPI` workflow manually from GitHub Actions after bumping to a fresh prerelease version.

After it succeeds, validate installation in a clean environment:

```bash
python -m venv /tmp/adagio-cli-testpypi
source /tmp/adagio-cli-testpypi/bin/activate
python -m pip install \
  --index-url https://test.pypi.org/simple/ \
  --extra-index-url https://pypi.org/simple \
  adagio-cli
adagio --version
```

## Production release

1. Create an annotated tag that matches the package version:

```bash
git tag -a v0.1.0a1 -m "adagio-cli 0.1.0a1"
```

2. Push the tag:

```bash
git push origin v0.1.0a1
```

3. Approve the pending `pypi` environment deployment in GitHub Actions.
4. Confirm the package appears on PyPI and installs cleanly:

```bash
python -m venv /tmp/adagio-cli-pypi
source /tmp/adagio-cli-pypi/bin/activate
python -m pip install adagio-cli
adagio --version
```

## Versioning and tags

- Use PEP 440 versions in `pyproject.toml`, for example `0.1.0a1`, `0.1.0`, `0.1.1`.
- Use Git tags prefixed with `v`, for example `v0.1.0a1`, `v0.1.0`, `v0.1.1`.
- The tagged version and `pyproject.toml` version must match exactly.
