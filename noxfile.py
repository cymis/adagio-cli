import nox

nox.options.default_venv_backend = "uv"
nox.options.sessions = ["dependencies", "test", "lint", "coverage"]

UV_SYNC_ARGS = (
    "sync",
    "--frozen",
    "--group",
    "dev",
)


def _uv_sync(session):
    session.run_install(
        "uv",
        *UV_SYNC_ARGS,
        env={"UV_PROJECT_ENVIRONMENT": session.virtualenv.location},
    )


@nox.session
def test(session):
    """Run the test suite."""
    _uv_sync(session)
    session.run("pytest", "tests")


@nox.session
def coverage(session):
    """Run tests with coverage reporting."""
    _uv_sync(session)
    session.run("pytest", "--cov-report=term-missing", "--cov=adagio", "--cov-fail-under=0", "tests")


@nox.session
def lint(session):
    """Run static lint checks."""
    _uv_sync(session)
    session.run("ruff", "check", ".")


@nox.session(venv_backend="none")
def dependencies(session):
    """Check that the uv lockfile is current."""
    session.run("uv", "lock", "--check", external=True)
