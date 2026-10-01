"""Availability failures stop execution and reach the existing node error channel."""

import json
import os
import subprocess
import sys
from pathlib import Path
from types import SimpleNamespace as NS
from unittest.mock import Mock

import pytest

from adagio.executors.apptainer import ApptainerTaskEnvironmentLauncher
from adagio.executors.base import TaskEnvironmentSpec, TaskExecutionResult
from adagio.executors.conda import (
    CondaTaskEnvironmentLauncher,
    _conda_python_executable,
)
from adagio.executors.docker import DockerTaskEnvironmentLauncher
from adagio.executors.preflight import RuntimePreflightError, preflight_environment
from adagio.executors.task_environments import TaskEnvironmentExecutor, _launch
from adagio.model.arguments import AdagioArguments
from adagio.model.pipeline import AdagioPipeline


@pytest.mark.parametrize(
    "response, message",
    [
        (FileNotFoundError(), "Install Docker"),
        (PermissionError("denied"), "executable permissions"),
        (subprocess.TimeoutExpired("docker info", 10), "within 10 seconds"),
        (
            subprocess.CompletedProcess([], 1, "", "Cannot connect to the daemon"),
            "Start Docker Desktop or the Docker service",
        ),
        (
            subprocess.CompletedProcess([], 1, "", "socket permission denied"),
            "socket permission denied",
        ),
    ],
)
def test_docker_failure_prevents_image_pull_and_launch(monkeypatch, response, message):
    probe = Mock(
        side_effect=response if isinstance(response, Exception) else None,
        return_value=response,
    )
    monkeypatch.setattr("adagio.executors.docker.subprocess.run", probe)
    launcher = DockerTaskEnvironmentLauncher()
    launch = Mock()
    monkeypatch.setattr(launcher, "launch", launch)

    with pytest.raises(RuntimePreflightError, match=message) as error:
        _launch(
            launcher,
            environment=TaskEnvironmentSpec(kind="docker", reference="test:latest"),
            request=NS(task=NS(id="node-1")),
        )

    assert "Cannot start node 'node-1'" in str(error.value)
    launch.assert_not_called()
    probe.assert_called_once_with(
        ["docker", "info", "--format", "{{.ServerVersion}}"],
        check=False,
        capture_output=True,
        text=True,
        timeout=10,
    )


def test_docker_is_checked_again_before_the_next_node(monkeypatch):
    probe = Mock(
        side_effect=[
            subprocess.CompletedProcess([], 0, "27.0", ""),
            subprocess.CompletedProcess([], 1, "", "daemon stopped"),
        ]
    )
    monkeypatch.setattr("adagio.executors.docker.subprocess.run", probe)
    launcher = DockerTaskEnvironmentLauncher()
    launch = Mock(return_value=TaskExecutionResult(outputs={}))
    monkeypatch.setattr(launcher, "launch", launch)
    environment = TaskEnvironmentSpec(kind="docker", reference="test:latest")

    _launch(launcher, environment=environment, request=NS(task=NS(id="first")))
    with pytest.raises(RuntimePreflightError, match="Cannot start node 'second'"):
        _launch(launcher, environment=environment, request=NS(task=NS(id="second")))
    assert probe.call_count == 2
    launch.assert_called_once()


@pytest.fixture
def conda_environment(tmp_path):
    executable = tmp_path / "conda"
    executable.write_text("stub")
    executable.chmod(0o755)
    prefix = tmp_path / "envs" / "qiime"
    python = Path(_conda_python_executable(reference=str(prefix)))
    python.parent.mkdir(parents=True)
    python.write_text("stub")
    python.chmod(0o755)
    return TaskEnvironmentSpec(
        kind="conda",
        reference=str(prefix),
        options={"conda_executable": str(executable)},
    )


def test_conda_uses_configured_executable_without_requiring_path(
    monkeypatch, conda_environment
):
    monkeypatch.setenv("PATH", "")
    preflight_environment(
        CondaTaskEnvironmentLauncher(), environment=conda_environment, task_id="conda"
    )


@pytest.mark.parametrize("missing", ["executable", "prefix", "python"])
def test_conda_missing_runtime_parts(conda_environment, missing):
    prefix = Path(conda_environment.reference)
    python = Path(_conda_python_executable(reference=str(prefix)))
    if missing == "executable":
        Path(conda_environment.options["conda_executable"]).unlink()
        message = "Configured conda executable not found"
    elif missing == "prefix":
        prefix.rename(prefix.with_name("moved"))
        message = "Create the environment"
    else:
        python.unlink()
        message = "Install Python"

    with pytest.raises(RuntimePreflightError, match=message):
        preflight_environment(
            CondaTaskEnvironmentLauncher(),
            environment=conda_environment,
            task_id="conda",
        )


@pytest.mark.parametrize("unexecutable", ["conda", "python"])
def test_conda_requires_executable_permissions(
    monkeypatch, conda_environment, unexecutable
):
    target = (
        Path(conda_environment.options["conda_executable"])
        if unexecutable == "conda"
        else Path(_conda_python_executable(reference=conda_environment.reference))
    )
    access = os.access
    monkeypatch.setattr(
        "adagio.executors.conda.os.access",
        lambda path, mode: False if Path(path) == target else access(path, mode),
    )
    with pytest.raises(RuntimePreflightError, match="executable"):
        preflight_environment(
            CondaTaskEnvironmentLauncher(),
            environment=conda_environment,
            task_id="conda",
        )


def test_missing_conda_has_configuration_guidance(monkeypatch, tmp_path):
    monkeypatch.delenv("ADAGIO_CONDA_EXE", raising=False)
    monkeypatch.delenv("CONDA_EXE", raising=False)
    monkeypatch.setattr("adagio.executors.conda.shutil.which", lambda _: None)
    with pytest.raises(RuntimePreflightError, match="Set ADAGIO_CONDA_EXE"):
        preflight_environment(
            CondaTaskEnvironmentLauncher(),
            environment=TaskEnvironmentSpec(kind="conda", reference=str(tmp_path)),
            task_id="conda",
        )


@pytest.mark.parametrize("runtime", ["apptainer", "singularity", None])
def test_apptainer_runtime_discovery(monkeypatch, tmp_path, runtime):
    image = tmp_path / "image.sif"
    image.touch()
    monkeypatch.setattr(
        "adagio.executors.apptainer.shutil.which",
        lambda name: f"/bin/{name}" if name == runtime else None,
    )
    kwargs = dict(
        environment=TaskEnvironmentSpec(kind="apptainer", reference=str(image)),
        task_id="apptainer",
    )
    if runtime:
        preflight_environment(ApptainerTaskEnvironmentLauncher(), **kwargs)
    else:
        with pytest.raises(RuntimePreflightError, match="not found in PATH"):
            preflight_environment(ApptainerTaskEnvironmentLauncher(), **kwargs)


@pytest.mark.parametrize("problem", ["missing", "directory", "unreadable"])
def test_apptainer_requires_readable_image(monkeypatch, tmp_path, problem):
    image = tmp_path / "image.sif"
    monkeypatch.setattr(
        "adagio.executors.apptainer.shutil.which", lambda _: "/bin/apptainer"
    )
    if problem == "directory":
        image.mkdir()
    elif problem == "unreadable":
        image.touch()
        monkeypatch.setattr("adagio.executors.apptainer.os.access", lambda *_: False)
    with pytest.raises(RuntimePreflightError, match="then retry"):
        preflight_environment(
            ApptainerTaskEnvironmentLauncher(),
            environment=TaskEnvironmentSpec(kind="apptainer", reference=str(image)),
            task_id="apptainer",
        )


def _pipeline():
    return AdagioPipeline.model_validate(
        {
            "type": "pipeline",
            "signature": {"inputs": [], "parameters": [], "outputs": []},
            "graph": [
                {
                    "id": node,
                    "kind": "plugin-action",
                    "plugin": node,
                    "action": "run",
                    "inputs": {},
                    "parameters": {},
                    "outputs": {},
                }
                for node in ("selected", "unselected")
            ],
        }
    )


def test_selected_node_does_not_probe_unused_environments(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    launchers = {
        node: NS(
            preflight=Mock(), launch=Mock(return_value=TaskExecutionResult(outputs={}))
        )
        for node in ("selected", "unselected")
    }
    launchers["unselected"].preflight.side_effect = RuntimeError("not installed")
    executor = TaskEnvironmentExecutor(
        environment_resolver=NS(
            resolve=lambda task: TaskEnvironmentSpec(kind=task.id, reference="test")
        ),
        launchers=launchers,
    )
    arguments = AdagioArguments(inputs={}, parameters={}, outputs={})
    executor.execute(pipeline=_pipeline(), arguments=arguments, target_ids={"selected"})
    launchers["selected"].preflight.assert_called_once()
    launchers["selected"].launch.assert_called_once()
    launchers["unselected"].preflight.assert_not_called()
    launchers["unselected"].launch.assert_not_called()


def test_preflight_failure_is_reported_as_failed_before_launch(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    launcher = NS(
        preflight=Mock(side_effect=SystemExit("Conda not found. Install Conda.")),
        launch=Mock(),
    )
    executor = TaskEnvironmentExecutor(
        environment_resolver=NS(
            resolve=lambda task: TaskEnvironmentSpec(kind="conda", reference="test")
        ),
        launchers={"conda": launcher},
    )
    monitor = Mock()
    with pytest.raises(RuntimePreflightError, match="Install Conda") as error:
        executor.execute(
            pipeline=_pipeline(),
            arguments=AdagioArguments(inputs={}, parameters={}, outputs={}),
            monitor=monitor,
        )
    launcher.launch.assert_not_called()
    reports = [call.kwargs for call in monitor.finish_task.call_args_list]
    assert [report["status"] for report in reports] == ["failed", "skipped"]
    assert reports[0]["error"] == str(error.value)


def test_legacy_launcher_without_preflight_still_runs():
    def launch(*, environment, request):
        return TaskExecutionResult(outputs={})

    result = _launch(
        NS(launch=launch),
        environment=TaskEnvironmentSpec(kind="custom", reference="test"),
        request=NS(task=NS(id="custom")),
        monitor=Mock(),
    )
    assert result.outputs == {}


@pytest.mark.parametrize("command", ["run", "runtime"])
def test_cli_prints_actionable_preflight_error_without_traceback(tmp_path, command):
    spec = tmp_path / "pipeline.adg"
    spec.write_text(_pipeline().model_dump_json())
    config = tmp_path / "config.json"
    config.write_text(
        json.dumps(
            {"version": 1, "defaults": {"kind": "docker", "image": "test:latest"}}
        )
    )
    options = (
        [
            "--spec",
            str(spec),
            "--output-dir",
            str(tmp_path / "outputs"),
            "--targets",
            "selected",
        ]
        if command == "runtime"
        else ["--pipeline", str(spec)]
    )
    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "adagio.cli.main",
            command,
            *options,
            "--config",
            str(config),
            "--cache-dir",
            str(tmp_path / "cache"),
        ],
        env={**os.environ, "PATH": ""},
        cwd=tmp_path,
        capture_output=True,
        text=True,
        timeout=20,
    )
    assert result.returncode == 1
    assert "Cannot start node 'selected'" in result.stderr
    assert "Docker was not found. Install Docker" in result.stderr
    assert "Traceback" not in result.stderr
