from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import json
from pathlib import Path
import platform
import shutil
from typing import Any, Callable

from adagio.backend.base import CommandResult, InstallReport, StepStatus, run_command

DEFAULT_FLUX_IMAGE = "docker.io/fluxrm/flux-sched:latest"
CommandRunner = Callable[[list[str]], CommandResult]
Which = Callable[[str], str | None]
CommandFactory = Callable[[], list[str]]
Condition = Callable[[], bool]
ResultPredicate = Callable[[CommandResult], bool]
DetailFactory = Callable[[], str]
ResultHook = Callable[[CommandResult], None]
DetailArg = str | DetailFactory | None


def _always() -> bool:
    return True


@dataclass(slots=True)
class StepResult:
    ok: bool = True
    stopped: bool = False


@dataclass(slots=True)
class InstallContext:
    request: InstallRequest
    report: InstallReport
    runner: CommandRunner


@dataclass(slots=True)
class InstallStep:
    name: str
    when: Condition = _always

    def run(self, ctx: InstallContext) -> StepResult:
        if not self.when():
            return StepResult()
        return self._run(ctx)

    def _run(self, _ctx: InstallContext) -> StepResult:
        raise NotImplementedError


@dataclass(slots=True)
class EmitStep(InstallStep):
    status: StepStatus = "ok"
    detail: DetailArg = None
    stop_on_emit: bool = False

    def _run(self, ctx: InstallContext) -> StepResult:
        ctx.report.add(
            self.name,
            self.status,
            _resolve_detail(self.detail, "No detail."),
        )
        return StepResult(stopped=self.stop_on_emit)


@dataclass(slots=True)
class CommandStep(InstallStep):
    command: list[str] | CommandFactory | None = None
    expect: ResultPredicate | None = None
    expect_fail_status: StepStatus = "failed"
    stop_on_expect_fail: bool = True
    success_detail: DetailArg = None
    success_status: StepStatus = "changed"
    fail_detail: DetailArg = None
    expect_fail_detail: DetailArg = None
    on_result: ResultHook | None = None
    stop_on_success: bool = False
    stop_on_failure: bool = True
    skip_detail: DetailArg = None
    stop_on_skip: bool = False

    def should_skip(self, _ctx: InstallContext) -> bool:
        return False

    def _run(self, ctx: InstallContext) -> StepResult:
        if self.command is None:
            raise ValueError(f"Install step `{self.name}` requires a command")

        cmd = self.command() if callable(self.command) else list(self.command)
        rendered = _render_cmd(cmd)

        if self.should_skip(ctx):
            ctx.report.add(
                self.name,
                "skipped",
                _resolve_detail(self.skip_detail, "Dry run: step not executed."),
                rendered,
            )
            return StepResult(stopped=self.stop_on_skip)

        result = ctx.runner(cmd)
        if self.on_result is not None:
            self.on_result(result)

        passed = self.expect(result) if self.expect else result.ok
        if passed:
            ctx.report.add(
                self.name,
                self.success_status,
                _resolve_detail(self.success_detail, "Step completed."),
                rendered,
            )
            return StepResult(stopped=self.stop_on_success)

        status = self.expect_fail_status
        detail = _resolve_detail(self.expect_fail_detail or self.fail_detail, "Step failed.")
        if status == "failed":
            detail = result.stderr or detail
        ctx.report.add(self.name, status, detail, rendered)

        if status == "failed" and self.stop_on_failure:
            return StepResult(ok=False, stopped=False)
        return StepResult(ok=True, stopped=self.stop_on_expect_fail)


@dataclass(slots=True)
class ApplyStep(CommandStep):
    skip_detail: DetailArg = "Dry run: step not executed."

    def should_skip(self, ctx: InstallContext) -> bool:
        return not ctx.request.apply


@dataclass(slots=True)
class CheckStep(CommandStep):
    pass


def _sentence(text: str) -> str:
    value = text.strip()
    if not value:
        return value
    if value.endswith("."):
        return value
    return f"{value}."


def _render_cmd(cmd: list[str]) -> str:
    return " ".join(cmd)


def _resolve_detail(detail: DetailArg, default: str | None = None) -> str:
    if callable(detail):
        return _sentence(detail())
    if detail is not None:
        return _sentence(detail)
    return _sentence(default or "")


def _run_install_actions(
    *,
    request: InstallRequest,
    report: InstallReport,
    runner: CommandRunner,
    actions: list[InstallStep],
) -> tuple[bool, bool]:
    ctx = InstallContext(request=request, report=report, runner=runner)
    for action in actions:
        result = action.run(ctx)
        if not result.ok:
            return False, False
        if result.stopped:
            return True, True

    return True, False


@dataclass(slots=True)
class InstallRequest:
    apply: bool = False
    image: str = DEFAULT_FLUX_IMAGE
    macos_profile: str = "adagio"


def _default_config_path() -> Path:
    if platform.system() == "Windows":
        appdata = Path.home() / "AppData" / "Roaming"
        return appdata / "adagio" / "compute-environment.json"
    xdg = Path.home() / ".config"
    return xdg / "adagio" / "compute-environment.json"


def _select_linux_runtime(which: Which) -> str | None:
    for candidate in ("podman", "nerdctl", "apptainer", "singularity"):
        if which(candidate):
            return candidate
    return None


def _normalize_image_ref(image: str) -> str:
    # Podman may reject short names if unqualified-search registries are disabled.
    # Treat refs without an explicit registry as Docker Hub images.
    first_segment = image.split("/", 1)[0]
    has_registry = "." in first_segment or ":" in first_segment or first_segment == "localhost"
    if has_registry:
        return image
    return f"docker.io/{image}"


def _build_config(
    *,
    request: InstallRequest,
    os_name: str,
    runtime: str,
    macos_profile: str,
) -> dict:
    runtime_cfg: dict[str, str | bool] = {
        "engine": runtime,
    }

    if os_name == "Darwin":
        runtime_cfg["colima_profile"] = macos_profile
        runtime_cfg["bridge_host"] = "host.lima.internal"
    elif os_name == "Windows":
        runtime_cfg["via_wsl"] = True

    return {
        "schema_version": 1,
        "created_at": datetime.now(timezone.utc).isoformat(),
        "platform": os_name,
        "flux": {
            "image": request.image,
        },
        "runtime": runtime_cfg,
    }


def _pull_image_cmd(runtime: str, image: str) -> list[str]:
    if runtime in {"podman", "nerdctl", "docker"}:
        return [runtime, "pull", image]

    if runtime in {"apptainer", "singularity"}:
        output = Path.home() / ".cache" / "adagio" / "flux.sif"
        return [runtime, "pull", "--force", str(output), f"docker://{image}"]

    return [runtime, "pull", image]


def _install_macos(
    request: InstallRequest,
    report: InstallReport,
    runner: CommandRunner,
    which: Which,
) -> str | None:
    state: dict[str, Any] = {
        "colima_exists": bool(which("colima")),
    }
    runtime = "nerdctl"

    actions = [
        EmitStep(
            name="colima-check",
            when=lambda: not bool(state["colima_exists"]),
            status="failed",
            detail="Colima is required on macOS. Install with `brew install colima`.",
            stop_on_emit=True,
        ),
        ApplyStep(
            name="colima-start",
            command=[
                "colima",
                "start",
                "--profile",
                request.macos_profile,
                "--runtime",
                "containerd",
            ],
            success_detail=f"Started isolated Colima profile `{request.macos_profile}`.",
            skip_detail=f"Dry run: would start isolated Colima profile `{request.macos_profile}`.",
            fail_detail="Failed to start Colima profile.",
        ),
        ApplyStep(
            name="image-pull",
            command=[
                "colima",
                "--profile",
                request.macos_profile,
                "nerdctl",
                "pull",
                request.image,
            ],
            success_detail="Prepared Flux image using Colima nerdctl.",
            skip_detail="Dry run: Colima nerdctl image pull not executed.",
            fail_detail="Unable to pull Flux image with Colima nerdctl.",
        ),
    ]
    ok, _stopped = _run_install_actions(
        request=request,
        report=report,
        runner=runner,
        actions=actions,
    )
    if not ok:
        return None

    return runtime


def _install_linux(
    request: InstallRequest,
    report: InstallReport,
    runner: CommandRunner,
    which: Which,
) -> str | None:
    runtime = _select_linux_runtime(which)
    state: dict[str, Any] = {"runtime": runtime}

    ok, stopped = _run_install_actions(
        request=request,
        report=report,
        runner=runner,
        actions=[
            EmitStep(
                name="runtime-check",
                when=lambda: not bool(state["runtime"]),
                status="failed",
                detail="Install one runtime: podman, nerdctl, apptainer, or singularity.",
                stop_on_emit=True,
            ),
            EmitStep(
                name="runtime-check",
                when=lambda: bool(state["runtime"]),
                status="ok",
                detail=lambda: f"Using runtime `{state['runtime']}`.",
            ),
        ],
    )
    if not ok or stopped:
        return None

    runtime = str(state["runtime"])
    if request.apply and runtime in {"apptainer", "singularity"}:
        cache_dir = Path.home() / ".cache" / "adagio"
        cache_dir.mkdir(parents=True, exist_ok=True)

    ok, _stopped = _run_install_actions(
        request=request,
        report=report,
        runner=runner,
        actions=[
            ApplyStep(
                name="image-pull",
                command=lambda: _pull_image_cmd(runtime, request.image),
                success_detail=lambda: f"Prepared Flux image using `{runtime}`.",
                skip_detail="Dry run: image preparation not executed.",
                fail_detail="Unable to pull Flux image.",
            )
        ],
    )
    if not ok:
        return None

    return runtime


def _extract_wsl_runtime(stdout: str) -> str | None:
    for line in stdout.splitlines():
        value = line.strip()
        if value:
            return Path(value).name
    return None


def _install_windows(
    request: InstallRequest,
    report: InstallReport,
    runner: CommandRunner,
    which: Which,
) -> str | None:
    state: dict[str, Any] = {
        "wsl_exists": bool(which("wsl.exe")),
        "wsl_status_stdout": "",
        "has_wsl2_default": False,
        "installed_distros": [],
        "runtime": None,
    }

    def _capture_status(result: CommandResult):
        state["wsl_status_stdout"] = result.stdout
        state["has_wsl2_default"] = "Default Version: 2" in result.stdout

    def _capture_distros(result: CommandResult):
        state["installed_distros"] = [
            line.strip() for line in result.stdout.splitlines() if line.strip()
        ]

    def _capture_runtime(result: CommandResult):
        state["runtime"] = _extract_wsl_runtime(result.stdout) if result.ok else None

    actions = [
        EmitStep(
            name="wsl-check",
            when=lambda: not bool(state["wsl_exists"]),
            status="failed",
            detail="WSL is required on Windows. Install with `wsl --install`.",
            stop_on_emit=True,
        ),
        CheckStep(
            name="wsl-check",
            command=["wsl.exe", "--status"],
            on_result=_capture_status,
            success_status="ok",
            success_detail="WSL is available.",
            fail_detail="Unable to query WSL status.",
        ),
        EmitStep(
            name="wsl-version",
            when=lambda: bool(state["has_wsl2_default"]),
            status="ok",
            detail="WSL default version is 2.",
        ),
        ApplyStep(
            name="wsl-version",
            when=lambda: not bool(state["has_wsl2_default"]),
            command=["wsl.exe", "--set-default-version", "2"],
            success_detail="Set WSL default version to 2.",
            skip_detail="Dry run: would set WSL default version to 2.",
            fail_detail="Unable to set WSL default version to 2.",
        ),
        CheckStep(
            name="wsl-distro-list",
            command=["wsl.exe", "--list", "--quiet"],
            on_result=_capture_distros,
            success_status="ok",
            success_detail="Queried WSL distro list.",
            fail_detail="Unable to list WSL distros.",
        ),
        ApplyStep(
            name="wsl-distro",
            when=lambda: len(state["installed_distros"]) == 0,
            command=["wsl.exe", "--install", "-d", "Ubuntu"],
            success_detail=(
                "Installed default Ubuntu distro for WSL. "
                "Complete first-launch setup and rerun install."
            ),
            skip_detail="Dry run: would install default WSL distro (Ubuntu).",
            fail_detail="Unable to install default WSL distro.",
            stop_on_success=True,
            stop_on_skip=True,
        ),
        EmitStep(
            name="wsl-distro",
            when=lambda: len(state["installed_distros"]) > 0,
            status="ok",
            detail=lambda: f"Using distro `{state['installed_distros'][0]}`.",
        ),
        CheckStep(
            name="runtime-check",
            command=[
                "wsl.exe",
                "-e",
                "sh",
                "-lc",
                "command -v podman || command -v nerdctl || command -v apptainer || command -v singularity",
            ],
            on_result=_capture_runtime,
            expect=lambda _result: bool(state["runtime"]),
            success_status="ok",
            success_detail=lambda: f"Using WSL runtime `{state['runtime']}`.",
            expect_fail_detail=(
                "No container runtime found in WSL distro. "
                "Install podman, nerdctl, apptainer, or singularity inside WSL."
            ),
        ),
        ApplyStep(
            name="image-pull",
            when=lambda: bool(state["runtime"]),
            command=lambda: [
                "wsl.exe",
                "-e",
                "sh",
                "-lc",
                (
                    f"{state['runtime']} pull {request.image}"
                    if state["runtime"] in {"podman", "nerdctl"}
                    else (
                        f"{state['runtime']} pull --force ~/.cache/adagio/flux.sif "
                        f"docker://{request.image}"
                    )
                ),
            ],
            success_detail="Prepared Flux image inside WSL.",
            skip_detail="Dry run: WSL image preparation not executed.",
            fail_detail="Unable to prepare Flux image in WSL.",
        ),
    ]
    ok, stopped = _run_install_actions(
        request=request,
        report=report,
        runner=runner,
        actions=actions,
    )
    if not ok or stopped:
        return None

    return str(state["runtime"])


def _write_config(report: InstallReport, config: dict, apply: bool):
    path = _default_config_path()
    report.config_path = path
    if not apply:
        report.add(
            "write-config",
            "skipped",
            "Dry run: config file not written.",
            f"write {path}",
        )
        return

    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(config, indent=2) + "\n", encoding="utf-8")
    report.add(
        "write-config",
        "changed",
        f"Wrote compute environment config to `{path}`.",
        f"write {path}",
    )


def install_compute_environment(
    request: InstallRequest,
    runner: CommandRunner = run_command,
    which: Which = shutil.which,
) -> InstallReport:
    effective_request = InstallRequest(
        apply=request.apply,
        image=_normalize_image_ref(request.image),
        macos_profile=request.macos_profile,
    )
    os_name = platform.system()
    report = InstallReport(platform=os_name, image=effective_request.image)

    runtime: str | None = None
    if os_name == "Darwin":
        runtime = _install_macos(effective_request, report, runner, which)
    elif os_name == "Linux":
        runtime = _install_linux(effective_request, report, runner, which)
    elif os_name == "Windows":
        runtime = _install_windows(effective_request, report, runner, which)
    else:
        report.add(
            "platform-check",
            "failed",
            f"Unsupported platform `{os_name}`.",
        )

    if runtime:
        report.runtime = runtime
        config = _build_config(
            request=effective_request,
            os_name=os_name,
            runtime=runtime,
            macos_profile=effective_request.macos_profile,
        )
        _write_config(report, config, effective_request.apply)

    return report
