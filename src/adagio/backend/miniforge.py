from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import json
from pathlib import Path
import platform
import shutil
from typing import Callable

from adagio.backend.base import CommandResult, InstallReport, run_command

DEFAULT_FLUX_IMAGE = "docker.io/fluxrm/flux-sched:latest"
CommandRunner = Callable[[list[str]], CommandResult]
Which = Callable[[str], str | None]


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
        runtime_cfg["docker_context"] = f"adagio-{macos_profile}"
        runtime_cfg["docker_host"] = (
            f"unix://{Path.home() / '.colima' / macos_profile / 'docker.sock'}"
        )
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


def _pull_image(runtime: str, image: str, runner: CommandRunner) -> CommandResult:
    if runtime in {"podman", "nerdctl"}:
        return runner([runtime, "pull", image])

    if runtime in {"apptainer", "singularity"}:
        cache_dir = Path.home() / ".cache" / "adagio"
        cache_dir.mkdir(parents=True, exist_ok=True)
        output = cache_dir / "flux.sif"
        return runner([runtime, "pull", "--force", str(output), f"docker://{image}"])

    if runtime == "docker":
        return runner(["docker", "pull", image])

    return CommandResult(returncode=1, stdout="", stderr=f"Unsupported runtime: {runtime}")


def _install_macos(
    request: InstallRequest,
    report: InstallReport,
    runner: CommandRunner,
    which: Which,
) -> str | None:
    colima = which("colima")
    if not colima:
        report.add(
            "colima-check",
            "failed",
            "Colima is required on macOS. Install with `brew install colima docker`.",
        )
        return None

    runtime = "docker"
    start_cmd = [
        "colima",
        "start",
        "--profile",
        request.macos_profile,
        "--runtime",
        "docker",
    ]

    if request.apply:
        started = runner(start_cmd)
        if started.ok:
            report.add(
                "colima-start",
                "changed",
                f"Started isolated Colima profile `{request.macos_profile}`.",
                " ".join(start_cmd),
            )
        else:
            report.add(
                "colima-start",
                "failed",
                started.stderr or "Failed to start Colima profile.",
                " ".join(start_cmd),
            )
            return None
    else:
        report.add(
            "colima-start",
            "skipped",
            "Dry run: Colima profile start not executed.",
            " ".join(start_cmd),
        )

    if which("docker"):
        context_name = f"adagio-{request.macos_profile}"
        sock = Path.home() / ".colima" / request.macos_profile / "docker.sock"
        inspect_cmd = ["docker", "context", "inspect", context_name]
        create_cmd = [
            "docker",
            "context",
            "create",
            context_name,
            "--docker",
            f"host=unix://{sock}",
        ]

        inspected = runner(inspect_cmd)
        if inspected.ok:
            report.add(
                "docker-context",
                "ok",
                f"Docker context `{context_name}` already exists.",
                " ".join(inspect_cmd),
            )
        elif request.apply:
            created = runner(create_cmd)
            if created.ok:
                report.add(
                    "docker-context",
                    "changed",
                    f"Created Docker context `{context_name}` for isolated Colima socket.",
                    " ".join(create_cmd),
                )
            else:
                report.add(
                    "docker-context",
                    "failed",
                    created.stderr or "Unable to create Docker context for Colima.",
                    " ".join(create_cmd),
                )
                return None
        else:
            report.add(
                "docker-context",
                "skipped",
                "Dry run: Docker context creation not executed.",
                " ".join(create_cmd),
            )

    return runtime


def _install_linux(
    request: InstallRequest,
    report: InstallReport,
    runner: CommandRunner,
    which: Which,
) -> str | None:
    runtime = _select_linux_runtime(which)
    if not runtime:
        report.add(
            "runtime-check",
            "failed",
            "Install one runtime: podman, nerdctl, apptainer, or singularity.",
        )
        return None

    report.add("runtime-check", "ok", f"Using runtime `{runtime}`.")

    pull_cmd = [runtime, "pull", request.image]
    if runtime in {"apptainer", "singularity"}:
        cache_dir = Path.home() / ".cache" / "adagio"
        output = cache_dir / "flux.sif"
        pull_cmd = [runtime, "pull", "--force", str(output), f"docker://{request.image}"]

    if request.apply:
        pulled = _pull_image(runtime, request.image, runner)
        if pulled.ok:
            report.add(
                "image-pull",
                "changed",
                f"Prepared Flux image using `{runtime}`.",
                " ".join(pull_cmd),
            )
        else:
            report.add(
                "image-pull",
                "failed",
                pulled.stderr or "Unable to pull Flux image.",
                " ".join(pull_cmd),
            )
            return None
    else:
        report.add(
            "image-pull",
            "skipped",
            "Dry run: image preparation not executed.",
            " ".join(pull_cmd),
        )

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
    if not which("wsl.exe"):
        report.add(
            "wsl-check",
            "failed",
            "WSL is required on Windows. Install with `wsl --install`.",
        )
        return None

    status_cmd = ["wsl.exe", "--status"]
    status = runner(status_cmd)
    if not status.ok:
        report.add(
            "wsl-check",
            "failed",
            status.stderr or "Unable to query WSL status.",
            " ".join(status_cmd),
        )
        return None

    has_wsl2_default = "Default Version: 2" in status.stdout
    if has_wsl2_default:
        report.add("wsl-version", "ok", "WSL default version is 2.", " ".join(status_cmd))
    elif request.apply:
        set_default_cmd = ["wsl.exe", "--set-default-version", "2"]
        set_default = runner(set_default_cmd)
        if set_default.ok:
            report.add(
                "wsl-version",
                "changed",
                "Set WSL default version to 2.",
                " ".join(set_default_cmd),
            )
        else:
            report.add(
                "wsl-version",
                "failed",
                set_default.stderr or "Unable to set WSL default version to 2.",
                " ".join(set_default_cmd),
            )
            return None
    else:
        report.add(
            "wsl-version",
            "skipped",
            "Dry run: would set WSL default version to 2.",
            "wsl.exe --set-default-version 2",
        )

    distros_cmd = ["wsl.exe", "--list", "--quiet"]
    distros = runner(distros_cmd)
    if not distros.ok:
        report.add(
            "wsl-distro",
            "failed",
            distros.stderr or "Unable to list WSL distros.",
            " ".join(distros_cmd),
        )
        return None

    installed_distros = [line.strip() for line in distros.stdout.splitlines() if line.strip()]
    if not installed_distros and request.apply:
        install_cmd = ["wsl.exe", "--install", "-d", "Ubuntu"]
        installed = runner(install_cmd)
        if installed.ok:
            report.add(
                "wsl-distro",
                "changed",
                "Installed default Ubuntu distro for WSL. "
                "Complete first-launch setup and rerun install.",
                " ".join(install_cmd),
            )
        else:
            report.add(
                "wsl-distro",
                "failed",
                installed.stderr or "Unable to install default WSL distro.",
                " ".join(install_cmd),
            )
        return None
    if not installed_distros:
        report.add(
            "wsl-distro",
            "skipped",
            "Dry run: would install default WSL distro (Ubuntu).",
            "wsl.exe --install -d Ubuntu",
        )
        return None

    report.add(
        "wsl-distro",
        "ok",
        f"Using distro `{installed_distros[0]}`.",
        " ".join(distros_cmd),
    )

    runtime_cmd = [
        "wsl.exe",
        "-e",
        "sh",
        "-lc",
        "command -v podman || command -v nerdctl || command -v apptainer || command -v singularity",
    ]
    runtime_probe = runner(runtime_cmd)
    runtime = _extract_wsl_runtime(runtime_probe.stdout) if runtime_probe.ok else None
    if not runtime:
        report.add(
            "runtime-check",
            "failed",
            "No container runtime found in WSL distro. "
            "Install podman, nerdctl, apptainer, or singularity inside WSL.",
            " ".join(runtime_cmd),
        )
        return None

    report.add(
        "runtime-check",
        "ok",
        f"Using WSL runtime `{runtime}`.",
        " ".join(runtime_cmd),
    )

    if request.apply:
        if runtime in {"podman", "nerdctl"}:
            pull_text = f"{runtime} pull {request.image}"
        else:
            pull_text = f"{runtime} pull --force ~/.cache/adagio/flux.sif docker://{request.image}"
        pull_cmd = ["wsl.exe", "-e", "sh", "-lc", pull_text]
        pulled = runner(pull_cmd)
        if pulled.ok:
            report.add(
                "image-pull",
                "changed",
                "Prepared Flux image inside WSL.",
                " ".join(pull_cmd),
            )
        else:
            report.add(
                "image-pull",
                "failed",
                pulled.stderr or "Unable to prepare Flux image in WSL.",
                " ".join(pull_cmd),
            )
            return None
    else:
        report.add(
            "image-pull",
            "skipped",
            "Dry run: WSL image preparation not executed.",
            f"wsl.exe -e sh -lc '<runtime pull {request.image}>'",
        )

    return runtime


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
