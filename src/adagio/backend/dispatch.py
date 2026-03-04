from __future__ import annotations

from concurrent.futures import Future
from dataclasses import dataclass, field
import json
import os
from pathlib import Path
import queue
import secrets
import shlex
import subprocess
import tempfile
import threading
import time
from typing import Any, Callable
import uuid

from adagio.backend.agent.protocol import (
    BridgeEvent,
    FluxRPCClient,
    RPCRequest,
    RPCResult,
    RemoteCallError,
)
from adagio.backend.bridge import BridgeServer
from adagio.backend.setup import _default_config_path
from adagio.backend.util import build_zipapp_from_subpackage


@dataclass(slots=True)
class RuntimeConfig:
    engine: str
    docker_context: str | None = None
    docker_host: str | None = None
    colima_profile: str | None = None
    bridge_host: str | None = None
    via_wsl: bool = False


@dataclass(slots=True)
class ComputeEnvironmentConfig:
    path: Path
    platform: str
    image: str
    runtime: RuntimeConfig
    raw: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class AgentLaunchRequest:
    agent_command: str | None
    config_path: Path | None = None
    workdir: Path | None = None
    container_workdir: str = "/workspace"
    bridge_bind: str = "0.0.0.0"
    bridge_port: int = 0
    bridge_host: str | None = None
    bridge_token: str | None = None
    runtime_mounts: list["RuntimeMount"] = field(default_factory=list)


@dataclass(slots=True)
class RuntimeMount:
    host_path: Path
    container_path: str
    read_only: bool = False


@dataclass(slots=True)
class AgentRunReport:
    ok: bool
    returncode: int
    command: list[str]
    host_bridge_url: str
    agent_bridge_url: str
    token: str
    events: list[BridgeEvent] = field(default_factory=list)


@dataclass(slots=True)
class BridgeBinding:
    command: list[str]
    host_bridge_url: str
    agent_bridge_url: str
    token: str


OutputCallback = Callable[[str, str], None]
EventCallback = Callable[[BridgeEvent], None]
StartCallback = Callable[[BridgeBinding], None]


def _default_bridge_host(engine: str) -> str:
    if engine == "podman":
        return "host.containers.internal"
    if engine in {"docker", "nerdctl"}:
        return "host.docker.internal"
    return "127.0.0.1"


def load_compute_environment(config_path: Path | None = None) -> ComputeEnvironmentConfig:
    path = config_path or _default_config_path()
    data = json.loads(path.read_text(encoding="utf-8"))

    runtime_data = data.get("runtime") or {}
    flux_data = data.get("flux") or {}

    engine = str(runtime_data.get("engine", "")).strip()
    image = str(flux_data.get("image", "")).strip()
    platform_name = str(data.get("platform", "unknown")).strip() or "unknown"

    if not engine:
        raise ValueError(f"Invalid compute config at {path}: runtime.engine is missing")
    if not image:
        raise ValueError(f"Invalid compute config at {path}: flux.image is missing")

    runtime = RuntimeConfig(
        engine=engine,
        docker_context=runtime_data.get("docker_context"),
        docker_host=runtime_data.get("docker_host"),
        colima_profile=runtime_data.get("colima_profile"),
        bridge_host=runtime_data.get("bridge_host"),
        via_wsl=bool(runtime_data.get("via_wsl", False)),
    )
    return ComputeEnvironmentConfig(
        path=path,
        platform=platform_name,
        image=image,
        runtime=runtime,
        raw=data,
    )


def _agent_env(
    agent_bridge_url: str,
    token: str,
) -> dict[str, str]:
    return {
        "ADAGIO_BRIDGE_URL": agent_bridge_url,
        "ADAGIO_BRIDGE_TOKEN": token,
    }


def _build_runtime_command(
    config: ComputeEnvironmentConfig,
    request: AgentLaunchRequest,
    agent_bridge_url: str,
    token: str,
) -> list[str]:
    engine = config.runtime.engine
    env = _agent_env(agent_bridge_url, token)
    agent_command = (request.agent_command or "").strip()
    if not agent_command:
        raise ValueError("agent_command is required")

    workdir = request.workdir.resolve() if request.workdir else None

    command: list[str]
    if engine in {"docker", "podman", "nerdctl"}:
        if engine == "nerdctl" and config.platform == "Darwin" and config.runtime.colima_profile:
            # `colima nerdctl` needs `--` to pass container-runtime flags (e.g. `--rm`).
            command = ["colima", "--profile", config.runtime.colima_profile, "nerdctl", "--"]
        else:
            command = [engine]
        if engine == "docker" and config.runtime.docker_context:
            command.extend(["--context", config.runtime.docker_context])

        command.extend(["run", "--rm"])
        if workdir:
            command.extend(["-v", f"{workdir}:{request.container_workdir}"])
            command.extend(["-w", request.container_workdir])
        for mount in request.runtime_mounts:
            spec = f"{mount.host_path.resolve()}:{mount.container_path}"
            if mount.read_only:
                spec = f"{spec}:ro"
            command.extend(["-v", spec])

        for key, value in env.items():
            command.extend(["-e", f"{key}={value}"])

        command.extend([config.image, "sh", "-lc", agent_command])

    elif engine in {"apptainer", "singularity"}:
        command = [engine, "exec"]
        if workdir:
            command.extend(["--bind", f"{workdir}:{request.container_workdir}"])
        for mount in request.runtime_mounts:
            spec = f"{mount.host_path.resolve()}:{mount.container_path}"
            if mount.read_only:
                spec = f"{spec}:ro"
            command.extend(["--bind", spec])

        for key, value in env.items():
            command.extend(["--env", f"{key}={value}"])

        sif_path = Path.home() / ".cache" / "adagio" / "flux.sif"
        if sif_path.exists():
            image_ref = str(sif_path)
        else:
            image_ref = f"docker://{config.image}"

        command.extend([image_ref, "sh", "-lc", agent_command])

    else:
        raise ValueError(f"Unsupported runtime engine: {engine}")

    if config.runtime.via_wsl:
        return ["wsl.exe", "-e", "sh", "-lc", shlex.join(command)]
    return command


def _bundle_temp_parent(
    config: ComputeEnvironmentConfig,
    workdir: Path | None,
) -> Path | None:
    if workdir is not None:
        return workdir.resolve()

    # Colima + nerdctl on macOS may not expose system temp paths into the VM.
    if (
        config.platform == "Darwin"
        and config.runtime.engine == "nerdctl"
        and config.runtime.colima_profile
    ):
        return Path.home() / ".cache" / "adagio"

    return None


_BUNDLED_AGENT_CONTAINER_DIR = "/tmp/adagio-agent"
_BUNDLED_AGENT_FILENAME = "rpc-agent.pyz"
_BUNDLED_AGENT_SUBPACKAGE = "adagio.backend.agent"


def _build_bundled_agent_zipapp(target: Path):
    build_zipapp_from_subpackage(target, _BUNDLED_AGENT_SUBPACKAGE)


def _start_stream_reader(
    pipe,
    stream_name: str,
    line_queue: "queue.Queue[tuple[str, str]]",
):
    for line in iter(pipe.readline, ""):
        line_queue.put((stream_name, line.rstrip("\n")))
    pipe.close()


def _drain_output(
    line_queue: "queue.Queue[tuple[str, str]]",
    on_output: OutputCallback | None,
    on_line: OutputCallback | None = None,
):
    while True:
        try:
            stream_name, line = line_queue.get_nowait()
        except queue.Empty:
            return

        if on_line is not None:
            on_line(stream_name, line)
        if on_output is not None:
            on_output(stream_name, line)


def run_agent_once(
    request: AgentLaunchRequest,
    on_start: StartCallback | None = None,
    on_output: OutputCallback | None = None,
    on_event: EventCallback | None = None,
) -> AgentRunReport:
    config = load_compute_environment(request.config_path)
    token = request.bridge_token or secrets.token_urlsafe(18)

    bridge_host = (
        request.bridge_host
        or config.runtime.bridge_host
        or _default_bridge_host(config.runtime.engine)
    )

    server = BridgeServer(
        bind=request.bridge_bind,
        port=request.bridge_port,
        token=token,
    )
    server.start()

    host_bridge_url = server.host_url
    agent_bridge_url = f"http://{bridge_host}:{server.port}"

    runtime_command = _build_runtime_command(config, request, agent_bridge_url, token)
    session = BridgeBinding(
        command=runtime_command,
        host_bridge_url=host_bridge_url,
        agent_bridge_url=agent_bridge_url,
        token=token,
    )
    if on_start is not None:
        on_start(session)

    launch_env = os.environ.copy()
    if config.runtime.engine == "docker" and config.runtime.docker_host:
        launch_env.setdefault("DOCKER_HOST", config.runtime.docker_host)

    process = subprocess.Popen(
        runtime_command,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        bufsize=1,
        env=launch_env,
    )

    line_queue: "queue.Queue[tuple[str, str]]" = queue.Queue()
    stdout_thread = threading.Thread(
        target=_start_stream_reader,
        args=(process.stdout, "stdout", line_queue),
        daemon=True,
    )
    stderr_thread = threading.Thread(
        target=_start_stream_reader,
        args=(process.stderr, "stderr", line_queue),
        daemon=True,
    )
    stdout_thread.start()
    stderr_thread.start()

    events: list[BridgeEvent] = []

    try:
        while True:
            _drain_output(line_queue, on_output)

            for event in server.drain_events():
                events.append(event)
                if on_event is not None:
                    on_event(event)

            if process.poll() is not None and line_queue.empty():
                break

            time.sleep(0.1)

        _drain_output(line_queue, on_output)
        for event in server.drain_events():
            events.append(event)
            if on_event is not None:
                on_event(event)

        returncode = process.wait()
    finally:
        server.stop()

    return AgentRunReport(
        ok=returncode == 0,
        returncode=returncode,
        command=runtime_command,
        host_bridge_url=host_bridge_url,
        agent_bridge_url=agent_bridge_url,
        token=token,
        events=events,
    )


class FluxRPCSession:
    def __init__(
        self,
        *,
        agent_command: str | None = None,
        config_path: Path | None = None,
        workdir: Path | None = None,
        container_workdir: str = "/workspace",
        bridge_bind: str = "0.0.0.0",
        bridge_port: int = 0,
        bridge_host: str | None = None,
        bridge_token: str | None = None,
        on_output: OutputCallback | None = None,
        on_event: EventCallback | None = None,
    ):
        self._request = AgentLaunchRequest(
            agent_command=agent_command,
            config_path=config_path,
            workdir=workdir,
            container_workdir=container_workdir,
            bridge_bind=bridge_bind,
            bridge_port=bridge_port,
            bridge_host=bridge_host,
            bridge_token=bridge_token,
        )
        self._agent_command_override = agent_command
        self._on_output = on_output
        self._on_event = on_event

        self._server: BridgeServer | None = None
        self._process: subprocess.Popen[str] | None = None
        self._line_queue: queue.Queue[tuple[str, str]] = queue.Queue()
        self._monitor_thread: threading.Thread | None = None
        self._pending: dict[str, Future[Any]] = {}
        self._pending_lock = threading.Lock()
        self._events: list[BridgeEvent] = []
        self._subscriptions: dict[str, tuple[str, EventCallback]] = {}
        self._subscription_lock = threading.Lock()
        self._session: BridgeBinding | None = None
        self._stop_requested = threading.Event()
        self._returncode: int | None = None
        self._agent_bundle_tmpdir: tempfile.TemporaryDirectory[str] | None = None
        self._stderr_tail: list[str] = []

    @property
    def started(self) -> bool:
        return self._process is not None and self._returncode is None

    @property
    def session(self) -> BridgeBinding:
        if self._session is None:
            raise RuntimeError("Session not started")
        return self._session

    @property
    def events(self) -> list[BridgeEvent]:
        return list(self._events)

    @property
    def returncode(self) -> int | None:
        return self._returncode

    def subscribe(
        self,
        callback: EventCallback,
        *,
        event_type: str = "*",
    ) -> str:
        if not event_type:
            raise ValueError("event_type cannot be empty")
        sub_id = uuid.uuid4().hex
        with self._subscription_lock:
            self._subscriptions[sub_id] = (event_type, callback)
        return sub_id

    def unsubscribe(self, subscription_id: str) -> bool:
        with self._subscription_lock:
            return self._subscriptions.pop(subscription_id, None) is not None

    def start(self) -> BridgeBinding:
        if self.started:
            return self.session

        self._stderr_tail.clear()
        config = load_compute_environment(self._request.config_path)
        token = self._request.bridge_token or secrets.token_urlsafe(18)
        bridge_host = (
            self._request.bridge_host
            or config.runtime.bridge_host
            or _default_bridge_host(config.runtime.engine)
        )

        server = BridgeServer(
            bind=self._request.bridge_bind,
            port=self._request.bridge_port,
            token=token,
        )
        server.start()
        try:
            host_bridge_url = server.host_url
            agent_bridge_url = f"http://{bridge_host}:{server.port}"
            launch_request = AgentLaunchRequest(
                agent_command=self._request.agent_command,
                config_path=self._request.config_path,
                workdir=self._request.workdir,
                container_workdir=self._request.container_workdir,
                bridge_bind=self._request.bridge_bind,
                bridge_port=self._request.bridge_port,
                bridge_host=self._request.bridge_host,
                bridge_token=self._request.bridge_token,
                runtime_mounts=list(self._request.runtime_mounts),
            )
            if self._agent_command_override is None:
                bundle_parent = _bundle_temp_parent(config, self._request.workdir)
                if bundle_parent is not None and self._request.workdir is None:
                    bundle_parent.mkdir(parents=True, exist_ok=True)
                bundle_tmpdir = tempfile.TemporaryDirectory(
                    prefix=".adagio-agent-runtime-",
                    dir=str(bundle_parent) if bundle_parent is not None else None,
                )
                bundle_file = Path(bundle_tmpdir.name) / _BUNDLED_AGENT_FILENAME
                _build_bundled_agent_zipapp(bundle_file)
                os.chmod(bundle_tmpdir.name, 0o755)
                os.chmod(bundle_file, 0o644)
                if self._request.workdir is not None:
                    bundle_container_path = (
                        f"{self._request.container_workdir.rstrip('/')}/{Path(bundle_tmpdir.name).name}"
                    )
                else:
                    launch_request.runtime_mounts.append(
                        RuntimeMount(
                            host_path=Path(bundle_tmpdir.name),
                            container_path=_BUNDLED_AGENT_CONTAINER_DIR,
                            read_only=True,
                        )
                    )
                    bundle_container_path = _BUNDLED_AGENT_CONTAINER_DIR
                launch_request.agent_command = (
                    f"python3 {bundle_container_path}/{_BUNDLED_AGENT_FILENAME}"
                )
                self._agent_bundle_tmpdir = bundle_tmpdir

            runtime_command = _build_runtime_command(config, launch_request, agent_bridge_url, token)

            launch_env = os.environ.copy()
            if config.runtime.engine == "docker" and config.runtime.docker_host:
                launch_env.setdefault("DOCKER_HOST", config.runtime.docker_host)

            process = subprocess.Popen(
                runtime_command,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                bufsize=1,
                env=launch_env,
            )
        except Exception:
            server.stop()
            if self._agent_bundle_tmpdir is not None:
                self._agent_bundle_tmpdir.cleanup()
                self._agent_bundle_tmpdir = None
            raise

        self._server = server
        self._process = process
        self._session = BridgeBinding(
            command=runtime_command,
            host_bridge_url=host_bridge_url,
            agent_bridge_url=agent_bridge_url,
            token=token,
        )

        stdout_thread = threading.Thread(
            target=_start_stream_reader,
            args=(process.stdout, "stdout", self._line_queue),
            daemon=True,
        )
        stderr_thread = threading.Thread(
            target=_start_stream_reader,
            args=(process.stderr, "stderr", self._line_queue),
            daemon=True,
        )
        stdout_thread.start()
        stderr_thread.start()

        self._monitor_thread = threading.Thread(target=self._monitor_loop, daemon=True)
        self._monitor_thread.start()
        return self._session

    def _monitor_loop(self):
        assert self._server is not None
        assert self._process is not None

        process = self._process
        server = self._server

        try:
            while True:
                _drain_output(
                    self._line_queue,
                    self._on_output,
                    on_line=self._capture_output_line,
                )

                for event in server.drain_events():
                    self._events.append(event)
                    self._dispatch_event(event)

                for rpc_result in server.drain_rpc_results():
                    self._resolve_result(rpc_result)

                if self._stop_requested.is_set():
                    if process.poll() is None:
                        process.terminate()
                    break

                if process.poll() is not None and self._line_queue.empty():
                    break

                time.sleep(0.1)

            _drain_output(
                self._line_queue,
                self._on_output,
                on_line=self._capture_output_line,
            )
            for event in server.drain_events():
                self._events.append(event)
                self._dispatch_event(event)
            for rpc_result in server.drain_rpc_results():
                self._resolve_result(rpc_result)
        finally:
            self._returncode = process.wait()
            server.stop()
            self._fail_unresolved(self._session_end_exception())
            if self._agent_bundle_tmpdir is not None:
                self._agent_bundle_tmpdir.cleanup()
                self._agent_bundle_tmpdir = None

    def _capture_output_line(self, stream_name: str, line: str):
        if stream_name != "stderr":
            return
        self._stderr_tail.append(line)
        if len(self._stderr_tail) > 20:
            self._stderr_tail.pop(0)

    def _session_end_exception(self) -> RuntimeError:
        message = (
            f"RPC session ended with return code {self._returncode}; pending calls canceled"
        )
        if not self._stderr_tail:
            return RuntimeError(message)

        tail = "\n".join(self._stderr_tail)
        return RuntimeError(f"{message}\nRuntime stderr tail:\n{tail}")

    def _dispatch_event(self, event: BridgeEvent):
        if self._on_event is not None:
            try:
                self._on_event(event)
            except Exception:
                pass

        with self._subscription_lock:
            subscribers = [
                callback
                for sub_type, callback in self._subscriptions.values()
                if sub_type == "*" or sub_type == event.event_type
            ]

        for callback in subscribers:
            try:
                callback(event)
            except Exception:
                pass

    def _resolve_result(self, rpc_result: RPCResult):
        with self._pending_lock:
            future = self._pending.pop(rpc_result.id, None)
        if future is None or future.done():
            return

        if rpc_result.error:
            future.set_exception(RemoteCallError(rpc_result.id, rpc_result.error))
        else:
            future.set_result(rpc_result.result)

    def _fail_unresolved(self, exc: Exception):
        with self._pending_lock:
            items = list(self._pending.items())
            self._pending.clear()
        for _, future in items:
            if not future.done():
                future.set_exception(exc)

    def call(self, method: str, **params: Any) -> Future[Any]:
        if self._server is None or self._process is None or self._returncode is not None:
            raise RuntimeError("RPC session is not active; call start() first")

        call = RPCRequest(id=uuid.uuid4().hex, method=method, params=params)
        future: Future[Any] = Future()
        with self._pending_lock:
            self._pending[call.id] = future
        self._server.add_rpc_call(call)
        return future

    def call_blocking(self, method: str, timeout: float | None = None, **params: Any) -> Any:
        return self.call(method, **params).result(timeout=timeout)

    def close(self):
        if self._process is None:
            return

        self._stop_requested.set()
        if self._process.poll() is None:
            self._process.terminate()
        if self._monitor_thread is not None:
            self._monitor_thread.join(timeout=1)
        if self._process.poll() is None:
            self._process.kill()
        if self._monitor_thread is not None and self._monitor_thread.is_alive():
            self._monitor_thread.join(timeout=1)
        if self._agent_bundle_tmpdir is not None:
            self._agent_bundle_tmpdir.cleanup()
            self._agent_bundle_tmpdir = None

    def wait(self, timeout: float | None = None) -> int | None:
        if self._monitor_thread is not None:
            self._monitor_thread.join(timeout=timeout)
        return self._returncode

    def __enter__(self) -> "FluxRPCSession":
        self.start()
        return self

    def __exit__(self, *_args):
        self.close()
