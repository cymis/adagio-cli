from __future__ import annotations

from concurrent.futures import Future
from dataclasses import dataclass, field
from datetime import datetime, timezone
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import os
from pathlib import Path
import queue
import secrets
import shlex
import subprocess
import threading
import time
from typing import Any, Callable, Sequence
from urllib import parse, request
import uuid
import inspect

from adagio.backend.miniforge import _default_config_path


@dataclass(slots=True)
class RuntimeConfig:
    engine: str
    docker_context: str | None = None
    docker_host: str | None = None
    via_wsl: bool = False


@dataclass(slots=True)
class ComputeEnvironmentConfig:
    path: Path
    platform: str
    image: str
    runtime: RuntimeConfig
    raw: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class BridgeEvent:
    timestamp: str
    event_type: str
    payload: dict[str, Any]


@dataclass(slots=True)
class DispatchRequest:
    agent_command: str
    tasks: list[str] = field(default_factory=list)
    config_path: Path | None = None
    workdir: Path | None = None
    container_workdir: str = "/workspace"
    bridge_bind: str = "0.0.0.0"
    bridge_port: int = 0
    bridge_host: str | None = None
    bridge_token: str | None = None


@dataclass(slots=True)
class DispatchReport:
    ok: bool
    returncode: int
    command: list[str]
    host_bridge_url: str
    agent_bridge_url: str
    token: str
    events: list[BridgeEvent] = field(default_factory=list)


@dataclass(slots=True)
class DispatchSession:
    command: list[str]
    host_bridge_url: str
    agent_bridge_url: str
    token: str


OutputCallback = Callable[[str, str], None]
EventCallback = Callable[[BridgeEvent], None]
StartCallback = Callable[[DispatchSession], None]


@dataclass(slots=True)
class RPCRequest:
    id: str
    method: str
    params: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class RPCResult:
    id: str
    result: Any | None = None
    error: dict[str, Any] | None = None


@dataclass(slots=True)
class RPCHandlerContext:
    client: "FluxRPCClient"
    call: RPCRequest

    @property
    def call_id(self) -> str:
        return self.call.id

    @property
    def method(self) -> str:
        return self.call.method

    def emit(self, event_type: str, **payload: Any):
        self.client.send_event(
            event_type=event_type,
            call_id=self.call.id,
            method=self.call.method,
            **payload,
        )


class _BridgeState:
    def __init__(self, initial_tasks: Sequence[str]):
        self._events: queue.Queue[BridgeEvent] = queue.Queue()
        self._tasks: queue.Queue[str] = queue.Queue()
        self._rpc_calls: queue.Queue[RPCRequest] = queue.Queue()
        self._rpc_results: queue.Queue[RPCResult] = queue.Queue()
        for item in initial_tasks:
            self._tasks.put(item)

    def add_event(self, payload: dict[str, Any]):
        event_type = str(payload.get("type", "event"))
        self._events.put(
            BridgeEvent(
                timestamp=datetime.now(timezone.utc).isoformat(),
                event_type=event_type,
                payload=payload,
            )
        )

    def pop_event(self) -> BridgeEvent | None:
        try:
            return self._events.get_nowait()
        except queue.Empty:
            return None

    def add_task(self, task: str):
        self._tasks.put(task)

    def next_task(self) -> str | None:
        try:
            return self._tasks.get_nowait()
        except queue.Empty:
            return None

    def add_rpc_call(self, call: RPCRequest):
        self._rpc_calls.put(call)

    def next_rpc_call(self) -> RPCRequest | None:
        try:
            return self._rpc_calls.get_nowait()
        except queue.Empty:
            return None

    def add_rpc_result(self, result: RPCResult):
        self._rpc_results.put(result)

    def pop_rpc_result(self) -> RPCResult | None:
        try:
            return self._rpc_results.get_nowait()
        except queue.Empty:
            return None


class BridgeServer:
    def __init__(
        self,
        bind: str,
        port: int,
        token: str,
        initial_tasks: Sequence[str],
    ):
        self.token = token
        self._state = _BridgeState(initial_tasks)
        handler = _make_bridge_handler(self._state, token)
        self._httpd = ThreadingHTTPServer((bind, port), handler)
        self._thread = threading.Thread(target=self._httpd.serve_forever, daemon=True)

    @property
    def port(self) -> int:
        return int(self._httpd.server_address[1])

    @property
    def host_url(self) -> str:
        return f"http://127.0.0.1:{self.port}"

    def start(self):
        self._thread.start()

    def stop(self):
        self._httpd.shutdown()
        self._httpd.server_close()
        self._thread.join(timeout=2)

    def add_task(self, task: str):
        self._state.add_task(task)

    def add_rpc_call(self, call: RPCRequest):
        self._state.add_rpc_call(call)

    def drain_events(self) -> list[BridgeEvent]:
        events: list[BridgeEvent] = []
        while (event := self._state.pop_event()) is not None:
            events.append(event)
        return events

    def drain_rpc_results(self) -> list[RPCResult]:
        results: list[RPCResult] = []
        while (result := self._state.pop_rpc_result()) is not None:
            results.append(result)
        return results

    def __enter__(self) -> "BridgeServer":
        self.start()
        return self

    def __exit__(self, *_args):
        self.stop()


def _default_bridge_host(engine: str) -> str:
    if engine == "podman":
        return "host.containers.internal"
    if engine in {"docker", "nerdctl"}:
        return "host.docker.internal"
    return "127.0.0.1"


def _make_bridge_handler(state: _BridgeState, token: str):
    class BridgeHandler(BaseHTTPRequestHandler):
        server_version = "AdagioBridge/0.1"

        def log_message(self, _format: str, *_args):
            # Keep CLI output clean and rely on explicit event logging.
            return

        def _authorized(self) -> bool:
            auth = self.headers.get("Authorization", "")
            if auth.startswith("Bearer ") and secrets.compare_digest(
                auth.removeprefix("Bearer ").strip(), token
            ):
                return True

            parsed = parse.urlparse(self.path)
            query_token = parse.parse_qs(parsed.query).get("token", [""])[0]
            return bool(query_token) and secrets.compare_digest(query_token, token)

        def _read_json(self) -> dict[str, Any] | None:
            length = int(self.headers.get("Content-Length", "0"))
            raw = self.rfile.read(length)
            try:
                value = json.loads(raw.decode("utf-8"))
            except json.JSONDecodeError:
                return None
            return value if isinstance(value, dict) else None

        def _send_json(self, status: int, payload: dict[str, Any] | None = None):
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            if payload is not None:
                self.wfile.write(json.dumps(payload).encode("utf-8"))

        def do_GET(self):
            path = parse.urlparse(self.path).path
            if path == "/health":
                self._send_json(HTTPStatus.OK, {"ok": True})
                return

            if path == "/tasks/next":
                if not self._authorized():
                    self._send_json(HTTPStatus.UNAUTHORIZED, {"error": "unauthorized"})
                    return

                task = state.next_task()
                if task is None:
                    self._send_json(HTTPStatus.NO_CONTENT)
                else:
                    self._send_json(HTTPStatus.OK, {"task": task})
                return

            if path == "/rpc/next":
                if not self._authorized():
                    self._send_json(HTTPStatus.UNAUTHORIZED, {"error": "unauthorized"})
                    return

                call = state.next_rpc_call()
                if call is None:
                    self._send_json(HTTPStatus.NO_CONTENT)
                else:
                    self._send_json(
                        HTTPStatus.OK,
                        {"id": call.id, "method": call.method, "params": call.params},
                    )
                return

            self._send_json(HTTPStatus.NOT_FOUND, {"error": "not-found"})

        def do_POST(self):
            path = parse.urlparse(self.path).path
            if path not in {"/events", "/tasks", "/rpc/result"}:
                self._send_json(HTTPStatus.NOT_FOUND, {"error": "not-found"})
                return

            if not self._authorized():
                self._send_json(HTTPStatus.UNAUTHORIZED, {"error": "unauthorized"})
                return

            payload = self._read_json()
            if payload is None:
                self._send_json(HTTPStatus.BAD_REQUEST, {"error": "invalid-json"})
                return

            if path == "/events":
                state.add_event(payload)
                self._send_json(HTTPStatus.ACCEPTED, {"accepted": True})
                return

            if path == "/rpc/result":
                call_id = payload.get("id")
                if not isinstance(call_id, str) or not call_id:
                    self._send_json(HTTPStatus.BAD_REQUEST, {"error": "missing-id"})
                    return

                result = RPCResult(
                    id=call_id,
                    result=payload.get("result"),
                    error=payload.get("error") if isinstance(payload.get("error"), dict) else None,
                )
                state.add_rpc_result(result)
                self._send_json(HTTPStatus.ACCEPTED, {"accepted": True})
                return

            task = payload.get("task")
            if not isinstance(task, str) or not task.strip():
                self._send_json(HTTPStatus.BAD_REQUEST, {"error": "missing-task"})
                return

            state.add_task(task)
            self._send_json(HTTPStatus.ACCEPTED, {"accepted": True})

    return BridgeHandler


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
    config: ComputeEnvironmentConfig,
    agent_bridge_url: str,
    token: str,
) -> dict[str, str]:
    return {
        "ADAGIO_BRIDGE_URL": agent_bridge_url,
        "ADAGIO_BRIDGE_EVENTS_URL": f"{agent_bridge_url}/events",
        "ADAGIO_BRIDGE_TASKS_URL": f"{agent_bridge_url}/tasks/next",
        "ADAGIO_BRIDGE_TOKEN": token,
        "ADAGIO_RUNTIME_ENGINE": config.runtime.engine,
        "ADAGIO_FLUX_IMAGE": config.image,
    }


def _build_runtime_command(
    config: ComputeEnvironmentConfig,
    request: DispatchRequest,
    agent_bridge_url: str,
    token: str,
) -> list[str]:
    engine = config.runtime.engine
    env = _agent_env(config, agent_bridge_url, token)
    workdir = request.workdir.resolve() if request.workdir else None

    command: list[str]
    if engine in {"docker", "podman", "nerdctl"}:
        command = [engine]
        if engine == "docker" and config.runtime.docker_context:
            command.extend(["--context", config.runtime.docker_context])

        command.extend(["run", "--rm"])
        if workdir:
            command.extend(["-v", f"{workdir}:{request.container_workdir}"])
            command.extend(["-w", request.container_workdir])

        for key, value in env.items():
            command.extend(["-e", f"{key}={value}"])

        command.extend([config.image, "sh", "-lc", request.agent_command])

    elif engine in {"apptainer", "singularity"}:
        command = [engine, "exec"]
        if workdir:
            command.extend(["--bind", f"{workdir}:{request.container_workdir}"])

        for key, value in env.items():
            command.extend(["--env", f"{key}={value}"])

        sif_path = Path.home() / ".cache" / "adagio" / "flux.sif"
        if sif_path.exists():
            image_ref = str(sif_path)
        else:
            image_ref = f"docker://{config.image}"

        command.extend([image_ref, "sh", "-lc", request.agent_command])

    else:
        raise ValueError(f"Unsupported runtime engine: {engine}")

    if config.runtime.via_wsl:
        return ["wsl.exe", "-e", "sh", "-lc", shlex.join(command)]
    return command


def _post_bridge_json(url: str, token: str, payload: dict[str, Any]) -> None:
    req = request.Request(
        url,
        data=json.dumps(payload).encode("utf-8"),
        headers={
            "Content-Type": "application/json",
            "Authorization": f"Bearer {token}",
        },
        method="POST",
    )
    with request.urlopen(req, timeout=10) as response:
        if response.status not in {HTTPStatus.OK, HTTPStatus.ACCEPTED}:
            raise RuntimeError(f"Bridge request failed with status {response.status}")


def _get_bridge_json(url: str, token: str) -> tuple[int, dict[str, Any] | None]:
    req = request.Request(
        url,
        headers={
            "Authorization": f"Bearer {token}",
        },
        method="GET",
    )
    with request.urlopen(req, timeout=10) as response:
        if response.status == HTTPStatus.NO_CONTENT:
            return int(response.status), None

        raw = response.read().decode("utf-8")
        if not raw:
            return int(response.status), None
        return int(response.status), json.loads(raw)


def enqueue_bridge_task(bridge_url: str, token: str, task: str) -> None:
    _post_bridge_json(f"{bridge_url.rstrip('/')}/tasks", token, {"task": task})


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
):
    while True:
        try:
            stream_name, line = line_queue.get_nowait()
        except queue.Empty:
            return

        if on_output is not None:
            on_output(stream_name, line)


def dispatch_to_flux(
    request: DispatchRequest,
    on_start: StartCallback | None = None,
    on_output: OutputCallback | None = None,
    on_event: EventCallback | None = None,
) -> DispatchReport:
    config = load_compute_environment(request.config_path)
    token = request.bridge_token or secrets.token_urlsafe(18)

    bridge_host = request.bridge_host or _default_bridge_host(config.runtime.engine)

    server = BridgeServer(
        bind=request.bridge_bind,
        port=request.bridge_port,
        token=token,
        initial_tasks=request.tasks,
    )
    server.start()

    host_bridge_url = server.host_url
    agent_bridge_url = f"http://{bridge_host}:{server.port}"

    runtime_command = _build_runtime_command(config, request, agent_bridge_url, token)
    session = DispatchSession(
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

    return DispatchReport(
        ok=returncode == 0,
        returncode=returncode,
        command=runtime_command,
        host_bridge_url=host_bridge_url,
        agent_bridge_url=agent_bridge_url,
        token=token,
        events=events,
    )


def send_bridge_event(
    bridge_url: str,
    token: str,
    event_type: str,
    **payload: Any,
):
    body = {"type": event_type, **payload}
    _post_bridge_json(f"{bridge_url.rstrip('/')}/events", token, body)


class RemoteCallError(RuntimeError):
    def __init__(self, call_id: str, detail: dict[str, Any]):
        self.call_id = call_id
        self.detail = detail
        message = str(detail.get("message", "remote call failed"))
        super().__init__(f"RPC call {call_id} failed: {message}")


class FluxRPCClient:
    def __init__(self, bridge_url: str, token: str):
        self.bridge_url = bridge_url.rstrip("/")
        self.token = token

    @classmethod
    def from_env(cls) -> "FluxRPCClient":
        bridge_url = os.environ.get("ADAGIO_BRIDGE_URL", "").strip()
        token = os.environ.get("ADAGIO_BRIDGE_TOKEN", "").strip()
        if not bridge_url or not token:
            raise ValueError(
                "ADAGIO_BRIDGE_URL and ADAGIO_BRIDGE_TOKEN must be present in environment"
            )
        return cls(bridge_url=bridge_url, token=token)

    def next_call(self) -> RPCRequest | None:
        status, payload = _get_bridge_json(
            f"{self.bridge_url}/rpc/next",
            token=self.token,
        )
        if status == HTTPStatus.NO_CONTENT or payload is None:
            return None

        call_id = str(payload.get("id", "")).strip()
        method = str(payload.get("method", "")).strip()
        params = payload.get("params")
        if not call_id or not method:
            raise RuntimeError("Invalid RPC payload from bridge")
        if not isinstance(params, dict):
            params = {}
        return RPCRequest(id=call_id, method=method, params=params)

    def submit_result(self, call_id: str, result: Any) -> None:
        _post_bridge_json(
            f"{self.bridge_url}/rpc/result",
            token=self.token,
            payload={"id": call_id, "result": result},
        )

    def submit_error(
        self,
        call_id: str,
        message: str,
        *,
        error_type: str = "RemoteError",
    ) -> None:
        _post_bridge_json(
            f"{self.bridge_url}/rpc/result",
            token=self.token,
            payload={
                "id": call_id,
                "error": {
                    "type": error_type,
                    "message": message,
                },
            },
        )

    def send_event(self, event_type: str, **payload: Any):
        send_bridge_event(
            bridge_url=self.bridge_url,
            token=self.token,
            event_type=event_type,
            **payload,
        )


def serve_rpc_forever(
    handlers: dict[str, Callable[..., Any]],
    client: FluxRPCClient | None = None,
    *,
    poll_interval: float = 0.2,
    max_calls: int | None = None,
):
    rpc = client or FluxRPCClient.from_env()
    handled_calls = 0
    while True:
        call = rpc.next_call()
        if call is None:
            time.sleep(poll_interval)
            continue

        handler = handlers.get(call.method)
        if handler is None:
            rpc.submit_error(
                call.id,
                f"Unknown method `{call.method}`",
                error_type="UnknownMethod",
            )
            continue

        ctx = RPCHandlerContext(client=rpc, call=call)
        try:
            result = _invoke_rpc_handler(handler, ctx, call.params)
            rpc.submit_result(call.id, result)
        except Exception as e:  # pragma: no cover - agent-side safety net
            rpc.submit_error(call.id, str(e), error_type=type(e).__name__)

        handled_calls += 1
        if max_calls is not None and handled_calls >= max_calls:
            return


def _invoke_rpc_handler(
    handler: Callable[..., Any],
    ctx: RPCHandlerContext,
    params: dict[str, Any],
) -> Any:
    try:
        sig = inspect.signature(handler)
        param_names = sig.parameters
        supports_kwargs = any(
            p.kind == inspect.Parameter.VAR_KEYWORD for p in param_names.values()
        )
        if "ctx" in param_names or supports_kwargs:
            return handler(ctx=ctx, **params)
    except (TypeError, ValueError):
        # Some callables cannot be introspected; use a runtime fallback.
        pass

    return handler(**params)


class FluxRPCSession:
    def __init__(
        self,
        *,
        agent_command: str,
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
        self._request = DispatchRequest(
            agent_command=agent_command,
            config_path=config_path,
            workdir=workdir,
            container_workdir=container_workdir,
            bridge_bind=bridge_bind,
            bridge_port=bridge_port,
            bridge_host=bridge_host,
            bridge_token=bridge_token,
        )
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
        self._session: DispatchSession | None = None
        self._stop_requested = threading.Event()
        self._returncode: int | None = None

    @property
    def started(self) -> bool:
        return self._process is not None and self._returncode is None

    @property
    def session(self) -> DispatchSession:
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

    def start(self) -> DispatchSession:
        if self.started:
            return self.session

        config = load_compute_environment(self._request.config_path)
        token = self._request.bridge_token or secrets.token_urlsafe(18)
        bridge_host = self._request.bridge_host or _default_bridge_host(config.runtime.engine)

        server = BridgeServer(
            bind=self._request.bridge_bind,
            port=self._request.bridge_port,
            token=token,
            initial_tasks=self._request.tasks,
        )
        server.start()

        host_bridge_url = server.host_url
        agent_bridge_url = f"http://{bridge_host}:{server.port}"
        runtime_command = _build_runtime_command(config, self._request, agent_bridge_url, token)

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

        self._server = server
        self._process = process
        self._session = DispatchSession(
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
                _drain_output(self._line_queue, self._on_output)

                for event in server.drain_events():
                    self._events.append(event)
                    self._dispatch_event(event)

                for rpc_result in server.drain_rpc_results():
                    self._resolve_result(rpc_result)

                if self._stop_requested.is_set():
                    if process.poll() is None:
                        process.terminate()

                if process.poll() is not None and self._line_queue.empty():
                    break

                time.sleep(0.1)

            _drain_output(self._line_queue, self._on_output)
            for event in server.drain_events():
                self._events.append(event)
                self._dispatch_event(event)
            for rpc_result in server.drain_rpc_results():
                self._resolve_result(rpc_result)
        finally:
            self._returncode = process.wait()
            server.stop()
            self._fail_unresolved(
                RuntimeError(
                    f"RPC session ended with return code {self._returncode}; pending calls canceled"
                )
            )

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
        if self._monitor_thread is not None:
            self._monitor_thread.join(timeout=5)
        if self._process.poll() is None:
            self._process.kill()

    def wait(self, timeout: float | None = None) -> int | None:
        if self._monitor_thread is not None:
            self._monitor_thread.join(timeout=timeout)
        return self._returncode

    def __enter__(self) -> "FluxRPCSession":
        self.start()
        return self

    def __exit__(self, *_args):
        self.close()
