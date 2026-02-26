from __future__ import annotations

import json
import os
from pathlib import Path
import socket
from tempfile import TemporaryDirectory
import unittest
from urllib import error, request
import zipfile

from adagio.backend.dispatch import (
    AgentLaunchRequest,
    BridgeEvent,
    BridgeServer,
    ComputeEnvironmentConfig,
    FluxRPCClient,
    FluxRPCSession,
    RPCRequest,
    RPCResult,
    RemoteCallError,
    RuntimeMount,
    RuntimeConfig,
    _build_bundled_agent_zipapp,
    _build_runtime_command,
    _default_bridge_host,
    enqueue_bridge_command,
    load_compute_environment,
    serve_rpc_loop,
    send_bridge_event,
)
from adagio.backend.util import build_zipapp_from_subpackage


def _can_bind_loopback() -> bool:
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            sock.bind(("127.0.0.1", 0))
            return True
        finally:
            sock.close()
    except OSError:
        return False


class TestDispatchBackend(unittest.TestCase):
    def test_load_compute_environment(self):
        with TemporaryDirectory() as tmp:
            cfg = Path(tmp) / "compute-environment.json"
            cfg.write_text(
                json.dumps(
                    {
                        "platform": "Linux",
                        "flux": {"image": "docker.io/fluxrm/flux-sched:latest"},
                        "runtime": {"engine": "podman"},
                    }
                ),
                encoding="utf-8",
            )

            loaded = load_compute_environment(cfg)
            self.assertEqual(loaded.runtime.engine, "podman")
            self.assertEqual(loaded.image, "docker.io/fluxrm/flux-sched:latest")

    def test_default_bridge_host(self):
        self.assertEqual(_default_bridge_host("podman"), "host.containers.internal")
        self.assertEqual(_default_bridge_host("docker"), "host.docker.internal")
        self.assertEqual(_default_bridge_host("apptainer"), "127.0.0.1")

    def test_rpc_client_from_env(self):
        old_url = os.environ.get("ADAGIO_BRIDGE_URL")
        old_token = os.environ.get("ADAGIO_BRIDGE_TOKEN")
        try:
            os.environ["ADAGIO_BRIDGE_URL"] = "http://127.0.0.1:1234"
            os.environ["ADAGIO_BRIDGE_TOKEN"] = "abc"
            client = FluxRPCClient.from_env()
            self.assertEqual(client.bridge_url, "http://127.0.0.1:1234")
            self.assertEqual(client.token, "abc")
        finally:
            if old_url is None:
                os.environ.pop("ADAGIO_BRIDGE_URL", None)
            else:
                os.environ["ADAGIO_BRIDGE_URL"] = old_url
            if old_token is None:
                os.environ.pop("ADAGIO_BRIDGE_TOKEN", None)
            else:
                os.environ["ADAGIO_BRIDGE_TOKEN"] = old_token

    def test_build_runtime_command_podman(self):
        config = ComputeEnvironmentConfig(
            path=Path("/tmp/compute-environment.json"),
            platform="Linux",
            image="docker.io/fluxrm/flux-sched:latest",
            runtime=RuntimeConfig(engine="podman"),
        )
        dispatch = AgentLaunchRequest(
            agent_command="flux run hostname",
            workdir=Path("/tmp"),
            bridge_host="host.containers.internal",
        )

        cmd = _build_runtime_command(
            config=config,
            request=dispatch,
            agent_bridge_url="http://host.containers.internal:49152",
            token="abc123",
        )

        self.assertEqual(cmd[0], "podman")
        self.assertIn("docker.io/fluxrm/flux-sched:latest", cmd)
        self.assertIn("ADAGIO_BRIDGE_TOKEN=abc123", cmd)
        self.assertIn("ADAGIO_BRIDGE_URL=http://host.containers.internal:49152", cmd)

    def test_build_runtime_command_wsl_wrapper(self):
        config = ComputeEnvironmentConfig(
            path=Path("/tmp/compute-environment.json"),
            platform="Windows",
            image="docker.io/fluxrm/flux-sched:latest",
            runtime=RuntimeConfig(engine="podman", via_wsl=True),
        )
        dispatch = AgentLaunchRequest(agent_command="flux run hostname")

        cmd = _build_runtime_command(
            config=config,
            request=dispatch,
            agent_bridge_url="http://host.containers.internal:49152",
            token="abc123",
        )

        self.assertEqual(cmd[:4], ["wsl.exe", "-e", "sh", "-lc"])
        self.assertIn("podman run --rm", cmd[4])

    def test_build_runtime_command_includes_runtime_mounts(self):
        config = ComputeEnvironmentConfig(
            path=Path("/tmp/compute-environment.json"),
            platform="Linux",
            image="docker.io/fluxrm/flux-sched:latest",
            runtime=RuntimeConfig(engine="podman"),
        )
        dispatch = AgentLaunchRequest(
            agent_command="python3 /tmp/adagio-agent/rpc-agent.pyz",
            runtime_mounts=[
                RuntimeMount(
                    host_path=Path("/tmp/adagio-agent-host"),
                    container_path="/tmp/adagio-agent",
                    read_only=True,
                )
            ],
        )
        cmd = _build_runtime_command(
            config=config,
            request=dispatch,
            agent_bridge_url="http://host.containers.internal:49152",
            token="abc123",
        )
        self.assertIn("-v", cmd)
        self.assertIn("/tmp/adagio-agent-host:/tmp/adagio-agent:ro", cmd)

    def test_build_bundled_agent_zipapp(self):
        with TemporaryDirectory() as tmp:
            target = Path(tmp) / "rpc-agent.pyz"
            _build_bundled_agent_zipapp(target)

            self.assertTrue(target.exists())
            with zipfile.ZipFile(target) as archive:
                names = set(archive.namelist())
            self.assertIn("__main__.py", names)
            self.assertIn("adagio/__init__.py", names)
            self.assertIn("adagio/embedded_agent/__init__.py", names)
            self.assertIn("adagio/embedded_agent/main.py", names)
            self.assertNotIn("adagio/embedded_agent/__main__.py", names)
            self.assertNotIn("adagio/backend/dispatch.py", names)

    def test_build_zipapp_from_subpackage(self):
        with TemporaryDirectory() as tmp:
            target = Path(tmp) / "embedded-agent.pyz"
            build_zipapp_from_subpackage(target, "adagio.embedded_agent")
            self.assertTrue(target.exists())
            with zipfile.ZipFile(target) as archive:
                names = set(archive.namelist())
            self.assertIn("__main__.py", names)
            self.assertIn("adagio/embedded_agent/main.py", names)

    def test_rpc_session_resolve_result(self):
        session = FluxRPCSession(agent_command="true")
        from concurrent.futures import Future

        future: Future[object] = Future()
        with session._pending_lock:
            session._pending["call-1"] = future
        session._resolve_result(RPCResult(id="call-1", result={"ok": True}))
        self.assertEqual(future.result(timeout=1), {"ok": True})

    def test_rpc_session_resolve_error(self):
        session = FluxRPCSession(agent_command="true")
        from concurrent.futures import Future

        future: Future[object] = Future()
        with session._pending_lock:
            session._pending["call-1"] = future
        session._resolve_result(
            RPCResult(
                id="call-1",
                error={"type": "ValueError", "message": "boom"},
            )
        )
        with self.assertRaises(RemoteCallError):
            future.result(timeout=1)

    def test_rpc_session_subscribe_filters(self):
        session = FluxRPCSession(agent_command="true")
        seen_all: list[str] = []
        seen_progress: list[str] = []

        sub_all = session.subscribe(lambda ev: seen_all.append(ev.event_type))
        sub_progress = session.subscribe(
            lambda ev: seen_progress.append(str(ev.payload.get("message", ""))),
            event_type="progress",
        )

        session._dispatch_event(
            BridgeEvent(
                timestamp="2026-02-25T00:00:00+00:00",
                event_type="progress",
                payload={"message": "step-1"},
            )
        )
        session._dispatch_event(
            BridgeEvent(
                timestamp="2026-02-25T00:00:01+00:00",
                event_type="info",
                payload={"message": "done"},
            )
        )

        self.assertEqual(seen_all, ["progress", "info"])
        self.assertEqual(seen_progress, ["step-1"])
        self.assertTrue(session.unsubscribe(sub_progress))
        self.assertTrue(session.unsubscribe(sub_all))

    def test_serve_rpc_loop_with_context_emit(self):
        class FakeClient:
            def __init__(self, calls: list[RPCRequest]):
                self.calls = list(calls)
                self.results: list[tuple[str, object]] = []
                self.errors: list[tuple[str, str, str]] = []
                self.events: list[tuple[str, dict[str, object]]] = []

            def next_call(self) -> RPCRequest | None:
                if self.calls:
                    return self.calls.pop(0)
                return None

            def submit_result(self, call_id: str, result: object):
                self.results.append((call_id, result))

            def submit_error(self, call_id: str, message: str, *, error_type: str = "RemoteError"):
                self.errors.append((call_id, message, error_type))

            def send_event(self, event_type: str, **payload: object):
                self.events.append((event_type, payload))

        fake = FakeClient([RPCRequest(id="call-1", method="ping", params={"message": "hi"})])

        def ping(*, ctx, message: str):
            ctx.emit("progress", message=f"handler:{message}")
            return {"message": message}

        serve_rpc_loop(
            handlers={"ping": ping},
            client=fake,  # type: ignore[arg-type]
            poll_interval=0,
            max_calls=1,
        )

        self.assertEqual(fake.errors, [])
        self.assertEqual(fake.results, [("call-1", {"message": "hi"})])
        self.assertEqual(fake.events[0][0], "progress")
        self.assertEqual(fake.events[0][1]["call_id"], "call-1")
        self.assertEqual(fake.events[0][1]["method"], "ping")

    def test_serve_rpc_loop_legacy_handler(self):
        class FakeClient:
            def __init__(self, calls: list[RPCRequest]):
                self.calls = list(calls)
                self.results: list[tuple[str, object]] = []
                self.errors: list[tuple[str, str, str]] = []

            def next_call(self) -> RPCRequest | None:
                if self.calls:
                    return self.calls.pop(0)
                return None

            def submit_result(self, call_id: str, result: object):
                self.results.append((call_id, result))

            def submit_error(self, call_id: str, message: str, *, error_type: str = "RemoteError"):
                self.errors.append((call_id, message, error_type))

        fake = FakeClient([RPCRequest(id="call-1", method="echo", params={"value": 9})])

        def echo(value: int):
            return value

        serve_rpc_loop(
            handlers={"echo": echo},
            client=fake,  # type: ignore[arg-type]
            poll_interval=0,
            max_calls=1,
        )

        self.assertEqual(fake.errors, [])
        self.assertEqual(fake.results, [("call-1", 9)])

    @unittest.skipUnless(
        _can_bind_loopback(),
        "Loopback socket binding unavailable in this environment",
    )
    def test_rpc_roundtrip_over_bridge(self):
        with BridgeServer(
            bind="127.0.0.1",
            port=0,
            token="token-1",
            initial_commands=[],
        ) as server:
            server.add_rpc_call(
                RPCRequest(id="call-1", method="echo", params={"value": 7})
            )
            client = FluxRPCClient(bridge_url=server.host_url, token="token-1")
            call = client.next_call()
            self.assertIsNotNone(call)
            assert call is not None
            self.assertEqual(call.id, "call-1")
            self.assertEqual(call.method, "echo")
            self.assertEqual(call.params["value"], 7)

            client.submit_result(call.id, {"ok": True})
            results = server.drain_rpc_results()
            self.assertEqual(len(results), 1)
            self.assertEqual(results[0].id, "call-1")
            self.assertEqual(results[0].result, {"ok": True})

    @unittest.skipUnless(
        _can_bind_loopback(),
        "Loopback socket binding unavailable in this environment",
    )
    def test_bridge_server_events_and_commands(self):
        with BridgeServer(
            bind="127.0.0.1",
            port=0,
            token="token-1",
            initial_commands=["command-a"],
        ) as server:
            send_bridge_event(
                bridge_url=server.host_url,
                token="token-1",
                event_type="progress",
                message="step 1",
            )

            events = server.drain_events()
            self.assertEqual(len(events), 1)
            self.assertEqual(events[0].event_type, "progress")
            self.assertEqual(events[0].payload["message"], "step 1")

            req = request.Request(
                f"{server.host_url}/commands/next",
                headers={"Authorization": "Bearer token-1"},
                method="GET",
            )
            with request.urlopen(req, timeout=5) as response:
                payload = json.loads(response.read().decode("utf-8"))
            self.assertEqual(payload["command"], "command-a")

            enqueue_bridge_command(
                server.host_url,
                token="token-1",
                command="command-b",
            )

            req = request.Request(
                f"{server.host_url}/commands/next",
                headers={"Authorization": "Bearer token-1"},
                method="GET",
            )
            with request.urlopen(req, timeout=5) as response:
                payload = json.loads(response.read().decode("utf-8"))
            self.assertEqual(payload["command"], "command-b")

    @unittest.skipUnless(
        _can_bind_loopback(),
        "Loopback socket binding unavailable in this environment",
    )
    def test_bridge_server_requires_token(self):
        with BridgeServer(
            bind="127.0.0.1",
            port=0,
            token="token-1",
            initial_commands=[],
        ) as server:
            req = request.Request(
                f"{server.host_url}/commands/next",
                method="GET",
            )
            with self.assertRaises(error.HTTPError) as cm:
                request.urlopen(req, timeout=5)

            self.assertEqual(cm.exception.code, 401)


if __name__ == "__main__":
    unittest.main()
