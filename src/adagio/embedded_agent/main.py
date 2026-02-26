from __future__ import annotations

from http import HTTPStatus
import inspect
import json
import os
import subprocess
import time
from typing import Any, Callable
from urllib import request


def _post_json(url: str, token: str, payload: dict[str, Any]) -> None:
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


def _get_json(url: str, token: str) -> tuple[int, dict[str, Any] | None]:
    req = request.Request(
        url,
        headers={"Authorization": f"Bearer {token}"},
        method="GET",
    )
    with request.urlopen(req, timeout=10) as response:
        if response.status == HTTPStatus.NO_CONTENT:
            return int(response.status), None
        raw = response.read().decode("utf-8")
        if not raw:
            return int(response.status), None
        return int(response.status), json.loads(raw)


class BridgeClient:
    def __init__(self, bridge_url: str, token: str):
        self.bridge_url = bridge_url.rstrip("/")
        self.token = token

    @classmethod
    def from_env(cls) -> "BridgeClient":
        bridge_url = os.environ.get("ADAGIO_BRIDGE_URL", "").strip()
        token = os.environ.get("ADAGIO_BRIDGE_TOKEN", "").strip()
        if not bridge_url or not token:
            raise ValueError(
                "ADAGIO_BRIDGE_URL and ADAGIO_BRIDGE_TOKEN must be present in environment"
            )
        return cls(bridge_url=bridge_url, token=token)

    def next_call(self) -> dict[str, Any] | None:
        status, payload = _get_json(f"{self.bridge_url}/rpc/next", self.token)
        if status == HTTPStatus.NO_CONTENT or payload is None:
            return None
        if not isinstance(payload, dict):
            raise RuntimeError("Invalid RPC payload from bridge")
        return payload

    def submit_result(self, call_id: str, result: Any):
        _post_json(
            f"{self.bridge_url}/rpc/result",
            self.token,
            {"id": call_id, "result": result},
        )

    def submit_error(self, call_id: str, message: str, error_type: str = "RemoteError"):
        _post_json(
            f"{self.bridge_url}/rpc/result",
            self.token,
            {"id": call_id, "error": {"type": error_type, "message": message}},
        )

    def send_event(self, event_type: str, **payload: Any):
        _post_json(f"{self.bridge_url}/events", self.token, {"type": event_type, **payload})


class HandlerContext:
    def __init__(self, client: BridgeClient, call_id: str, method: str):
        self.client = client
        self.call_id = call_id
        self.method = method

    def emit(self, event_type: str, **payload: Any):
        self.client.send_event(
            event_type=event_type,
            call_id=self.call_id,
            method=self.method,
            **payload,
        )


def _invoke(handler: Callable[..., Any], ctx: HandlerContext, params: dict[str, Any]) -> Any:
    try:
        sig = inspect.signature(handler)
        accepts_kwargs = any(
            p.kind == inspect.Parameter.VAR_KEYWORD for p in sig.parameters.values()
        )
        if "ctx" in sig.parameters or accepts_kwargs:
            return handler(ctx=ctx, **params)
    except (TypeError, ValueError):
        pass
    return handler(**params)


def serve_loop(
    handlers: dict[str, Callable[..., Any]],
    *,
    client: BridgeClient | None = None,
    poll_interval: float = 0.2,
    max_calls: int | None = None,
):
    bridge = client or BridgeClient.from_env()
    handled_calls = 0
    while True:
        call = bridge.next_call()
        if call is None:
            time.sleep(poll_interval)
            continue

        call_id = str(call.get("id", "")).strip()
        method = str(call.get("method", "")).strip()
        params = call.get("params")
        if not call_id or not method:
            raise RuntimeError("Invalid RPC payload from bridge")
        if not isinstance(params, dict):
            params = {}

        handler = handlers.get(method)
        if handler is None:
            bridge.submit_error(call_id, f"Unknown method `{method}`", error_type="UnknownMethod")
            continue

        ctx = HandlerContext(client=bridge, call_id=call_id, method=method)
        try:
            result = _invoke(handler, ctx, params)
            bridge.submit_result(call_id, result)
        except Exception as exc:  # pragma: no cover - runtime safety net
            bridge.submit_error(call_id, str(exc), error_type=type(exc).__name__)

        handled_calls += 1
        if max_calls is not None and handled_calls >= max_calls:
            return


def ping(*, ctx: HandlerContext, message: str = "pong") -> dict[str, str]:
    ctx.emit("progress", message=f"ping called with: {message}")
    return {"message": message}


def run_shell(*, ctx: HandlerContext, cmd: str) -> dict[str, object]:
    ctx.emit("progress", message=f"run_shell starting: {cmd}")
    proc = subprocess.run(cmd, shell=True, text=True, capture_output=True, check=False)
    ctx.emit("progress", message=f"run_shell returncode={proc.returncode}")
    return {
        "returncode": proc.returncode,
        "stdout": proc.stdout,
        "stderr": proc.stderr,
    }


def main():
    serve_loop({"ping": ping, "run_shell": run_shell})


if __name__ == "__main__":
    main()
