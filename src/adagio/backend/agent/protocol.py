from __future__ import annotations

from dataclasses import dataclass, field
from http import HTTPStatus
import inspect
import json
import os
import time
from typing import Any, Callable
from urllib import request


@dataclass(slots=True)
class BridgeEvent:
    timestamp: str
    event_type: str
    payload: dict[str, Any]


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
        pass

    return handler(**params)


def serve_rpc_loop(
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
        except Exception as exc:  # pragma: no cover - agent-side safety net
            rpc.submit_error(call.id, str(exc), error_type=type(exc).__name__)

        handled_calls += 1
        if max_calls is not None and handled_calls >= max_calls:
            return
