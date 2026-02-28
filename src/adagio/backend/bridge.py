from __future__ import annotations

from datetime import datetime, timezone
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import queue
import secrets
import threading
from typing import Any
from urllib import parse

from adagio.backend.agent.protocol import BridgeEvent, RPCRequest, RPCResult


class _BridgeState:
    def __init__(self):
        self._events: queue.Queue[BridgeEvent] = queue.Queue()
        self._rpc_calls: queue.Queue[RPCRequest] = queue.Queue()
        self._rpc_results: queue.Queue[RPCResult] = queue.Queue()

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


def _make_bridge_handler(state: _BridgeState, token: str):
    class BridgeHandler(BaseHTTPRequestHandler):
        server_version = "AdagioBridge/0.1"

        def log_message(self, _format: str, *_args):
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
            if path not in {"/events", "/rpc/result"}:
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
                    error=payload.get("error")
                    if isinstance(payload.get("error"), dict)
                    else None,
                )
                state.add_rpc_result(result)
                self._send_json(HTTPStatus.ACCEPTED, {"accepted": True})
                return

    return BridgeHandler


class BridgeServer:
    def __init__(
        self,
        bind: str,
        port: int,
        token: str,
    ):
        self.token = token
        self._state = _BridgeState()
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
