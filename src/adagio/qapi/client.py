import json
import os
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen


def submit_qapi_payload(
    payload: dict[str, Any],
    *,
    action_url: str | None = None,
    timeout: int = 60,
) -> tuple[str, int, str]:
    resolved_action_url = action_url or os.getenv("ACTION_URL")
    if not resolved_action_url:
        raise SystemExit(
            "No Action URL configured. Set --action-url or ACTION_URL environment variable."
        )

    url = resolved_action_url.rstrip("/") + "/qapi/"
    req = Request(
        url=url,
        data=json.dumps(payload).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )

    try:
        with urlopen(req, timeout=timeout) as resp:  # nosec: B310 - user-supplied API URL is intended
            status = resp.status
            response_body = resp.read().decode("utf-8", errors="replace")
    except HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")
        raise SystemExit(f"QAPI submit failed ({exc.code}): {body}") from exc
    except URLError as exc:
        raise SystemExit(f"QAPI submit failed: {exc.reason}") from exc

    return url, status, response_body
