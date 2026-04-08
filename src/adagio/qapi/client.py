import json
import os
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen


def submit_qapi_payload(
    payload: dict[str, Any],
    *,
    action_url: str | None = None,
    submission_token: str | None = None,
    timeout: int = 60,
    dry_run: bool = False,
    force_overwrite: bool = False,
) -> tuple[str, int, Any]:
    resolved_action_url = action_url or os.getenv("ACTION_URL")
    if not resolved_action_url:
        raise SystemExit(
            "No Action URL configured. Set --action-url or ACTION_URL environment variable."
        )

    url = resolved_action_url.rstrip("/") + "/qapi/"
    resolved_submission_token = submission_token or os.getenv("QAPI_SUBMISSION_TOKEN")
    request_body = {
        **payload,
        "dry_run": dry_run,
        "force_overwrite": force_overwrite,
    }
    headers = {"Content-Type": "application/json"}
    if resolved_submission_token:
        headers["Authorization"] = f"Bearer {resolved_submission_token}"
    req = Request(
        url=url,
        data=json.dumps(request_body).encode("utf-8"),
        headers=headers,
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

    if not response_body.strip():
        return url, status, ""

    try:
        return url, status, json.loads(response_body)
    except json.JSONDecodeError:
        return url, status, response_body
