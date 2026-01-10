from __future__ import annotations

import copy
import time
from typing import Any


def _walk(obj: Any, fn) -> Any:
    if isinstance(obj, dict):
        return {k: _walk(fn(k, v), fn) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_walk(x, fn) for x in obj]
    return obj


def resolve_now(body: dict[str, Any], *, now_ms: int | None = None) -> dict[str, Any]:
    """Rewrite timeSeries.first/last values of 'now()' to epoch ms.

    Some Pendo endpoints accept only numeric timestamps.
    """
    if now_ms is None:
        now_ms = int(time.time() * 1000)

    out = copy.deepcopy(body)

    def replace(_k: str, v: Any) -> Any:
        if isinstance(v, str) and v.strip().lower() == "now()":
            return now_ms
        return v

    return _walk(out, replace)  # type: ignore[return-value]


def rewrite_on_error(body: dict[str, Any], *, attempt: int, error_text: str) -> dict[str, Any]:
    """Best-effort safe rewrites when the server rejects the request.

    This is intentionally conservative: it should never invent stages.
    """
    lowered = error_text.lower()

    # Common failure mode: server rejects literal now().
    if "now()" in lowered or "timeseries" in lowered or "timestamp" in lowered:
        return resolve_now(body)

    # Default: no rewrite.
    return body
