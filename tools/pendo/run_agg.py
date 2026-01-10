from __future__ import annotations

import argparse
import json
import os
import sys
import time
import urllib.error
import urllib.request
from typing import Any, Tuple

try:
    # Preferred usage: `python -m tools.pendo.run_agg ...`
    from .dsl_compile import compile_dsl_text
    from .env import load_dotenv
    from .rewrite import rewrite_on_error
    from .validate import validate_aggregation_body
except ImportError:  # pragma: no cover
    # Fallback for direct execution: `python tools/pendo/run_agg.py ...`
    from tools.pendo.dsl_compile import compile_dsl_text
    from tools.pendo.env import load_dotenv
    from tools.pendo.rewrite import rewrite_on_error
    from tools.pendo.validate import validate_aggregation_body


class PendoRequestError(RuntimeError):
    def __init__(self, message: str, *, status: int | None = None, body: Any | None = None):
        super().__init__(message)
        self.status = status
        self.body = body


def _env(name: str, *, required: bool = True, default: str | None = None) -> str:
    v = os.getenv(name)
    if v is None or v == "":
        if required and default is None:
            raise SystemExit(f"error: missing required env var {name}")
        return default or ""
    return v


def _env_any(names: list[str], *, required: bool = True, default: str | None = None) -> str:
    for n in names:
        v = os.getenv(n)
        if v is not None and v != "":
            return v
    if required and default is None:
        raise SystemExit(f"error: missing required env var (any of): {', '.join(names)}")
    return default or ""


def _load_text(path: str | None, *, use_stdin: bool) -> str:
    if use_stdin:
        return sys.stdin.read()
    if path is None:
        raise SystemExit("error: provide a .dsl/.json file or --stdin")
    with open(path, "r", encoding="utf-8") as f:
        return f.read()


def _load_json_from_text(text: str) -> Any:
    return json.loads(text)


def _detect_format(path: str | None, text: str) -> str:
    if path:
        if path.lower().endswith(".dsl"):
            return "dsl"
        if path.lower().endswith(".json"):
            return "json"
    # Heuristic: JSON starts with { or [
    s = text.lstrip()
    if s.startswith("{") or s.startswith("["):
        return "json"
    return "dsl"


def _http_post_json(url: str, api_key: str, api_key_header: str, payload: dict[str, Any]) -> Tuple[int, Any]:
    data = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    req = urllib.request.Request(url, data=data, method="POST")
    req.add_header("Content-Type", "application/json")
    req.add_header("Accept", "application/json")
    req.add_header(api_key_header, api_key)

    try:
        with urllib.request.urlopen(req, timeout=60) as resp:
            raw = resp.read().decode("utf-8", errors="replace")
            try:
                return resp.status, json.loads(raw)
            except Exception:
                return resp.status, raw
    except urllib.error.HTTPError as e:
        raw = e.read().decode("utf-8", errors="replace")
        try:
            parsed = json.loads(raw)
        except Exception:
            parsed = raw
        raise PendoRequestError(
            f"HTTP {e.code} from Pendo",
            status=e.code,
            body=parsed,
        )


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description="Run a Pendo Aggregation request (DSL or JSON) and print the response JSON")
    p.add_argument("path", nargs="?", help="Path to .dsl or .json")
    p.add_argument("--stdin", action="store_true", help="Read DSL/JSON from stdin")
    p.add_argument("--format", choices=["auto", "dsl", "json"], default="auto")
    p.add_argument("--max-attempts", type=int, default=5)
    p.add_argument("--keep-now", action="store_true", help="Do not resolve now() during DSL compilation")
    p.add_argument("--pretty", action="store_true", help="Pretty-print response JSON")
    args = p.parse_args(argv)

    # Helpful for local usage in VS Code terminals: if env vars aren't set,
    # allow loading them from a local .env file.
    if os.getenv("PENDO_API_KEY") in (None, "") and os.getenv("PENDO_INTEGRATION_KEY") in (None, ""):
        load_dotenv(".env")

    # Default to the most common public endpoint if not configured.
    url = _env("PENDO_AGG_URL", required=False, default="https://app.pendo.io/api/v1/aggregation")
    # Support either name (people commonly call this an integration key).
    api_key = _env_any(["PENDO_API_KEY", "PENDO_INTEGRATION_KEY"])
    api_key_header = _env("PENDO_API_KEY_HEADER", required=False, default="x-pendo-integration-key")

    text = _load_text(args.path, use_stdin=args.stdin)
    fmt = args.format if args.format != "auto" else _detect_format(args.path, text)

    body: dict[str, Any]
    if fmt == "dsl":
        body = compile_dsl_text(text, resolve_now=not args.keep_now)
    else:
        loaded = _load_json_from_text(text)
        if not isinstance(loaded, dict):
            print("error: JSON input must be an aggregation body object", file=sys.stderr)
            return 2
        body = loaded

    # Retry build+send up to max attempts.
    current = body
    last_err: PendoRequestError | None = None

    for attempt in range(1, max(1, args.max_attempts) + 1):
        try:
            validate_aggregation_body(current)
            _status, resp = _http_post_json(url, api_key, api_key_header, current)

            if args.pretty:
                json.dump(resp, sys.stdout, indent=2, ensure_ascii=False)
            else:
                json.dump(resp, sys.stdout, ensure_ascii=False)
            sys.stdout.write("\n")
            return 0
        except PendoRequestError as e:
            last_err = e
            err_text = json.dumps(e.body, ensure_ascii=False) if e.body is not None else str(e)
            if attempt >= args.max_attempts:
                break
            current = rewrite_on_error(current, attempt=attempt, error_text=err_text)
            # small backoff for transient issues
            time.sleep(min(0.5 * attempt, 2.0))
        except Exception as e:
            print(f"error: {e}", file=sys.stderr)
            return 2

    if last_err is not None:
        # Provide the last error body for the agent/user to fix the DSL.
        print(f"error: {last_err}", file=sys.stderr)
        if last_err.status is not None:
            print(f"status: {last_err.status}", file=sys.stderr)
        if last_err.body is not None:
            print("response:", file=sys.stderr)
            json.dump(last_err.body, sys.stderr, indent=2, ensure_ascii=False)
            sys.stderr.write("\n")
        return 2

    print("error: request failed", file=sys.stderr)
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
