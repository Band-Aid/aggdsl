"""PoC: one Code Mode invocation may execute several read-only Pendo aggregations.

The untrusted-ish JavaScript worker never receives Pendo credentials. This is a
prototype for trusted local use, not a hardened multi-tenant sandbox.
"""
from __future__ import annotations

import argparse
from dataclasses import dataclass
import json
import os
from pathlib import Path
import queue
import shutil
import subprocess
import sys
import threading
import time
from typing import Any, Protocol
import urllib.error
import urllib.request

from aggdsl import compile_to_pendo_aggregation, parse


class CodeModeError(RuntimeError):
    """An invalid program, execution failure, or resource limit."""


@dataclass(frozen=True)
class Limits:
    timeout_seconds: float = 20.0
    max_calls: int = 5
    max_code_bytes: int = 20_000
    max_dsl_bytes: int = 20_000
    max_response_bytes: int = 256_000
    max_output_bytes: int = 64_000


class AggregationBackend(Protocol):
    def run(self, dsl: str, *, timeout_seconds: float) -> Any: ...


class PendoBackend:
    """Compile aggDSL and optionally execute it against Pendo's read-only API."""

    def __init__(self, *, dry_run: bool = False, max_response_bytes: int = 256_000) -> None:
        self.dry_run = dry_run
        self.max_response_bytes = max_response_bytes

    def run(self, dsl: str, *, timeout_seconds: float) -> Any:
        try:
            body = compile_to_pendo_aggregation(parse(dsl), now_ms=int(time.time() * 1000))
        except Exception as exc:
            raise CodeModeError(f"aggDSL compile failed: {exc}") from exc

        if not isinstance(body.get("request", {}).get("pipeline"), list):
            raise CodeModeError("aggDSL did not compile to a pipeline")
        if body.get("response", {}).get("mimeType") != "application/json":
            raise CodeModeError("Code Mode requires RESPONSE mimeType=application/json")
        if self.dry_run:
            return {"compiled": body, "executed": False}

        key = os.environ.get("PENDO_INTEGRATION_KEY") or os.environ.get("PENDO_API_KEY")
        if not key:
            raise CodeModeError("Set PENDO_INTEGRATION_KEY (or PENDO_API_KEY) on the Python host")

        url = os.environ.get("PENDO_AGG_URL", "https://app.pendo.io/api/v1/aggregation")
        if not url.startswith("https://"):
            raise CodeModeError("PENDO_AGG_URL must use HTTPS")
        payload = json.dumps(body, ensure_ascii=False).encode("utf-8")
        request = urllib.request.Request(
            url,
            data=payload,
            method="POST",
            headers={
                "Content-Type": "application/json",
                "Accept": "application/json",
                "x-pendo-integration-key": key,
            },
        )
        try:
            with urllib.request.urlopen(request, timeout=max(0.1, min(timeout_seconds, 15.0))) as response:
                raw = response.read(self.max_response_bytes + 1)
                if len(raw) > self.max_response_bytes:
                    raise CodeModeError("Pendo response too large; add aggregation/limit to your DSL")
        except urllib.error.HTTPError as exc:
            # Don't include request headers, credentials or a large server error dump.
            detail = exc.read(1000).decode("utf-8", errors="replace")
            raise CodeModeError(f"Pendo API HTTP {exc.code}: {detail}") from exc
        except urllib.error.URLError as exc:
            raise CodeModeError(f"Pendo connection failed: {exc.reason}") from exc
        try:
            return json.loads(raw)
        except (ValueError, UnicodeDecodeError) as exc:
            raise CodeModeError("Pendo did not return a JSON response") from exc


def _reader(stream: Any, out: queue.Queue[str | None]) -> None:
    for line in stream:
        out.put(line)
    out.put(None)


def execute_code(
    code: str,
    *,
    backend: AggregationBackend | None = None,
    limits: Limits | None = None,
    node: str = "node",
) -> dict[str, Any]:
    """Run one JS program, brokering only `pendo.aggdsl.run(dsl)` calls.

    Returns {result, calls, elapsed_ms}. Backend can be a test double.
    Never pass authentication secrets to Node; all requests run in Python.
    """
    limits = limits or Limits()
    if not isinstance(code, str) or not code.strip():
        raise CodeModeError("code must be a non-empty JavaScript string")
    if len(code.encode("utf-8")) > limits.max_code_bytes:
        raise CodeModeError("JavaScript code exceeds size limit")
    backend = backend or PendoBackend(max_response_bytes=limits.max_response_bytes)
    worker = Path(__file__).with_name("codemode_worker.cjs").resolve()
    node_path = shutil.which(node)
    if not node_path:
        raise CodeModeError("Node.js 22.13+ is required for Code Mode")
    if not worker.is_file():
        raise CodeModeError("JavaScript worker not found in installed package")

    command = [
        node_path,
        "--permission",
        f"--allow-fs-read={worker}",
        "--max-old-space-size=64",
        str(worker),
    ]
    start = time.monotonic()
    # No credential environment variables or caller-supplied NODE_OPTIONS in the worker.
    try:
        process = subprocess.Popen(
            command,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            text=True,
            encoding="utf-8",
            bufsize=1,
            env={"NODE_NO_WARNINGS": "1"},
        )
    except OSError as exc:
        raise CodeModeError(f"Could not launch Node.js: {exc}") from exc

    assert process.stdin is not None and process.stdout is not None
    messages: queue.Queue[str | None] = queue.Queue()
    threading.Thread(target=_reader, args=(process.stdout, messages), daemon=True).start()
    calls = 0
    response_bytes = 0

    def send(message: dict[str, Any]) -> None:
        try:
            process.stdin.write(json.dumps(message, ensure_ascii=False) + "\n")
            process.stdin.flush()
        except (OSError, BrokenPipeError) as exc:
            raise CodeModeError("JavaScript worker exited before accepting a response") from exc

    try:
        send({"type": "start", "code": code})
        while True:
            remaining = limits.timeout_seconds - (time.monotonic() - start)
            if remaining <= 0:
                raise CodeModeError("Code Mode execution timed out")
            try:
                line = messages.get(timeout=remaining)
            except queue.Empty as exc:
                raise CodeModeError("Code Mode execution timed out") from exc
            if line is None:
                raise CodeModeError("JavaScript worker terminated without a result")
            if len(line.encode("utf-8")) > limits.max_output_bytes + limits.max_dsl_bytes + 4096:
                raise CodeModeError("JavaScript worker message exceeded limit")
            try:
                message = json.loads(line)
            except ValueError as exc:
                raise CodeModeError("Invalid message from JavaScript worker") from exc
            kind = message.get("type")
            if kind == "call":
                if message.get("method") != "aggdsl.run" or not isinstance(message.get("id"), int):
                    raise CodeModeError("Unsupported JavaScript bridge request")
                dsl = message.get("dsl")
                if not isinstance(dsl, str) or not dsl.strip():
                    raise CodeModeError("aggdsl.run expects a non-empty DSL string")
                if len(dsl.encode("utf-8")) > limits.max_dsl_bytes:
                    raise CodeModeError("aggDSL query exceeds size limit")
                if calls >= limits.max_calls:
                    raise CodeModeError(f"Maximum aggregation calls exceeded ({limits.max_calls})")
                calls += 1
                remaining = limits.timeout_seconds - (time.monotonic() - start)
                if remaining <= 0:
                    raise CodeModeError("Code Mode execution timed out")
                try:
                    value = backend.run(dsl, timeout_seconds=remaining)
                    result_data = json.dumps(value, ensure_ascii=False, allow_nan=False)
                    response_bytes += len(result_data.encode("utf-8"))
                    if response_bytes > limits.max_response_bytes:
                        raise CodeModeError("Combined aggregation responses too large; narrow the query")
                    send({"type": "result", "id": message["id"], "value": value})
                except Exception as exc:
                    # JS may catch a failed request; never turn a failure into fake data.
                    send({"type": "result", "id": message["id"], "error": str(exc)[:1000]})
            elif kind == "done":
                result = message.get("value")
                serialized = json.dumps(result, ensure_ascii=False, allow_nan=False)
                if len(serialized.encode("utf-8")) > limits.max_output_bytes:
                    raise CodeModeError("JavaScript result too large; return a concise summary")
                return {
                    "result": result,
                    "calls": calls,
                    "elapsed_ms": round((time.monotonic() - start) * 1000),
                }
            elif kind == "error":
                raise CodeModeError(f"JavaScript error: {str(message.get('message', 'unknown'))[:1000]}")
            else:
                raise CodeModeError("Unknown message from JavaScript worker")
    finally:
        if process.poll() is None:
            process.terminate()
            try:
                process.wait(timeout=1)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=1)
        process.stdin.close()
        process.stdout.close()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Execute a Pendo aggregation Code Mode JavaScript program")
    parser.add_argument("file", nargs="?", help="JavaScript file; reads standard input if omitted")
    parser.add_argument("--dry-run", action="store_true", help="Compile aggDSL only; no Pendo API calls")
    parser.add_argument("--timeout", type=float, default=20.0, help="Total program timeout in seconds")
    args = parser.parse_args(argv)
    code = Path(args.file).read_text(encoding="utf-8") if args.file else sys.stdin.read()
    try:
        result = execute_code(
            code,
            backend=PendoBackend(dry_run=args.dry_run),
            limits=Limits(timeout_seconds=args.timeout),
        )
    except CodeModeError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2
    json.dump(result, sys.stdout, indent=2, ensure_ascii=False)
    sys.stdout.write("\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
