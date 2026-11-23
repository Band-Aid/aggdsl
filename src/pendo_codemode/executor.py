"""Run one JavaScript program whose `pendo.<tool>()` calls go to the Pendo MCP server.

The Node worker never holds the MCP connection or its credentials: every call
crosses a JSON-lines bridge and is checked against the catalog here first.
This is a trusted-local prototype, not a hardened multi-tenant sandbox.
"""
from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
import json
from pathlib import Path
import queue
import shutil
import subprocess
import threading
import time
from typing import Any, Protocol

from .catalog import Catalog
from .mcp_wire import McpClient, unwrap_tool_result


class CodeModeError(RuntimeError):
    """An invalid program, execution failure, or resource limit."""


@dataclass(frozen=True)
class Limits:
    timeout_seconds: float = 60.0
    max_calls: int = 25
    max_parallel: int = 8
    max_code_bytes: int = 20_000
    max_call_result_bytes: int = 1_000_000  # one tool result handed to JavaScript
    max_upstream_bytes: int = 4_000_000  # all tool results of one program
    max_output_bytes: int = 32_000  # what goes back to the model


class ToolBackend(Protocol):
    def call_tool(self, name: str, arguments: dict[str, Any], *, timeout: float) -> Any:
        """Return the tool's plain JSON value, or raise on failure."""


class McpBackend:
    """Forward calls to an upstream MCP server and unwrap the result envelope."""

    def __init__(self, client: McpClient) -> None:
        self._client = client

    def call_tool(self, name: str, arguments: dict[str, Any], *, timeout: float) -> Any:
        ok, value = unwrap_tool_result(self._client.call_tool(name, arguments, timeout=timeout))
        if not ok:
            raise CodeModeError(str(value))
        return value


def _reader(stream: Any, out: "queue.Queue[str | None]") -> None:
    try:
        for line in stream:
            out.put(line)
    except (OSError, ValueError):
        pass
    out.put(None)


def execute_code(
    code: str,
    *,
    catalog: Catalog,
    backend: ToolBackend,
    limits: Limits | None = None,
    intent: str | None = None,
    node: str = "node",
) -> dict[str, Any]:
    """Run `code` and return {result, logs, calls, stats}.

    `intent` is a one-line task summary; it is forwarded to tools that accept
    a `userQuery` argument, and the catalog's pre-set defaults are filled in.

    `calls` lists each tool call with its duration and result size, so the
    caller can see how much data stayed out of the model's context.
    """
    limits = limits or Limits()
    if not isinstance(code, str) or not code.strip():
        raise CodeModeError("code must be a non-empty JavaScript string")
    if len(code.encode("utf-8")) > limits.max_code_bytes:
        raise CodeModeError("JavaScript code exceeds size limit")
    worker = Path(__file__).with_name("worker.cjs").resolve()
    node_path = shutil.which(node)
    if not node_path:
        raise CodeModeError("Node.js 22.13+ is required for Code Mode")
    if not worker.is_file():
        raise CodeModeError("JavaScript worker not found in installed package")

    command = [node_path, "--permission", f"--allow-fs-read={worker}", "--max-old-space-size=128", str(worker)]
    start = time.monotonic()
    try:
        # A minimal environment: no credentials and no caller-supplied NODE_OPTIONS.
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
    messages: "queue.Queue[str | None]" = queue.Queue()
    threading.Thread(target=_reader, args=(process.stdout, messages), daemon=True).start()
    pool = ThreadPoolExecutor(max_workers=max(1, limits.max_parallel), thread_name_prefix="pendo-call")
    send_lock = threading.Lock()
    stats_lock = threading.Lock()
    trace: list[dict[str, Any]] = []
    upstream_bytes = 0

    def send(message: dict[str, Any]) -> None:
        try:
            with send_lock:
                process.stdin.write(json.dumps(message, ensure_ascii=False) + "\n")
                process.stdin.flush()
        except (OSError, ValueError) as exc:
            raise CodeModeError("JavaScript worker exited before accepting a response") from exc

    def remaining() -> float:
        return limits.timeout_seconds - (time.monotonic() - start)

    def run_call(call_id: int, tool: str, args: dict[str, Any], round_: int) -> None:
        nonlocal upstream_bytes
        began = time.monotonic()
        args = catalog.bind(tool, args, intent)
        # `round` groups calls the program issued together; `args` are as sent upstream.
        entry: dict[str, Any] = {"tool": tool, "ok": False, "ms": 0, "bytes": 0, "round": round_, "args": args}
        with stats_lock:
            trace.append(entry)
        reply: dict[str, Any] = {"type": "result", "id": call_id}
        try:
            value = backend.call_tool(tool, args, timeout=max(0.1, remaining()))
            size = len(json.dumps(value, ensure_ascii=False, allow_nan=False).encode("utf-8"))
            entry["bytes"] = size
            if size > limits.max_call_result_bytes:
                raise CodeModeError(f"{tool} result too large ({size} bytes); narrow the query")
            with stats_lock:
                upstream_bytes += size
                over = upstream_bytes > limits.max_upstream_bytes
            if over:
                raise CodeModeError("Combined tool results too large; narrow the queries")
            entry["ok"] = True
            catalog.observe(tool, value)
            reply["value"] = value
        except Exception as exc:
            # JavaScript may catch a failed call; a failure never becomes fake data.
            reply["error"] = f"{tool} failed: {str(exc)[:1000]}"
        entry["ms"] = round((time.monotonic() - began) * 1000)
        try:
            send(reply)
        except CodeModeError:
            pass  # the program already ended

    try:
        send({"type": "start", "code": code, "tools": catalog.names()})
        calls = 0
        rounds: dict[Any, int] = {}
        while True:
            if remaining() <= 0:
                raise CodeModeError("Code Mode execution timed out")
            try:
                line = messages.get(timeout=remaining())
            except queue.Empty as exc:
                raise CodeModeError("Code Mode execution timed out") from exc
            if line is None:
                raise CodeModeError("JavaScript worker terminated without a result")
            if len(line) > 4 * limits.max_output_bytes + 200_000:
                raise CodeModeError("JavaScript worker message exceeded limit")
            try:
                message = json.loads(line)
            except ValueError as exc:
                raise CodeModeError("Invalid message from JavaScript worker") from exc
            kind = message.get("type")
            if kind == "call":
                tool, args, call_id = message.get("tool"), message.get("args"), message.get("id")
                if not isinstance(call_id, int) or not isinstance(tool, str) or not isinstance(args, dict):
                    raise CodeModeError("Malformed JavaScript bridge request")
                if tool == "$help":
                    # Documentation from inside a program; free, and never reaches Pendo.
                    topic = str(args.get("topic") or "")
                    send({"type": "result", "id": call_id, "value": catalog.describe(topic, 4) if topic else catalog.index()})
                    continue
                if tool not in catalog.tools:
                    # Checked here, not only in the worker: the worker is not trusted.
                    send({"type": "result", "id": call_id, "error": f"Tool {tool} is not available in Code Mode"})
                    continue
                if calls >= limits.max_calls:
                    raise CodeModeError(f"Maximum tool calls exceeded ({limits.max_calls})")
                calls += 1
                pool.submit(run_call, call_id, tool, args, rounds.setdefault(message.get("after"), len(rounds) + 1))
            elif kind == "done":
                result = message.get("value")
                serialized = json.dumps(result, ensure_ascii=False, allow_nan=False)
                output_bytes = len(serialized.encode("utf-8"))
                if output_bytes > limits.max_output_bytes:
                    raise CodeModeError(
                        f"JavaScript result too large ({output_bytes} bytes, limit "
                        f"{limits.max_output_bytes}); aggregate in code and return a concise summary"
                    )
                with stats_lock:
                    finished = [dict(entry) for entry in trace]
                    total = upstream_bytes
                return {
                    "result": result,
                    "logs": [str(entry) for entry in message.get("logs") or []],
                    "calls": finished,
                    "stats": {
                        "tool_calls": calls,
                        "rounds": len(rounds),
                        "upstream_bytes": total,
                        "output_bytes": output_bytes,
                        "elapsed_ms": round((time.monotonic() - start) * 1000),
                    },
                }
            elif kind == "error":
                logs = [str(entry) for entry in message.get("logs") or []]
                detail = f" (console: {' | '.join(logs)[:500]})" if logs else ""
                raise CodeModeError(f"JavaScript error: {str(message.get('message', 'unknown'))[:1000]}{detail}")
            else:
                raise CodeModeError("Unknown message from JavaScript worker")
    finally:
        pool.shutdown(wait=False, cancel_futures=True)
        if process.poll() is None:
            process.terminate()
            try:
                process.wait(timeout=1)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=1)
        for stream in (process.stdin, process.stdout):
            try:
                stream.close()
            except OSError:
                pass
