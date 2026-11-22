"""Minimal MCP (JSON-RPC 2.0) client and stdio server, standard library only.

Only what Code Mode needs: initialize, tools/list and tools/call. The official
SDK is deliberately not used; its v1 and v2 APIs are incompatible and this
keeps the package dependency-free like the rest of the repository.
"""
from __future__ import annotations

import json
import os
import subprocess
import sys
import threading
from typing import Any, Callable
import urllib.error
import urllib.parse
import urllib.request

PROTOCOL_VERSION = "2025-06-18"
CLIENT_INFO = {"name": "pendo-codemode", "version": "0.1.0"}


class McpError(RuntimeError):
    """Transport failure or JSON-RPC error from the peer."""


def _rpc_result(message: dict[str, Any]) -> Any:
    if "error" in message:
        error = message["error"] or {}
        raise McpError(f"MCP error {error.get('code')}: {str(error.get('message'))[:1000]}")
    return message.get("result")


class StdioTransport:
    """Talk to an MCP server subprocess over newline-delimited JSON."""

    def __init__(self, command: list[str], *, env: dict[str, str] | None = None) -> None:
        try:
            self._process = subprocess.Popen(
                command,
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=subprocess.DEVNULL,
                text=True,
                encoding="utf-8",
                bufsize=1,
                env=env,
            )
        except OSError as exc:
            raise McpError(f"Could not launch MCP server {command[0]!r}: {exc}") from exc
        self._write_lock = threading.Lock()
        self._pending: dict[int, tuple[threading.Event, list[Any]]] = {}
        self._pending_lock = threading.Lock()
        self._closed = False
        threading.Thread(target=self._read_loop, daemon=True).start()

    def _read_loop(self) -> None:
        assert self._process.stdout is not None
        for line in self._process.stdout:
            try:
                message = json.loads(line)
            except ValueError:
                continue
            if not isinstance(message, dict) or "method" in message:
                continue  # server-initiated requests/notifications are not needed
            with self._pending_lock:
                waiter = self._pending.pop(message.get("id"), None)
            if waiter:
                waiter[1].append(message)
                waiter[0].set()
        self._closed = True
        with self._pending_lock:
            waiters, self._pending = list(self._pending.values()), {}
        for event, _slot in waiters:
            event.set()

    def _write(self, message: dict[str, Any]) -> None:
        assert self._process.stdin is not None
        try:
            with self._write_lock:
                self._process.stdin.write(json.dumps(message, ensure_ascii=False) + "\n")
                self._process.stdin.flush()
        except (OSError, ValueError) as exc:
            raise McpError("MCP server process is not accepting input") from exc

    def request(self, message: dict[str, Any], timeout: float) -> Any:
        event, slot = threading.Event(), []
        with self._pending_lock:
            if self._closed:
                raise McpError("MCP server process exited")
            self._pending[message["id"]] = (event, slot)
        self._write(message)
        if not event.wait(timeout):
            with self._pending_lock:
                self._pending.pop(message["id"], None)
            raise McpError(f"MCP request {message['method']} timed out")
        if not slot:
            raise McpError("MCP server process exited")
        return _rpc_result(slot[0])

    def notify(self, message: dict[str, Any]) -> None:
        self._write(message)

    def close(self) -> None:
        process = self._process
        for stream in (process.stdin, process.stdout):
            try:
                if stream:
                    stream.close()
            except OSError:
                pass
        if process.poll() is None:
            process.terminate()
            try:
                process.wait(timeout=2)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=2)


class HttpTransport:
    """MCP Streamable HTTP: one POST per message, JSON or SSE response."""

    def __init__(self, url: str, *, headers: dict[str, str] | None = None) -> None:
        parsed = urllib.parse.urlparse(url)
        local = parsed.hostname in {"localhost", "127.0.0.1", "::1"}
        if parsed.scheme != "https" and not (parsed.scheme == "http" and local):
            raise McpError("MCP URL must use HTTPS (plain HTTP is allowed for localhost only)")
        self._url = url
        self._headers = dict(headers or {})
        self._session_id: str | None = None
        self.protocol_version: str | None = None

    def _post(self, message: dict[str, Any], timeout: float) -> Any:
        headers = {
            "Content-Type": "application/json",
            "Accept": "application/json, text/event-stream",
            **self._headers,
        }
        if self._session_id:
            headers["Mcp-Session-Id"] = self._session_id
        if self.protocol_version:
            headers["MCP-Protocol-Version"] = self.protocol_version
        request = urllib.request.Request(
            self._url, data=json.dumps(message).encode("utf-8"), method="POST", headers=headers
        )
        try:
            with urllib.request.urlopen(request, timeout=max(0.1, timeout)) as response:
                session_id = response.headers.get("Mcp-Session-Id")
                if session_id:
                    self._session_id = session_id
                if "id" not in message:
                    return None
                if "text/event-stream" in (response.headers.get("Content-Type") or ""):
                    return self._read_sse(response, message["id"])
                return json.loads(response.read())
        except urllib.error.HTTPError as exc:
            # Never echo request headers: they carry the credential.
            detail = exc.read(500).decode("utf-8", errors="replace")
            raise McpError(f"MCP server HTTP {exc.code}: {detail}") from exc
        except (urllib.error.URLError, TimeoutError, OSError) as exc:
            raise McpError(f"MCP connection failed: {getattr(exc, 'reason', exc)}") from exc
        except ValueError as exc:
            raise McpError("MCP server returned invalid JSON") from exc

    @staticmethod
    def _read_sse(response: Any, request_id: int) -> dict[str, Any]:
        data: list[str] = []
        for raw in response:
            line = raw.decode("utf-8").rstrip("\r\n")
            if line.startswith("data:"):
                data.append(line[5:].lstrip(" "))
            elif not line and data:
                payload, data = "\n".join(data), []
                try:
                    message = json.loads(payload)
                except ValueError:
                    continue
                if isinstance(message, dict) and message.get("id") == request_id and "method" not in message:
                    return message
        raise McpError("MCP event stream ended without a response")

    def request(self, message: dict[str, Any], timeout: float) -> Any:
        return _rpc_result(self._post(message, timeout))

    def notify(self, message: dict[str, Any]) -> None:
        self._post(message, 30.0)

    def close(self) -> None:
        pass


class McpClient:
    """Thread-safe MCP client; `call_tool` may be used from several threads."""

    def __init__(self, transport: StdioTransport | HttpTransport) -> None:
        self._transport = transport
        self._id_lock = threading.Lock()
        self._next_id = 0
        self.server_info: dict[str, Any] = {}

    def _request(self, method: str, params: dict[str, Any] | None, timeout: float) -> Any:
        with self._id_lock:
            self._next_id += 1
            request_id = self._next_id
        message: dict[str, Any] = {"jsonrpc": "2.0", "id": request_id, "method": method}
        if params is not None:
            message["params"] = params
        return self._transport.request(message, timeout)

    def initialize(self, timeout: float = 30.0) -> None:
        result = self._request(
            "initialize",
            {"protocolVersion": PROTOCOL_VERSION, "capabilities": {}, "clientInfo": CLIENT_INFO},
            timeout,
        ) or {}
        self.server_info = result.get("serverInfo") or {}
        if isinstance(self._transport, HttpTransport):
            self._transport.protocol_version = result.get("protocolVersion") or PROTOCOL_VERSION
        self._transport.notify({"jsonrpc": "2.0", "method": "notifications/initialized"})

    def list_tools(self, timeout: float = 30.0) -> list[dict[str, Any]]:
        tools: list[dict[str, Any]] = []
        cursor: str | None = None
        for _page in range(100):
            result = self._request("tools/list", {"cursor": cursor} if cursor else {}, timeout) or {}
            tools.extend(t for t in result.get("tools") or [] if isinstance(t, dict))
            cursor = result.get("nextCursor")
            if not cursor:
                break
        return tools

    def call_tool(self, name: str, arguments: dict[str, Any], timeout: float = 60.0) -> dict[str, Any]:
        result = self._request("tools/call", {"name": name, "arguments": arguments}, timeout)
        if not isinstance(result, dict):
            raise McpError(f"Tool {name} returned no result")
        return result

    def close(self) -> None:
        self._transport.close()


def unwrap_tool_result(result: dict[str, Any]) -> tuple[bool, Any]:
    """Turn an MCP CallToolResult into (ok, plain JSON value).

    Prefers structuredContent; otherwise joins the text blocks and parses them
    as JSON when possible, so JavaScript receives data rather than an envelope.
    """
    texts = [
        block.get("text", "")
        for block in result.get("content") or []
        if isinstance(block, dict) and block.get("type") == "text"
    ]
    text = "\n".join(texts)
    if result.get("isError"):
        return False, text or "tool reported an error"
    if result.get("structuredContent") is not None:
        return True, result["structuredContent"]
    try:
        return True, json.loads(text)
    except ValueError:
        return True, text


def text_result(value: Any, *, is_error: bool = False) -> dict[str, Any]:
    """Build a CallToolResult carrying `value` as both text and structured JSON."""
    text = value if isinstance(value, str) else json.dumps(value, ensure_ascii=False)
    result: dict[str, Any] = {"content": [{"type": "text", "text": text}], "isError": is_error}
    if isinstance(value, dict) and not is_error:
        result["structuredContent"] = value
    return result


def serve_stdio(
    *,
    name: str,
    list_tools: Callable[[], list[dict[str, Any]]],
    call_tool: Callable[[str, dict[str, Any]], dict[str, Any]],
    instructions: str | None = None,
) -> None:
    """Serve tools over stdio until stdin closes. Requests run one at a time."""
    # Keep stdout for protocol frames only; anything printed by handlers goes to stderr.
    out = os.fdopen(os.dup(sys.stdout.fileno()), "w", encoding="utf-8", buffering=1)
    sys.stdout = sys.stderr
    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue
        try:
            message = json.loads(line)
        except ValueError:
            continue
        if not isinstance(message, dict) or "id" not in message or "method" not in message:
            continue  # notifications and responses need no reply
        method, params = message["method"], message.get("params") or {}
        reply: dict[str, Any] = {"jsonrpc": "2.0", "id": message["id"]}
        try:
            if method == "initialize":
                result: dict[str, Any] = {
                    "protocolVersion": PROTOCOL_VERSION,
                    "capabilities": {"tools": {}},
                    "serverInfo": {"name": name, "version": CLIENT_INFO["version"]},
                }
                if instructions:
                    result["instructions"] = instructions
                reply["result"] = result
            elif method == "ping":
                reply["result"] = {}
            elif method == "tools/list":
                reply["result"] = {"tools": list_tools()}
            elif method == "tools/call":
                arguments = params.get("arguments")
                reply["result"] = call_tool(str(params.get("name")), arguments if isinstance(arguments, dict) else {})
            else:
                reply["error"] = {"code": -32601, "message": f"Method not found: {method}"}
        except Exception as exc:  # a handler bug must not take the server down
            reply.pop("result", None)
            reply["error"] = {"code": -32603, "message": str(exc)[:1000]}
        out.write(json.dumps(reply, ensure_ascii=False) + "\n")
