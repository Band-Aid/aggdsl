"""Code Mode front end for the Pendo MCP server.

Connects to the upstream Pendo MCP server as a client and serves two tools:
`search` (find tools, get TypeScript signatures) and `execute` (run JavaScript
that calls them). The model sees two tool definitions instead of the whole
upstream tool list, and intermediate results stay out of its context.
"""
from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import shlex
import sys
from typing import Any

from .catalog import Catalog, Tool
from .executor import CodeModeError, Limits, McpBackend, execute_code
from .mcp_wire import HttpTransport, McpClient, McpError, StdioTransport, serve_stdio, text_result

INSTRUCTIONS = (
    "Pendo in Code Mode. Call `search` to get the signatures of the Pendo tools you need, then "
    "`execute` ONE JavaScript program that calls them, joins the results and returns only the answer."
)


def connect(args: argparse.Namespace) -> McpClient:
    """Open the upstream MCP connection described by flags or environment."""
    if args.mock:
        env = {**os.environ, "PYTHONPATH": str(Path(__file__).resolve().parent.parent)}
        transport: Any = StdioTransport([sys.executable, "-m", "pendo_codemode.mock_pendo"], env=env)
    elif args.command or os.environ.get("PENDO_MCP_COMMAND"):
        transport = StdioTransport(shlex.split(args.command or os.environ["PENDO_MCP_COMMAND"]))
    elif args.url or os.environ.get("PENDO_MCP_URL"):
        headers = {}
        # The credential is read from the environment only, never from argv.
        if os.environ.get("PENDO_MCP_TOKEN"):
            headers["Authorization"] = f"Bearer {os.environ['PENDO_MCP_TOKEN']}"
        transport = HttpTransport(args.url or os.environ["PENDO_MCP_URL"], headers=headers)
    else:
        raise McpError("No upstream: set PENDO_MCP_URL or PENDO_MCP_COMMAND, or pass --url, --command or --mock")
    client = McpClient(transport)
    try:
        client.initialize()
    except McpError:
        client.close()
        raise
    return client


def build_catalog(client: McpClient, args: argparse.Namespace) -> Catalog:
    tools = [Tool.from_mcp(raw) for raw in client.list_tools()]
    return Catalog(tools, allow_writes=args.allow_writes, allow_tools=args.allow_tool)


def tool_definitions(catalog: Catalog, limits: Limits) -> list[dict[str, Any]]:
    execute_description = (
        "Run a JavaScript program against Pendo. Each Pendo tool is an async function: "
        "`await pendo.<tool>(args)` takes the tool's arguments object and resolves to its parsed JSON "
        "result (it rejects if the tool fails). Chain calls, use Promise.all for independent calls, "
        "filter/join/aggregate in code, and `return` only the small JSON the answer needs: intermediate "
        "results never enter the conversation. console.log output is returned as `logs`. "
        f"Limits: {limits.max_calls} tool calls, {int(limits.timeout_seconds)}s, "
        f"{limits.max_output_bytes // 1000} KB returned. No network, files or imports. "
        "Use `search` first if you do not know a tool's arguments.\n\nTools:\n" + catalog.index()
    )
    return [
        {
            "name": "search",
            "description": (
                "Find Pendo tools by keyword and get their TypeScript signatures for use in `execute`. "
                "An empty query lists the first tools."
            ),
            "inputSchema": {
                "type": "object",
                "properties": {
                    "query": {"type": "string", "description": "Keywords or an exact tool name"},
                    "limit": {"type": "integer", "description": "Maximum tools to return (default 8)"},
                },
            },
            "annotations": {"readOnlyHint": True},
        },
        {
            "name": "execute",
            "description": execute_description,
            "inputSchema": {
                "type": "object",
                "properties": {
                    "code": {"type": "string", "description": "Body of an async function; use `return`."}
                },
                "required": ["code"],
            },
            # Read-only exactly when no write tool was let into the catalog.
            "annotations": {"readOnlyHint": all(t.read_only for t in catalog.tools.values())},
        },
    ]


def handle_call(name: str, arguments: dict[str, Any], *, catalog: Catalog, client: McpClient, limits: Limits) -> dict[str, Any]:
    if name == "search":
        limit = arguments.get("limit")
        limit = limit if isinstance(limit, int) and 0 < limit <= 50 else 8
        return text_result(catalog.describe(str(arguments.get("query") or ""), limit))
    if name == "execute":
        try:
            output = execute_code(arguments.get("code"), catalog=catalog, backend=McpBackend(client), limits=limits)
            # The per-call trace is for humans at the CLI; the model only needs the answer.
            del output["calls"]
            if not output["logs"]:
                del output["logs"]
            return text_result(output)
        except (CodeModeError, McpError) as exc:
            return text_result(str(exc), is_error=True)
    return text_result(f"Unknown tool: {name}", is_error=True)


def _add_upstream_flags(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--url", help="Pendo MCP Streamable HTTP URL (or PENDO_MCP_URL; token in PENDO_MCP_TOKEN)")
    parser.add_argument("--command", help="Command that runs an MCP stdio server or bridge (or PENDO_MCP_COMMAND)")
    parser.add_argument("--mock", action="store_true", help="Use the bundled synthetic Pendo MCP server")
    parser.add_argument("--allow-writes", action="store_true", help="Also expose tools not marked read-only")
    parser.add_argument("--allow-tool", action="append", default=[], metavar="NAME", help="Expose one non-read-only tool")
    parser.add_argument("--timeout", type=float, default=60.0, help="Total seconds per program")
    parser.add_argument("--max-calls", type=int, default=25, help="Tool calls per program")


def serve_main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="MCP stdio server: Pendo MCP in Code Mode (search + execute)")
    _add_upstream_flags(parser)
    args = parser.parse_args(argv)
    limits = Limits(timeout_seconds=args.timeout, max_calls=args.max_calls)
    try:
        client = connect(args)
        catalog = build_catalog(client, args)
    except McpError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2
    print(f"pendo-codemode: {len(catalog.tools)} tools exposed", file=sys.stderr)
    if catalog.blocked:
        print(f"pendo-codemode: hidden (not read-only): {', '.join(catalog.blocked)}", file=sys.stderr)
    try:
        serve_stdio(
            name="pendo-codemode",
            instructions=INSTRUCTIONS,
            list_tools=lambda: tool_definitions(catalog, limits),
            call_tool=lambda name, arguments: handle_call(
                name, arguments, catalog=catalog, client=client, limits=limits
            ),
        )
    finally:
        client.close()
    return 0


def cli_main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run or inspect Pendo Code Mode from the shell")
    sub = parser.add_subparsers(dest="action", required=True)
    run = sub.add_parser("run", help="Execute a JavaScript program")
    run.add_argument("file", nargs="?", help="JavaScript file; reads standard input if omitted")
    tools = sub.add_parser("tools", help="Print tool signatures as the model would see them")
    tools.add_argument("query", nargs="?", default="")
    for command in (run, tools):
        _add_upstream_flags(command)
    args = parser.parse_args(argv)
    try:
        client = connect(args)
    except McpError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2
    try:
        catalog = build_catalog(client, args)
        if args.action == "tools":
            print(catalog.describe(args.query, limit=50 if not args.query else 8))
            if catalog.blocked:
                print(f"\n// hidden (not read-only): {', '.join(catalog.blocked)}")
            return 0
        code = Path(args.file).read_text(encoding="utf-8") if args.file else sys.stdin.read()
        result = execute_code(
            code,
            catalog=catalog,
            backend=McpBackend(client),
            limits=Limits(timeout_seconds=args.timeout, max_calls=args.max_calls),
        )
    except (CodeModeError, McpError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2
    finally:
        client.close()
    json.dump(result, sys.stdout, indent=2, ensure_ascii=False)
    sys.stdout.write("\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(serve_main())
