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
    defaults = {}
    for item in args.default:
        key, separator, value = item.partition("=")
        if not separator or not key:
            raise McpError(f"--default expects NAME=VALUE, got {item!r}")
        defaults[key] = value
    if args.mock:
        defaults.setdefault("subId", "mock-sub")
    return Catalog(tools, allow_writes=args.allow_writes, allow_tools=args.allow_tool, defaults=defaults)


def tool_definitions(catalog: Catalog, limits: Limits) -> list[dict[str, Any]]:
    execute_description = (
        "Run a JavaScript program against Pendo. Each Pendo tool is an async function: "
        "`await pendo.<tool>(args)` takes the tool's arguments object and resolves to its parsed JSON "
        "result (it rejects if the tool fails). Chain calls, use Promise.all for independent calls, "
        "filter/join/aggregate in code, and `return` only the small JSON the answer needs: intermediate "
        "results never enter the conversation. console.log output is returned as `logs`. "
        f"Limits: {limits.max_calls} tool calls, {int(limits.timeout_seconds)}s, "
        f"{limits.max_output_bytes // 1000} KB returned. No network, files or imports. "
        "ALWAYS call `search` for a tool's arguments and result shape before using it; never guess them."
        + (f" Pre-set arguments you may omit: {', '.join(catalog.defaults)}." if catalog.defaults else "")
        + "\n\nTools:\n" + catalog.index()
    )
    return [
        {
            "name": "search",
            "description": (
                "Find Pendo tools by keyword and get their TypeScript signatures, including what each "
                "returns, for use in `execute`. Pass an exact tool name with detail='full' for its complete "
                "documentation."
            ),
            "inputSchema": {
                "type": "object",
                "properties": {
                    "query": {"type": "string", "description": "Keywords or an exact tool name"},
                    "limit": {"type": "integer", "description": "Maximum tools to return (default 8)"},
                    "detail": {
                        "type": "string",
                        "enum": ["summary", "signature", "full"],
                        "description": "summary: one line each; signature (default): arguments and returns; full: everything",
                    },
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
                    "code": {"type": "string", "description": "Body of an async function; use `return`."},
                    "intent": {
                        "type": "string",
                        "description": "The task in under 20 words; passed to Pendo with each call for its usage analytics.",
                    },
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
        detail = arguments.get("detail") if arguments.get("detail") in {"summary", "full"} else "signature"
        return text_result(catalog.describe(str(arguments.get("query") or ""), limit, detail))
    if name == "execute":
        try:
            intent = arguments.get("intent")
            output = execute_code(
                arguments.get("code"),
                catalog=catalog,
                backend=McpBackend(client),
                limits=limits,
                intent=intent if isinstance(intent, str) else None,
            )
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
    parser.add_argument(
        "--allow-tool", action="append", default=[], metavar="NAME", help="Expose non-read-only tools by name or glob"
    )
    parser.add_argument(
        "--default", action="append", default=[], metavar="NAME=VALUE", help="Pre-set an argument, e.g. subId=123"
    )
    parser.add_argument("--timeout", type=float, default=60.0, help="Total seconds per program")
    parser.add_argument("--max-calls", type=int, default=25, help="Tool calls per program")


def serve_main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="MCP stdio server: Pendo MCP in Code Mode (search + execute)")
    _add_upstream_flags(parser)
    args = parser.parse_args(argv)
    limits = Limits(timeout_seconds=args.timeout, max_calls=args.max_calls)
    try:
        client = connect(args)
    except McpError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2
    try:
        catalog = build_catalog(client, args)
    except McpError as exc:
        client.close()
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
    tools.add_argument("--detail", choices=["summary", "signature", "full"], default="signature")
    run.add_argument("--intent", help="One-line task summary forwarded to Pendo as userQuery")
    bench = sub.add_parser("bench", help="Compare regular MCP calls with Code Mode (read-only)")
    bench.add_argument("files", nargs="+", help="JavaScript programs to measure")
    bench.add_argument("--runs", type=int, default=3, help="Repetitions; the median is reported")
    bench.add_argument("--simulate-latency-ms", type=float, default=0, help="Extra delay per upstream call")
    bench.add_argument("--tokens", action="store_true", help="Exact token counts from the Anthropic API")
    bench.add_argument("--token-model", default="claude-opus-5-5", help="Model whose tokenizer is used with --tokens")
    bench.add_argument("--json", action="store_true", help="Print raw measurements instead of a table")
    for command in (run, tools, bench):
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
            print(catalog.describe(args.query, 200 if not args.query else 8, args.detail))
            if catalog.blocked:
                print(f"\n// hidden (not read-only): {', '.join(catalog.blocked)}")
            return 0
        if args.action == "bench":
            from . import bench as benchmark

            if args.allow_writes or args.allow_tool:
                raise CodeModeError("bench is read-only: do not pass --allow-writes or --allow-tool")
            limits = Limits(timeout_seconds=args.timeout, max_calls=args.max_calls)
            backend: Any = McpBackend(client)
            if args.simulate_latency_ms:
                backend = benchmark.DelayedBackend(backend, args.simulate_latency_ms)
            counter = benchmark.AnthropicCounter(args.token_model) if args.tokens else None
            rows = [
                benchmark.measure(
                    Path(name),
                    catalog=catalog,
                    backend=backend,
                    raw_tools=[raw for raw in client.list_tools() if raw.get("name") in catalog.tools],
                    codemode_tools=tool_definitions(catalog, limits),
                    limits=limits,
                    runs=max(1, args.runs),
                    counter=counter,
                )
                for name in args.files
            ]
            if args.json:
                json.dump(rows, sys.stdout, indent=2)
                sys.stdout.write("\n")
            else:
                upstream = "mock" if args.mock else client.server_info.get("name") or "MCP server"
                print(benchmark.render(rows, upstream=upstream, tool_count=len(catalog.tools),
                                       runs=max(1, args.runs), delay_ms=args.simulate_latency_ms))
            return 0
        code = Path(args.file).read_text(encoding="utf-8") if args.file else sys.stdin.read()
        result = execute_code(
            code,
            catalog=catalog,
            backend=McpBackend(client),
            limits=Limits(timeout_seconds=args.timeout, max_calls=args.max_calls),
            intent=args.intent,
        )
    except (CodeModeError, McpError, OSError, RuntimeError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2
    finally:
        client.close()
    json.dump(result, sys.stdout, indent=2, ensure_ascii=False)
    sys.stdout.write("\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(serve_main())
