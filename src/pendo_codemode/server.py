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
from . import stdlib
from .recipes import RecipeStore
from .mcp_wire import HttpTransport, McpClient, McpError, StdioTransport, serve_stdio, text_result

# Listed in full when the upstream has too many tools to list them all. These are the
# read primitives most questions are built from; names absent upstream are skipped.
DEFAULT_CORE = (
    "listPagesFeaturesAndEvents,getEntity,aggregateEntityUsage,entityUsage,appUsage,queryFunnel,"
    "sessionReplayList,listAccounts,listVisitors,visitorActivity,segmentList,listGuides,"
    "aggregateGuideMetrics,guideEffectivenessMetrics"
)


def describe_state(state: dict[str, Any]) -> dict[str, str]:
    """A receipt of what is stored, so the model knows what it can reuse without seeing it."""
    receipt = {}
    for key, value in state.items():
        size = len(json.dumps(value, ensure_ascii=False).encode("utf-8"))
        kind = f"array[{len(value)}]" if isinstance(value, list) else f"object[{len(value)} keys]" if isinstance(value, dict) else type(value).__name__
        receipt[key] = f"{kind}, {size:,} bytes"
    return receipt


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
    core = [name.strip() for name in (args.core or DEFAULT_CORE).split(",") if name.strip()]
    catalog = Catalog(
        tools, allow_writes=args.allow_writes, allow_tools=args.allow_tool, defaults=defaults, core=core
    )
    if args.shapes:
        catalog.load_shapes(args.shapes)
    return catalog


def tool_definitions(catalog: Catalog, limits: Limits, recipes: RecipeStore | None = None) -> list[dict[str, Any]]:
    saved = recipes.index() if recipes else ""
    functions = stdlib.available(set(catalog.tools))
    execute_description = (
        "Run a JavaScript program against Pendo and get back only what it returns. The program is the body of an "
        "async function; use `await`, `Promise.all` for independent calls, and `return` a small JSON answer. "
        "Intermediate data never enters the conversation, so filter, join and aggregate in code. "
        f"Limits: {limits.max_calls} tool calls, {int(limits.timeout_seconds)}s, "
        f"{limits.max_output_bytes // 1000} KB returned. No network, files or imports."
        + (f" Pre-set arguments you may omit: {', '.join(catalog.defaults)}." if catalog.defaults else "")
        + " `state` is an object that persists between your programs: keep ids and intermediate data there "
        "instead of returning them."
        + (
            f" A result over {limits.preview_bytes // 1000} KB comes back as a preview with the full value in "
            "`state.last`; `return full(value)` overrides that."
            if limits.preview_bytes else ""
        )
        + (" `await recipes.<name>(params)` runs a saved program." if recipes else "")
        + (
            "\n\nHigh-level functions. Use these first: each makes the tool calls a common question needs and "
            "returns exactly the shape shown.\n" + "\n".join(f"- {line}" for line in functions)
            if functions else ""
        )
        + "\n\nRaw tools, as `await pendo.<tool>(args)` (? = optional; → shows the result when it has been "
        "observed). For argument details call `search` with the tool "
        "name, or `await pendo.help(name)` inside a program.\n" + catalog.index()
        + (f"\n\nSaved recipes:\n{saved}" if saved else "")
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
                        "enum": ["summary", "compact", "signature", "full"],
                        "description": "summary or compact: one line each; signature (default): arguments and returns; full: everything",
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
                    "save_as": {
                        "type": "object",
                        "description": "Save this program as a recipe if it succeeds. Write it to read its inputs from `params`.",
                        "properties": {"name": {"type": "string"}, "description": {"type": "string"}},
                        "required": ["name", "description"],
                    },
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


def handle_call(
    name: str, arguments: dict[str, Any], *, catalog: Catalog, client: McpClient, limits: Limits,
    shapes_path: str | None = None,
    session: dict[str, Any] | None = None,
    recipes: RecipeStore | None = None,
) -> dict[str, Any]:
    session = session if session is not None else {}
    if name == "search":
        limit = arguments.get("limit")
        limit = limit if isinstance(limit, int) and 0 < limit <= 50 else 8
        detail = arguments.get("detail") if arguments.get("detail") in {"summary", "compact", "full"} else "signature"
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
                state=session.get("state"),
                recipes=recipes.code() if recipes else None,
            )
            session["state"] = output.pop("state")
            if session["state"]:
                output["state"] = describe_state(session["state"])
            save_as = arguments.get("save_as")
            if recipes and isinstance(save_as, dict):
                recipes.save(str(save_as.get("name")), str(save_as.get("description")), arguments.get("code"))
                output["saved"] = f"recipes.{save_as.get('name')}"
            if shapes_path:
                catalog.save_shapes(shapes_path)
            # The per-call trace is for humans at the CLI; the model only needs the answer.
            del output["calls"]
            if not output["logs"]:
                del output["logs"]
            return text_result(output)
        except (CodeModeError, McpError, ValueError) as exc:
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
    parser.add_argument(
        "--shapes", metavar="FILE",
        help="Remember the structure of each tool's results here (key names and types only) and show it in signatures",
    )
    parser.add_argument("--core", metavar="NAMES", help="Comma-separated tools always listed in full on a large server")
    parser.add_argument("--recipes", metavar="DIR", help="Directory of saved programs, callable as recipes.<name>(params)")
    parser.add_argument(
        "--preview-bytes", type=int, default=0,
        help="Results larger than this come back as a preview with the full value in state.last (0 = off)",
    )
    parser.add_argument("--timeout", type=float, default=60.0, help="Total seconds per program")
    parser.add_argument("--max-calls", type=int, default=25, help="Tool calls per program")


def serve_main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="MCP stdio server: Pendo MCP in Code Mode (search + execute)")
    _add_upstream_flags(parser)
    args = parser.parse_args(argv)
    limits = Limits(timeout_seconds=args.timeout, max_calls=args.max_calls, preview_bytes=max(0, args.preview_bytes))
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
    session: dict[str, Any] = {}
    recipes = RecipeStore(args.recipes) if args.recipes else None
    print(f"pendo-codemode: {len(catalog.tools)} tools exposed", file=sys.stderr)
    if catalog.blocked:
        print(f"pendo-codemode: hidden (not read-only): {', '.join(catalog.blocked)}", file=sys.stderr)
    try:
        serve_stdio(
            name="pendo-codemode",
            instructions=INSTRUCTIONS,
            list_tools=lambda: tool_definitions(catalog, limits, recipes),
            call_tool=lambda name, arguments: handle_call(
                name, arguments, catalog=catalog, client=client, limits=limits, shapes_path=args.shapes,
                session=session, recipes=recipes,
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
    tools.add_argument("--detail", choices=["summary", "compact", "signature", "full"], default="signature")
    run.add_argument("--intent", help="One-line task summary forwarded to Pendo as userQuery")
    run.add_argument("--state", metavar="FILE", help="JSON file holding session state between runs")
    bench = sub.add_parser("bench", help="Compare regular MCP calls with Code Mode (read-only)")
    bench.add_argument("files", nargs="+", help="JavaScript programs to measure")
    bench.add_argument("--runs", type=int, default=3, help="Repetitions; the median is reported")
    bench.add_argument("--simulate-latency-ms", type=float, default=0, help="Extra delay per upstream call")
    bench.add_argument(
        "--recorded", action="append", default=[], metavar="DIR",
        help="Replay a recorded regular-MCP session (NN.json files) instead of calling upstream; one per file",
    )
    bench.add_argument("--tokens", action="store_true", help="Exact token counts from the Anthropic API")
    bench.add_argument("--token-model", default="claude-opus-5-5", help="Model whose tokenizer is used with --tokens")
    bench.add_argument("--json", action="store_true", help="Print raw measurements instead of a table")
    generate = sub.add_parser("generate", help="Write the tools as a browsable tree of typed files (servers/pendo/*.ts)")
    generate.add_argument("out", help="Directory to write into")
    for command in (run, tools, bench, generate):
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
        if args.action == "generate":
            from .generate import write_tree

            summary = write_tree(catalog, args.out, recipes=RecipeStore(args.recipes) if args.recipes else None)
            print(json.dumps(summary))
            return 0
        if args.action == "bench":
            from . import bench as benchmark

            if args.allow_writes or args.allow_tool:
                raise CodeModeError("bench is read-only: do not pass --allow-writes or --allow-tool")
            limits = Limits(timeout_seconds=args.timeout, max_calls=args.max_calls, preview_bytes=max(0, args.preview_bytes))
            backend: Any = McpBackend(client)
            if args.simulate_latency_ms:
                backend = benchmark.DelayedBackend(backend, args.simulate_latency_ms)
            counter = benchmark.AnthropicCounter(args.token_model) if args.tokens else None
            if args.recorded:
                if len(args.recorded) != len(args.files):
                    raise CodeModeError("give one --recorded directory per program, in the same order")
                rows = [
                    benchmark.measure_recorded(Path(name), Path(directory), catalog=catalog, limits=limits, counter=counter)
                    for name, directory in zip(args.files, args.recorded)
                ]
                if args.json:
                    json.dump(rows, sys.stdout, indent=2, ensure_ascii=False)
                    sys.stdout.write("\n")
                else:
                    print(benchmark.render_recorded(rows))
                return 0
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
        state_file = Path(args.state) if args.state else None
        result = execute_code(
            code,
            catalog=catalog,
            backend=McpBackend(client),
            limits=Limits(timeout_seconds=args.timeout, max_calls=args.max_calls, preview_bytes=max(0, args.preview_bytes)),
            intent=args.intent,
            state=json.loads(state_file.read_text(encoding="utf-8")) if state_file and state_file.exists() else None,
            recipes=RecipeStore(args.recipes).code() if args.recipes else None,
        )
        if state_file:
            state_file.write_text(json.dumps(result["state"], ensure_ascii=False), encoding="utf-8")
        result["state"] = describe_state(result["state"])
        if args.shapes:
            catalog.save_shapes(args.shapes)
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
