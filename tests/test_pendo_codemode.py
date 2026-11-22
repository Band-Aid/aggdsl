from __future__ import annotations

from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import os
from pathlib import Path
import shutil
import sys
import threading
import time

import pytest

from pendo_codemode import mock_pendo
from pendo_codemode.catalog import Catalog, Tool, ts_type
from pendo_codemode.executor import CodeModeError, Limits, execute_code
from pendo_codemode.mcp_wire import HttpTransport, McpClient, McpError, StdioTransport, unwrap_tool_result

SRC = str(Path(__file__).resolve().parent.parent / "src")
needs_node = pytest.mark.skipif(not shutil.which("node"), reason="Node.js is required")


def mock_catalog(**policy) -> Catalog:
    policy.setdefault("defaults", {"subId": "mock-sub"})
    return Catalog([Tool.from_mcp(spec) for spec in mock_pendo.list_tools()], **policy)


class MockBackend:
    """Calls the mock Pendo handlers in-process and records what was asked."""

    def __init__(self, delay: float = 0.0) -> None:
        self.calls: list[tuple[str, dict]] = []
        self.delay = delay

    def call_tool(self, name, arguments, *, timeout):
        self.calls.append((name, arguments))
        time.sleep(self.delay)
        ok, value = unwrap_tool_result(mock_pendo.call_tool(name, arguments))
        if not ok:
            raise CodeModeError(value)
        return value


def run(code: str, **kwargs):
    kwargs.setdefault("catalog", mock_catalog())
    kwargs.setdefault("backend", MockBackend())
    return execute_code(code, **kwargs)


# --- catalog -----------------------------------------------------------------


def test_ts_type_covers_common_json_schema():
    assert ts_type({"type": "array", "items": {"type": "string", "enum": ["a", "b"]}}) == '("a" | "b")[]'
    assert ts_type({"type": ["string", "null"]}) == "string | null"
    assert ts_type({"anyOf": [{"type": "integer"}, {"type": "number"}]}) == "number"
    assert (
        ts_type({"type": "object", "properties": {"id": {"type": "string"}, "n": {}}, "required": ["id"]})
        == "{ id: string; n?: unknown }"
    )
    assert ts_type({"$ref": "#/defs/x"}) == "unknown"


def test_signature_keeps_returns_and_hides_telemetry_argument():
    catalog = mock_catalog()
    signature = catalog.signature(catalog.tools["entityUsage"])
    assert "pendo.entityUsage(args: {" in signature
    assert "  entityId: string; // The Pendo page, feature, or track event ID." in signature
    assert '  groupBy?: "visitor" | "account";' in signature
    assert " * RETURNS: - summary: totalNumEvents" in signature
    assert "USE FOR" not in signature and "userQuery" not in signature
    assert "USE FOR: 'How many people used it'" in catalog.signature(catalog.tools["entityUsage"], "full")
    odd = Catalog([Tool.from_mcp({"name": "get-thing", "annotations": {"readOnlyHint": True},
                                  "inputSchema": {"type": "object", "properties": {"a-b": {}}}})])
    assert 'pendo["get-thing"](args: {' in odd.describe() and '"a-b"?: unknown;' in odd.describe()


def test_repeated_parameter_types_are_declared_once():
    text = mock_catalog().describe("usage", limit=20)
    assert text.count("dateRange: DateRange;") >= 3 and "compareToDateRange?: DateRange;" in text
    assert text.count("type DateRange = {") == 1 and text.count("type ProductArea = ") == 1
    assert 'type?: "absolute" | "relative";' in text.split("// shared types")[1]
    # A lookup that uses no shared type does not pay for their definitions.
    assert "shared types" not in mock_catalog().describe("segmentList", limit=1)


def test_defaults_make_arguments_optional_and_are_bound_at_call_time():
    catalog = mock_catalog(defaults={"subId": "sub-1"})
    assert '  subId?: string; // pre-set to "sub-1"' in catalog.signature(catalog.tools["segmentList"])
    assert "  subId: string;" in mock_catalog(defaults={}).signature(catalog.tools["segmentList"])
    assert catalog.bind("segmentList", {}, "find paying segment") == {"subId": "sub-1", "userQuery": "find paying segment"}
    assert catalog.bind("segmentList", {"subId": "other"}) == {"subId": "other"}
    assert catalog.bind("listSubscriptions", {}) == {}  # a default is only sent to tools that take it


def test_only_read_only_tools_are_exposed_by_default():
    catalog = mock_catalog()
    assert "createSegment" not in catalog.tools and catalog.blocked == ["createSegment"]
    assert "createSegment" in mock_catalog(allow_tools=["createSegment"]).tools
    assert "createSegment" in mock_catalog(allow_tools=["create*"]).tools
    assert "createSegment" in mock_catalog(allow_writes=True).tools
    # A tool with no annotations is not assumed to be safe.
    assert Catalog([Tool.from_mcp({"name": "mystery"})]).names() == []


def test_search_ranks_name_matches_and_splits_camel_case():
    catalog = mock_catalog()
    assert catalog.search("guide effectiveness")[0].name == "guideEffectivenessMetrics"
    assert catalog.search("listAccounts")[0].name == "listAccounts"
    assert catalog.search("segments")[0].name == "segmentList"
    assert "No Pendo tool matches" in catalog.describe("zzzz")
    assert catalog.describe("segments", limit=1, detail="summary") == "- pendo.segmentList: List or search the subscription's segments."


def test_index_stays_small_for_a_server_with_many_tools():
    small = mock_catalog().index()
    assert "- pendo.appUsage: Get per-visitor or per-account app usage metrics for a date range." in small
    read = {"readOnlyHint": True}
    many = Catalog(Tool.from_mcp({"name": f"{verb}Thing{i}", "description": "x" * 5000, "annotations": read})
                   for verb in ("list", "get", "query") for i in range(45))
    index = many.index()
    assert index.splitlines()[0].startswith("- list: listThing0, listThing1,")
    assert len(index.splitlines()) == 3 and len(index) < 3000


def test_unwrap_prefers_structured_content_then_json_text():
    assert unwrap_tool_result({"content": [{"type": "text", "text": "x"}], "structuredContent": {"a": 1}}) == (True, {"a": 1})
    assert unwrap_tool_result({"content": [{"type": "text", "text": "[1, 2]"}]}) == (True, [1, 2])
    assert unwrap_tool_result({"content": [{"type": "text", "text": "plain"}]}) == (True, "plain")
    assert unwrap_tool_result({"content": [{"type": "text", "text": "boom"}], "isError": True}) == (False, "boom")


def test_mock_rejects_arguments_the_real_schema_would_reject():
    ok, message = unwrap_tool_result(mock_pendo.call_tool("entityUsage", {"subId": "s", "entityType": "widget", "days": 3}))
    assert not ok and "args.entityId: is required" in message and "must be one of" in message
    assert "args.days: unknown argument" in message


# --- executor ----------------------------------------------------------------

RANGE = "{ type: 'relative', lastNDays: 30 }"


@needs_node
def test_chained_calls_return_only_the_summary():
    backend = MockBackend()
    output = run(
        f"""
const top = await pendo.aggregateEntityUsage({{ entityType: 'feature', dateRange: {RANGE}, limit: 1 }});
const usage = await pendo.entityUsage({{
  entityType: 'feature', entityId: top.rows[0].entityId, dateRange: {RANGE}, groupBy: 'account', limit: 1
}});
return {{ feature: top.rows[0].entityName, topAccount: usage.rows[0].accountId }};
""",
        backend=backend,
        intent="top account of the top feature",
    )
    assert output["result"] == {"feature": "Filter Results", "topAccount": "Example Co 00"}
    assert [name for name, _args in backend.calls] == ["aggregateEntityUsage", "entityUsage"]
    # Defaults and the task summary were added by the host, not by the program.
    assert all(args["subId"] == "mock-sub" and args["userQuery"] == "top account of the top feature" for _n, args in backend.calls)
    assert output["stats"]["tool_calls"] == 2
    assert [call["ok"] for call in output["calls"]] == [True, True]


@needs_node
def test_promise_all_runs_upstream_calls_concurrently():
    start = time.monotonic()
    output = run(
        "const r = await Promise.all([1, 2, 3, 4].map(() => pendo.segmentList())); return r.length;",
        backend=MockBackend(delay=0.4),
    )
    assert output["result"] == 4
    assert time.monotonic() - start < 1.4  # four 0.4s calls in sequence would take 1.6s


@needs_node
def test_tool_failure_rejects_and_can_be_caught():
    output = run("try { await pendo.entityUsage({ entityType: 'page' }); } catch (e) { return e.message; }")
    assert output["result"].startswith("entityUsage failed: Invalid arguments for entityUsage: args.entityId: is required")
    assert output["calls"][0]["ok"] is False
    with pytest.raises(CodeModeError, match="JavaScript error: entityUsage failed"):
        run("await pendo.entityUsage({ entityType: 'page' });")


@needs_node
def test_hidden_and_unknown_tools_cannot_be_called():
    backend = MockBackend()
    with pytest.raises(CodeModeError, match='Unknown Pendo tool "createSegment"'):
        run("return await pendo.createSegment({ name: 'x' });", backend=backend)
    assert backend.calls == []
    allowed = run(
        "return await pendo.createSegment({ name: 'x' });",
        catalog=mock_catalog(allow_tools=["createSegment"]),
        backend=backend,
    )
    assert allowed["result"]["created"]["name"] == "x"


@needs_node
def test_arguments_must_be_an_object():
    with pytest.raises(CodeModeError, match="takes one arguments object"):
        run("return await pendo.segmentList('paying');")


@needs_node
def test_console_log_is_captured():
    output = run("console.log('rows', { n: 2 }); return 1;")
    assert output["logs"] == ['rows {"n":2}']


@needs_node
def test_call_limit_stops_the_program():
    with pytest.raises(CodeModeError, match="Maximum tool calls exceeded"):
        run("for (let i = 0; i < 4; i++) await pendo.segmentList(); return 1;", limits=Limits(max_calls=3))


@needs_node
def test_oversized_tool_result_rejects_that_call():
    output = run(
        "try { await pendo.listAccounts({ limit: 30 }); } catch (e) { return e.message; }",
        limits=Limits(max_call_result_bytes=500),
    )
    assert "result too large" in output["result"]


@needs_node
def test_oversized_output_and_code_are_rejected():
    with pytest.raises(CodeModeError, match="result too large"):
        run("return (await pendo.listAccounts({ limit: 30 })).rows;", limits=Limits(max_output_bytes=1000))
    with pytest.raises(CodeModeError, match="size limit"):
        run("return 123456789;", limits=Limits(max_code_bytes=4))


@needs_node
def test_runaway_program_is_bounded():
    start = time.monotonic()
    with pytest.raises(CodeModeError, match="JavaScript error|timed out"):
        run("await pendo.segmentList(); while (true) {}", limits=Limits(timeout_seconds=2))
    assert time.monotonic() - start < 5


@needs_node
def test_worker_has_no_host_access():
    assert run("return [typeof require, typeof process, typeof fetch];")["result"] == [
        "undefined",
        "undefined",
        "undefined",
    ]


RECIPES = sorted(Path("examples/pendo_code_mode").glob("*.js"))


@needs_node
@pytest.mark.parametrize("recipe", RECIPES, ids=lambda path: path.stem)
def test_recipes_run_with_arguments_the_real_schemas_accept(recipe):
    """The mock validates every call against the real argument schema."""
    output = run(recipe.read_text(encoding="utf-8"))
    assert all(call["ok"] for call in output["calls"]), output["calls"]
    assert "error" not in output["result"]
    assert output["stats"]["output_bytes"] < 4000


@needs_node
def test_recipe_results_are_what_the_question_asks():
    read = lambda name: Path(f"examples/pendo_code_mode/{name}.js").read_text(encoding="utf-8")
    adoption = run(read("feature_adoption_by_account"))
    assert adoption["stats"]["tool_calls"] == 6 and len(adoption["result"]["features"]) == 5
    assert all(0 < f["topAccountSharePct"] <= 100 and len(f["topAccounts"]) == 3 for f in adoption["result"]["features"])
    movers = run(read("usage_movers"))["result"]
    assert movers["gainers"] and all(row["change"] > 0 for row in movers["gainers"])
    assert all(row["change"] < 0 for row in movers["decliners"])
    assert [row["change"] for row in movers["gainers"]] == sorted((row["change"] for row in movers["gainers"]), reverse=True)
    audit = run(read("guide_audit"))["result"]
    assert audit["lowestEngagement"][0]["engagementPct"] == min(g["engagementPct"] for g in audit["lowestEngagement"])
    brief = run(read("account_brief"))
    assert brief["result"]["account"] == "Example Co 07" and brief["stats"]["tool_calls"] == 10
    assert len(brief["result"]["topFeatureAdoption"]) == 5


# --- over the wire -----------------------------------------------------------


def codemode_server(*flags: str) -> McpClient:
    env = {**os.environ, "PYTHONPATH": SRC}
    client = McpClient(StdioTransport([sys.executable, "-m", "pendo_codemode.server", "--mock", *flags], env=env))
    client.initialize()
    return client


@needs_node
def test_server_exposes_two_tools_over_a_real_mcp_chain():
    """client -> code mode server (stdio) -> mock Pendo MCP server (stdio)."""
    client = codemode_server()
    try:
        tools = {tool["name"]: tool for tool in client.list_tools()}
        assert sorted(tools) == ["execute", "search"]
        assert "- pendo.entityUsage: Get usage analytics for one known page" in tools["execute"]["description"]
        assert "Pre-set arguments you may omit: subId." in tools["execute"]["description"]
        assert "createSegment" not in tools["execute"]["description"]
        assert tools["execute"]["annotations"] == {"readOnlyHint": True}

        ok, signatures = unwrap_tool_result(client.call_tool("search", {"query": "accounts segment", "limit": 1}))
        assert ok and "pendo.listAccounts(args: {" in signatures and "RETURNS: - summary: numAccounts" in signatures
        ok, full = unwrap_tool_result(client.call_tool("search", {"query": "appUsage", "limit": 1, "detail": "full"}))
        assert ok and "WORKFLOW: To get more rows than limit allows" in full

        code = Path("examples/pendo_code_mode/account_brief.js").read_text(encoding="utf-8")
        ok, output = unwrap_tool_result(client.call_tool("execute", {"code": code, "intent": "brief one account"}))
        assert ok and output["stats"]["tool_calls"] == 10 and output["result"]["activity"]["daysActive"] > 0
        assert "calls" not in output and "logs" not in output

        ok, message = unwrap_tool_result(client.call_tool("execute", {"code": "return pendo.createSegment({name:'x'})"}))
        assert not ok and "Unknown Pendo tool" in message
    finally:
        client.close()


@needs_node
def test_server_can_opt_in_to_one_write_tool():
    client = codemode_server("--allow-tool", "createSegment", "--default", "subId=sub-9")
    try:
        execute = next(tool for tool in client.list_tools() if tool["name"] == "execute")
        assert execute["annotations"] == {"readOnlyHint": False}
        ok, output = unwrap_tool_result(
            client.call_tool("execute", {"code": "return (await pendo.createSegment({name:'x'})).created.id"})
        )
        assert ok and output["result"] == "seg-new"
    finally:
        client.close()


class _Handler(BaseHTTPRequestHandler):
    seen: list[dict] = []

    def log_message(self, *args):
        pass

    def do_POST(self):
        message = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
        type(self).seen.append({"method": message["method"], "headers": dict(self.headers)})
        if self.headers.get("Authorization") != "Bearer secret":
            self.send_error(401, "nope")
            return
        if "id" not in message:
            self.send_response(202)
            self.end_headers()
            return
        if message["method"] == "initialize":
            result = {"protocolVersion": "2025-06-18", "capabilities": {}, "serverInfo": {"name": "http-mock"}}
        elif message["method"] == "tools/list":
            result = {"tools": mock_pendo.list_tools()}
        else:
            result = mock_pendo.call_tool(message["params"]["name"], message["params"]["arguments"])
        reply = json.dumps({"jsonrpc": "2.0", "id": message["id"], "result": result})
        sse = message["method"] == "tools/call"  # exercise both response styles
        body = (f"event: message\ndata: {reply}\n\n" if sse else reply).encode()
        self.send_response(200)
        self.send_header("Content-Type", "text/event-stream" if sse else "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Mcp-Session-Id", "session-1")
        self.end_headers()
        self.wfile.write(body)


@pytest.fixture()
def http_url():
    _Handler.seen = []
    server = ThreadingHTTPServer(("127.0.0.1", 0), _Handler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    yield f"http://127.0.0.1:{server.server_address[1]}/mcp"
    server.shutdown()


def test_streamable_http_client_handles_json_and_sse(http_url):
    client = McpClient(HttpTransport(http_url, headers={"Authorization": "Bearer secret"}))
    client.initialize()
    assert client.server_info == {"name": "http-mock"}
    assert len(client.list_tools()) == len(mock_pendo.TOOLS)
    ok, value = unwrap_tool_result(client.call_tool("listSubscriptions", {}))
    assert ok and value == [{"subscriptionId": "mock-sub", "subscriptionName": "Mock subscription"}]
    last = _Handler.seen[-1]["headers"]
    assert last["Mcp-Session-Id"] == "session-1" and last["Mcp-Protocol-Version"] == "2025-06-18"


def test_http_errors_do_not_leak_the_credential(http_url):
    client = McpClient(HttpTransport(http_url, headers={"Authorization": "Bearer wrong-token"}))
    with pytest.raises(McpError, match="HTTP 401") as error:
        client.initialize()
    assert "wrong-token" not in str(error.value)


def test_remote_urls_must_use_https():
    with pytest.raises(McpError, match="HTTPS"):
        HttpTransport("http://pendo.example.com/mcp")
