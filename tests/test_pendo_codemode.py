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
    assert [t.name for t in catalog.search("listAccounts")] == ["listAccounts"]  # exact name: that tool only
    assert [t.name for t in catalog.search("getentity")] == ["getEntity"]
    assert catalog.search("segments")[0].name == "segmentList"
    assert "No Pendo tool matches" in catalog.describe("zzzz")
    assert catalog.describe("segments", limit=1, detail="summary") == "- pendo.segmentList: List or search the subscription's segments."


def test_index_stays_bounded_for_a_server_with_many_tools():
    small = mock_catalog().index()
    assert "- pendo.segmentList({substring?, offset?})  // List or search the subscription's segments." in small
    # Required arguments first; small enums and maximums are shown because guessing them wastes a run.
    assert "- pendo.entityUsage({entityType: page|feature|trackEvent, entityId, dateRange, groupBy?: visitor|account, accountId?, limit?≤200," in small
    read = {"readOnlyHint": True}
    props = {"type": "object", "properties": {"id": {"type": "string"}}, "required": ["id"]}
    tools = [Tool.from_mcp({"name": f"{verb}Thing{i}", "description": "x" * 5000, "annotations": read, "inputSchema": props})
             for verb in ("list", "get", "query") for i in range(45)]
    many = Catalog(tools, core=["getThing7", "notUpstream"])
    many.observe("queryThing3", {"rows": [{"n": 1}]})
    lines = many.index().splitlines()
    # Core tools and tools that have been used get a full line; the other 133 are names only.
    assert lines[0] == "- pendo.getThing7({id})  // " + "x" * 109 + "…"
    assert lines[1] == "- pendo.queryThing3({id}) → {rows: [{n}]}"
    assert lines[2].startswith("More tools (133); get a signature with")
    assert len(lines) == 6 and len(many.index()) < 3500
    # Doubling the tool count adds names, not signatures.
    bigger = Catalog(tools + [Tool.from_mcp({"name": f"showThing{i}", "annotations": read}) for i in range(135)], core=["getThing7"])
    assert len(bigger.index()) - len(many.index()) < 2000


def test_observed_result_shapes_describe_structure_without_data():
    from pendo_codemode.catalog import shape_of

    value = {"meta": {"hasMore": True}, "rows": [{"accountId": "Acme", "events": 3, "note": None},
                                                   {"accountId": "Beta", "events": 5, "note": "x", "extra": 1.5}]}
    shape = shape_of(value)
    assert shape == "{meta: {hasMore: boolean}, rows: {accountId: string, events: number, note: string, extra?: number}[]}"
    assert "Acme" not in shape
    assert shape_of([{"requestId": "r", "rows": []}]) == "{requestId: string, rows: unknown[]}[]"
    catalog = mock_catalog()
    catalog.observe("entityUsage", value)
    catalog.observe("notATool", value)
    assert catalog.shapes == {"entityUsage": shape}
    assert f" * RESULT SHAPE (observed; ? = not always present): {shape}" in catalog.signature(catalog.tools["entityUsage"])
    assert catalog.compact(catalog.tools["entityUsage"]).endswith("}) → {meta: {hasMore}, rows: [{accountId, events, note, extra?}]}")


def test_fields_that_depend_on_the_arguments_are_marked_optional():
    """Page rankings carry U-turn counts and feature rankings do not; a merged shape must say so."""
    catalog = mock_catalog()
    catalog.observe("aggregateEntityUsage", {"rows": [{"entityId": "p", "totalEvents": 1, "totalUTurnCount": 2}]})
    catalog.observe("aggregateEntityUsage", {"rows": [{"entityId": "f", "totalEvents": 1}], "summary": {"n": 1}})
    assert catalog.shapes["aggregateEntityUsage"] == (
        "{rows: {entityId: string, totalEvents: number, totalUTurnCount?: number}[], summary?: {n: number}}"
    )
    assert catalog.uses["aggregateEntityUsage"] == 2


@needs_node
def test_running_a_program_teaches_the_catalog_result_shapes(tmp_path):
    catalog = mock_catalog()
    assert "→" not in catalog.index()
    run("return (await pendo.segmentList()).totalCount;", catalog=catalog)
    assert "- pendo.segmentList({substring?, offset?}) → {totalCount, offset, returned, segments: [{id, name}]}" in catalog.index()
    path = tmp_path / "shapes.json"
    catalog.save_shapes(path)
    fresh = mock_catalog()
    fresh.load_shapes(path)
    assert fresh.shapes == catalog.shapes and fresh.uses == catalog.uses


@needs_node
def test_state_carries_data_between_programs_without_returning_it():
    first = run("""
const features = await pendo.listPagesFeaturesAndEvents({ type: 'feature' });
state.features = Object.fromEntries(features.map((f) => [f.name, f.id]));
return { stored: features.length };
""")
    assert first["result"] == {"stored": 10} and len(first["state"]["features"]) == 10
    backend = MockBackend()
    second = run(f"""
const usage = await pendo.entityUsage({{
  entityType: 'feature', entityId: state.features['Export CSV'], dateRange: {RANGE}, groupBy: 'account', limit: 1
}});
state.lastTop = usage.rows[0].accountId;
return state.lastTop;
""", state=first["state"], backend=backend)
    assert second["result"] == "Example Co 29"
    assert [name for name, _args in backend.calls] == ["entityUsage"]  # no second lookup of the id
    assert set(second["state"]) == {"features", "lastTop"}
    with pytest.raises(CodeModeError, match="state too large"):
        run("state.big = 'x'.repeat(5000); return 1;", limits=Limits(max_state_bytes=1000))


@needs_node
def test_recipes_are_saved_programs_called_with_params(tmp_path):
    from pendo_codemode.recipes import RecipeStore

    store = RecipeStore(tmp_path / "recipes")
    store.save("topFeature", "Most-used feature over params.days days", f"""
const top = await pendo.aggregateEntityUsage({{ entityType: 'feature', dateRange: {{ type: 'relative', lastNDays: params.days }}, limit: 1 }});
return top.rows[0].entityName;
""")
    assert store.index() == "- recipes.topFeature(params)  // Most-used feature over params.days days"
    output = run("return [await recipes.topFeature({ days: 30 }), typeof recipes.missing];", recipes=store.code())
    assert output["result"] == ["Filter Results", "undefined"] and output["stats"]["tool_calls"] == 1
    with pytest.raises(ValueError, match="recipe name"):
        store.save("../evil", "x", "return 1")
    broken = run("try { await recipes.bad(); } catch (e) { return e.message; }", recipes={"bad": "return ("})
    assert "recipe bad does not compile" in broken["result"]


@needs_node
def test_large_results_come_back_as_a_preview_with_the_full_value_in_state():
    code = "return { accounts: (await pendo.listAccounts({ limit: 30 })).rows, note: 'x'.repeat(500), total: 30 };"
    output = run(code, limits=Limits(preview_bytes=600))
    assert output["result"]["total"] == 30 and output["result"]["note"].endswith("… (500 chars)")
    shown = output["result"]["accounts"]
    assert shown[-1].endswith("more not shown") and len(shown) <= 6 and shown[0]["accountId"] == "Example Co 00"
    assert output["stats"]["output_bytes"] <= 600 and "state.last" in output["preview"]
    assert len(output["state"]["last"]["accounts"]) == 30  # nothing is lost
    # The next program narrows the stored value without calling Pendo again.
    backend = MockBackend()
    second = run("return state.last.accounts.filter((a) => a.name.endsWith('7')).map((a) => a.name);",
                 state=output["state"], backend=backend)
    assert second["result"] == ["Example Co 07", "Example Co 17", "Example Co 27"] and backend.calls == []
    assert "preview" not in second


@needs_node
def test_preview_can_be_overridden_or_switched_off():
    code = "const rows = (await pendo.listAccounts({ limit: 30 })).rows; return %s;"
    whole = run(code % "full(rows)", limits=Limits(preview_bytes=600))
    assert len(whole["result"]) == 30 and "preview" not in whole and "last" not in whole["state"]
    off = run(code % "rows", limits=Limits(preview_bytes=0))
    assert len(off["result"]) == 30 and "preview" not in off
    small = run("return { n: 1 };", limits=Limits(preview_bytes=600))
    assert small["result"] == {"n": 1} and "preview" not in small and small["state"] == {}
    with pytest.raises(CodeModeError, match="result too large"):  # the hard limit still applies to full()
        run(code % "full(rows)", limits=Limits(preview_bytes=600, max_output_bytes=1000))


def test_preview_gives_up_detail_in_steps_until_it_fits():
    from pendo_codemode.executor import make_preview

    rows = [{"id": i, "text": "y" * 300} for i in range(50)]
    roomy = make_preview({"rows": rows}, 4000)
    assert len(roomy["rows"]) == 6 and roomy["rows"][-1] == "… 45 more not shown"
    tight = make_preview({"rows": rows}, 300)
    assert tight["rows"][-1] == "… 48 more not shown" and tight["rows"][0]["text"].endswith("… (300 chars)")
    assert len(json.dumps(tight, ensure_ascii=False).encode()) <= 300
    assert make_preview({f"k{i}": "z" * 100 for i in range(200)}, 250) == {"shape": make_preview({f"k{i}": "z" * 100 for i in range(200)}, 250)["shape"]}


@needs_node
def test_table_helper_drops_repeated_keys():
    output = run("return table([{ a: 1, b: 2 }, { a: 3, c: 4 }]);")
    assert output["result"] == {"columns": ["a", "b", "c"], "rows": [[1, 2, None], [3, None, 4]]}


@needs_node
def test_help_is_available_inside_a_program_and_costs_no_tool_call():
    output = run("const doc = await pendo.help('segmentList'); const all = await pendo.help(); return [doc.includes('pendo.segmentList(args'), all.includes('pendo.appUsage')];")
    assert output["result"] == [True, True]
    assert output["stats"]["tool_calls"] == 0


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
    polls = run(read("guide_poll_responses"))
    assert [g["guide"] for g in polls["result"]] == ["NPS survey", "Setup checklist"] and polls["stats"]["tool_calls"] == 3
    checklist = polls["result"][1]["polls"]
    assert set(checklist[0]["answers"]) <= {"Yes", "No"} and sum(checklist[0]["answers"].values()) > 0
    assert list(checklist[1]["answers"]) == ["freeText"]  # free-text answers are counted, not returned
    assert [g["id"] for g in polls["state"]["guidesWithPolls"]] == ["guide-03", "guide-07"]
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
        assert "- pendo.entityUsage({entityType: page|feature|trackEvent, entityId, dateRange" in tools["execute"]["description"]
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
def test_server_keeps_state_and_recipes_across_execute_calls(tmp_path):
    client = codemode_server("--recipes", str(tmp_path))
    try:
        ok, first = unwrap_tool_result(client.call_tool("execute", {
            "code": "state.ids = (await pendo.segmentList()).segments.map((s) => s.id); return state.ids.length;",
        }))
        assert ok and first["result"] == 3 and first["state"] == {"ids": "array[3], 27 bytes"}
        ok, second = unwrap_tool_result(client.call_tool("execute", {
            "code": "return (params.prefix || '') + state.ids[0];",
            "save_as": {"name": "firstSegment", "description": "First stored segment id, with params.prefix"},
        }))
        assert ok and second["result"] == "seg-0" and second["saved"] == "recipes.firstSegment"
        execute = next(tool for tool in client.list_tools() if tool["name"] == "execute")
        assert "- recipes.firstSegment(params)  // First stored segment id, with params.prefix" in execute["description"]
        ok, third = unwrap_tool_result(client.call_tool("execute", {"code": "return await recipes.firstSegment({ prefix: '#' });"}))
        assert ok and third["result"] == "#seg-0"
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


# --- benchmark ---------------------------------------------------------------


@needs_node
def test_benchmark_replays_the_same_read_calls_directly():
    from pendo_codemode import bench
    from pendo_codemode.server import tool_definitions

    catalog, backend = mock_catalog(), MockBackend()
    row = bench.measure(
        Path("examples/pendo_code_mode/feature_adoption_by_account.js"),
        catalog=catalog,
        backend=bench.DelayedBackend(backend, 100),
        raw_tools=mock_pendo.list_tools(),
        codemode_tools=tool_definitions(catalog, Limits()),
        limits=Limits(),
        runs=1,
    )
    regular, code = row["regular_mcp"], row["code_mode"]
    assert regular["tool_calls"] == 6 and code["tool_calls"] == 2
    # One ranking call, then five breakdowns issued together.
    assert regular["model_turns_sequential"] == 7 and regular["model_turns_parallel"] == 3
    # Identical calls on both sides; the parallel ones may reach the backend in any order.
    key = lambda call: json.dumps(call, sort_keys=True)
    assert len(backend.calls) == 12 and sorted(backend.calls[:6], key=key) == sorted(backend.calls[6:], key=key)
    assert regular["upstream_ms_sequential"] >= 600 > regular["upstream_ms_parallel"] >= 200
    assert code["upstream_ms"] < regular["upstream_ms_sequential"]
    assert code["definition_bytes"] < regular["definition_bytes"]
    assert code["conversation_bytes"] == code["search_result_bytes"] + code["program_bytes"] + code["result_bytes"]
    assert "| feature_adoption_by_account | 6 → 2 |" in bench.render([row], upstream="mock", tool_count=9, runs=1, delay_ms=100)


def test_benchmark_refuses_a_catalog_with_write_tools():
    from pendo_codemode import bench

    with pytest.raises(ValueError, match="read-only"):
        bench.measure(Path("x.js"), catalog=mock_catalog(allow_writes=True), backend=MockBackend(),
                      raw_tools=[], codemode_tools=[], limits=Limits())


@needs_node
def test_benchmark_reports_tokens_from_the_supplied_counter():
    from pendo_codemode import bench
    from pendo_codemode.server import tool_definitions

    class WordCounter:
        exact, label = True, "counted by a test double"

        def text(self, text):
            return len(text.split())

        def tools(self, tools):
            return 1000 * len(tools)

    catalog = mock_catalog()
    kwargs = dict(catalog=catalog, backend=MockBackend(), raw_tools=mock_pendo.list_tools()[:9],
                  codemode_tools=tool_definitions(catalog, Limits()), limits=Limits(), runs=1)
    path = Path("examples/pendo_code_mode/usage_movers.js")
    row = bench.measure(path, counter=WordCounter(), **kwargs)
    regular, code = row["regular_mcp"], row["code_mode"]
    assert (regular["definition_tokens"], code["definition_tokens"]) == (9000, 2000)
    assert code["program_tokens"] == len(path.read_text(encoding="utf-8").split())
    assert code["conversation_tokens"] == code["search_result_tokens"] + code["program_tokens"] + code["result_tokens"]
    assert regular["conversation_tokens"] == regular["argument_tokens"] + regular["result_tokens"] > 0
    table = bench.render([row], upstream="mock", tool_count=9, runs=1, delay_ms=0)
    assert "Tokens: counted by a test double." in table and "~" not in table
    # Without a counter the figures are marked as estimates.
    estimate = bench.render([bench.measure(path, **kwargs)], upstream="mock", tool_count=9, runs=1, delay_ms=0)
    assert "Tokens: estimated as bytes / 4." in estimate and "- regular MCP: ~" in estimate


# --- generated file tree -----------------------------------------------------


def test_generate_writes_one_typed_file_per_tool(tmp_path):
    from pendo_codemode.generate import write_tree
    from pendo_codemode.recipes import RecipeStore

    catalog = mock_catalog()
    catalog.observe("segmentList", {"totalCount": 1, "segments": [{"id": "s", "name": "n"}]})
    store = RecipeStore(tmp_path / "saved")
    store.save("topFeature", "Most-used feature", "return 1;")
    summary = write_tree(catalog, tmp_path / "out", recipes=store)
    pendo = tmp_path / "out" / "servers" / "pendo"
    assert summary["tools"] == len(catalog.tools) and summary["recipes"] == 1
    assert sorted(p.stem for p in pendo.glob("*.ts")) == sorted([*catalog.tools, "index", "types"])
    segment = (pendo / "segmentList.ts").read_text(encoding="utf-8")
    assert "export interface SegmentListInput {" in segment and "userQuery" not in segment
    assert "export type SegmentListResponse = {totalCount: number, segments: {id: string, name: string}[]};" in segment
    assert 'return callMCPTool<SegmentListResponse>("segmentList", input);' in segment
    assert "export async function segmentList(input: SegmentListInput = {})" in segment  # nothing required once subId is pre-set
    funnel = (pendo / "queryFunnel.ts").read_text(encoding="utf-8")
    assert 'import type { DateRange } from "./types.js";' in funnel and "export type QueryFunnelResponse = unknown;" in funnel
    assert "export async function queryFunnel(input: QueryFunnelInput): Promise" in funnel
    assert "export type DateRange = {" in (pendo / "types.ts").read_text(encoding="utf-8")
    index = (pendo / "index.ts").read_text(encoding="utf-8")
    assert 'export * from "./segmentList.js";' in index and "→ {totalCount, segments: [{id, name}]}" in index
    # Browsing costs the index plus the files actually opened, not every definition.
    assert summary["index_bytes"] + summary["largest"] < summary["tool_file_bytes"] / 2
    skill = (tmp_path / "out" / "skills" / "pendo-analytics" / "SKILL.md").read_text(encoding="utf-8")
    assert skill.startswith("---\nname: pendo-analytics\n") and "- `topFeature.js`: Most-used feature" in skill
    assert "createSegment" not in index  # write tools stay out of the tree too
