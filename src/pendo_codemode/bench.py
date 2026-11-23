"""Read-only comparison of regular MCP tool calling and Code Mode.

For each program, Code Mode is run for real and its upstream calls are
recorded. The regular-MCP side then makes the SAME calls directly, one result
per tool call, as a model using the server without Code Mode would have to.

No model is in the loop, so this measures the mechanics only: calls, model
turns, bytes that enter the conversation, and time spent waiting on Pendo.
The regular-MCP figures are a best case (a model that picks exactly the right
calls first time).

Token figures are exact with `--tokens` (Anthropic's count_tokens endpoint; needs
the `anthropic` package and API credentials). Without it they are bytes / 4, a
rough estimate that tends to undercount JSON.
"""
from __future__ import annotations

import json
from pathlib import Path
import statistics
import time
from typing import Any, Protocol

from .catalog import Catalog
from .executor import Limits, ToolBackend, execute_code


class DelayedBackend:
    """Add a fixed delay per call, to stand in for network latency on the mock."""

    def __init__(self, inner: ToolBackend, delay_ms: float) -> None:
        self._inner, self._delay = inner, delay_ms / 1000

    def call_tool(self, name: str, arguments: dict[str, Any], *, timeout: float) -> Any:
        time.sleep(self._delay)
        return self._inner.call_tool(name, arguments, timeout=timeout)


class TokenCounter(Protocol):
    exact: bool
    label: str

    def text(self, text: str) -> int: ...

    def tools(self, tools: list[dict[str, Any]]) -> int: ...


class EstimateCounter:
    """bytes / 4. Crude: JSON usually costs more tokens than this."""

    exact, label = False, "estimated as bytes / 4"

    def text(self, text: str) -> int:
        return round(len(text.encode("utf-8")) / 4)

    def tools(self, tools: list[dict[str, Any]]) -> int:
        return self.text(json.dumps(tools, ensure_ascii=False))


class AnthropicCounter:
    """Exact counts from the Anthropic count_tokens endpoint for one model."""

    exact = True

    def __init__(self, model: str) -> None:
        try:
            import anthropic
        except ImportError as exc:
            raise RuntimeError("--tokens needs the Anthropic SDK: pip install -e '.[tokens]'") from exc
        self._anthropic = anthropic
        self._client = anthropic.Anthropic()  # credentials come from the environment
        self._model = model
        self.label = f"counted by the Anthropic API for {model}"
        self._probe = [{"role": "user", "content": "x"}]
        self._overhead = self._count(messages=self._probe) - 1

    def _count(self, **params: Any) -> int:
        try:
            return self._client.messages.count_tokens(model=self._model, **params).input_tokens
        except self._anthropic.AuthenticationError as exc:
            raise RuntimeError("--tokens needs Anthropic API credentials (for example ANTHROPIC_API_KEY)") from exc
        except self._anthropic.APIStatusError as exc:
            raise RuntimeError(f"token counting failed ({exc.status_code}): {exc.message}") from exc
        except self._anthropic.APIConnectionError as exc:
            raise RuntimeError("token counting failed: could not reach the Anthropic API") from exc

    def text(self, text: str) -> int:
        if not text:
            return 0
        return self._count(messages=[{"role": "user", "content": text}]) - self._overhead

    def tools(self, tools: list[dict[str, Any]]) -> int:
        """Tokens the definitions add to a request, measured against the same request without them."""
        converted = [
            {"name": t["name"], "description": t.get("description", ""),
             "input_schema": t.get("inputSchema") or {"type": "object"}}
            for t in tools
        ]
        return self._count(messages=self._probe, tools=converted) - (self._overhead + 1)


def _size(value: Any) -> int:
    return len(json.dumps(value, ensure_ascii=False).encode("utf-8"))


def measure(
    path: Path,
    *,
    catalog: Catalog,
    backend: ToolBackend,
    raw_tools: list[dict[str, Any]],
    codemode_tools: list[dict[str, Any]],
    limits: Limits,
    runs: int = 3,
    counter: TokenCounter | None = None,
) -> dict[str, Any]:
    if any(not tool.read_only for tool in catalog.tools.values()):
        raise ValueError("the benchmark only runs with a read-only catalog")
    code = path.read_text(encoding="utf-8")
    intent = f"benchmark {path.stem}"

    code_times, output = [], {}
    for _ in range(runs):
        output = execute_code(code, catalog=catalog, backend=backend, limits=limits, intent=intent)
        code_times.append(output["stats"]["elapsed_ms"])
    calls = output["calls"]
    if not all(call["ok"] for call in calls):
        raise RuntimeError(f"{path.name}: a tool call failed, so the comparison would be meaningless")

    # Regular MCP: the same calls, made directly. Results are kept to size them.
    direct_runs: list[list[float]] = []
    result_bytes, result_texts = 0, []
    for _ in range(runs):
        timings, result_bytes, result_texts = [], 0, []
        for call in calls:
            began = time.monotonic()
            value = backend.call_tool(call["tool"], call["args"], timeout=limits.timeout_seconds)
            timings.append((time.monotonic() - began) * 1000)
            result_bytes += _size(value)
            result_texts.append(json.dumps(value, ensure_ascii=False))
        direct_runs.append(timings)
    per_call = [statistics.median(run[i] for run in direct_runs) for i in range(len(calls))]
    rounds = sorted({call["round"] for call in calls})
    # A client that issues every independent call in one turn waits for the slowest of each round.
    parallel_ms = sum(max(per_call[i] for i, call in enumerate(calls) if call["round"] == r) for r in rounds)

    used = sorted({call["tool"] for call in calls})
    search_text = "\n\n".join(catalog.describe(name, limit=1) for name in used)
    regular = {
        "tool_calls": len(calls),
        "model_turns_sequential": len(calls) + 1,
        "model_turns_parallel": len(rounds) + 1,
        "definition_bytes": _size(raw_tools),
        "argument_bytes": sum(_size(call["args"]) for call in calls),
        "result_bytes": result_bytes,
        "upstream_ms_sequential": round(sum(per_call)),
        "upstream_ms_parallel": round(parallel_ms),
    }
    codemode = {
        "tool_calls": 2,  # one search for the signatures, one execute
        "model_turns": 3,
        "definition_bytes": _size(codemode_tools),
        "search_result_bytes": len(search_text.encode("utf-8")),
        "program_bytes": len(code.encode("utf-8")),
        "result_bytes": output["stats"]["output_bytes"],
        "upstream_ms": round(statistics.median(code_times)),
    }
    regular["conversation_bytes"] = regular["argument_bytes"] + regular["result_bytes"]
    codemode["conversation_bytes"] = (
        codemode["search_result_bytes"] + codemode["program_bytes"] + codemode["result_bytes"]
    )
    counter = counter or EstimateCounter()
    regular["argument_tokens"] = counter.text("\n".join(json.dumps(call["args"], ensure_ascii=False) for call in calls))
    regular["result_tokens"] = counter.text("\n".join(result_texts))
    regular["definition_tokens"] = counter.tools(raw_tools)
    codemode["search_result_tokens"] = counter.text(search_text)
    codemode["program_tokens"] = counter.text(code)
    codemode["result_tokens"] = counter.text(json.dumps(output["result"], ensure_ascii=False))
    codemode["definition_tokens"] = counter.tools(codemode_tools)
    regular["conversation_tokens"] = regular["argument_tokens"] + regular["result_tokens"]
    codemode["conversation_tokens"] = (
        codemode["search_result_tokens"] + codemode["program_tokens"] + codemode["result_tokens"]
    )
    return {"program": path.stem, "tools_used": used, "tokens": counter.label, "tokens_exact": counter.exact,
            "regular_mcp": regular, "code_mode": codemode}


def _call_key(tool: str, args: dict[str, Any]) -> str:
    # The telemetry argument differs between a person's wording and the program's, so it is ignored.
    return json.dumps([tool, {k: v for k, v in args.items() if k != "userQuery"}], sort_keys=True)


class ReplayBackend:
    """Serve tool results recorded from an earlier session; no network.

    A recording is a directory of NN.json files, each {"tool", "args", "result"}.
    A call that was never recorded fails, so a program measured this way can
    only use data that the recorded regular-MCP session also fetched.
    """

    def __init__(self, directory: Path) -> None:
        self.calls: list[dict[str, Any]] = []
        for path in sorted(directory.glob("[0-9]*.json")):
            self.calls.append(json.loads(path.read_text(encoding="utf-8")))
        if not self.calls:
            raise ValueError(f"no recorded calls (NN.json) in {directory}")
        self._results = {_call_key(c["tool"], c["args"]): c for c in self.calls}

    def call_tool(self, name: str, arguments: dict[str, Any], *, timeout: float) -> Any:
        found = self._results.get(_call_key(name, arguments))
        if found is None:
            raise RuntimeError(f"not in the recording: {json.dumps(arguments, sort_keys=True)[:300]}")
        if found.get("error"):
            raise RuntimeError(str(found["result"])[:500])
        return found["result"]


def measure_recorded(
    path: Path,
    recording: Path,
    *,
    catalog: Catalog,
    limits: Limits,
    counter: TokenCounter | None = None,
) -> dict[str, Any]:
    """Compare a recorded regular-MCP session with a program replayed over the same results."""
    counter = counter or EstimateCounter()
    replay = ReplayBackend(recording)
    code = path.read_text(encoding="utf-8")
    output = execute_code(code, catalog=catalog, backend=replay, limits=limits, intent=f"benchmark {path.stem}")
    failed = [call for call in output["calls"] if not call["ok"]]
    used = sorted({call["tool"] for call in output["calls"]})
    search_text = "\n\n".join(catalog.describe(name, limit=1) for name in used if name in catalog.tools)
    args_text = "\n".join(json.dumps(c["args"], ensure_ascii=False) for c in replay.calls)
    results_text = "\n".join(json.dumps(c["result"], ensure_ascii=False) for c in replay.calls)
    result_text = json.dumps(output["result"], ensure_ascii=False)
    regular = {
        "tool_calls": len(replay.calls),
        "failed_calls": sum(1 for c in replay.calls if c.get("error")),
        "argument_bytes": len(args_text.encode("utf-8")),
        "result_bytes": len(results_text.encode("utf-8")),
        "argument_tokens": counter.text(args_text),
        "result_tokens": counter.text(results_text),
    }
    codemode = {
        "tool_calls": 2,
        "upstream_calls": len(output["calls"]),
        "upstream_failed": len(failed),
        "rounds": output["stats"]["rounds"],
        "search_result_bytes": len(search_text.encode("utf-8")),
        "program_bytes": len(code.encode("utf-8")),
        "result_bytes": len(result_text.encode("utf-8")),
        "search_result_tokens": counter.text(search_text),
        "program_tokens": counter.text(code),
        "result_tokens": counter.text(result_text),
    }
    regular["conversation_tokens"] = regular["argument_tokens"] + regular["result_tokens"]
    codemode["conversation_tokens"] = (
        codemode["search_result_tokens"] + codemode["program_tokens"] + codemode["result_tokens"]
    )
    return {"program": path.stem, "tools_used": used, "tokens": counter.label, "tokens_exact": counter.exact,
            "regular_mcp": regular, "code_mode": codemode, "result": output["result"]}


def render_recorded(rows: list[dict[str, Any]]) -> str:
    mark = "" if rows[0]["tokens_exact"] else "~"
    lines = [
        f"Recorded regular-MCP sessions against programs replayed over the same results. Tokens: {rows[0]['tokens']}.",
        "",
        "| Scenario | Model tool calls | Pendo calls | Result tokens into conversation | Conversation tokens |",
        "| --- | --- | --- | --- | --- |",
    ]
    for row in rows:
        r, c = row["regular_mcp"], row["code_mode"]
        lines.append(
            f"| {row['program']} | {r['tool_calls']} → {c['tool_calls']} | {r['tool_calls']} → {c['upstream_calls']} "
            f"| {mark}{r['result_tokens']:,} → {mark}{c['result_tokens']:,} "
            f"| {mark}{r['conversation_tokens']:,} → {mark}{c['conversation_tokens']:,} |"
        )
    lines += [
        "",
        "Code Mode conversation tokens (search result + program + result):",
        *[
            f"- {row['program']}: {mark}{row['code_mode']['search_result_tokens']:,} + "
            f"{mark}{row['code_mode']['program_tokens']:,} + {mark}{row['code_mode']['result_tokens']:,}"
            for row in rows
        ],
        "",
        "Each cell is regular MCP → Code Mode. Tool definitions and timing are not part of a recording.",
    ]
    return "\n".join(lines)


def render(rows: list[dict[str, Any]], *, upstream: str, tool_count: int, runs: int, delay_ms: float) -> str:
    first = rows[0]
    mark = "" if first["tokens_exact"] else "~"
    r0, c0 = first["regular_mcp"], first["code_mode"]
    lines = [
        f"Upstream: {upstream}, {tool_count} tools exposed. Median of {runs} run(s)."
        + (f" Simulated latency: {delay_ms:g} ms per call." if delay_ms else ""),
        f"Tokens: {first['tokens']}.",
        "",
        "Tool definitions (paid in every conversation):",
        f"- regular MCP: {mark}{r0['definition_tokens']:,} tokens ({r0['definition_bytes']:,} bytes)",
        f"- Code Mode:   {mark}{c0['definition_tokens']:,} tokens ({c0['definition_bytes']:,} bytes)",
        "",
        "| Program | Tool calls | Model turns | Conversation tokens | With definitions | Waiting on Pendo (ms) |",
        "| --- | --- | --- | --- | --- | --- |",
    ]
    for row in rows:
        r, c = row["regular_mcp"], row["code_mode"]
        lines.append(
            f"| {row['program']} | {r['tool_calls']} → {c['tool_calls']} "
            f"| {r['model_turns_sequential']} (or {r['model_turns_parallel']} parallel) → {c['model_turns']} "
            f"| {mark}{r['conversation_tokens']:,} → {mark}{c['conversation_tokens']:,} "
            f"| {mark}{r['conversation_tokens'] + r['definition_tokens']:,} → "
            f"{mark}{c['conversation_tokens'] + c['definition_tokens']:,} "
            f"| {r['upstream_ms_sequential']} (or {r['upstream_ms_parallel']}) → {c['upstream_ms']} |"
        )
    lines += [
        "",
        "Code Mode token breakdown (search result + program + result):",
        *[
            f"- {row['program']}: {mark}{row['code_mode']['search_result_tokens']:,} + "
            f"{mark}{row['code_mode']['program_tokens']:,} + {mark}{row['code_mode']['result_tokens']:,}"
            for row in rows
        ],
        "",
        "Each cell is regular MCP → Code Mode. Conversation tokens are arguments plus results for regular MCP,",
        "and search result plus program plus result for Code Mode. 'With definitions' is a one-question",
        "conversation. Tokens are counted once each; a real conversation re-reads earlier turns on every",
        "model turn, which costs regular MCP more because it takes more turns. Code Mode's time includes",
        "starting Node. No model is in the loop: regular MCP is given exactly the right calls.",
    ]
    return "\n".join(lines)
