"""Read-only comparison of regular MCP tool calling and Code Mode.

For each program, Code Mode is run for real and its upstream calls are
recorded. The regular-MCP side then makes the SAME calls directly, one result
per tool call, as a model using the server without Code Mode would have to.

No model is in the loop, so this measures the mechanics only: calls, model
turns, bytes that enter the conversation, and time spent waiting on Pendo.
The regular-MCP figures are a best case (a model that picks exactly the right
calls first time). Token figures are bytes / 4, an estimate.
"""
from __future__ import annotations

import json
from pathlib import Path
import statistics
import time
from typing import Any

from .catalog import Catalog
from .executor import Limits, ToolBackend, execute_code


class DelayedBackend:
    """Add a fixed delay per call, to stand in for network latency on the mock."""

    def __init__(self, inner: ToolBackend, delay_ms: float) -> None:
        self._inner, self._delay = inner, delay_ms / 1000

    def call_tool(self, name: str, arguments: dict[str, Any], *, timeout: float) -> Any:
        time.sleep(self._delay)
        return self._inner.call_tool(name, arguments, timeout=timeout)


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
    result_bytes = 0
    for _ in range(runs):
        timings, result_bytes = [], 0
        for call in calls:
            began = time.monotonic()
            value = backend.call_tool(call["tool"], call["args"], timeout=limits.timeout_seconds)
            timings.append((time.monotonic() - began) * 1000)
            result_bytes += _size(value)
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
    return {"program": path.stem, "tools_used": used, "regular_mcp": regular, "code_mode": codemode}


def render(rows: list[dict[str, Any]], *, upstream: str, tool_count: int, runs: int, delay_ms: float) -> str:
    def tokens(count: int) -> str:
        return f"{count:,} (~{round(count / 4):,} tok)"

    first = rows[0]
    lines = [
        f"Upstream: {upstream}, {tool_count} tools exposed. Median of {runs} run(s)."
        + (f" Simulated latency: {delay_ms:g} ms per call." if delay_ms else ""),
        "",
        "Fixed cost per conversation (tool definitions):",
        f"- regular MCP: {tokens(first['regular_mcp']['definition_bytes'])}",
        f"- Code Mode:   {tokens(first['code_mode']['definition_bytes'])}",
        "",
        "| Program | Tool calls | Model turns | Conversation bytes | Waiting on Pendo (ms) |",
        "| --- | --- | --- | --- | --- |",
    ]
    for row in rows:
        r, c = row["regular_mcp"], row["code_mode"]
        lines.append(
            f"| {row['program']} | {r['tool_calls']} → {c['tool_calls']} "
            f"| {r['model_turns_sequential']} (or {r['model_turns_parallel']} with parallel calls) → {c['model_turns']} "
            f"| {r['conversation_bytes']:,} → {c['conversation_bytes']:,} "
            f"| {r['upstream_ms_sequential']} (or {r['upstream_ms_parallel']}) → {c['upstream_ms']} |"
        )
    lines += [
        "",
        "Each cell is regular MCP → Code Mode. Conversation bytes are arguments plus results for regular MCP,",
        "and search result plus program plus result for Code Mode. Code Mode's time includes starting Node.",
        "No model is in the loop: regular MCP is given exactly the right calls, and model thinking time,",
        "which grows with turns, is not measured.",
    ]
    return "\n".join(lines)
