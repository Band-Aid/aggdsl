from __future__ import annotations

from typing import Any
from typing import Literal

import json
import sys

try:
    # Normal package import path
    from .dsl_ast import Query, Stage
except ImportError:  # pragma: no cover
    # Allows running this file directly: `python src/aggdsl/compiler.py ...`
    from dsl_ast import Query, Stage  # type: ignore


class CompileError(ValueError):
    pass


def compile_pipeline(query: Query, *, now_ms: int | None = None) -> list[dict[str, Any]]:
    """Compile a parsed DSL query into a list of Pendo pipeline stages."""
    pipeline: list[dict[str, Any]] = []

    # FROM mode: emit an initial source stage.
    if query.event_source is not None:
        source_obj: dict[str, Any] = {
            query.event_source.source_type: dict(query.event_source.params)
        }

        if query.time_series is not None:
            first = query.time_series.first
            count = query.time_series.count
            last = query.time_series.last

            # Preserve literal now() unless the caller explicitly provides now_ms
            # (useful for deterministic compilation in tests).
            if first == "now()" and now_ms is not None:
                first = now_ms

            if last == "now()" and now_ms is not None:
                last = now_ms

            ts: dict[str, Any] = {
                "period": query.time_series.period,
                "first": int(first) if isinstance(first, int) else str(first),
            }
            if count is not None:
                ts["count"] = int(count)
            if last is not None:
                ts["last"] = int(last) if isinstance(last, int) else str(last)
            source_obj["timeSeries"] = ts

        pipeline.append({"source": source_obj})

    for stage in query.stages:
        pipeline.append(_compile_stage(stage, now_ms=now_ms))

    return pipeline


def compile_to_pendo_aggregation(query: Query, *, now_ms: int | None = None) -> dict[str, Any]:
    """Compile a parsed DSL query into a Pendo Aggregation API JSON body."""
    return compile_to_pendo_aggregation_with_format(query, now_ms=now_ms, request_format="object")


def compile_to_pendo_aggregation_with_format(
    query: Query,
    *,
    now_ms: int | None = None,
    request_format: Literal["object"] = "object",
) -> dict[str, Any]:
    """Compile a parsed DSL query into a Pendo Aggregation API JSON body.

    Pendo aggregation requests are always sent as an object containing `pipeline`.
    """
    pipeline = compile_pipeline(query, now_ms=now_ms)

    body: dict[str, Any] = {
        "response": {"location": "request", "mimeType": query.response_mime_type},
    }

    if request_format != "object":
        raise CompileError(f"Unsupported request_format: {request_format}")

    req_obj: dict[str, Any] = {"pipeline": pipeline}
    if query.request_name is not None:
        req_obj["name"] = query.request_name
    body["request"] = req_obj

    return body


def _compile_stage(stage: Stage, *, now_ms: int | None) -> dict[str, Any]:
    if stage.kind == "filter":
        return {"filter": str(stage.payload)}

    if stage.kind == "identified":
        return {"identified": str(stage.payload)}

    if stage.kind == "eval":
        return {"eval": dict(stage.payload)}

    if stage.kind == "select":
        return {"select": dict(stage.payload)}

    if stage.kind == "join":
        return {"join": dict(stage.payload)}

    if stage.kind == "merge":
        fields = list(stage.payload["fields"])
        mappings_in = stage.payload.get("mappings")
        query = stage.payload["query"]
        if not isinstance(query, Query):
            raise CompileError("merge stage requires a parsed Query for pipeline")

        merge_obj: dict[str, Any] = {
            "fields": fields,
            "pipeline": compile_pipeline(query, now_ms=now_ms),
        }
        if mappings_in is not None:
            merge_obj["mappings"] = dict(mappings_in)

        return {"merge": merge_obj}

    if stage.kind == "unmarshal":
        return {"unmarshal": dict(stage.payload)}

    if stage.kind == "unwind":
        return {"unwind": dict(stage.payload)}

    if stage.kind == "segment":
        return {"segment": dict(stage.payload)}

    if stage.kind == "bulkExpand":
        return {"bulkExpand": dict(stage.payload)}

    if stage.kind == "fork":
        # Two forms are supported:
        # - Inline JSON array: `| fork [ ... ]` -> payload is a list of JSON values.
        # - Fork block: `| fork` with `branch ... endbranch` blocks -> payload is list[Query].
        if isinstance(stage.payload, list) and all(isinstance(x, Query) for x in stage.payload):
            fork_pipelines = [compile_pipeline(q, now_ms=now_ms) for q in stage.payload]
            return {"fork": fork_pipelines}

        if not isinstance(stage.payload, list):
            raise CompileError("fork stage payload must be a JSON array or list of Queries")

        return {"fork": stage.payload}

    if stage.kind == "sessionReplays":
        if not isinstance(stage.payload, dict):
            raise CompileError("sessionReplays stage payload must be a JSON object")
        return {"sessionReplays": dict(stage.payload)}

    if stage.kind == "switch":
        out_var = stage.payload["out"]
        field = stage.payload["field"]
        cases = list(stage.payload["cases"])
        return {"switch": {out_var: {field: cases}}}

    if stage.kind == "spawn":
        branches = stage.payload
        spawn_pipelines = [compile_pipeline(q, now_ms=now_ms) for q in branches]
        return {"spawn": spawn_pipelines}

    if stage.kind == "raw":
        # User-provided stage object.
        return dict(stage.payload)

    if stage.kind == "limit":
        return {"limit": int(stage.payload)}

    if stage.kind == "sort":
        keys = list(stage.payload)
        return {"sort": keys}

    if stage.kind == "group":
        grp = stage.payload
        if getattr(grp, "emit_map", False):
            fields_map: dict[str, Any] = {}
            for alias, agg, arg in grp.fields:
                fields_map[alias] = {agg: None if arg is None else arg}
            return {"group": {"group": grp.group, "fields": fields_map}}

        fields_json: list[dict[str, Any]] = []
        for alias, agg, arg in grp.fields:
            if arg is None:
                fields_json.append({alias: {agg: None}})
            else:
                fields_json.append({alias: {agg: arg}})
        return {"group": {"group": grp.group, "fields": fields_json}}

    raise CompileError(f"Unknown stage kind: {stage.kind}")


def _main(argv: list[str]) -> int:
    if len(argv) != 2:
        print("usage: python compiler.py path/to/query.dsl", file=sys.stderr)
        return 2

    path = argv[1]
    try:
        try:
            from .parser import parse
        except ImportError:  # pragma: no cover
            from parser import parse  # type: ignore

        with open(path, "r", encoding="utf-8") as f:
            dsl = f.read()
        q = parse(dsl)
        body = compile_to_pendo_aggregation(q)
        json.dump(body, sys.stdout, indent=2, ensure_ascii=False)
        sys.stdout.write("\n")
        return 0
    except Exception as e:
        print(f"error: {e}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(_main(sys.argv))
