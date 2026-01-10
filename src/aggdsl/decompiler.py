from __future__ import annotations

import json
from typing import Any


class DecompileError(ValueError):
    pass


def decompile_pendo_aggregation_to_dsl(body: Any) -> str:
    """Convert a Pendo Aggregation API JSON body (or request pipeline) into aggdsl DSL.

    Supported inputs:
      - Full body: {"response": {...}, "request": [...]}
      - Named request: {"request": {"name": "...", "pipeline": [...]}}


    Unknown/unsupported stages are emitted as `| raw { ... }`.
    """
    normalized = _normalize_body(body)

    lines: list[str] = []

    response = normalized.get("response")
    if isinstance(response, dict) and "mimeType" in response:
        lines.append(f"RESPONSE mimeType={response['mimeType']}")

    request = normalized["request"]
    request_name: str | None = None
    pipeline: list[dict[str, Any]]

    if isinstance(request, dict):
        request_name = request.get("name")
        pipeline_val = request.get("pipeline")
        if not isinstance(pipeline_val, list):
            raise DecompileError("request.pipeline must be a list")
        pipeline = pipeline_val  # type: ignore[assignment]
    else:
        pipeline = request

    if request_name is not None:
        lines.append(f'REQUEST name="{request_name}"')

    # FROM mode if the first stage is a source stage.
    if pipeline and isinstance(pipeline[0], dict) and "source" in pipeline[0]:
        from_lines = _decompile_source_stage(pipeline[0])
        lines.extend(from_lines)
        stage_start_idx = 1
    else:
        lines.append("PIPELINE")
        stage_start_idx = 0

    for stage in pipeline[stage_start_idx:]:
        lines.extend(_decompile_stage(stage, prefix="|"))

    return "\n".join(lines) + "\n"


def _normalize_body(body: Any) -> dict[str, Any]:
    if isinstance(body, list):
        return {"request": body}
    if not isinstance(body, dict):
        raise DecompileError("Expected a JSON object (aggregation body) or a pipeline array")
    if "request" not in body:
        raise DecompileError("Missing 'request' in aggregation body")

    request = body["request"]
    if isinstance(request, list):
        pass
    elif isinstance(request, dict):
        # named request format
        if "pipeline" not in request:
            raise DecompileError("Named request must contain 'pipeline'")
    else:
        raise DecompileError("'request' must be a list or an object")

    return body


def _decompile_source_stage(stage: dict[str, Any]) -> list[str]:
    src = stage.get("source")
    if not isinstance(src, dict):
        return ["PIPELINE", *_decompile_stage(stage, prefix="|")]

    # src is like: {"pageEvents": {...}, "timeSeries": {...}}
    source_type: str | None = None
    source_params: dict[str, Any] | None = None

    for k, v in src.items():
        if k == "timeSeries":
            continue
        # Some exports represent the event config as null, e.g. {"featureEvents": null}.
        if v is None:
            source_type = str(k)
            source_params = {}
            break
        if isinstance(v, dict):
            source_type = str(k)
            source_params = v
            break

    if source_type is None or source_params is None:
        raise DecompileError("source stage missing event source")

    bracket_parts: list[str] = [f"source={source_type}"]
    for k, v in source_params.items():
        bracket_parts.append(f"{k}={_format_bracket_value(v)}")

    lines = [f"FROM event([{','.join(bracket_parts)}])"]

    ts = src.get("timeSeries")
    if isinstance(ts, dict):
        period = ts.get("period")
        first = ts.get("first")
        if period is None or first is None:
            raise DecompileError("timeSeries requires period and first")

        ts_parts = [f"period={period}", f"first={_format_time_value(first)}"]
        if "count" in ts:
            ts_parts.append(f"count={ts['count']}")
        elif "last" in ts:
            ts_parts.append(f"last={_format_time_value(ts['last'])}")
        else:
            raise DecompileError("timeSeries requires count or last")

        lines.append("TIMESERIES " + " ".join(ts_parts))

    return lines


def _decompile_stage(stage: Any, *, prefix: str) -> list[str]:
    if not isinstance(stage, dict) or not stage:
        return [f"{prefix} raw {_json_one_line(stage)}"]

    if "filter" in stage and len(stage) == 1:
        return [f"{prefix} filter {stage['filter']}"]

    if "identified" in stage and len(stage) == 1:
        return [f"{prefix} identified {stage['identified']}"]

    if "eval" in stage and len(stage) == 1 and isinstance(stage["eval"], dict):
        return [f"{prefix} eval {_format_brace_map(stage['eval'])}"]

    if "select" in stage and len(stage) == 1 and isinstance(stage["select"], dict):
        return [f"{prefix} select {_format_brace_map(stage['select'])}"]

    if "join" in stage and len(stage) == 1 and isinstance(stage["join"], dict):
        join = stage["join"]
        fields = join.get("fields")
        if isinstance(fields, list) and all(isinstance(x, str) for x in fields):
            return [f"{prefix} join fields [{','.join(fields)}]"]

    if "merge" in stage and len(stage) == 1 and isinstance(stage["merge"], dict):
        merge = stage["merge"]
        fields = merge.get("fields")
        pipeline = merge.get("pipeline")
        if (
            isinstance(fields, list)
            and all(isinstance(x, str) for x in fields)
            and isinstance(pipeline, list)
        ):
            mappings = merge.get("mappings")
            if mappings is None:
                out: list[str] = [f"{prefix} merge fields [{','.join(fields)}]"]
            elif isinstance(mappings, dict):
                out = [
                    f"{prefix} merge fields [{','.join(fields)}] mappings {_format_brace_map(mappings)}"
                ]
            else:
                return [f"{prefix} raw {_json_one_line(stage)}"]

            out.extend(_decompile_merge_pipeline(pipeline))
            out.append("endmerge")
            return out

    if "unmarshal" in stage and len(stage) == 1 and isinstance(stage["unmarshal"], dict):
        return [f"{prefix} unmarshal {_format_brace_map(stage['unmarshal'])}"]

    if "unwind" in stage and len(stage) == 1 and isinstance(stage["unwind"], dict):
        return [f"{prefix} unwind {_format_brace_map(stage['unwind'])}"]

    if "segment" in stage and len(stage) == 1 and isinstance(stage["segment"], dict):
        seg = stage["segment"]
        seg_id = seg.get("id")
        if isinstance(seg_id, (str, int)):
            # Use quotes for safety; parser will unquote.
            return [f"{prefix} segment id={json.dumps(str(seg_id), ensure_ascii=False)}"]

    if "bulkExpand" in stage and len(stage) == 1 and isinstance(stage["bulkExpand"], dict):
        return [f"{prefix} bulkExpand {_json_one_line(stage['bulkExpand'])}"]

    if "switch" in stage and len(stage) == 1 and isinstance(stage["switch"], dict):
        sw = stage["switch"]
        if len(sw) == 1:
            out_var = next(iter(sw.keys()))
            inner = sw[out_var]
            if isinstance(inner, dict) and len(inner) == 1:
                field = next(iter(inner.keys()))
                cases = inner[field]
                if isinstance(cases, list):
                    cases_text: list[str] = []
                    for c in cases:
                        if not isinstance(c, dict) or "value" not in c or "==" not in c:
                            return [f"{prefix} raw {_json_one_line(stage)}"]
                        cases_text.append(f"{_format_switch_scalar(c['value'])}=={_format_switch_scalar(c['=='])}")
                    return [f"{prefix} switch {out_var} from {field} {{ " + ", ".join(cases_text) + " }"]

    if "group" in stage and len(stage) == 1 and isinstance(stage["group"], dict):
        grp = stage["group"]
        group_fields = grp.get("group")
        fields = grp.get("fields")
        if isinstance(group_fields, list) and isinstance(fields, list):
            group_text = ",".join(str(x) for x in group_fields)
            agg_parts: list[str] = []
            ok = True
            for item in fields:
                if not isinstance(item, dict) or len(item) != 1:
                    ok = False
                    break
                alias = next(iter(item.keys()))
                agg_obj = item[alias]
                if not isinstance(agg_obj, dict) or len(agg_obj) != 1:
                    ok = False
                    break
                agg = next(iter(agg_obj.keys()))
                arg = agg_obj[agg]
                if arg is None:
                    agg_parts.append(f"{alias}={agg}(null)")
                else:
                    agg_parts.append(f"{alias}={agg}({arg})")
            if ok:
                return [f"{prefix} group by {group_text} fields {{ " + ", ".join(agg_parts) + " }"]

        # Also support Pendo's object-map form: fields: { alias: {agg: arg}, ... }
        if isinstance(group_fields, list) and isinstance(fields, dict):
            group_text = ",".join(str(x) for x in group_fields)
            agg_parts: list[str] = []
            for alias, agg_obj in fields.items():
                if not isinstance(agg_obj, dict) or len(agg_obj) != 1:
                    return [f"{prefix} raw {_json_one_line(stage)}"]
                agg = next(iter(agg_obj.keys()))
                arg = agg_obj[agg]
                if arg is None:
                    agg_parts.append(f"{alias}={agg}(null)")
                else:
                    agg_parts.append(f"{alias}={agg}({arg})")
            return [
                f"{prefix} group by {group_text} fields map {{ "
                + ", ".join(agg_parts)
                + " }"
            ]

    if "sort" in stage and len(stage) == 1 and isinstance(stage["sort"], list):
        keys = stage["sort"]
        if all(isinstance(x, str) for x in keys):
            # DSL sort uses comma-separated keys.
            return [f"{prefix} sort " + ",".join(keys)]

    if "limit" in stage and len(stage) == 1:
        return [f"{prefix} limit {stage['limit']}"]

    if "spawn" in stage and len(stage) == 1 and isinstance(stage["spawn"], list):
        branches = stage["spawn"]
        if not all(isinstance(b, list) for b in branches):
            return [f"{prefix} raw {_json_one_line(stage)}"]

        out: list[str] = [f"{prefix} spawn"]
        for branch in branches:
            out.append("branch")
            out.extend(_decompile_branch_pipeline(branch))
            out.append("endbranch")
        out.append("| endspawn" if prefix == "|" else f"{prefix} endspawn")
        return out

    # source stage that isn't first in the pipeline can't be expressed as FROM/TIMESERIES.
    # Preserve it as a raw stage so the pipeline stays semantically equivalent.
    return [f"{prefix} raw {_json_one_line(stage)}"]


def _decompile_merge_pipeline(pipeline: list[Any]) -> list[str]:
    """Decompile a merge.pipeline into merge-block lines.

    Merge blocks use normal `|` pipeline stages.
    """
    out: list[str] = []
    if pipeline and isinstance(pipeline[0], dict) and "source" in pipeline[0]:
        out.extend(_decompile_source_stage(pipeline[0]))
        for st in pipeline[1:]:
            out.extend(_decompile_stage(st, prefix="|"))
        return out

    out.append("PIPELINE")
    for st in pipeline:
        out.extend(_decompile_stage(st, prefix="|"))
    return out


def _decompile_branch_pipeline(pipeline: list[Any]) -> list[str]:
    if pipeline and isinstance(pipeline[0], dict) and "source" in pipeline[0]:
        # Emit FROM/TIMESERIES as plain lines; remaining stages get || prefix.
        lines = _decompile_source_stage(pipeline[0])
        for st in pipeline[1:]:
            lines.extend(_decompile_stage(st, prefix="||"))
        return lines

    # Pipeline-only branch.
    lines = ["PIPELINE"]
    for st in pipeline:
        lines.extend(_decompile_stage(st, prefix="||"))
    return lines


def _format_brace_map(m: dict[str, Any]) -> str:
    # IMPORTANT: keys must be emitted without quotes because the current DSL parser
    # treats the key literally (including quotes) when compiling.
    parts: list[str] = []
    for k, v in m.items():
        parts.append(f"{k}={v}")
    return "{ " + ", ".join(parts) + " }"


def _format_bracket_value(v: Any) -> str:
    if isinstance(v, str):
        # In bracket args, strings must be quoted to be parsed as strings.
        return json.dumps(v, ensure_ascii=False)
    return str(v)


def _format_time_value(v: Any) -> str:
    if isinstance(v, int):
        return str(v)
    if isinstance(v, str):
        return v
    return str(v)


def _format_switch_scalar(v: Any) -> str:
    # Switch cases are safest as quoted strings.
    return json.dumps(str(v), ensure_ascii=False)


def _json_one_line(obj: Any) -> str:
    return json.dumps(obj, ensure_ascii=False, separators=(",", ":"))
