from __future__ import annotations

import json
import re
from dataclasses import dataclass
from typing import Any

try:
    from .dsl_ast import EventSource, Query, Stage, TimeSeries
except ImportError:  
    # Allows running/importing from within src/aggdsl directly
    from dsl_ast import EventSource, Query, Stage, TimeSeries  # type: ignore


class DslParseError(ValueError):
    pass


_WS_RE = re.compile(r"\s+")


def _split_kv_pairs(text: str) -> dict[str, Any]:
    """Parse `key=value` pairs separated by whitespace.

    Values may be quoted strings or bare tokens.
    """
    parts = _split_tokens_preserving_groups(text.strip())
    out: dict[str, Any] = {}
    for part in parts:
        if "=" not in part:
            raise DslParseError(f"Expected key=value, got: {part}")
        key, value = part.split("=", 1)
        out[key.strip()] = _parse_scalar(value.strip())
    return out


def _split_tokens_preserving_groups(text: str) -> list[str]:
    """Split by whitespace, but keep whitespace inside quotes or parentheses.

    Example:
      'first=now() last=date(2025, 1, 1, 12, 30, 00)'
    becomes:
      ['first=now()', 'last=date(2025, 1, 1, 12, 30, 00)']
    """
    tokens: list[str] = []
    buf: list[str] = []
    in_quotes = False
    escape = False
    paren_depth = 0

    def flush() -> None:
        s = "".join(buf).strip()
        if s:
            tokens.append(s)

    for ch in text:
        if escape:
            buf.append(ch)
            escape = False
            continue
        if ch == "\\":
            escape = True
            buf.append(ch)
            continue
        if ch == '"':
            in_quotes = not in_quotes
            buf.append(ch)
            continue

        if not in_quotes:
            if ch == "(":
                paren_depth += 1
            elif ch == ")" and paren_depth > 0:
                paren_depth -= 1

            if ch.isspace() and paren_depth == 0:
                flush()
                buf = []
                continue

        buf.append(ch)

    flush()
    return tokens


def _parse_scalar(token: str) -> Any:
    if token.startswith('"') and token.endswith('"') and len(token) >= 2:
        return token[1:-1]
    if token == "[]":
        return []
    if token.isdigit() or (token.startswith("-") and token[1:].isdigit()):
        return int(token)
    return token


def _parse_bracket_args(text: str) -> dict[str, Any]:
    """Parse `[a=1,b="x"]` style lists with quotes."""
    s = text.strip()
    if not (s.startswith("[") and s.endswith("]")):
        raise DslParseError(f"Expected [..] argument list, got: {text}")
    inner = s[1:-1].strip()
    if not inner:
        return {}

    items: list[str] = []
    buf: list[str] = []
    in_quotes = False
    escape = False
    for ch in inner:
        if escape:
            buf.append(ch)
            escape = False
            continue
        if ch == "\\":
            escape = True
            buf.append(ch)
            continue
        if ch == '"':
            in_quotes = not in_quotes
            buf.append(ch)
            continue
        if ch == "," and not in_quotes:
            items.append("".join(buf).strip())
            buf = []
            continue
        buf.append(ch)
    if buf:
        items.append("".join(buf).strip())

    out: dict[str, Any] = {}
    for item in items:
        if not item:
            continue
        if "=" not in item:
            raise DslParseError(f"Expected key=value inside [], got: {item}")
        key, value = item.split("=", 1)
        out[key.strip()] = _parse_scalar(value.strip())
    return out


_FROM_RE = re.compile(r"^FROM\s+event\((?P<args>\[.*\])\)\s*$", re.IGNORECASE)
_TIMESERIES_RE = re.compile(r"^TIMESERIES\s+(?P<rest>.+)$", re.IGNORECASE)
_PIPE_RE = re.compile(r"^\|\s*(?P<rest>.+)$")
_RESPONSE_RE = re.compile(r"^RESPONSE\s+(?P<rest>.+)$", re.IGNORECASE)
_REQUEST_RE = re.compile(r"^REQUEST\s+(?P<rest>.+)$", re.IGNORECASE)
_PIPELINE_RE = re.compile(r"^PIPELINE\s*$", re.IGNORECASE)
_SWITCH_RE = re.compile(
    r"^switch\s+(?P<out>[A-Za-z_][A-Za-z0-9_]*)\s+from\s+(?P<field>[A-Za-z_][A-Za-z0-9_.]*)\s*\{(?P<body>.*)\}\s*$",
    re.IGNORECASE,
)
_MERGE_HEADER_RE = re.compile(
    r"^merge\s+fields\s*\[(?P<fields>[^\]]*)\](?:\s+mappings\s*(?P<mappings>\{.*\}))?\s*$",
    re.IGNORECASE,
)


@dataclass(frozen=True)
class _GroupFields:
    group: list[str]
    fields: list[tuple[str, str, str | None]]
    emit_map: bool = False


def parse(dsl: str) -> Query:
    lines = [ln.strip() for ln in dsl.splitlines()]
    lines = [ln for ln in lines if ln and not ln.startswith("#")]
    if not lines:
        raise DslParseError("Empty DSL")

    response_mime_type = "application/json"
    request_name: str | None = None

    # Optional header lines before FROM
    idx = 0
    while idx < len(lines):
        rm = _RESPONSE_RE.match(lines[idx])
        if rm:
            kv = _split_kv_pairs(rm.group("rest"))
            # Supported: mimeType=..., location=request (ignored for now, always request)
            if "mimeType" in kv:
                response_mime_type = str(kv["mimeType"])
            idx += 1
            continue

        rq = _REQUEST_RE.match(lines[idx])
        if rq:
            kv = _split_kv_pairs(rq.group("rest"))
            if "name" in kv:
                request_name = str(kv["name"])
            idx += 1
            continue

        break

    if idx >= len(lines):
        raise DslParseError("Missing FROM or PIPELINE")

    event_source: EventSource | None = None
    time_series: TimeSeries | None = None

    if _PIPELINE_RE.match(lines[idx]):
        # Pipeline-only query: stages start immediately after PIPELINE.
        idx += 1
    else:
        m = _FROM_RE.match(lines[idx])
        if not m:
            raise DslParseError("Expected: FROM event([..]) or PIPELINE")
        args = _parse_bracket_args(m.group("args"))
        if "source" not in args:
            raise DslParseError("FROM event([...]) requires source=...")
        source_type = str(args.pop("source"))
        event_source = EventSource(source_type=source_type, params=args)

        idx += 1
        if idx < len(lines):
            tm = _TIMESERIES_RE.match(lines[idx])
            if tm:
                kv = _split_kv_pairs(tm.group("rest"))
                if "period" not in kv or "first" not in kv:
                    raise DslParseError("TIMESERIES requires period=..., first=..., and count=... or last=...")

                has_count = "count" in kv
                has_last = "last" in kv
                if has_count == has_last:
                    raise DslParseError("TIMESERIES requires exactly one of count=... or last=...")

                first = _coerce_time_value(kv["first"])
                if has_count:
                    time_series = TimeSeries(
                        period=str(kv["period"]),
                        first=first,
                        count=int(kv["count"]),
                        last=None,
                    )
                else:
                    last = _coerce_time_value(kv["last"])
                    time_series = TimeSeries(
                        period=str(kv["period"]),
                        first=first,
                        count=None,
                        last=last,
                    )
                idx += 1

    stages: list[Stage] = []
    while idx < len(lines):
        pm = _PIPE_RE.match(lines[idx])
        if not pm:
            raise DslParseError(f"Expected pipeline stage starting with '|': {lines[idx]}")

        stage_text = pm.group("rest").strip()
        # Spawn block support.
        if stage_text.lower() == "spawn":
            spawn_queries, idx = _parse_spawn_block(lines, idx + 1)
            stages.append(Stage(kind="spawn", payload=spawn_queries))
            continue

        # Merge block support.
        if stage_text.lower().startswith("merge "):
            merge_spec = _parse_merge_header(stage_text)
            merge_query, idx = _parse_merge_block(lines, idx + 1)
            stages.append(
                Stage(
                    kind="merge",
                    payload={
                        "fields": merge_spec["fields"],
                        "mappings": merge_spec["mappings"],
                        "query": merge_query,
                    },
                )
            )
            continue

        # Multiline raw JSON stage support.
        if stage_text.lower().startswith("raw "):
            raw_start = stage_text[len("raw ") :].strip()
            obj, idx = _parse_raw_json_object_multiline(raw_start, lines, idx + 1)
            stages.append(Stage(kind="raw", payload=obj))
            continue

        # Multiline bulkExpand stage support.
        if stage_text.lower().startswith("bulkexpand "):
            bulk_start = stage_text[len("bulkexpand ") :].strip()
            obj, idx = _parse_raw_json_object_multiline(bulk_start, lines, idx + 1)
            stages.append(Stage(kind="bulkExpand", payload=obj))
            continue

        stages.append(_parse_stage(stage_text))
        idx += 1

    return Query(
        event_source=event_source,
        time_series=time_series,
        stages=stages,
        response_mime_type=response_mime_type,
        request_name=request_name,
    )


def _coerce_time_value(value: Any) -> int | str:
    if isinstance(value, int):
        return value
    if isinstance(value, str) and value.lower() == "now()":
        # actual timestamp resolution happens in compiler
        return "now()"
    # Allow raw expressions like date(`firstDate`) or other DSL/pipeline expressions.
    if isinstance(value, str):
        return value
    raise DslParseError(f"Unsupported time value: {value!r}")


def _parse_stage(text: str) -> Stage:
    # filter <expr>
    if text.lower().startswith("filter "):
        return Stage(kind="filter", payload=text[len("filter ") :].strip())

    # identified <field>
    if text.lower().startswith("identified "):
        field = text[len("identified ") :].strip()
        if not field:
            raise DslParseError("identified requires a field, e.g. | identified visitorId")
        return Stage(kind="identified", payload=field)

    # eval { a=b, c=d }
    if text.lower().startswith("eval "):
        return Stage(kind="eval", payload=_parse_brace_map(text[len("eval ") :].strip(), context="eval"))

    # select { a=b, c=d }
    if text.lower().startswith("select "):
        return Stage(kind="select", payload=_parse_brace_map(text[len("select ") :].strip(), context="select"))

    # join fields [a,b,c]
    if text.lower().startswith("join "):
        rest = text[len("join ") :].strip()
        jm = re.match(r"^fields\s*\[(?P<fields>.*)\]\s*$", rest, flags=re.IGNORECASE)
        if not jm:
            raise DslParseError("join syntax: | join fields [field1,field2]")
        fields = [f.strip() for f in jm.group("fields").split(",") if f.strip()]
        if not fields:
            raise DslParseError("join fields [...] cannot be empty")
        return Stage(kind="join", payload={"fields": fields})

    # switch outVar from field { "1"=="abc", "2"=="def" }
    sm = _SWITCH_RE.match(text)
    if sm:
        out_var = sm.group("out")
        field = sm.group("field")
        body = sm.group("body").strip()
        parts = _split_by_comma_respecting_groups(body)
        cases: list[dict[str, str]] = []
        for part in parts:
            m = re.match(r"^(?P<value>.+?)\s*==\s*(?P<id>.+?)\s*$", part)
            if not m:
                raise DslParseError(f"Invalid switch case: {part}")
            value = _parse_scalar(m.group("value").strip())
            ident = _parse_scalar(m.group("id").strip())
            cases.append({"value": str(value), "==": str(ident)})
        return Stage(kind="switch", payload={"out": out_var, "field": field, "cases": cases})

    # unmarshal { field=expr, ... }
    if text.lower().startswith("unmarshal "):
        rest = text[len("unmarshal ") :].strip()
        return Stage(kind="unmarshal", payload=_parse_brace_map(rest, context="unmarshal"))

    # unwind { field=list, index=listIndex }
    if text.lower().startswith("unwind "):
        rest = text[len("unwind ") :].strip()
        raw_map = _parse_brace_map(rest, context="unwind")
        coerced: dict[str, Any] = {}
        for k, v in raw_map.items():
            if isinstance(v, str) and v.lower() == "true":
                coerced[k] = True
            elif isinstance(v, str) and v.lower() == "false":
                coerced[k] = False
            else:
                coerced[k] = v
        return Stage(kind="unwind", payload=coerced)

    # segment id=...  (or segment { id=... })
    if text.lower().startswith("segment"):
        rest = text[len("segment") :].strip()
        if not rest:
            raise DslParseError('segment syntax: | segment id="segmentId"')

        if rest.startswith("{"):
            seg = _parse_brace_map(rest, context="segment")
        elif "=" in rest:
            seg = _split_kv_pairs(rest)
        else:
            # Allow: | segment <id>
            token = rest
            if token.startswith('"') and token.endswith('"') and len(token) >= 2:
                token = token[1:-1]
            seg = {"id": token}

        if "id" not in seg:
            raise DslParseError('segment requires id=..., e.g. | segment id="segmentId"')

        return Stage(kind="segment", payload=seg)

    # bulkExpand { ...json... }
    if text.lower().startswith("bulkexpand "):
        rest = text[len("bulkexpand ") :].strip()
        obj = _parse_raw_json_object(rest)
        return Stage(kind="bulkExpand", payload=obj)

    # raw { ...json... }
    if text.lower().startswith("raw "):
        obj = _parse_raw_json_object(text[len("raw ") :].strip())
        return Stage(kind="raw", payload=obj)

    # limit N
    if text.lower().startswith("limit "):
        n = text[len("limit ") :].strip()
        if not n.isdigit():
            raise DslParseError("limit must be an integer")
        return Stage(kind="limit", payload=int(n))

    # sort -field,+field
    if text.lower().startswith("sort "):
        rest = text[len("sort ") :].strip()
        keys = [k.strip() for k in rest.split(",") if k.strip()]
        if not keys:
            raise DslParseError("sort requires at least one key")
        return Stage(kind="sort", payload=keys)

    # group by a,b fields { x=sum(y) }
    if text.lower().startswith("group "):
        return Stage(kind="group", payload=_parse_group(text))

    raise DslParseError(f"Unknown stage: {text}")


def _parse_merge_header(text: str) -> dict[str, Any]:
    m = _MERGE_HEADER_RE.match(text)
    if not m:
        raise DslParseError(
            "merge syntax: | merge fields [field1,field2] (optional: mappings { out=in, ... })"
        )

    fields_text = m.group("fields").strip()
    fields = [f.strip() for f in fields_text.split(",") if f.strip()]
    if not fields:
        raise DslParseError("merge fields [...] cannot be empty")

    mappings_group = m.group("mappings")
    if mappings_group is None:
        return {"fields": fields, "mappings": None}

    mappings_text = mappings_group.strip()
    mappings = _parse_brace_map(mappings_text, context="mappings")
    return {"fields": fields, "mappings": mappings}


def _parse_merge_block(lines: list[str], idx: int) -> tuple[Query, int]:
    """Parse a merge pipeline block starting at lines[idx].

    Expected form:
            | merge fields [...] (optional: mappings { ... })
      FROM event([source=..., ...])    # or PIPELINE
      TIMESERIES ...                  # optional
            | filter ...                    # internal pipeline stages
            | eval { ... }
      endmerge

    Notes:
      - Internal pipeline stages normally start with `|`.
      - For backwards compatibility, `>>` is also accepted and treated as `|`.
      - Lines `FROM`, `TIMESERIES`, and `PIPELINE` are allowed without prefixes.
      - The block ends at a line `endmerge` (or `| endmerge`).
    """
    inner_lines: list[str] = []
    i = idx
    while i < len(lines):
        line = lines[i]
        control = line
        if control.startswith("|") and not control.startswith("||"):
            control = control[1:].strip()

        if control.lower() == "endmerge":
            i += 1
            break

        if line.startswith(">>"):
            # Legacy prefix: treat `>>` as a normal pipeline stage.
            inner_lines.append("|" + line[2:].lstrip())
        elif line.startswith("||"):
            raise DslParseError("Inside merge block, stages must start with '|' (not '||')")
        else:
            inner_lines.append(line)
        i += 1

    if not inner_lines:
        raise DslParseError("Empty merge block")
    if i > len(lines):
        raise DslParseError("merge block missing 'endmerge'")

    return parse("\n".join(inner_lines)), i


def _parse_spawn_block(lines: list[str], idx: int) -> tuple[list[Query], int]:
    """Parse a spawn block starting at lines[idx].

    Expected form:
      | spawn
      branch
      FROM ...
      TIMESERIES ...
      || identified visitorId
      || switch out from field { ... }
      ...
      endbranch
      branch
      ...
      endbranch
      | endspawn

    Notes:
      - Inside a branch, pipeline stages must start with `||`.
      - The spawn block ends at a top-level line `| endspawn`.
    """
    queries: list[Query] = []
    i = idx
    while i < len(lines):
        line = lines[i]
        control = line
        if control.startswith("|") and not control.startswith("||"):
            control = control[1:].strip()

        if control.lower() == "branch":
            i += 1
            branch_lines: list[str] = []
            in_merge_block = False
            while i < len(lines):
                l2 = lines[i]
                control2 = l2
                if control2.startswith("|") and not control2.startswith("||"):
                    control2 = control2[1:].strip()

                # Allow an optional `|| endmerge` terminator inside a branch.
                if l2.startswith("||") and l2[2:].strip().lower() == "endmerge":
                    control2 = "endmerge"

                if control2.lower() == "endmerge" and in_merge_block:
                    in_merge_block = False

                if control2.lower() == "endbranch":
                    i += 1
                    break

                # Detect merge block start (the merge header stage itself is `|| merge ...`).
                if l2.startswith("||") and l2[2:].lstrip().lower().startswith("merge "):
                    in_merge_block = True

                # Only allow nested pipeline stages via `||`.
                # Exception: inside a merge block, internal stages are normal `|` stages.
                if l2.startswith("|") and not l2.startswith("||") and not in_merge_block:
                    raise DslParseError(
                        "Inside spawn branch, pipeline stages must start with '||'"
                    )
                if l2.startswith("||"):
                    branch_lines.append("|" + l2[2:])
                else:
                    branch_lines.append(l2)
                i += 1

            if not branch_lines:
                raise DslParseError("Empty branch in spawn")

            queries.append(parse("\n".join(branch_lines)))
            continue

        if control.lower().startswith("endspawn") or line.lower().startswith("| endspawn"):
            return queries, i + 1

        raise DslParseError(f"Unexpected line in spawn block: {line}")

    raise DslParseError("spawn block missing '| endspawn'")


def _parse_raw_json_object(text: str) -> dict[str, Any]:
    s = text.strip()
    if not (s.startswith("{") and s.endswith("}")):
        raise DslParseError("raw stage must be a JSON object: | raw { ... }")
    try:
        val = json.loads(s)
    except json.JSONDecodeError as e:
        raise DslParseError(f"Invalid JSON in raw stage: {e}") from e
    if not isinstance(val, dict):
        raise DslParseError("raw stage JSON must be an object")
    return val


def _parse_raw_json_object_multiline(
    first_fragment: str,
    lines: list[str],
    next_idx: int,
) -> tuple[dict[str, Any], int]:
    """Parse a raw JSON object that may span multiple lines.

    The first fragment is the text after `raw` on the current pipe line.
    Continuation lines are consumed until a full JSON object is balanced.
    Returns (parsed_object, next_index_after_consumed_lines).
    """
    buf = [first_fragment]
    candidate = "\n".join(buf).strip()

    def balanced_json_object(s: str) -> bool:
        s = s.strip()
        if not s.startswith("{"):
            return False
        in_quotes = False
        escape = False
        depth = 0
        for ch in s:
            if escape:
                escape = False
                continue
            if ch == "\\":
                escape = True
                continue
            if ch == '"':
                in_quotes = not in_quotes
                continue
            if in_quotes:
                continue
            if ch == "{":
                depth += 1
            elif ch == "}":
                depth -= 1
                if depth == 0:
                    # Only balanced if the remaining non-whitespace is empty.
                    # (Allows trailing whitespace/newlines)
                    pass
        return depth == 0 and s.endswith("}")

    while candidate and not balanced_json_object(candidate) and next_idx < len(lines):
        # Continuation lines must NOT start a new pipe stage.
        if _PIPE_RE.match(lines[next_idx]):
            break
        buf.append(lines[next_idx])
        next_idx += 1
        candidate = "\n".join(buf).strip()

    return _parse_raw_json_object(candidate), next_idx


def _parse_brace_map(text: str, *, context: str) -> dict[str, str]:
    s = text.strip()
    if not (s.startswith("{") and s.endswith("}")):
        raise DslParseError(f"{context} syntax: | {context} {{ a=b, c=d }}")
    inner = s[1:-1].strip()
    if not inner:
        raise DslParseError(f"{context} map cannot be empty")

    items = _split_by_comma_respecting_groups(inner)

    out: dict[str, str] = {}
    for item in items:
        if not item:
            continue
        if "=" not in item:
            raise DslParseError(f"Invalid {context} mapping: {item}")
        key, value = [x.strip() for x in item.split("=", 1)]
        out[key] = value
    return out


def _split_by_comma_respecting_groups(text: str) -> list[str]:
    """Split a,b,c but keep commas inside quotes or parentheses."""
    items: list[str] = []
    buf: list[str] = []
    in_quotes = False
    escape = False
    paren_depth = 0
    bracket_depth = 0

    def flush() -> None:
        s = "".join(buf).strip()
        if s:
            items.append(s)

    for ch in text:
        if escape:
            buf.append(ch)
            escape = False
            continue
        if ch == "\\":
            escape = True
            buf.append(ch)
            continue
        if ch == '"':
            in_quotes = not in_quotes
            buf.append(ch)
            continue

        if not in_quotes:
            if ch == "(":
                paren_depth += 1
            elif ch == ")" and paren_depth > 0:
                paren_depth -= 1
            elif ch == "[":
                bracket_depth += 1
            elif ch == "]" and bracket_depth > 0:
                bracket_depth -= 1

            if ch == "," and paren_depth == 0 and bracket_depth == 0:
                flush()
                buf = []
                continue

        buf.append(ch)

    flush()
    return items


_GROUP_RE = re.compile(
    r"^group\s+by\s+(?P<group>[^\s]+)\s+(?P<fields_kw>fields|fieldsMap|field)(?:\s+(?P<mode>map|list))?\s*\{(?P<fields>.*)\}\s*$",
    re.IGNORECASE,
)


def _parse_group(text: str) -> _GroupFields:
    m = _GROUP_RE.match(text)
    if not m:
        raise DslParseError(
            "Group syntax: | group by field1,field2 fields { alias=sum(field) }"
        )
    group = [g.strip() for g in m.group("group").split(",") if g.strip()]
    mode = (m.group("mode") or "").lower()
    kw = m.group("fields_kw").lower()
    if mode == "map":
        emit_map = True
    elif mode == "list":
        emit_map = False
    else:
        # Back-compat:
        # - `fields { ... }` -> list form
        # - `fieldsMap { ... }` / `field { ... }` -> map form
        emit_map = kw in {"fieldsmap", "field"}
    fields_text = m.group("fields").strip()
    if not fields_text:
        raise DslParseError("group fields {...} cannot be empty")

    # support comma-separated assignments inside fields block
    # Each assignment: alias=agg(arg)
    assignments: list[str] = []
    buf: list[str] = []
    in_quotes = False
    depth = 0
    for ch in fields_text:
        if ch == '"':
            in_quotes = not in_quotes
        if not in_quotes:
            if ch == "(":
                depth += 1
            elif ch == ")":
                depth = max(0, depth - 1)
            elif ch == "," and depth == 0:
                assignments.append("".join(buf).strip())
                buf = []
                continue
        buf.append(ch)
    if buf:
        assignments.append("".join(buf).strip())

    fields: list[tuple[str, str, str | None]] = []
    for a in assignments:
        if not a:
            continue
        if "=" not in a:
            raise DslParseError(f"Invalid fields assignment: {a}")
        alias, expr = [x.strip() for x in a.split("=", 1)]
        em = re.match(r"^(?P<agg>[a-zA-Z_][a-zA-Z0-9_]*)\((?P<arg>.*)\)$", expr)
        if not em:
            raise DslParseError(f"Invalid aggregate expression: {expr}")
        agg = em.group("agg")
        arg = em.group("arg").strip()
        if arg.lower() == "null":
            arg_val: str | None = None
        else:
            arg_val = arg
        fields.append((alias, agg, arg_val))

    return _GroupFields(group=group, fields=fields, emit_map=emit_map)
