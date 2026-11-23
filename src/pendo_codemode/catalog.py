"""Turn upstream MCP tool definitions into a JavaScript API description.

The catalog is built from `tools/list`, so Code Mode follows whatever the Pendo
MCP server exposes; no Pendo tool name or schema is hard-coded here.

The real server has well over a hundred tools whose descriptions run to
several kilobytes each, so the catalog is tiered: a names-only index, short
signatures that keep the RETURNS section (code needs the result shape), and
full detail on request. Parameter types repeated across many tools, such as
`dateRange`, are declared once as shared types.
"""
from __future__ import annotations

from collections import Counter
from dataclasses import dataclass, field
from fnmatch import fnmatchcase
import json
import re
from typing import Any, Iterable, Mapping

_IDENT = re.compile(r"^[A-Za-z_$][A-Za-z0-9_$]*$")
_WORD = re.compile(r"[a-z0-9]+")
_SECTION = re.compile(r"^([A-Z][A-Z ]{2,30}):[ \t]*", re.MULTILINE)
_VERB = re.compile(r"^[a-z]+")

# Telemetry arguments the host fills in itself; they only add noise to signatures.
INTENT_PARAM = "userQuery"


def _squash(text: Any, limit: int) -> str:
    flat = " ".join(str(text).split())
    return flat if len(flat) <= limit else flat[: limit - 1].rstrip() + "…"


@dataclass(frozen=True)
class Tool:
    name: str
    description: str = ""
    input_schema: dict[str, Any] = field(default_factory=dict)
    output_schema: dict[str, Any] | None = None
    read_only: bool = False

    @classmethod
    def from_mcp(cls, raw: dict[str, Any]) -> "Tool":
        annotations = raw.get("annotations") or {}
        return cls(
            name=str(raw.get("name", "")),
            description=str(raw.get("description") or "").strip(),
            input_schema=raw.get("inputSchema") or {},
            output_schema=raw.get("outputSchema"),
            read_only=annotations.get("readOnlyHint") is True,
        )

    @property
    def accessor(self) -> str:
        return f"pendo.{self.name}" if _IDENT.match(self.name) else f"pendo[{json.dumps(self.name)}]"

    @property
    def properties(self) -> dict[str, Any]:
        properties = self.input_schema.get("properties")
        return properties if isinstance(properties, dict) else {}

    def sections(self) -> dict[str, str]:
        """Split a description into its lead text and `HEADING:` sections."""
        parts = _SECTION.split(self.description)
        found = {"": parts[0].strip()}
        for heading, body in zip(parts[1::2], parts[2::2]):
            found[heading.strip()] = body.strip()
        return found

    @property
    def summary(self) -> str:
        lead = " ".join(self.sections()[""].split())
        match = re.match(r"(.+?[.!?])(\s|$)", lead)
        return _squash(match.group(1) if match else lead, 160)


def _key(name: str) -> str:
    return name if _IDENT.match(name) else json.dumps(name)


def _shape(schema: Any) -> str:
    """Canonical form of a schema ignoring prose and defaults, for de-duplication."""
    if isinstance(schema, dict):
        return json.dumps(
            {k: json.loads(_shape(v)) for k, v in sorted(schema.items()) if k not in {"description", "default"}}
        )
    if isinstance(schema, list):
        return json.dumps([json.loads(_shape(v)) for v in schema])
    return json.dumps(schema)


def ts_type(schema: Any, depth: int = 0, shared: Mapping[str, str] | None = None) -> str:
    """Render a JSON Schema as a compact TypeScript type (best effort)."""
    if not isinstance(schema, dict) or depth > 6:
        return "unknown"
    if shared and depth:
        name = shared.get(_shape(schema))
        if name:
            return name
    if "const" in schema:
        return json.dumps(schema["const"])
    if isinstance(schema.get("enum"), list) and schema["enum"]:
        return " | ".join(json.dumps(v) for v in schema["enum"])
    for union in ("anyOf", "oneOf"):
        if isinstance(schema.get(union), list) and schema[union]:
            return " | ".join(dict.fromkeys(ts_type(s, depth + 1, shared) for s in schema[union]))
    kind = schema.get("type")
    if isinstance(kind, list):
        return " | ".join(dict.fromkeys(ts_type({**schema, "type": k}, depth + 1, shared) for k in kind))
    if kind == "array":
        item = ts_type(schema.get("items"), depth + 1, shared)
        return f"({item})[]" if " " in item and not item.startswith("{") else f"{item}[]"
    if kind == "object" or "properties" in schema:
        properties = schema.get("properties")
        if not isinstance(properties, dict) or not properties:
            return "Record<string, unknown>"
        required = set(schema.get("required") or [])
        parts = [
            f"{_key(k)}{'' if k in required else '?'}: {ts_type(v, depth + 1, shared)}"
            for k, v in properties.items()
        ]
        return "{ " + "; ".join(parts) + " }"
    return {"string": "string", "number": "number", "integer": "number", "boolean": "boolean", "null": "null"}.get(
        kind, "unknown"
    )


def shape_of(value: Any, depth: int = 0) -> str:
    """Describe a JSON value's structure compactly: key names and types, no data."""
    if isinstance(value, dict):
        if depth > 3:
            return "{…}"
        parts = [f"{_key(k)}: {shape_of(v, depth + 1)}" for k, v in list(value.items())[:14]]
        return "{" + ", ".join(parts + (["…"] if len(value) > 14 else [])) + "}"
    if isinstance(value, list):
        if not value:
            return "unknown[]"
        items = value[:5]
        if all(isinstance(item, dict) for item in items):
            merged: dict[str, Any] = {}
            for item in items:
                for key, inner in item.items():
                    if merged.get(key) is None:
                        merged[key] = inner  # prefer a non-null sample of each field
            common = set.intersection(*(set(item) for item in items))
            body = shape_of({(k if k in common else f"{k}?"): v for k, v in merged.items()}, depth)
            return body.replace('"', "") + "[]" if depth <= 3 else "{…}[]"
        return shape_of(items[0], depth + 1) + "[]"
    if isinstance(value, bool):
        return "boolean"
    if isinstance(value, (int, float)):
        return "number"
    return "null" if value is None else "string"


def brief_shape(value: Any, depth: int = 0) -> str:
    """Key names only, for one-line listings: {summary: {total}, rows: [{id, name}]}."""
    if isinstance(value, list):
        dicts = [item for item in value[:5] if isinstance(item, dict)]
        if not dicts:
            return "[]"
        merged: dict[str, Any] = {}
        for item in dicts:
            for key, inner in item.items():
                if merged.get(key) is None:
                    merged[key] = inner
        return "[" + brief_shape(merged, depth) + "]"
    if not isinstance(value, dict):
        return ""
    parts = []
    for key, inner in list(value.items())[:24]:
        nested = brief_shape(inner, depth + 1) if depth < 2 and isinstance(inner, (dict, list)) else ""
        parts.append(f"{_key(key)}: {nested}" if nested not in ("", "[]") else (f"{_key(key)}[]" if nested == "[]" else _key(key)))
    return "{" + ", ".join(parts) + "}"


def _tokens(text: str) -> set[str]:
    # Split camelCase and snake_case so "activity" finds activityQuery; fold plurals.
    spaced = re.sub(r"([a-z0-9])([A-Z])", r"\1 \2", text)
    return {w[:-1] if len(w) > 3 and w.endswith("s") else w for w in _WORD.findall(spaced.lower())}


class Catalog:
    """The tools a program may call, after the read-only policy is applied."""

    def __init__(
        self,
        tools: Iterable[Tool],
        *,
        allow_writes: bool = False,
        allow_tools: Iterable[str] = (),
        defaults: Mapping[str, Any] | None = None,
    ) -> None:
        patterns = list(allow_tools)  # exact names or globs such as "list*"
        self.defaults: dict[str, Any] = dict(defaults or {})
        self.tools: dict[str, Tool] = {}
        self.blocked: list[str] = []
        for tool in tools:
            if not tool.name:
                continue
            if tool.read_only or allow_writes or any(fnmatchcase(tool.name, p) for p in patterns):
                self.tools[tool.name] = tool
            else:
                self.blocked.append(tool.name)
        self.shared = self._find_shared_types()
        # Result shapes seen in real calls. Pendo's tools publish no output schema, so this
        # is how a program learns what a tool returns without a trial run.
        self.shapes: dict[str, str] = {}
        self.briefs: dict[str, str] = {}

    def observe(self, name: str, value: Any) -> None:
        """Remember the structure of a tool's result (structure only, never data)."""
        shape = shape_of(value)
        if name in self.tools and len(shape) > len(self.shapes.get(name, "")):
            self.shapes[name] = shape
            self.briefs[name] = brief_shape(value)

    def load_shapes(self, path: Any) -> None:
        try:
            with open(path, encoding="utf-8") as handle:
                stored = json.load(handle)
        except (OSError, ValueError):
            return
        for name, entry in stored.items():
            if name in self.tools and isinstance(entry, dict) and isinstance(entry.get("shape"), str):
                self.shapes[name] = entry["shape"]
                self.briefs[name] = str(entry.get("brief") or "")

    def save_shapes(self, path: Any) -> None:
        with open(path, "w", encoding="utf-8") as handle:
            stored = {name: {"shape": shape, "brief": self.briefs.get(name, "")} for name, shape in self.shapes.items()}
            json.dump(stored, handle, ensure_ascii=False, indent=1, sort_keys=True)

    # -- shared parameter types ------------------------------------------------

    def _find_shared_types(self) -> dict[str, str]:
        """Name object-like parameter schemas that at least three tools repeat."""
        uses: Counter[str] = Counter()
        first_key: dict[str, str] = {}
        sample: dict[str, Any] = {}
        for tool in self.tools.values():
            for key, schema in tool.properties.items():
                if not isinstance(schema, dict) or len(ts_type(schema)) < 60:
                    continue
                shape = _shape(schema)
                uses[shape] += 1
                first_key.setdefault(shape, key)
                sample.setdefault(shape, schema)
        shared: dict[str, str] = {}
        taken: set[str] = set()
        for shape, count in uses.items():
            if count < 3:
                continue
            base = first_key[shape][:1].upper() + re.sub(r"[^A-Za-z0-9]", "", first_key[shape][1:])
            name, suffix = base, 2
            while name in taken:
                name, suffix = f"{base}{suffix}", suffix + 1
            taken.add(name)
            shared[shape] = name
        self._shared_schema = {shared[shape]: sample[shape] for shape in shared}
        return shared

    def _fields(self, schema: dict[str, Any], comment: int, optional: set[str] = frozenset()) -> list[str]:
        required = set(schema.get("required") or []) - optional
        lines = []
        for key, prop in (schema.get("properties") or {}).items():
            note = ""
            if key in self.defaults:
                note = f" // pre-set to {json.dumps(self.defaults[key])}"
            elif comment and isinstance(prop, dict) and prop.get("description"):
                note = " // " + _squash(prop["description"], comment)
            lines.append(f"  {_key(key)}{'' if key in required else '?'}: {ts_type(prop, 1, self.shared)};{note}")
        return lines

    # -- rendering --------------------------------------------------------------

    def signature(self, tool: Tool, detail: str = "signature") -> str:
        """TypeScript declaration of one tool. `detail` is 'signature' or 'full'."""
        full = detail == "full"
        sections = tool.sections()
        doc = [_squash(sections[""], 6000 if full else 400)] if sections[""] else []
        for heading, body in sections.items():
            if heading and (full or heading == "RETURNS"):
                doc.append(f"{heading}: {_squash(body, 6000 if full else 900)}")
        if tool.name in self.shapes:
            doc.append(f"RESULT SHAPE (observed): {_squash(self.shapes[tool.name], 6000 if full else 900)}")
        lines = ["/**"] + [f" * {line}" for line in doc] + [" */"] if doc else []
        returns = ts_type(tool.output_schema) if tool.output_schema else "unknown"
        visible = {k: v for k, v in tool.properties.items() if k != INTENT_PARAM}
        if not visible:
            lines.append(f"{tool.accessor}(args?: {{}}): Promise<{returns}>")
            return "\n".join(lines)
        schema = {**tool.input_schema, "properties": visible}
        lines.append(f"{tool.accessor}(args: {{")
        lines.extend(self._fields(schema, 1000 if full else 200, optional=set(self.defaults)))
        lines.append(f"}}): Promise<{returns}>")
        return "\n".join(lines)

    def _shared_block(self, text: str) -> str:
        used = [name for name in self._shared_schema if re.search(rf"\b{name}\b", text)]
        blocks = []
        for name in used:
            schema = self._shared_schema[name]
            if schema.get("properties"):
                blocks.append("\n".join([f"type {name} = {{", *self._fields(schema, 140), "}"]))
            else:
                blocks.append(f"type {name} = {ts_type(schema, 0, None)}")
        return "\n\n".join(blocks)

    def names(self) -> list[str]:
        return list(self.tools)

    def compact(self, tool: Tool) -> str:
        """One line: argument names (optional ones marked ?) and what comes back."""
        required = set(tool.input_schema.get("required") or []) - set(self.defaults)
        names = [k for k in tool.properties if k != INTENT_PARAM and k not in self.defaults]
        names.sort(key=lambda k: k not in required)
        args = ", ".join(k if k in required else f"{k}?" for k in names)
        returns = self.briefs.get(tool.name)
        tail = f" → {_squash(returns, 520)}" if returns else f"  // {_squash(tool.summary, 110)}"
        return f"{tool.accessor}({{{args}}}){tail}"

    def index(self) -> str:
        """Every tool, small enough to live in a tool description."""
        if len(self.tools) <= 25:
            return "\n".join(f"- {self.compact(t)}" for t in self.tools.values())
        groups: dict[str, list[str]] = {}
        for name in self.tools:
            verb = _VERB.match(name)
            groups.setdefault(verb.group(0) if verb else "other", []).append(name)
        return "\n".join(f"- {verb}: {', '.join(names)}" for verb, names in groups.items())

    def search(self, query: str, limit: int = 8) -> list[Tool]:
        wanted = _tokens(query)
        if not wanted:
            return list(self.tools.values())[:limit]
        # An exact tool name is a lookup, not a search: return that tool alone.
        exact = [t for t in self.tools.values() if t.name.lower() == query.strip().lower()]
        if exact:
            return exact
        scored = []
        for position, tool in enumerate(self.tools.values()):
            name, text = _tokens(tool.name), _tokens(tool.description)
            params = _tokens(" ".join(tool.properties))
            score = sum(4 * (w in name) + (w in text) + (w in params) for w in wanted)
            if query.strip().lower() == tool.name.lower():
                score += 100
            if score:
                scored.append((-score, position, tool))
        return [tool for _score, _position, tool in sorted(scored)[:limit]]

    def describe(self, query: str = "", limit: int = 8, detail: str = "signature") -> str:
        matches = self.search(query, limit)
        if not matches:
            return f"No Pendo tool matches {query!r}. Available tools:\n{self.index()}"
        if detail == "summary":
            return "\n".join(f"- {t.accessor}: {t.summary}" for t in matches)
        if detail == "compact":
            return "\n".join(f"- {self.compact(t)}" for t in matches)
        body = "\n\n".join(self.signature(tool, detail) for tool in matches)
        shared = self._shared_block(body)
        return f"{body}\n\n// shared types\n{shared}" if shared else body

    # -- call preparation -------------------------------------------------------

    def bind(self, name: str, arguments: dict[str, Any], intent: str | None = None) -> dict[str, Any]:
        """Add pre-set defaults and the task summary to one call's arguments."""
        properties = self.tools[name].properties
        bound = dict(arguments)
        for key, value in self.defaults.items():
            if key in properties:
                bound.setdefault(key, value)
        if intent and INTENT_PARAM in properties:
            bound.setdefault(INTENT_PARAM, _squash(intent, 200))
        return bound
