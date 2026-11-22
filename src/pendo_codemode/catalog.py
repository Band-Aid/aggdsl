"""Turn upstream MCP tool definitions into a JavaScript API description.

The catalog is built from `tools/list`, so Code Mode follows whatever the Pendo
MCP server exposes; no Pendo tool name or schema is hard-coded here.
"""
from __future__ import annotations

from dataclasses import dataclass, field
import json
import re
from typing import Any, Iterable

_IDENT = re.compile(r"^[A-Za-z_$][A-Za-z0-9_$]*$")
_WORD = re.compile(r"[a-z0-9]+")


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
    def summary(self) -> str:
        first = self.description.split("\n", 1)[0].strip()
        match = re.match(r"(.+?[.!?])(\s|$)", first)
        return (match.group(1) if match else first)[:160]

    def signature(self) -> str:
        """TypeScript declaration of the tool as a function on `pendo`."""
        lines = []
        if self.description:
            lines.append("/** " + " ".join(self.description.split())[:600] + " */")
        returns = ts_type(self.output_schema) if self.output_schema else "unknown"
        properties = self.input_schema.get("properties")
        if not isinstance(properties, dict) or not properties:
            lines.append(f"{self.accessor}(args?: {{}}): Promise<{returns}>")
            return "\n".join(lines)
        required = set(self.input_schema.get("required") or [])
        lines.append(f"{self.accessor}(args: {{")
        for key, schema in properties.items():
            note = ""
            if isinstance(schema, dict) and schema.get("description"):
                note = " // " + " ".join(str(schema["description"]).split())[:200]
            lines.append(f"  {_key(key)}{'' if key in required else '?'}: {ts_type(schema)};{note}")
        lines.append(f"}}): Promise<{returns}>")
        return "\n".join(lines)


def _key(name: str) -> str:
    return name if _IDENT.match(name) else json.dumps(name)


def ts_type(schema: Any, depth: int = 0) -> str:
    """Render a JSON Schema as a compact TypeScript type (best effort)."""
    if not isinstance(schema, dict) or depth > 6:
        return "unknown"
    if "const" in schema:
        return json.dumps(schema["const"])
    if isinstance(schema.get("enum"), list) and schema["enum"]:
        return " | ".join(json.dumps(v) for v in schema["enum"])
    for union in ("anyOf", "oneOf"):
        if isinstance(schema.get(union), list) and schema[union]:
            return " | ".join(dict.fromkeys(ts_type(s, depth + 1) for s in schema[union]))
    kind = schema.get("type")
    if isinstance(kind, list):
        return " | ".join(dict.fromkeys(ts_type({**schema, "type": k}, depth + 1) for k in kind))
    if kind == "array":
        item = ts_type(schema.get("items"), depth + 1)
        return f"({item})[]" if " " in item and not item.startswith("{") else f"{item}[]"
    if kind == "object" or "properties" in schema:
        properties = schema.get("properties")
        if not isinstance(properties, dict) or not properties:
            return "Record<string, unknown>"
        required = set(schema.get("required") or [])
        parts = [
            f"{_key(k)}{'' if k in required else '?'}: {ts_type(v, depth + 1)}" for k, v in properties.items()
        ]
        return "{ " + "; ".join(parts) + " }"
    return {"string": "string", "number": "number", "integer": "number", "boolean": "boolean", "null": "null"}.get(
        kind, "unknown"
    )


def _tokens(text: str) -> set[str]:
    # Split camelCase and snake_case so "activity" finds activityQuery.
    spaced = re.sub(r"([a-z0-9])([A-Z])", r"\1 \2", text)
    return set(_WORD.findall(spaced.lower()))


class Catalog:
    """The tools a program may call, after the read-only policy is applied."""

    def __init__(
        self,
        tools: Iterable[Tool],
        *,
        allow_writes: bool = False,
        allow_tools: Iterable[str] = (),
    ) -> None:
        allowed = set(allow_tools)
        self.tools: dict[str, Tool] = {}
        self.blocked: list[str] = []
        for tool in tools:
            if not tool.name:
                continue
            if tool.read_only or allow_writes or tool.name in allowed:
                self.tools[tool.name] = tool
            else:
                self.blocked.append(tool.name)

    def names(self) -> list[str]:
        return list(self.tools)

    def index(self) -> str:
        """One line per tool; small enough to live in a tool description."""
        return "\n".join(f"- {t.accessor}: {t.summary}" for t in self.tools.values())

    def search(self, query: str, limit: int = 8) -> list[Tool]:
        wanted = _tokens(query)
        if not wanted:
            return list(self.tools.values())[:limit]
        scored = []
        for position, tool in enumerate(self.tools.values()):
            name, text = _tokens(tool.name), _tokens(tool.description)
            params = _tokens(" ".join((tool.input_schema.get("properties") or {}).keys()))
            score = sum(4 * (w in name) + (w in text) + (w in params) for w in wanted)
            if query.strip().lower() == tool.name.lower():
                score += 100
            if score:
                scored.append((-score, position, tool))
        return [tool for _score, _position, tool in sorted(scored)[:limit]]

    def describe(self, query: str = "", limit: int = 8) -> str:
        matches = self.search(query, limit)
        if not matches:
            return f"No Pendo tool matches {query!r}. Available tools:\n{self.index()}"
        return "\n\n".join(tool.signature() for tool in matches)
