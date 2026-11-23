"""Write the tool catalog as a tree of typed files an agent can browse.

This is the presentation described in Anthropic's "Code execution with MCP":
one file per tool, so an agent with a filesystem lists a directory and reads
only the definitions it needs. Nothing has to be loaded into context up front.
Response types come from learned result shapes, because the upstream tools
publish no output schema.
"""
from __future__ import annotations

from pathlib import Path
import re
from typing import Any

from .catalog import INTENT_PARAM, Catalog, Tool, _squash, render_tree
from .recipes import RecipeStore

CLIENT = '''// Supplied by the host: forwards one call to the MCP server and returns its parsed JSON result.
// The connection and its credentials live in the host, never in generated code.
export async function callMCPTool<T>(tool: string, input: unknown): Promise<T> {
  const hook = (globalThis as { callMCPTool?: (tool: string, input: unknown) => Promise<unknown> }).callMCPTool;
  if (!hook) throw new Error('callMCPTool is not provided by this environment');
  return (await hook(tool, input)) as T;
}
'''


def _type_name(tool: Tool) -> str:
    return re.sub(r"[^A-Za-z0-9]", "", tool.name[:1].upper() + tool.name[1:])


def tool_file(catalog: Catalog, tool: Tool) -> str:
    name = _type_name(tool)
    sections = tool.sections()
    doc = [_squash(sections[""], 500)] if sections[""] else []
    doc += [f"{heading}: {_squash(body, 1200)}" for heading, body in sections.items() if heading in {"RETURNS", "WORKFLOW"}]
    visible = {k: v for k, v in tool.properties.items() if k != INTENT_PARAM}
    schema = {**tool.input_schema, "properties": visible}
    fields = catalog._fields(schema, 300, optional=set(catalog.defaults))
    uses_shared = sorted(n for n in catalog._shared_schema if re.search(rf"\b{n}\b", "\n".join(fields)))
    known = tool.name in catalog.trees
    response = render_tree(catalog.trees[tool.name]) if known else "unknown"
    lines = ['import { callMCPTool } from "../../client.js";']
    if uses_shared:
        lines.append(f'import type {{ {", ".join(uses_shared)} }} from "./types.js";')
    lines += ["", f"export interface {name}Input {{", *fields, "}", ""]
    lines.append("/** Structure seen in real results (? = not always present). */" if known
                 else "/** No result has been observed yet; inspect one before relying on its fields. */")
    lines += [f"export type {name}Response = {response};", "", "/**", *[f" * {line}" for line in doc], " */"]
    optional = "" if set(schema.get("required") or []) - set(catalog.defaults) else " = {}"
    lines += [
        f"export async function {tool.name}(input: {name}Input{optional}): Promise<{name}Response> {{",
        f"  return callMCPTool<{name}Response>({tool.name!r}, input);".replace("'", '"'),
        "}",
        "",
    ]
    return "\n".join(lines)


def write_tree(catalog: Catalog, out: str | Path, *, server: str = "pendo", recipes: RecipeStore | None = None) -> dict[str, Any]:
    """Write servers/<server>/*.ts, client.ts and, when recipes exist, a skills folder."""
    root = Path(out)
    directory = root / "servers" / server
    directory.mkdir(parents=True, exist_ok=True)
    (root / "client.ts").write_text(CLIENT, encoding="utf-8")
    index = [f"// {len(catalog.tools)} tools. One line each; open ./<name>.ts for argument types and the result shape."]
    sizes = {}
    for tool in catalog.tools.values():
        if not re.match(r"^[A-Za-z_$][A-Za-z0-9_$]*$", tool.name):
            continue  # not a valid module or function name; reach it through search instead
        text = tool_file(catalog, tool)
        (directory / f"{tool.name}.ts").write_text(text, encoding="utf-8")
        sizes[tool.name] = len(text.encode("utf-8"))
        index += [f"// {catalog.compact(tool)}", f'export * from "./{tool.name}.js";']
    shared = catalog._shared_block(" ".join(catalog._shared_schema))
    if shared:
        (directory / "types.ts").write_text(re.sub(r"(?m)^type ", "export type ", shared) + "\n", encoding="utf-8")
    index_text = "\n".join(index) + "\n"
    (directory / "index.ts").write_text(index_text, encoding="utf-8")
    saved = recipes.all() if recipes else {}
    if saved:
        skills = root / "skills" / f"{server}-analytics"
        skills.mkdir(parents=True, exist_ok=True)
        listing = []
        for name, entry in saved.items():
            (skills / f"{name}.js").write_text(f"// {entry['description']}\n{entry['code']}", encoding="utf-8")
            listing.append(f"- `{name}.js`: {entry['description']}")
        (skills / "SKILL.md").write_text(
            f"---\nname: {server}-analytics\n"
            f"description: Saved {server.capitalize()} analytics programs that already work. Use one before writing a new "
            "program for the same question.\n---\n\n"
            f"Each file is the body of an async function that reads its inputs from `params` and calls tools from "
            f"`servers/{server}`. Read only the file you need.\n\n" + "\n".join(listing) + "\n",
            encoding="utf-8",
        )
    return {"tools": len(sizes), "index_bytes": len(index_text.encode("utf-8")),
            "tool_file_bytes": sum(sizes.values()), "largest": max(sizes.values(), default=0), "recipes": len(saved)}
