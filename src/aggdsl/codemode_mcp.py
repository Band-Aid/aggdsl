"""Optional MCP server exposing a single execute_pendo_code tool."""
from __future__ import annotations

from .codemode import CodeModeError, execute_code


def main() -> None:
    try:
        from mcp.server.fastmcp import FastMCP
    except ImportError as exc:
        raise SystemExit("Install the MCP extra: pip install -e '.[codemode]'") from exc

    server = FastMCP("aggdsl Code Mode")

    @server.tool()
    def execute_pendo_code(code: str) -> dict:
        """Analyze Pendo with generated JavaScript, not a fixed set of samples.

        Exposes these read-only JavaScript methods inside one tool invocation:
        - await pendo.help('workflow'|'syntax'|'sources'|'examples'|'troubleshooting'|'grammar'|'patterns'|'pattern:<name>')
        - await pendo.catalog.search({type:'pages'|'features'|'trackTypes'|'guides',
                                      query:'checkout', appId?:'123', limit?:20})
        - await pendo.catalog.apps() // observed App IDs only, NOT all apps
        - await pendo.aggdsl.validate(dslString) // compile-only diagnostics
        - await pendo.aggdsl.run(dslString) // live Aggregation API result

        Workflow: discover a real entity and app ID, generate new DSL, validate
        its syntax, run it, and return a compact summary. Use Promise.all for
        independent calls. Await every call. Only return JSON-serializable
        values. No writes. The compiler cannot verify live Pendo field names.
        Integration keys grant subscription-wide access; local/trusted PoC only.
        """
        try:
            return execute_code(code)
        except CodeModeError as exc:
            return {"error": str(exc)}

    server.run(transport="stdio")


if __name__ == "__main__":
    main()
