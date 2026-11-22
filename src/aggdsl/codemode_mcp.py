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
        """Run read-only Pendo Aggregation API analysis from JavaScript.

        Available API: await pendo.aggdsl.run(dslString).
        Compose multiple calls and return compact JSON-serializable insights.
        Each call compiles aggDSL and runs one Pendo aggregation. No writes.
        """
        try:
            return execute_code(code)
        except CodeModeError as exc:
            return {"error": str(exc)}

    server.run(transport="stdio")


if __name__ == "__main__":
    main()
