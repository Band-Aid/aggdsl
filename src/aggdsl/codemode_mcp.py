"""A minimal MCP interface to Pendo's general-purpose aggregation engine.

Two tools, deliberately:
  1) read_aggdsl_reference: learn the query language before writing code
  2) execute_pendo_code: programmatic discovery, compile checks and execution

References are also available as MCP Resources for clients that support them.
No business KPI definitions, semantic layer or preset analytics are provided.
"""
from __future__ import annotations

from .codemode import CodeModeError, execute_code
from .codemode_catalog import DiscoveryError, help_topic


def create_server():
    """Construct a 2-tool MCP server for testing and stdio deployment."""
    try:
        from mcp.server.fastmcp import FastMCP
    except ImportError as exc:
        raise SystemExit("Install the MCP extra: pip install -e '.[codemode]'") from exc

    server = FastMCP("aggdsl Code Mode")

    @server.tool()
    def read_aggdsl_reference(topic: str = "workflow") -> dict:
        """Read aggDSL documentation BEFORE writing a query.

        Choose a focused topic: workflow, syntax, sources, grammar, patterns,
        examples, troubleshooting, or pattern:<name> (e.g. pattern:top-features).
        'patterns' lists available names; each individual pattern gives actual
        syntax, NOT a fixed list of allowed queries.

        This tool provides language reference only. It does NOT define what a
        business metric means or choose a KPI on behalf of the user.
        """
        try:
            return help_topic(topic)
        except DiscoveryError as exc:
            return {"error": str(exc)}

    @server.tool()
    def execute_pendo_code(code: str) -> dict:
        """Execute generated read-only JavaScript analysis against Pendo.

        First read reference with read_aggdsl_reference when unfamiliar with
        aggDSL syntax, then write arbitrary JavaScript using the bridge below:
        - await pendo.catalog.search({type:'pages'|'features'|'trackTypes'|'guides',
                                      query:'checkout',appId?:'123',limit?:20})
        - await pendo.catalog.apps() // observed app IDs, not all apps
        - await pendo.catalog.metadata({kind:'visitor'|'account',query?:'plan'})
        - await pendo.aggdsl.validate(dslString) // compile-only diagnostics
        - await pendo.aggdsl.run(dslString) // real read-only aggregation
        - await pendo.help('grammar'|'patterns'|'pattern:<name>'|...).

        Compose independent operations with Promise.all; await all requests.
        Return concise JSON rather than bulk rows or sensitive visitor data.
        No hidden KPI or metric definitions: user/agent must select and explain
        metrics. No writes. Compile success does not prove analytical accuracy.
        Trusted-local proof of concept only, not a multi-tenant JS sandbox.
        """
        try:
            return execute_code(code)
        except CodeModeError as exc:
            return {"error": str(exc)}

    # Resources are a zero-additional-tool way to expose reference material.
    # Some MCP clients do not automatically present resources to the LLM;
    # the read_aggdsl_reference tool above is the portable fallback.
    @server.resource("aggdsl://reference/workflow", mime_type="text/plain")
    def workflow_reference() -> str:
        """Minimal guide to planning a syntax-first Pendo aggregation."""
        return help_topic("workflow")["text"]

    @server.resource("aggdsl://reference/grammar", mime_type="text/plain")
    def grammar_reference() -> str:
        """Complete supported aggDSL grammar and source-field reference."""
        return help_topic("grammar")["text"]

    @server.resource("aggdsl://reference/patterns", mime_type="text/plain")
    def pattern_index() -> str:
        """Names of syntax templates, not a registry of permitted queries."""
        return help_topic("patterns")["text"]

    @server.resource("aggdsl://patterns/{name}", mime_type="text/plain")
    def pattern_reference(name: str) -> str:
        """One on-demand DSL pattern."""
        return help_topic("pattern:" + name)["text"]

    return server


def main() -> None:
    create_server().run(transport="stdio")


if __name__ == "__main__":
    main()
