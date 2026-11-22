"""Code Mode for the Pendo MCP server.

One `execute(code)` tool runs a JavaScript program in which every upstream
Pendo MCP tool is an async function on `pendo`. Nothing here depends on aggdsl.
"""
from .catalog import Catalog, Tool
from .executor import CodeModeError, Limits, execute_code
from .mcp_wire import McpClient, McpError

__all__ = ["Catalog", "CodeModeError", "Limits", "McpClient", "McpError", "Tool", "execute_code"]
