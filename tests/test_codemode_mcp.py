"""The MCP surface must stay small, while DSL references remain discoverable.

No Pendo credentials are needed to list these tools/resources.
"""
from __future__ import annotations

import asyncio

from aggdsl.codemode_mcp import create_server
from aggdsl.codemode_catalog import help_topic


def test_server_exposes_only_reference_and_execution_tools():
    server = create_server()
    tools = asyncio.run(server.list_tools())
    names = {tool.name for tool in tools}

    assert len(tools) == 2
    assert names == {"read_aggdsl_reference", "execute_pendo_code"}
    # The LLM can learn the language before running a program.
    ref_tool = next(t for t in tools if t.name == "read_aggdsl_reference")
    assert "BEFORE" in (ref_tool.description or "")
    assert "grammar" in (ref_tool.description or "")
    assert "pattern:" in (ref_tool.description or "")
    execute_tool = next(t for t in tools if t.name == "execute_pendo_code")
    assert "No hidden KPI" in (execute_tool.description or "")


def test_reference_resources_and_template_are_published():
    server = create_server()
    resources = asyncio.run(server.list_resources())
    templates = asyncio.run(server.list_resource_templates())

    uris = {str(resource.uri) for resource in resources}
    template_uris = {str(template.uri_template) for template in templates}

    assert "aggdsl://reference/workflow" in uris
    assert "aggdsl://reference/grammar" in uris
    assert "aggdsl://reference/patterns" in uris
    assert "aggdsl://patterns/{name}" in template_uris


def test_runtime_reference_is_the_existing_compiler_grammar():
    grammar = help_topic("grammar")["text"]
    assert "Event sources" in grammar
    assert "group" in grammar
    assert "TIMESERIES" in grammar
    patterns = help_topic("patterns")["text"]
    assert "top-features" in patterns
    assert "featureEvents" in help_topic("pattern:top-features")["text"]
