# Pendo MCP in Code Mode (concept)

What if the Pendo MCP server supported Code Mode? This package is a working sketch of
the answer. It sits in front of the Pendo MCP server and gives the model two tools
instead of the full tool list:

| Tool | Purpose |
| --- | --- |
| `search(query)` | Find Pendo tools and return their TypeScript signatures. |
| `execute(code)` | Run one JavaScript program in which every Pendo tool is `await pendo.<tool>(args)`. |

The model writes a program that chains calls, runs independent ones with `Promise.all`,
joins and aggregates in code, and returns only the answer. Intermediate results never
enter the conversation.

## How it differs from the aggDSL Code Mode PoC (PR #8)

| | PR #8 | This branch |
| --- | --- | --- |
| Core | aggDSL compiler + Aggregation API | The Pendo MCP server's own tools |
| JavaScript API | `pendo.aggdsl.run(dsl)` | `pendo.<tool>(args)`, generated from `tools/list` |
| Credential | Integration key | Whatever the Pendo MCP connection uses |
| Model must know | aggDSL | Nothing new: tool schemas arrive as TypeScript |
| Upstream calls | One at a time | Concurrent (`Promise.all` really is parallel) |
| Dependencies | `mcp` SDK | None (small built-in MCP client and server) |

Nothing in `src/pendo_codemode/` imports `aggdsl`, and no Pendo tool name or schema is
hard-coded: the catalog is built from the upstream `tools/list`, so new Pendo MCP
tools appear automatically.

```
model ── search / execute ──> pendo-code-mcp ── tools/call ──> Pendo MCP server
                                   │  ▲
                        JSON lines │  │ tool name + args, results
                                   ▼  │
                            Node worker (no credentials, no network API)
```

## Try it without Pendo

Requires Python >= 3.10 and Node.js >= 22.13.

```bash
pip install -e .
pendo-code tools --mock                 # the API as the model sees it
pendo-code run --mock examples/pendo_code_mode/top_features_by_account.js
```

`--mock` starts a bundled stand-in Pendo MCP server with synthetic data. Its tool names
follow Pendo's public tool list, but its argument and result shapes are simplified
inventions.

The example answers "which enterprise accounts drive the three most-used features
across all apps?". Measured against the mock:

| | Direct tool calls | Code Mode |
| --- | --- | --- |
| Model tool calls | 9 | 1 |
| Tool-result bytes entering the conversation | 17,853 | 794 |

The Code Mode side also costs the roughly 1.7 KB program the model writes. These
numbers come from synthetic data and show the mechanism only; they are not a
measurement of the real Pendo MCP server.

## Connect to the real Pendo MCP server

```bash
# Streamable HTTP with a bearer token (token is read from the environment only)
export PENDO_MCP_URL='<your Pendo MCP endpoint>'
export PENDO_MCP_TOKEN='<access token>'
pendo-code-mcp

# or through any stdio MCP bridge that handles the OAuth sign-in for you
export PENDO_MCP_COMMAND='npx mcp-remote <your Pendo MCP endpoint>'
pendo-code-mcp
```

Register `pendo-code-mcp` as a stdio MCP server in your client. Use
`pendo-code tools` with the same settings to inspect the generated API first.

Only tools the upstream marks `readOnlyHint: true` are exposed. Others are listed on
stderr at startup and stay hidden unless you pass `--allow-tool NAME` or
`--allow-writes`. `execute` advertises itself as read-only exactly when no write tool
was let in.

## Limits and safety

Defaults per program: 60 s, 25 tool calls (8 in parallel), 20 KB of code, 1 MB per
tool result, 4 MB of tool results in total, 32 KB returned to the model. Change the
first two with `--timeout` and `--max-calls`.

The worker is a separate Node process started with the permission model, file access
limited to its own script, an empty environment, and no `require`, `process` or
`fetch` in scope. The MCP connection and its credential stay in the Python host,
which re-checks every call against the catalog.

**This is not a hardened sandbox.** Node's `vm` module is not a security boundary.
Use it for trusted local experiments. A production version belongs inside the Pendo
MCP server itself, with an isolate-based sandbox, per-user authorization, and audit
logging.

## Not done

- Not run against the live Pendo MCP server. The transport, catalog and policy are
  tested against the mock over stdio and against a local HTTP server (JSON and SSE).
- No built-in OAuth flow: supply a token or use a stdio bridge.
- If the real server does not set `readOnlyHint`, every tool starts hidden and must
  be allowed explicitly.
- Tool results that are not JSON are passed to JavaScript as plain text.
- No benchmark against the ontology experiment's real questions yet; the
  `stats` block in each `execute` result (tool calls, upstream bytes, output bytes) is
  there to make that comparison easy.

## Development

```bash
pip install -e '.[dev]'
pytest tests/test_pendo_codemode.py
```
