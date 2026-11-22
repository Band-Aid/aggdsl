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
| Scale | One method | Built for the real server's ~130 tools |

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

## What the real Pendo MCP server looks like

Checked against a live connection (November 2026):

- About 130 tools: usage analytics, guides, segments, feedback, Orchestrate journeys,
  agent analytics, session replay and more, including roughly 40 that write.
- Descriptions are long and structured (`USE FOR`, `EXAMPLES`, `NOT FOR`, `WORKFLOW`,
  `RETURNS`). Single definitions run to several kilobytes, so the full tool list is a
  large fixed cost in every conversation before any question is asked.
- Every analytics tool needs `subId`, and every tool accepts an optional `userQuery`
  task summary for Pendo's own usage analytics.
- The same parameter types (`dateRange`, `productArea`, `segmentPipeline`) are
  spelled out in full on tool after tool.
- Answers are spread over tools on purpose: rank with `aggregateEntityUsage`, then
  call `entityUsage` once per entity for the per-account breakdown, and so on. That
  is where the tool-call count comes from.

Code Mode is shaped around those facts:

| Real-server trait | What Code Mode does |
| --- | --- |
| 130 definitions | Two definitions. With more than 25 tools the index inside `execute` is names only, grouped by verb. |
| Multi-kilobyte descriptions | `search` returns a short signature that keeps the `RETURNS` section, because code needs the result shape. `detail: "full"` returns everything for one tool. |
| Repeated parameter types | Declared once as shared types (`DateRange`, `ProductArea`) and referenced by name. |
| `subId` everywhere | `--default subId=<id>` pre-sets it; programs leave it out. |
| `userQuery` telemetry | Hidden from signatures. `execute` takes an `intent` and the host adds it to every call, so Pendo's analytics still see the task. |
| Fan-out by design | `Promise.all` over the per-entity calls, joined in code. |

## Recipes

[`examples/pendo_code_mode/`](../examples/pendo_code_mode/) holds four programs written
against the real tool names, arguments and result shapes:

| Recipe | Question | Calls replaced |
| --- | --- | --- |
| `feature_adoption_by_account.js` | Which accounts drive the most-used features, and how concentrated is each? | 6 |
| `usage_movers.js` | What grew and dropped versus the previous period, across pages, features and track events? | 6 |
| `guide_audit.js` | Which live guides reach many people but are ignored or dismissed? | 2, a few hundred rows |
| `account_brief.js` | How is one account using the product? | 10 |

How far they are verified:

- All four run in the test suite against the mock, which rejects any call the real
  argument schema would reject.
- The first three were also run over responses recorded from live calls to a Pendo
  demo subscription (11 calls, with small `limit` values), so the result shapes they
  read are the real ones.
- `account_brief.js` relies on `listAccounts` search and `appUsage` shapes that were
  each seen live, but the whole program has only run against the mock.
- No recipe has run end to end through `pendo-code-mcp` against the live server; this
  environment has no credential for it.

## Try it without Pendo

Requires Python >= 3.10 and Node.js >= 22.13.

```bash
pip install -e .
pendo-code tools --mock "guide effectiveness"     # signatures as the model sees them
pendo-code tools --mock appUsage --detail full
pendo-code run --mock examples/pendo_code_mode/account_brief.js
```

`--mock` starts a bundled stand-in server. It models ten tools with the real argument
schemas and result shapes, condensed descriptions and synthetic data.

Measured on the mock, which understates the real gap because its tables are small:

| Recipe | Model tool calls: direct → Code Mode | Result bytes into the conversation |
| --- | --- | --- |
| `feature_adoption_by_account.js` | 6 → 1 | 3,863 → 1,526 |
| `usage_movers.js` | 6 → 1 | 13,421 → 1,259 |
| `guide_audit.js` | 2 → 1 | 4,370 → 2,448 |
| `account_brief.js` | 10 → 1 | 4,387 → 875 |

Each program is itself 1.3 to 2.0 KB that the model has to write. Tool definitions for
the ten mock tools are 18.8 KB raw against 2.7 KB for `search` plus `execute`.

## Connect to the real Pendo MCP server

```bash
# Streamable HTTP with a bearer token (token is read from the environment only)
export PENDO_MCP_URL='<your Pendo MCP endpoint>'
export PENDO_MCP_TOKEN='<access token>'
pendo-code-mcp --default subId=<subscription id>

# or through any stdio MCP bridge that handles the OAuth sign-in for you
export PENDO_MCP_COMMAND='npx mcp-remote <your Pendo MCP endpoint>'
pendo-code-mcp --default subId=<subscription id>
```

Register `pendo-code-mcp` as a stdio MCP server in your client. Use
`pendo-code tools` with the same settings to inspect the generated API first.

Only tools the upstream marks `readOnlyHint: true` are exposed. Others are listed on
stderr at startup and stay hidden unless you pass `--allow-tool` (a name or a glob
such as `'list*'`) or `--allow-writes`. `execute` advertises itself as read-only
exactly when no write tool was let in.

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

- Not run end to end against the live Pendo MCP server (see Recipes for what was
  checked).
- Whether the real server sets `readOnlyHint` was not visible from here. If it does
  not, every tool starts hidden; `--allow-tool 'list*' --allow-tool 'get*'` and
  similar globs are the workaround.
- No built-in OAuth flow: supply a token or use a stdio bridge.
- The real tools publish no output schema, so results are typed `unknown` and the
  model relies on the `RETURNS` text.
- Some tools return text rather than JSON; those reach JavaScript as plain strings.
- No pagination helper. Programs loop on `meta.hasMore` or `totalCount` themselves.
- No benchmark against the ontology experiment's questions; the `stats` block in each
  `execute` result (tool calls, upstream bytes, output bytes) is there to make that
  comparison easy.

## Development

```bash
pip install -e '.[dev]'
pytest tests/test_pendo_codemode.py
```
