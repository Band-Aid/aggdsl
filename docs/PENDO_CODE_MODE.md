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

## High-level functions (`lib`)

Four cold-start runs showed where a model's tokens go once it writes its own programs:
reading tool documentation, probing result shapes, and returning data it only needs in
order to choose an entity. Features that tried to manage that from the outside (learned
shapes, previews, `full()`) helped little and sometimes misled the model.

`lib` takes the other route. It is a small library that ships with the server and runs
inside the sandbox on top of the raw tools:

| Function | Replaces |
| --- | --- |
| `lib.find(query, opts)` | Searching pages, features and track events and trimming the result |
| `lib.frustration(opts)` | Four rankings plus one lookup per offending element |
| `lib.funnel(steps, opts)` | `queryFunnel` and picking the numbers out of its result |
| `lib.visitors(entity, opts)` | `entityUsage` per visitor |
| `lib.cohorts({among, did, behaviours})` | One visitor list per entity, intersected in code |
| `lib.guidePolls(opts)` | Finding guides with polls and one response query per guide |

Each returns a shape this package defines, so the one line shown to the model is exact
and no trial run is needed. A function is listed only when the upstream has the tools
it calls. The raw tools stay available for everything else.

Replayed over real responses collected earlier (not a live run), the five functions
that answer the four benchmark questions returned about 2 KB in total from about 83 KB
of raw tool results.

Result previews are now off by default (`--preview-bytes` turns them on).

## Keeping cost flat as Pendo adds tools

Measured with a model writing its own programs (three questions on one app, one
run each, tokens estimated as bytes / 4):

| | Regular MCP | Code Mode, signatures via `search` | Code Mode, API in the description |
| --- | --- | --- | --- |
| Model tool calls | 26 | 12 | 5 |
| Conversation tokens | ~22,500 | ~20,400 | ~13,100 |

In the best run 76% of the tokens were results the programs returned, much of it
data returned only so a later program could use it. Four features target that and
the growth in tool count:

| Feature | What it does |
| --- | --- |
| API in the `execute` description | One line per tool: argument names and the shape of the result. No `search` call is needed for listed tools. |
| Learned result shapes (`--shapes FILE`) | Pendo's tools publish no output schema, so the server records the structure of real results (key names and types, never data) and shows it. Fields that appear only for some arguments are marked `?`. |
| Bounded listing (`--core a,b,c`) | With more than 25 tools, only core tools and the ones actually used get a full line; the rest are names. `await pendo.help(name)` returns a signature from inside a program. |
| Session state | `state` persists between programs. A program stores ids and bulky intermediate data (`state.catalog = ...`) and returns a short answer; each result lists what is stored and how large it is. |
| Saved recipes (`--recipes DIR`) | `execute` with `save_as: {name, description}` stores a working program. Later programs call `await recipes.<name>(params)`; saved recipes are listed in the description. |
| `table(rows)` | Returns `{columns, rows}` so repeated keys are not sent back. |
| Result previews (`--preview-bytes`, default 4000) | A larger result comes back as a preview: the first items of each list, long strings trimmed. The full value is kept in `state.last` for the next program to filter. `return full(value)` overrides it, up to the 32 KB limit. |

Description size as the tool count grows (the real-shaped tools plus a synthetic long
tail with 4 KB descriptions, an assumption about future tools):

| Tools | Regular MCP definitions | Code Mode definitions |
| --- | --- | --- |
| 12 | ~6,200 tokens | ~1,200 |
| 132 | ~131,000 | ~1,700 |
| 412 | ~423,000 | ~2,600 |

A third cold-start run with these features, on the same three questions (one run):

| | API in the description | Plus state, recipes, merged shapes |
| --- | --- | --- |
| Executes | 5 | 6 |
| Conversation tokens | ~13,100 | ~13,100 |
| Pendo queries | 43 | 82 |

Tokens did not drop. The model stored about 64 KB in `state` and reused it, but it
did roughly twice the analysis, still returned the entity catalogue once in order to
choose from it, read signatures through `pendo.help` (~3,300 tokens), and lost one
run to an undocumented maximum (`limit` above 200). Maximums and small enums are now
shown on each tool's line. A fourth question, pulling guide poll responses, took
about 1,200 conversation tokens against about 3,400 for regular MCP.

A fourth run added result previews (one run):

| | Without previews | With previews |
| --- | --- | --- |
| Questions 1-3, conversation tokens | ~13,100 | ~10,600 |
| Guide polls, conversation tokens | ~1,200 | ~2,000 |
| Executes, all four questions | 8 | 9 |

Only one result was actually previewed. The model avoided the cap mostly by returning
less, and wrapped six results in `full()`, once wastefully for 11 KB of help text. The
guide-poll question got worse for an unrelated reason: the learned shape for `listGuides`
showed poll fields that only appear with `expand: true`, and the model's first attempt
left that argument out. Shapes that depend on the arguments remain a known weakness.

What this repo cannot fix, and Pendo's server can: published output schemas, batch
forms of the per-entity tools (one program needed 22 `entityUsage` calls), and running
programs next to the aggregation engine to remove round trips.

## Tools as a file tree

```bash
pendo-code generate ./pendo-api --default subId=<id> --shapes shapes.json --recipes ./recipes
```

writes the catalog the way Anthropic's "Code execution with MCP" article describes it:

```
pendo-api/
  client.ts                     callMCPTool, supplied by the host
  servers/pendo/index.ts        one line per tool
  servers/pendo/<tool>.ts       typed input, learned response type, wrapper function
  servers/pendo/types.ts        shared types such as DateRange
  skills/pendo-analytics/       SKILL.md plus each saved recipe
```

An agent that has a filesystem (Claude Code, for example) lists the directory and opens
only the files it needs, so no tool definition is loaded up front. For the 14 mock tools
the index is 6.2 KB and all tool files together 27.8 KB. The response types are the
learned shapes; a tool that has never been called is typed `unknown`.

This is generated output only. Nothing in this repo runs those TypeScript files, and
they have not been compiled or tried with an agent.

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

## Read performance: regular MCP against Code Mode

```bash
pendo-code bench --mock examples/pendo_code_mode/*.js
pendo-code bench --mock --simulate-latency-ms 300 examples/pendo_code_mode/*.js
# against the real server (read-only; --allow-writes and --allow-tool are refused)
pendo-code bench --default subId=<subscription id> --runs 5 your_program.js
```

For each program the benchmark runs Code Mode for real and records its upstream
calls. It then makes the same calls directly, one result per tool call, which is
what a model using the server without Code Mode has to do. It reports tool calls,
model turns, bytes entering the conversation, and time spent waiting on the server.
Code Mode is charged for a `search` call and for the program the model writes.

Token counts are exact with `--tokens`, which calls Anthropic's `count_tokens`
endpoint (`pip install -e '.[tokens]'` and API credentials such as
`ANTHROPIC_API_KEY`; `--token-model` picks the tokenizer, default `claude-opus-5-5`).
Without it they are bytes / 4, marked `~`, which tends to undercount JSON.

On the mock (9 read tools, small tables), tokens **estimated**:

| Program | Tool calls | Model turns | Conversation tokens | With definitions |
| --- | --- | --- | --- | --- |
| account_brief | 10 → 2 | 11 (or 4 parallel) → 3 | ~1,573 → ~2,800 | ~6,141 → ~3,472 |
| feature_adoption_by_account | 6 → 2 | 7 (or 3 parallel) → 3 | ~1,274 → ~1,669 | ~5,842 → ~2,341 |
| guide_audit | 2 → 2 | 3 (or 2 parallel) → 3 | ~1,166 → ~1,979 | ~5,734 → ~2,651 |
| usage_movers | 6 → 2 | 7 (or 2 parallel) → 3 | ~3,751 → ~1,311 | ~8,319 → ~1,983 |

Tool definitions, paid in every conversation: ~4,568 tokens for regular MCP against
~672 for Code Mode. "With definitions" is a conversation that asks one question.

With 300 ms of simulated latency per call, time waiting on the server:

| Program | Regular MCP, sequential | Regular MCP, parallel calls | Code Mode |
| --- | --- | --- | --- |
| account_brief | 3011 ms | 904 ms | 931 ms |
| feature_adoption_by_account | 1806 ms | 603 ms | 629 ms |
| guide_audit | 602 ms | 301 ms | 326 ms |
| usage_movers | 1813 ms | 303 ms | 331 ms |

Reading these numbers:

- Tool calls and model turns drop whenever a question fans out.
- Per-question conversation tokens are **worse** for Code Mode in three of the four
  programs here. The signatures returned by `search` cost roughly 600 to 2,100 tokens, and the mock's
  result tables are too small to repay that. Code Mode wins on this measure only
  when results are large (`usage_movers`) or when signatures are already in context
  from an earlier question.
- The fixed definition cost is the clearer saving, and it grows with the number of
  tools: the mock has 9, the real server about 130.
- Waiting time matches a client that issues parallel tool calls and beats one that
  calls sequentially. Code Mode adds about 25 ms to start Node.
- No model is in the loop. Regular MCP is handed exactly the right calls, and model
  thinking time, which grows with turns, is not measured.
- Each token is counted once. A real conversation re-reads earlier turns on every
  model turn, which costs regular MCP more because it takes more turns; prompt
  caching reduces that for both.

These are mock numbers. The benchmark has not been run against the live server.

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
