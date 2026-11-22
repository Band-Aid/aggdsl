# Pendo Code Mode (proof of concept)

Run a **single MCP tool** that accepts JavaScript. The script can call multiple
Pendo aggregations with `await pendo.aggdsl.run(dsl)` and return only the relevant
comparison or summary to the LLM. The existing aggDSL parser/compiler is reused.

## Install

Requirements: Python >=3.10, Node.js >=22.13 (the worker uses `--permission`).

```bash
pip install -e '.[codemode]'
export PENDO_INTEGRATION_KEY='...'
# For OpenAI / Claude Desktop / MCP clients, configure the stdio command:
aggdsl-code-mcp
```

The MCP server exposes **one read-only tool**, `execute_pendo_code(code: str)`.
It does not expose the underlying aggregation credentials or other MCP tools.

## Example: two calls, one AI tool invocation

Save the following as `compare.js`:

```js
const dsl = (count) => `
FROM event([source=featureEvents, appId=123456, blacklist="apply"])
TIMESERIES period=dayRange first=now() count=${count}
| group by featureId fields { clicks=sum(numEvents) }
| sort -clicks
| limit 5
`;

const [last7Days, last30Days] = await Promise.all([
  pendo.aggdsl.run(dsl(-7)),
  pendo.aggdsl.run(dsl(-30))
]);

// Pendo response JSON is passed through without inventing a row schema.
return { last7Days, last30Days };
```

Test the execution **without** network access or credentials:

```bash
aggdsl-code --dry-run compare.js
```

Then execute against Pendo:

```bash
aggdsl-code compare.js
```

This example demonstrates two calls in one tool invocation but returns both raw
responses. For maximum token savings, have the AI program inspect the real
response shape and return a small summary, not the full result set. The
Aggregation API's response format depends on the aggregation request.

## Pipeline

```
AI writes JS code
    -> execute_pendo_code (one MCP call)
    -> Node.js JS worker (no Pendo key)
    -> JSON-lines RPC: aggdsl.run(DSL)
    -> Python compiler: aggDSL -> Pendo JSON
    -> Python HTTP client: Pendo Aggregation API
    -> result JSON to JS worker
    -> local calculation / compact result to AI
```

API keys are kept in Python and are **not** given to Node. The HTTP method is
always a POST to the **read-only aggregation endpoint**. This PoC does not
create or modify guides, segments, or other Pendo resources.

## Limits and safety

Default limits: 20 seconds total, 5 aggregation calls, 20 KB of JS, 20 KB of
DSL per call, 256 KB combined API responses, and 64 KB final JS output. The
worker runs as a separate Node process with the permission model enabled,
with file access limited to its script, and with a minimal process environment.

**This is NOT a hardened sandbox for adversarial or multi-tenant code.** Node's
`vm` module is not a security boundary, and Node's permission system expressly
does not guarantee security against malicious code. In particular, some Node
versions do not enforce network access restrictions. Deploy only for trusted
local experimentation. Before accepting arbitrary remote AI-generated scripts,
move the worker into an independently isolated execution environment with
network egress denied, OS-level filesystem/process restrictions, CPU/memory
limits, per-user Pendo permissions, audit logging, and explicit authorization.

Never pass Pendo credentials, secrets, or write-capable tools into the worker.
Prefer an integration key scoped to the minimum necessary privileges. Avoid
storing JS programs containing sensitive visitor data.

## Out of scope

- Entity ID/name discovery (can be added as a second read-only bridge method)
- General Pendo MCP actions like creating guides or editing segments
- Verification against live Pendo data without credentials
- Guaranteeing response equivalence with the existing specialized Pendo MCP tools

## Development

```bash
pip install -e '.[dev,codemode]'
pytest tests/test_codemode.py
```
