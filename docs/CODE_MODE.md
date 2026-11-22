# Pendo Code Mode: discovery-first analytical programming

This branch adds an experimental **single MCP tool**, `execute_pendo_code(code)`.
An AI writes arbitrary JavaScript analysis logic. The JavaScript can discover
real Pendo entity IDs, compile-check novel aggDSL queries, issue several
read-only aggregation requests, and return a *small* final result.

**It is not restricted to the included examples.** The examples demonstrate
the programming model, not a fixed query registry.

## Supported JavaScript API

| Method | Result | Network |
| --- | --- | --- |
| `pendo.help(topic)` | On-demand documentation ("workflow", "syntax", "sources", "examples", "troubleshooting", "grammar", "patterns", "pattern:<name>") | None |
| `pendo.catalog.search({type,query?,appId?,limit?})` | ID, name and app ID for matching Pendo definitions | GET only |
| `pendo.catalog.apps()` | App IDs **observed on tagged entities**, never a complete app list | GET only |
| `pendo.aggdsl.validate(dsl, {includeBody?:true})` | Syntax/structure checks, warnings and optionally compiled JSON | None |
| `pendo.aggdsl.run(dsl)` | The actual Pendo Aggregation API JSON response | POST aggregation, read only |

Catalog `type` values: `pages`, `features`, `trackTypes`, and `guides`.
Search defaults to all accessible apps (`?expand=*`); when a numeric
`appId` is supplied, it scopes the request to that app. The bridge only
calls documented, allowlisted Pendo API paths: `/api/v1/page`, `feature`,
`tracktype` and `guide`. This is not a general HTTP proxy. Returned entity
objects contain only selected fields, not complete page rules or guide HTML.

The app discovery function compiles IDs from entities already tagged in Pendo.
It **cannot find empty apps or reliably provide app names**, because no
verified public app-inventory API is used. It labels that limitation in its
response. Application names should come from an independently verified
inventory, not a guess. Likewise, the compiler does not validate arbitrary
Pendo event-property names against live data.

## Install

Requires Python 3.10+ and Node.js >=22.13 (Node permission model).

```bash
pip install -e '.[dev,codemode]'
# Use a read-only integration key where possible.
export PENDO_INTEGRATION_KEY='...'
# Japan:
export PENDO_API_BASE_URL='https://app.jpn.pendo.io'
export PENDO_AGG_URL='https://app.jpn.pendo.io/api/v1/aggregation'
aggdsl-code-mcp
```

Configure the command `aggdsl-code-mcp` as an MCP stdio server in the agent
client. It exposes one read-only tool rather than dozens of separate tools.
Other Pendo actions such as guide edits are intentionally unavailable.

**Important:** Pendo Integration Keys grant access to *every app in their
subscription*, independently of the Pendo user's normal UI permissions.
Store this key only on the server host, never in the JavaScript or a
client-facing prompt. Apply tenant boundaries *outside* this PoC.

## Discovery-first example (no preselected feature IDs)

Save as `examples/code_mode_discover.js` or write the JS directly as the
`execute_pendo_code` tool's `code` argument:

```js
const matches = await pendo.catalog.search({
  type: 'features', query: 'checkout', limit: 10
});
if (matches.items.length === 0) return {message: 'No matching tagged features'};
const feature = matches.items[0];
if (!feature.appId) return {message: 'Feature has no appId', feature};
const dsl = `
FROM event([source=featureEvents,appId=${feature.appId},featureId="${feature.id}",blacklist="apply"])
TIMESERIES period=dayRange first=now() count=-30
| group by featureId fields { clicks=sum(numEvents) }
| limit 1
`;
const check = await pendo.aggdsl.validate(dsl);
if (!check.valid || check.warnings.length) return {check, dsl};
const result = await pendo.aggdsl.run(dsl);
// Shape of the API response varies with each aggregation. Inspect the
// returned shape and report a compact trustworthy summary.
return {feature: {id:feature.id,name:feature.name}, result};
```

This example selects the *first* match for demonstration only. For real use,
disambiguate multiple matches by appId and name before analyzing. Return
concise aggregate results when possible; do not echo visitor PII or secrets.

To run a fixed-ID example without an API key, compile only:

```bash
aggdsl-code --dry-run examples/code_mode_compare.js
```

Dry-run avoids **aggregation** API calls; a program that asks the *live
catalog* for entity IDs still requires a Pendo integration key. Tests use an
injected mock catalog and backend to run fully offline.

## Execution mechanics

```text
AI writes JavaScript (one MCP request)
  -> Node.js worker (no credentials)
  -> JSON-lines bridge
     -> help / validate: local read-only reference and aggDSL compiler
     -> catalog.search / apps: allowlisted Pendo read-only GET endpoints
     -> aggdsl.run: compile + read-only POST /api/v1/aggregation
  -> independent backend calls executed concurrently (max 3)
  -> JavaScript computes and returns a concise answer
```

The JavaScript worker has no direct Pendo API access and cannot call arbitrary
Python tools. Its bridge restricts methods and payload sizes. Concurrent
`Promise.all` operations are now handled by Python's bounded thread pool,
rather than serialized by the bridge.

Default budgets per JavaScript invocation:
- 20 seconds wall time (individual Pendo HTTP calls are also time-bounded)
- 15 total bridge calls, of which at most 6 aggregations and 7 discovery calls
- 3 concurrent backend requests
- 20 KB JavaScript and 20 KB DSL per call
- 256 KB total backend responses, 64 KB final result

Use `pendo.help('grammar')` or `pendo.help('pattern:top-features')` to
learn additional stages and patterns, and `pendo.aggdsl.validate(dsl)` to inspect compile
errors. If the Pendo API rejects a query (e.g. nonexistent property), the
error is returned to JavaScript so the agent can revise and try again.
Validation is *compile-only*, and a passing result does not prove that the
query has rows or measures the intended metric.

## Security and production gaps

**Trusted-local experimentation only. Not a secure sandbox for arbitrary
adversarial scripts or production multi-tenancy.** Node's `vm` is not a
security boundary; the permission model is defense-in-depth only. For
production, isolate execution in a network-denied OS sandbox, container or
microVM, with process/filesystem restrictions, CPU and memory caps, egress
allowlists and per-tenant authorization. Add request audit trails, structured
observability, explicit data retention controls and query cost budgets.
Do not expose this MCP tool to untrusted users before those safeguards exist.

Pendo aggregation data is subject to API availability/entitlements. There is
no claim of live-data verification without a valid key. Discovery results may
omit entities due to Pendo API access or response caps, and integration keys
are not scoped to individual apps.

## Tests

```bash
pip install -e '.[dev,codemode]'
pytest -q tests/test_codemode.py tests/test_codemode_discovery.py
```

Tests include dynamically discovered IDs, compile errors, catalog filtering,
allowlisted URLs, accidental return of guide HTML, concurrency and call limits.
The suite does not need Pendo credentials. A separate permissioned integration
smoke test is needed before production.
