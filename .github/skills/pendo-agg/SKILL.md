# Pendo Aggregation (DSL → Fetch → Summarize)

This skill turns a natural-language question into a Pendo Aggregation request body, fetches the data from Pendo, then summarizes and/or charts the results.

## Requirements

Environment variables:

- `PENDO_API_KEY` (required): your Pendo integration/API key.
   - Alias supported: `PENDO_INTEGRATION_KEY`.
- `PENDO_AGG_URL` (optional): the full HTTPS URL to the aggregation endpoint you want to call.
   - Default: `https://app.pendo.io/api/v1/aggregation`.
- `PENDO_API_KEY_HEADER` (optional): header name for the key. Defaults to `x-pendo-integration-key`.

Notes:

- Never print or paste `PENDO_API_KEY` in chat.
- The tools in `tools/pendo/` use only environment variables for secrets.
- If you prefer, create a local `.env` (see `.env.example`). `run_agg.py` will load it if the env vars aren't set.
- READ the manual to understand how to generate DSL - `Pendo Aggregation Spec Sheet (Project Truth).md`
## Tools

### 1) Compile DSL → Aggregation JSON

- Script: `tools/pendo/dsl_compile.py`
- Input: `.dsl` file or stdin
- Output: aggregation JSON body to stdout

Examples:

- `python tools/pendo/dsl_compile.py query.dsl > body.json`
- `cat query.dsl | python tools/pendo/dsl_compile.py --stdin > body.json`

Recommended (module execution):

- `python -m tools.pendo.dsl_compile query.dsl > body.json`

By default it resolves `now()` to the current epoch-ms (Pendo endpoints often require numeric timestamps).

### 2) Validate aggregation JSON

- Script: `tools/pendo/validate.py`
- Input: JSON file or stdin
- Output: exits non-zero on schema issues; prints a short reason.

Recommended:

- `python -m tools.pendo.validate body.json`

### 3) Run aggregation (fetch data)

- Script: `tools/pendo/run_agg.py`
- Input: JSON request body file or DSL file (auto-detected)
- Output: response JSON to stdout

Examples:

- `python tools/pendo/run_agg.py body.json > result.json`
- `python tools/pendo/run_agg.py query.dsl > result.json`

Recommended:

- `python -m tools.pendo.run_agg body.json > result.json`

Retry behavior:

- `run_agg.py` will attempt up to 5 build+send attempts on failure.
- Between attempts it may apply safe rewrites (e.g., resolving `now()` if it slipped through).
- If failures persist, it prints the last error response body (when available) so you can adjust the DSL.

### 4) Summarize + chart

- Script: `tools/pendo/chart.py`
- Input: result JSON file or stdin
- Output: Markdown summary and an optional Vega-Lite spec (JSON) for quick visualization.

Examples:

- `python tools/pendo/chart.py result.json --summary`
- `python tools/pendo/chart.py result.json --vega --x groupId --y totalEvents > chart.vega.json`

Recommended:

- `python -m tools.pendo.chart result.json --summary`

## Agent workflow (how to use this skill)

1. Ask clarifying questions only if required fields are unknown (e.g., appId, product area definition).
2. Generate a DSL query that matches the spec sheet.
3. Compile DSL and validate:
   - `python tools/pendo/dsl_compile.py query.dsl > body.json`
   - `python tools/pendo/validate.py body.json`
4. Run the aggregation:
   - `python tools/pendo/run_agg.py body.json > result.json`
5. If the request fails, iterate up to 5 times:
   - adjust DSL, recompile, re-run
   - use the exact error message from the previous attempt to guide the fix
6. Summarize and chart:
   - `python tools/pendo/chart.py result.json --summary`
   - optionally produce a Vega spec via `--vega`
