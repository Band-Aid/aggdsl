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

### 5) Lookup segments

- Script: `tools/pendo/lookup_segments.py`
- Input: Optional search term and appId
- Output: List of segment IDs and names

The Pendo Aggregation API does not provide a `segments` source, so use the REST API instead.

Examples:

- `python tools/pendo/lookup_segments.py` - List all segments
- `python tools/pendo/lookup_segments.py 'JAPAN'` - Search for segments containing "JAPAN"
- `python tools/pendo/lookup_segments.py 'paying' -323232` - Search with specific appId

Recommended:

- `python -m tools.pendo.lookup_segments "search term"`

The script requires `PENDO_API_KEY` environment variable and uses the REST API endpoint:
`https://app.pendo.io/api/v1/segment`

Once you have the segment ID, use it in your DSL queries:

```dsl
PIPELINE
| pes {"appId":-323232,"segment":{"id":"SEGMENT_ID_HERE"},"dayCount":-30}
```

Or with FROM queries:

```dsl
FROM event([source=events,appId=-323232])
TIMESERIES period=dayRange first=now() count=30
| segment id="SEGMENT_ID_HERE"
| group by visitorId fields { totalEvents=sum(numEvents) }
```

## Agent workflow (how to use this skill)

1. Ask clarifying questions only if required fields are unknown (e.g., appId, product area definition).
2. **If segment filtering is needed**: Use `tools/pendo/lookup_segments.py` to find the segment ID by name.
3. Generate a DSL query that matches the spec sheet.

Notes:
- PES requests use `PIPELINE` mode with a `| pes { ... }` stage (PES replaces the normal `FROM` source stage).
- **All output files must be saved to `./results/<with topic name>` directory**. Create the directory if it doesn't exist.

4. Compile DSL and validate:
   - `python tools/pendo/dsl_compile.py query.dsl > results/<topic name>/body.json`
   - `python tools/pendo/validate.py results/<topic name>/body.json`
5. Run the aggregation:
   - `python tools/pendo/run_agg.py query.dsl > results/<topic name>/result.json`
6. If the request fails, iterate up to 5 times:
   - adjust DSL, recompile, re-run
   - use the exact error message from the previous attempt to guide the fix
7. Summarize and chart:
   - `python tools/pendo/chart.py results/<topic name>/result.json --summary`
   - optionally produce a Vega spec: `python tools/pendo/chart.py results/<topic name>/result.json --vega --x field --y metric > results/<topic name>/chart.vega.json`