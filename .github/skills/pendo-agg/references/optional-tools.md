# Optional Pendo tools

Run from the repository root with the same interpreter as the main workflow.
Load only the section needed for the task.

## Validate without fetching

```bash
python -m tools.pendo.validate results/<topic>/body.json
```

This checks the request envelope and pipeline structure, not source fields or metrics.

## Find a segment ID

There is no aggregation `segments` source. Search the REST catalog only when a
segment ID is missing, and reuse the match in the selected DSL pattern:

```bash
python -m tools.pendo.lookup_segments "paying" -323232
```

Arguments are positional: optional search text, then app ID. Omit both to list all
segments. If multiple names match, resolve ambiguity before choosing an ID.
This helper requires `requests`, an environment key, and uses the fixed endpoint
`https://app.pendo.io/api/v1/segment` with `x-pendo-integration-key`.

## Add feature and page names

Use when readable names are needed and absent from the response:

```bash
python -m tools.pendo.lookup_names results/<topic>/result.json --app-id -323232 > results/<topic>/enriched-result.json
```

Omit `--app-id` for cross-app results. The helper fetches expanded catalogs across
apps and filters locally; the flag does not reduce catalog downloads. Unknown or
deleted IDs remain usable. Feature and page lookups fail independently and emit
warnings; a successful exit does not guarantee all names were resolved.

## Summarize or chart

```bash
python -m tools.pendo.chart results/<topic>/result.json --summary > results/<topic>/summary.md
python -m tools.pendo.chart results/<topic>/result.json --vega --x featureName --y totalEvents > results/<topic>/chart.vega.json
```

Use `enriched-result.json` when selecting enriched name fields. The helper recognizes
a list of row objects or a top-level `results`, `result`, `data`, or `rows` list.
Its summary reports row frequency and numeric ranges, not business conclusions.
The Vega helper makes a bar chart with categorical x; adapt the saved spec when
the question needs a time series or another chart type. Keep summary and Vega
commands separate so Markdown does not contaminate the JSON spec.

## Endpoint overrides

| Helper | Configuration |
| --- | --- |
| `run_agg` | `PENDO_AGG_URL` (default `https://app.pendo.io/api/v1/aggregation`); `PENDO_API_KEY_HEADER` (default `x-pendo-integration-key`) |
| `lookup_names` | `PENDO_API_BASE_URL` (default `https://app.pendo.io`); fixed key header |
| `lookup_segments` | Fixed public endpoint and key header |

An aggregation endpoint override does not configure the lookup helpers. Preserve
the user's endpoint settings and check compatibility before using lookups.
