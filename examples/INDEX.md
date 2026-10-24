# aggDSL example queries

These ready-to-adapt queries demonstrate the portable aggDSL syntax against
Pendo Aggregation API sources. All application, entity, visitor, and session
identifiers are synthetic placeholders; replace them before running a query.

Every `.dsl` file in this directory is compiled by the test suite.

## Categories

- `events__*`: event volume, daily counts, errors, and dead clicks
- `feature_events__*` and `feature_retention_*`: feature usage and retention
- `funnel_*`: page-to-feature conversion funnels
- `pes__*` and `pes_score_*`: Product Engagement Score queries
- `recording_metadata__*`: recording metadata and session-level diagnostics
- `session_replays__*`: replay lists, frustration, and error analysis
- `merge__*` and `spawn__*`: joins, cohorts, and nested branch pipelines
- `recording_replay__aggregation_*`: fork and raw-JSON equivalents
- `product_areas__*`: native Pendo page and feature group analysis
- `agentic_events__*`: Pendo agentic event aggregation

The machine-readable [`index.json`](index.json) maps common query intents to a
smaller set of representative examples.

## Compile an example

From the repository root:

```bash
python -m tools.pendo.dsl_compile examples/feature_retention_top5.dsl
```

To execute an adapted query when Pendo credentials are configured:

```bash
python -m tools.pendo.run_agg examples/feature_retention_top5.dsl
```

Use moustache placeholders such as `{{APP_ID}}`, `{{PAGE_ID}}`, and
`{{FEATURE_ID}}` when a query will be populated by another tool.
