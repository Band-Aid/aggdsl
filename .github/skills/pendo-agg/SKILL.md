---
name: pendo-agg
description: >-
  Run Pendo aggregation queries from natural-language questions, aggDSL, or request
  JSON, then summarize or chart the returned data. Use for fetching Pendo analytics;
  for DSL-only authoring or review, use the repository's aggdsl skill.
---

# Pendo aggregation

Turn the question into a checked request, fetch once, and answer from the returned data.

## Setup and scope

- Run commands from the repository root (three directories above this skill), where
  `tools/pendo/` and `skills/aggdsl/` live. Use Python 3.10+ with `aggdsl` importable;
  prefer the existing `.venv/bin/python`. Commands below use `python` for that interpreter.
  If needed, use `PYTHONPATH=src` for module commands rather than reinstalling dependencies.
- Credentials: `PENDO_API_KEY` or `PENDO_INTEGRATION_KEY`. Never print keys, read `.env`
  into chat, or put secrets in commands. `run_agg` loads the root `.env` only when neither
  key is set; lookup helpers require the key in their environment.
- Save generated artifacts under `results/<topic>/` unless the user specifies another
  destination. Use a new topic directory when existing files would be overwritten.
- Reuse supplied IDs, queries, and results. Ask only for missing information that changes
  the query, such as an ambiguous app, metric definition, or time window; never invent IDs.
  For existing results, skip directly to analysis. For query-only requests, stop before fetching.

## Build and fetch

1. For new or edited DSL, follow [the aggdsl skill](../../../skills/aggdsl/SKILL.md):
   read the closest pattern, adapt it, and run the checker until it prints `OK`.
   Resolve named segments only when their IDs are missing; see
   [optional tools](references/optional-tools.md). For historical windows ending at
   `now()`, use a negative count. PES uses `PIPELINE` with a `| pes {...}` stage.
2. Compile the checked DSL to a saved body. Default compilation resolves `now()` to
   epoch milliseconds. If the user supplied JSON, use that body directly.
3. Send the saved body, so the request matches the artifact and is not recompiled.
   `run_agg` validates JSON before sending; use the standalone validator for
   validation-only work. Continue only when each command succeeds.

For additional portable query patterns, see [examples/INDEX.md](../../../examples/INDEX.md)
or the intent map in [examples/index.json](../../../examples/index.json). Treat every
identifier in the examples as a synthetic placeholder and replace it with values for
the current Pendo subscription.

```bash
python skills/aggdsl/scripts/check.py results/<topic>/query.dsl
python -m tools.pendo.dsl_compile results/<topic>/query.dsl > results/<topic>/body.json
python -m tools.pendo.run_agg results/<topic>/body.json --max-attempts 1 > results/<topic>/result.json
```

The runner defaults to five attempts, so explicitly use `--max-attempts 1` when
handling errors yourself. Allow at most five HTTP attempts total per query task:
fix request errors from the exact response, then recheck and recompile changed DSL;
back off for transient or rate-limit errors. Stop on authentication/permission errors,
an unchanged permanent failure, or exhausted attempts. Report the last error and the
needed correction. A failed command can leave an empty result file; do not analyze it.

## Answer from results

Inspect the response shape, row count, and requested metrics. Report the app, time
window, filters, units, and findings supported by the data; link saved artifacts.
Distinguish empty rows from an unrecognized response shape. Compilation establishes
syntax only; neither a clean check nor an empty response proves the intended scope.

Read [optional tools](references/optional-tools.md) only for segment lookup, name
enrichment, standalone validation, or charts. Reuse a successful response for these
steps rather than fetching again. The summary helper describes rows and columns;
calculate the requested metrics yourself. Use explicit metric fields for charts.
