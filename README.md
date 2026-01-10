# aggdsl

A small DSL that compiles to the JSON body required by the Pendo Aggregation API.

## Install (CLI)

From the repo root:

```bash
python -m venv .venv
source .venv/bin/activate
python -m pip install -U pip
python -m pip install -e .
```

After that, you can run:

```bash
aggdsl --help
aggdsl compile query.dsl
```

Alternative (installs as an isolated global CLI):

```bash
pipx install -e .
```

## Status

MVP: supports `FROM event([...])`, `TIMESERIES ...`, plus pipeline stages like `filter`, `identified`, `group`, `sort`, and `limit`.

Also supports translating Pendo Aggregation JSON back into DSL (`decompile`) so you can round-trip PowerBI/exports into something editable.

## Example

Input DSL:

```text
RESPONSE mimeType=text/csv
REQUEST name="NPSResponses"
FROM event([source=pageEvents,pageId="Mal8cGcc-XXqBEJ-c5C1L3frUz8",blacklist="apply"])
TIMESERIES period=dayRange first=1731769200000 count=180
| filter !isNull(parameters.parameter) && parameters.parameter != ""
| group by pageId,parameters.parameter fields { numEvents=sum(numEvents) }
| sort -numEvents
| limit 100
```

## Advanced: spawn and switch

Some real Pendo aggregation pipelines use branching (via `spawn`) and value mapping (via `switch`). `aggdsl` supports both as first-class DSL stages.

Minimal example:

```text
RESPONSE mimeType=application/json
REQUEST name="ExampleSpawnSwitch"
PIPELINE
| spawn
branch
FROM event([source=pollEvents,guideId="ccccc",pollId="xxxxx",blacklist="apply"])
TIMESERIES period=dayRange first=date(2024, 1, 1, 0, 0, 0) last=date(2024, 12, 31, 23, 59, 59)
|| filter pollResponse != ""
|| switch mappedPollResponse from pollResponse { "1"=="promoter", "2"=="passive", "3"=="detractor" }
|| group by mappedPollResponse fields { numEvents=sum(numEvents) }
endbranch
| endspawn
```

Notes:

- `PIPELINE` starts a pipeline without an initial `FROM ...` source stage.
- `branch` / `endbranch` are control lines; both `branch` and `| branch` (same for `endbranch`) are accepted.
- Stages inside a `spawn` branch are prefixed with `||`.
- `switch <outVar> from <field> { "value"=="mapped" , ... }` compiles to Pendo's `switch` stage.
- For constant strings in `eval`/`select`/`unmarshal`, quote the expression: `| eval { resultType="page" }`.
	- Without quotes (`resultType=page`), Pendo treats `page` as a field reference and it may evaluate to `null`.
	- In compiled JSON you will see escaped quotes (e.g. `"page"`) because the JSON contains the expression string.

Merge blocks:

- Merge blocks contain an inner pipeline that uses normal `|` stages.
- For backwards compatibility, the older `>>` prefix is still accepted (treated as `|`).
- `mappings { ... }` is optional; some Pendo exports omit it.

For a full, real-world example, see [examples/nps_responses.dsl](examples/nps_responses.dsl).

For advanced pipelines that include stages not yet modeled in the DSL, you can inject a raw JSON stage:

```text
| raw {"switch": {"mapped": {"pollResponse": [{"value": "1", "==": "abc"}]}}}
```

Compile:

```bash
python -m aggdsl compile query.dsl
# or
aggdsl compile query.dsl
```

By default, the CLI emits the common request-object form:

```json
{ "request": { "pipeline": [ /* stages */ ] } }
```

Aggregation requests are emitted in this request-object form (with optional `name`) since Pendo aggregations are sent as `request.pipeline`.

## JSON  DSL (decompile)

You can translate an aggregation JSON body into DSL:

```bash
python -m aggdsl decompile body.json
# or
aggdsl decompile body.json
```

Accepted JSON shapes:

- Full body with `response` + `request`.
- Legacy form: `{"request": [ ...pipeline stages... ]}`.
- Named form: `{"request": {"name": "...", "pipeline": [ ... ]}}`.
- Pipeline-only array: `[ ...pipeline stages... ]`.

Notes:

- Unsupported/unknown stages are emitted as `| raw { ... }` so the output stays semantically equivalent.
- JSON output uses UTF-8 and does not escape Unicode keys (no `\uXXXX` sequences).
