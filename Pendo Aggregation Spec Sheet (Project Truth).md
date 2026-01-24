# Pendo Aggregation Spec Sheet (Project Truth)

A. Overview
- This project parses a custom DSL into a Pendo Aggregation request body shaped as `{ "response": ..., "request": {"pipeline": [...] } }` and optionally decompiles JSON back to DSL. The compilation path is `parse(...)` -> `compile_pipeline(...)` -> `compile_to_pendo_aggregation(...)`. (sources: `src/aggdsl/parser.py:parse`, `src/aggdsl/compiler.py:compile_pipeline`, `src/aggdsl/compiler.py:compile_to_pendo_aggregation`)
- The CLI entrypoint `aggdsl compile` reads a `.dsl` file, calls `parse`, then emits JSON via `compile_to_pendo_aggregation`. (source: `src/aggdsl/cli.py:main`)
- DSL supports two entry modes:
  - `FROM event([...])` optionally followed by `TIMESERIES ...`, then pipeline stages. (source: `src/aggdsl/parser.py:parse`)
  - `PIPELINE` to start with pipeline stages and no source stage. (source: `src/aggdsl/parser.py:parse`)

B. Canonical Request Envelope (top-level JSON)
- The compiler ALWAYS emits a JSON object with:
  - `response.location` fixed to `"request"`.
  - `response.mimeType` defaulting to `"application/json"`, overridden by a DSL `RESPONSE` header.
  - `request` as an object containing `pipeline` and optional `name`.
  (sources: `src/aggdsl/compiler.py:compile_to_pendo_aggregation_with_format`, `src/aggdsl/parser.py:parse`)
- The compiler does NOT emit the legacy array request form; it always wraps the pipeline in `request.pipeline`. (source: `src/aggdsl/compiler.py:compile_to_pendo_aggregation_with_format`)
- The compiler ignores `RESPONSE location=...` even if provided; only `mimeType` is honored. (source: `src/aggdsl/parser.py:parse`)

Golden Schema (canonical template)
```json
{
  "response": {
    "location": "request",
    "mimeType": "application/json"
  },
  "request": {
    "name": "<optional string>",
    "pipeline": [
      { "source": { "<sourceType>": { /* params */ }, "timeSeries": { /* optional */ } } },
      { "filter": "<expression>" },
      { "group": { "group": ["field1"], "fields": [ { "alias": { "sum": "field" } } ] } }
    ]
  }
}
```
(sources: `src/aggdsl/compiler.py:compile_to_pendo_aggregation_with_format`, `src/aggdsl/compiler.py:_compile_stage`)

C. Source Stage
- DSL syntax: `FROM event([source=<sourceType>, key=value, ...])`. The `source` key is REQUIRED; other keys are passed through verbatim as source parameters. (source: `src/aggdsl/parser.py:parse`)
- JSON shape emitted:
```json
{ "source": { "<sourceType>": { "key": "value", "other": 123 }, "timeSeries": { ... } } }
```
(source: `src/aggdsl/compiler.py:compile_pipeline`)
- Source type and params are NOT validated against an allowlist; any string `source` value becomes a JSON key. (source: `src/aggdsl/parser.py:parse`)
- Bracket args parsing rules:
  - Strings must be quoted with double quotes to be emitted as strings (e.g. `pollId="p"`).
  - Unquoted tokens are emitted as bare strings (e.g. `blacklist=apply`).
  - `[]` is parsed as an empty list.
  - Integers (including negatives) become JSON numbers.
  (sources: `src/aggdsl/parser.py:_parse_bracket_args`, `src/aggdsl/parser.py:_parse_scalar`)
- Optional time series line:
  - DSL syntax: `TIMESERIES period=<period> first=<value> count=<n>` OR `last=<value>`.
  - Must appear immediately after `FROM` (if present at all).
  - If omitted, no `timeSeries` key is emitted.
  (sources: `src/aggdsl/parser.py:parse`, `src/aggdsl/compiler.py:compile_pipeline`)
- Additional source stages are NOT supported by DSL syntax; if needed, a user must inject a raw stage (`| raw {"source": ... }`). (source: `src/aggdsl/decompiler.py:_decompile_stage`)

Source descriptions (external reference)
- The following source descriptions are provided by the user from EngageAPI documentation and are not validated by this codebase. They describe snapshot-style object sources (not event streams). (source: user-provided external reference)
- These object sources return one row per object with metadata and are typically used for inventory, audits, and joining with events via `merge` or `bulkExpand`. They are not intended for time-series analytics by themselves. (source: user-provided external reference)

Object sources (snapshot-style)
- visitors
  - Summary: all visitors seen in the subscription.
  - Typical fields: visitorId, auto metadata (first visit, last visit, browser, OS), custom metadata, account associations.
  - Typical uses: export user profiles, sync visitor traits, enrich event queries.
- accounts
  - Summary: all accounts defined in Pendo.
  - Typical fields: accountId, account metadata, first/last visit timestamps, per-app metadata.
  - Typical uses: account health reporting, joining usage to customer records, CRM/billing reconciliation.
- doNotProcessAccounts
  - Summary: accounts marked "Do Not Process".
  - Typical fields: exclusion metadata.
  - Typical uses: compliance audits, GDPR coverage checks.
- pages
  - Summary: all tagged pages.
  - Typical fields: pageId, name, URL rules, creation/update metadata, grouping, core event status.
  - Typical uses: page inventory, joining page IDs to analytics, auditing tagging rules.
- features
  - Summary: all tagged features.
  - Typical fields: featureId, name, element path rules, associated pageId, event property configuration.
  - Typical uses: feature catalog exports, mapping feature IDs to usage, validating tagging coverage.
- guides
  - Summary: all guides configured in the subscription.
  - Typical fields: guide metadata (state, launch method, audience), steps, polls, localization state.
  - Typical uses: guide audits, lifecycle tracking, joining guide metadata with guide events.
- groups
  - Summary: logical groupings used by pages or features (often product areas).
  - Typical fields: groupId, name, associated items, visual metadata.
  - Typical uses: product area reporting, organizing pages/features, consistent taxonomy.

Event source descriptions (external reference)
- The following event source descriptions are provided by the user from EngageAPI documentation and are not validated by this codebase. They describe event-based sources that operate over a time window. (source: user-provided external reference)
- The codebase does not validate source names or enforce a timeSeries requirement for these sources; this section is informational only. (source: `src/aggdsl/parser.py:parse`)

Event sources (time series, activity-based)
- events
  - Summary: lowest-level event source, includes page loads (tagged/untagged), feature clicks (tagged/untagged), identify events, metadata updates, and track events if enabled.
  - Behavior: grouped source; returns numEvents and numMinutes.
  - Typical uses: total activity baselines, time-in-app calculations, catch-all activity.
- pageEvents
  - Summary: tagged page view events.
  - Behavior: grouped source; counts page views over time.
  - Typical uses: page traffic, page funnels, navigation behavior.
- featureEvents
  - Summary: tagged feature interaction events.
  - Behavior: grouped source; returns usage counts per feature.
  - Typical uses: feature adoption, feature ranking, core event engagement.
- trackEvents
  - Summary: custom events sent via track API (pendo.track()).
  - Behavior: grouped source; no automatic UI events included.
  - Typical uses: business events, backend actions, non-UI flows.
- guideEvents
  - Summary: guide interactions (impressions, dismissals, steps, advanced behaviors).
  - Behavior: grouped source.
  - Typical uses: guide engagement, walkthrough performance, guide effectiveness.
- guidesSeen
  - Summary: deduplicated guide impressions within the time range.
  - Behavior: grouped; binary-style signal per visitor.
  - Typical uses: reach, "did users see this guide", funnel entry checks.
- guidesSeenEver
  - Summary: lifetime version of guidesSeen (ever seen).
  - Behavior: described as lifetime exposure, but still requires timeSeries in requests per external docs.
  - Typical uses: lifetime adoption, avoiding double counting.
- pollEvents
  - Summary: poll interactions (submissions, dismissals, impressions).
  - Behavior: grouped source.
  - Typical uses: poll engagement, response rates, feedback analysis.
- pollsSeen
  - Summary: deduplicated poll impressions within the time range.
  - Behavior: grouped; binary-style signal per visitor.
  - Typical uses: poll reach, response rate calculations.
- pollsSeenEver
  - Summary: lifetime poll impressions (ever seen).
  - Behavior: described as lifetime exposure, but still requires timeSeries in requests per external docs.
  - Typical uses: long-term exposure analysis, preventing repeated targeting.
- emailEvents
  - Summary: guide email events (sent, opened, clicked).
  - Behavior: grouped source.
  - Typical uses: email performance, open/click analysis, email-driven adoption.

- recordingMetadata
  - Summary: Session Replay recording/session metadata over a time window.
  - Behavior: event source; used to enumerate and analyze replay sessions/recordings and their characteristics.
  - Typical uses: finding candidate session replays, drilling into a specific `recordingSessionId`, building timelines and session-level rollups, grouping by session identifiers for aggregated metrics.
  - Complete pattern: Start with `recordingMetadata` as source, group by `recordingSessionId` to aggregate session metrics, optionally merge with `featureEvents`, `guideEvents`, `pollEvents` to enrich with cross-source data. Use `singleEvents` source if you need individual (non-grouped) event details within a session.
  - Observed params (not enforced by DSL):
    - `appId` (int or array)
    - `blacklist` (string, e.g. `ignore`, `apply`)

- agenticEvents
  - Summary: AI Agent event stream (AiAgents) capturing agent-related interactions such as prompts.
  - Behavior: event source over a time window; returns one row per event.
  - Typical uses: agent prompt audits, troubleshooting, “what did the agent see/say” timelines.
  - Observed params (not enforced by DSL):
    - `agentId` (string)
    - `appId` (int) — observed as required for results in practice
    - `blacklist` (string, e.g. `apply`)
  - Example request (from user):
    - Source stage: `{ "source": { "agenticEvents": {"blacklist":"apply","appId":6304859583021056,"agentId":"..."}, "timeSeries": {"period":"dayRange","first":"dateAdd(startOfPeriod(\"daily\", now()), -30, \"days\")","count":30} } }`

Major columns by source (external reference)
- The following column lists are provided by the user from external documentation and are not validated by this codebase. Field availability may vary by subscription and source configuration. (source: user-provided external reference)
- visitors: visitorld, metadata, metadata.auto.accountid, metadata.auto.accountids, metadata.auto.firstvisit, metadata.auto.lastvisit, metadata.auto.lastbrowsername, metadata.auto.lastbrowserversion, metadata.auto.lastoperatingsystem, metadata.auto.lastservername, metadata.auto.lastupdated, metadata.auto.lastuseragent, metadata.auto_*.*, metadata.agent.*, metadata.custom.*, metadata.salesforce.*, metadata.hubspot.*, metadata.segmentio.*
- accounts: accountId, metadata, metadata.auto.firstvisit, metadata.auto.lastvisit, metadata.auto.lastupdated, metadata.auto_*.*, metadata.agent.*, metadata.custom.*, metadata.salesforce.*, metadata.hubspot.*, metadata.segmentio.*
- pages: id, appId, name, createdByUser, createdAt, lastUpdatedByUser, lastUpdatedAt, group, isCoreEvent, rules, excludeRules, isSuggested, suggestedTagRules
- features: id, appId, name, createdByUser, createdAt, lastUpdatedByUser, lastUpdatedAt, group, isCoreEvent, pageId, appWide, eventPropertyConfigurations, elementPathRules, isSuggested, suggestedMatch
- trackTypes: id, appId, name, createdByUser, createdAt, lastUpdatedByUser, lastUpdatedAt, group, isCoreEvent, rules, eventPropertyNameList
- guides: id, appId, name, createdByUser, createdAt, lastUpdatedByUser, lastUpdatedAt, attributes.*, audience, conversion, currentFirstEligibleToBeSeenAt, dependentMetadata, description, emailState, expiresAfter, isModule, isMultiStep, isTopLevel, launchMethod, polls.*, publishedAt, publishedEver, recurrence, recurrenceEligibilityWindow, redisplay, resetAt, showsAfter, state, steps.*, translationStates, validThrough
- groups: id, name, createdByUser, createdAt, lastUpdatedByUser, lastUpdatedAt, color, description, length, type, feedbackProductId, feedbackVisibility
- events: visitorld, accountId, appId, hour, day, week, month, quarter, firstTime, lastTime, pageId, numEvents, numMinutes, remoteIp, server, userAgent, tabId, rageClickCount, errorClickCount, uTurnCount, deadClickCount, country, region, recordingId, recordingSessionId, lastKeyFrameTimestamp, properties
- pageEvents: visitorld, accountId, appId, hour, day, week, month, quarter, pageId, numEvents, numMinutes, remoteIp, server, userAgent, tabId, rageClickCount, errorClickCount, uTurnCount, deadClickCount, properties
- featureEvents: visitorld, accountId, appId, hour, day, week, month, quarter, featureId, numEvents, numMinutes, remoteIp, server, userAgent, tabId, rageClickCount, errorClickCount, uTurnCount, deadClickCount, properties
- trackEvents: visitorld, accountId, appId, hour, day, week, month, quarter, trackTypeId, numEvents, numMinutes, remoteIp, server, userAgent, tabId, properties
- guideEvents: visitorld, accountId, appId, browserTime, type, guideId, guideSeenReason, guideStepId, destinationStepId, guideStepPollTypes, language, remoteIp, serverName, country, region, latitude, longitude, tabId, url, userAgent, properties, uiElementid, uiElementType, uiElementText, uiElementActions
- pollEvents: visitorld, accountId, appId, browserTime, type, guideId, guideStepId, pollId, pollType, pollResponse, language, remoteIp, serverName, country, region, latitude, longitude, tabId, url, userAgent, properties
- guidesSeen: visitorld, guideId, guideStepId, firstSeenAt, lastAdvancedAutoAt, lastDismissedAutoAt, lastSeenAt, lastTimeoutAt, seenCount, lastState
- pollsSeen: visitorld, guideId, pollId, time, pollResponse
- singleEvents: visitorld, accountId, appId, hour, day, week, month, quarter, numEvents, numMinutes, remoteIp, server, userAgent, tabId, properties, recordingSessionId
- emailEvents: WIP
- recordingMetadata (observed): visitorId, accountId, appId, tabId, recordingId, recordingSessionId, startTime, endTime, minBrowserTime, maxBrowserTime, recordingSize, recordingRrwebEventCount, isBroken, isSessionStart, activityTimelineTimestamps, recordingFirstActiveTs, recordingLastActiveTs, recordingInactivityPeriodStartTimes, recordingInactivityPeriodEndTimes, recordingStartTime, recordingEndTime, recordingLastMobileState
- agenticEvents : visitorId, accountId, eventId, content, browserTime

D. Pipeline Stages (catalog)
General rules
- Every stage line MUST start with `|` (except within spawn branches, which use `||`). (source: `src/aggdsl/parser.py:parse`)
- No ordering constraints are enforced in code beyond the syntax rules above; semantic ordering is left to Pendo. (source: `src/aggdsl/parser.py:parse`)

Stage catalog (DSL -> JSON)
- filter
  - DSL: `| filter <expr>`
  - JSON: `{ "filter": "<expr>" }`
  - Expr is stored as a raw string. (source: `src/aggdsl/parser.py:_parse_stage`, `src/aggdsl/compiler.py:_compile_stage`)
- identified
  - DSL: `| identified <field>`
  - JSON: `{ "identified": "<field>" }`
  (source: `src/aggdsl/parser.py:_parse_stage`, `src/aggdsl/compiler.py:_compile_stage`)
- eval
  - DSL: `| eval { a=b, c=d }`
  - JSON: `{ "eval": { "a": "b", "c": "d" } }`
  - Values are raw expressions (strings). (source: `src/aggdsl/parser.py:_parse_stage`, `src/aggdsl/parser.py:_parse_brace_map`, `src/aggdsl/compiler.py:_compile_stage`)
- select
  - DSL: `| select { out=in, ... }`
  - JSON: `{ "select": { "out": "in", ... } }`
  (source: `src/aggdsl/parser.py:_parse_stage`, `src/aggdsl/compiler.py:_compile_stage`)
- join
  - DSL: `| join fields [field1,field2]`
  - JSON: `{ "join": { "fields": ["field1", "field2"] } }`
  (source: `src/aggdsl/parser.py:_parse_stage`, `src/aggdsl/compiler.py:_compile_stage`)
- sort
  - DSL: `| sort -field,+field`
  - JSON: `{ "sort": ["-field", "+field"] }`
  (source: `src/aggdsl/parser.py:_parse_stage`, `src/aggdsl/compiler.py:_compile_stage`)
- limit
  - DSL: `| limit 100`
  - JSON: `{ "limit": 100 }`
  - Must be a non-negative integer (digits only). (source: `src/aggdsl/parser.py:_parse_stage`)
- group
  - DSL (list form): `| group by a,b fields { x=sum(y), z=count(null) }`
  - JSON: `{ "group": { "group": ["a", "b"], "fields": [ {"x": {"sum": "y"}}, {"z": {"count": null}} ] } }`
  - DSL (map form): `| group by a,b fields map { x=sum(y) }`
  - DSL (object arg): aggregate args may be an object map for aggregators that require structured configs.
    - Example: `inactivityPeriods=inactivityPeriods({ recordingStartTime=recordingStartTime, recordingEndTime=recordingEndTime, appId=appId })`

- fork
  - Purpose: split the current stream into multiple independent pipelines.
  - JSON: `{ "fork": [ <pipeline1>, <pipeline2>, ... ] }` where each `<pipelineN>` is a JSON array of stages.
  - DSL (inline JSON array): `| fork [ ... ]`
  - DSL (block form, no JSON):
    - `| fork`
    - `branch` / `endbranch` blocks containing an inner query
    - stages inside branches use `||` prefixes
    - terminates with `| endfork`
  - JSON: `{ "group": { "group": ["a", "b"], "fields": { "x": {"sum": "y"} } } }`
  - Aggregate name must match `[A-Za-z_][A-Za-z0-9_]*`; argument is raw string, `null` maps to JSON null. (sources: `src/aggdsl/parser.py:_parse_group`, `src/aggdsl/compiler.py:_compile_stage`)
- merge (block)
  - DSL header: `| merge fields [field1,field2]` with optional `mappings { out=in }`.
  - Block contains a full inner query (FROM/PIPELINE + stages); ends with `endmerge` or `| endmerge`.
  - JSON: `{ "merge": { "fields": [...], "mappings": {...?}, "pipeline": [ ... ] } }`
  - Inside merge blocks, pipeline stages use single `|`; legacy `>>` is accepted as `|`.
  (sources: `src/aggdsl/parser.py:_parse_merge_header`, `src/aggdsl/parser.py:_parse_merge_block`, `src/aggdsl/compiler.py:_compile_stage`)
- spawn (block)
  - DSL header: `| spawn` then one or more `branch` blocks, ended by `| endspawn`.
  - Branch structure:
    - `branch` (or `| branch`)
    - inner query (FROM/PIPELINE + stages using `||` prefix)
    - `endbranch` (or `| endbranch`)
  - JSON: `{ "spawn": [ [ ...pipeline... ], [ ...pipeline... ] ] }`
  - Inside spawn branches, stages must use `||`, except for merge blocks where internal stages use `|`. (sources: `src/aggdsl/parser.py:_parse_spawn_block`, `src/aggdsl/compiler.py:_compile_stage`)
- switch
  - DSL: `| switch <out> from <field> { "1"=="aaa", "2"=="bbb" }`
  - JSON: `{ "switch": { "<out>": { "<field>": [ {"value": "1", "==": "aaa"}, ... ] } } }`
  - `<out>` must match `[A-Za-z_][A-Za-z0-9_]*`; `<field>` must match `[A-Za-z_][A-Za-z0-9_.]*`. (sources: `src/aggdsl/parser.py:_parse_stage`, `src/aggdsl/compiler.py:_compile_stage`)
- unmarshal
  - DSL: `| unmarshal { out=in, ... }`
  - JSON: `{ "unmarshal": { "out": "in" } }`
  (source: `src/aggdsl/parser.py:_parse_stage`, `src/aggdsl/compiler.py:_compile_stage`)
- unwind
  - DSL: `| unwind { field=list, index=listIndex, keepEmpty=True }`
  - JSON: `{ "unwind": { "field": "list", "index": "listIndex", "keepEmpty": true } }`
  - Values `true`/`false` (case-insensitive) are coerced to booleans; other values remain strings. (source: `src/aggdsl/parser.py:_parse_stage`, `src/aggdsl/compiler.py:_compile_stage`)
- segment
  - DSL: `| segment id="segmentId"` or `| segment { id="segmentId" }` or `| segment segmentId`
  - JSON: `{ "segment": { "id": "segmentId" } }`
  (source: `src/aggdsl/parser.py:_parse_stage`, `src/aggdsl/compiler.py:_compile_stage`)
- bulkExpand
  - DSL: `| bulkExpand { ...json... }` (JSON object)
  - JSON: `{ "bulkExpand": { ... } }`
  - Supports multi-line JSON objects as long as continuation lines do not start with `|`. (sources: `src/aggdsl/parser.py:_parse_stage`, `src/aggdsl/parser.py:_parse_raw_json_object_multiline`, `src/aggdsl/compiler.py:_compile_stage`)

- sessionReplays
  - DSL: `| sessionReplays { ...json... }` (JSON object)
  - JSON: `{ "sessionReplays": { ... } }`
  - Supports multi-line JSON objects as long as continuation lines do not start with `|`. (sources: `src/aggdsl/parser.py:_parse_stage`, `src/aggdsl/parser.py:parse`, `src/aggdsl/parser.py:_parse_raw_json_object_multiline`, `src/aggdsl/compiler.py:_compile_stage`)
- raw
  - DSL: `| raw { ...json... }` (JSON object)
  - JSON: the object itself (passed through exactly).
  - Supports multi-line JSON objects as long as continuation lines do not start with `|`. (sources: `src/aggdsl/parser.py:_parse_stage`, `src/aggdsl/parser.py:_parse_raw_json_object_multiline`, `src/aggdsl/compiler.py:_compile_stage`)

- fork
  - DSL: `| fork [ ...json... ]` (JSON array)
  - JSON: `{ "fork": [ ... ] }`
  - Supports multi-line JSON arrays as long as continuation lines do not start with `|`. (sources: `src/aggdsl/parser.py:_parse_stage`, `src/aggdsl/parser.py:parse`, `src/aggdsl/parser.py:_parse_raw_json_array_multiline`, `src/aggdsl/compiler.py:_compile_stage`)

E. Expressions & Field Paths
- Expressions are not parsed or validated; they are stored and emitted as raw strings in JSON. This applies to:
  - `filter` expressions.
  - `eval`, `select`, and `unmarshal` map values.
  - group aggregate arguments.
  - `switch` case values and ids.
  - `timeSeries.first` and `timeSeries.last` values when provided as non-integers.
  (sources: `src/aggdsl/parser.py:_parse_stage`, `src/aggdsl/parser.py:_parse_brace_map`, `src/aggdsl/compiler.py:_compile_stage`, `src/aggdsl/parser.py:_coerce_time_value`)
- Commas inside values are supported if they are inside parentheses, brackets, or quotes; this allows list literals like `[1,2,3]` in `eval`. (source: `src/aggdsl/parser.py:_split_by_comma_respecting_groups`)
- Constant strings in `eval`/`select`/`unmarshal` must be quoted in the expression (e.g. `| eval { resultType="page" }`).
  - Unquoted tokens (e.g. `resultType=page`) are treated as field references and may evaluate to `null`.
  - The compiled JSON will contain escaped quotes (e.g. `"page"`) because the value is a JSON string holding the expression.
- Brace map keys are taken literally; quoted keys remain quoted in JSON. The decompiler intentionally emits keys without quotes because the parser treats keys literally. (source: `src/aggdsl/decompiler.py:_format_brace_map`, `src/aggdsl/parser.py:_parse_brace_map`)
- Field paths are case-sensitive and not validated, except for `switch` out/field identifiers (regex rules above) and aggregate function names in `group`. (sources: `src/aggdsl/parser.py:_parse_stage`, `src/aggdsl/parser.py:_parse_group`)

Expression semantics (external reference)
- The following description of expression syntax and functions is provided by the user from external documentation. The codebase does not parse or validate expression semantics; these rules are not enforced here. (sources: user-provided external reference, `src/aggdsl/parser.py:_parse_stage`)
- Expressions are described as a C-style, row-level formula language used in `filter`, `eval`, `select`, and `group` contexts. (source: user-provided external reference)
- Claimed operators include comparison (`==`, `!=`, `<`, `<=`, `>`, `>=`), logical (`&&`, `||`, `!`), and math (`+`, `-`, `*`, `/`, `%`). (source: user-provided external reference)
- Operator explanations (external reference):
  - Comparison: equality and ordering for filters (e.g., `numEvents > 10`).
  - Logical: combine conditions with AND/OR/NOT (e.g., `a && b`, `a || b`, `!a`).
  - Math: basic arithmetic on numeric fields (e.g., `numEvents / numMinutes`).
  (source: user-provided external reference)
- Claimed helper functions include `if(...)`, type checks like `isBoolean`, `isNumber`, `isString`, `isList`, `isNil`, null/empty checks like `isNull`, `isEmpty`, and string helpers like `contains`, `startsWith`, `split`, `toLowerCase`, `toString`. (source: user-provided external reference)
- Claimed null-handling behaviors and list semantics are external and not validated by this project. (source: user-provided external reference)

Field aggregators (external reference)
- The following aggregator descriptions are provided by the user from external documentation. The codebase does not validate aggregator names or semantics beyond basic parsing in `group`. (sources: user-provided external reference, `src/aggdsl/parser.py:_parse_group`)
- Aggregators are described as functions that collapse multiple values into one, typically used in `group` (per-group) and `reduce` (global). This codebase only models `group` directly. (source: user-provided external reference)
- Claimed common aggregators:
  - `sum`: totals numeric values.
  - `count`: counts rows when argument is `null`, or counts distinct non-null values when given a field.
  - `countIf`: counts rows matching a condition.
  - `avg`/`mean`: averages numeric values.
  - `median`: middle value of sorted numeric values.
  - `min`/`max`: smallest/largest value.
  - `first`: first non-null value (order-dependent).
  - `list`: collects values into a list (may include duplicates).
  (source: user-provided external reference)

F. Time Series & Date Handling
- `TIMESERIES` MUST include `period` and `first`, and MUST include exactly one of `count` or `last`. (source: `src/aggdsl/parser.py:parse`)
- `first`/`last` accept:
  - Integers (epoch ms).
  - `now()` (string) which can be replaced with `now_ms` at compile time.
  - Any other raw expression string (e.g., `date(2025, 1, 1, 12, 30, 00)`).
  (sources: `src/aggdsl/parser.py:_coerce_time_value`, `src/aggdsl/compiler.py:compile_pipeline`)
- If `now_ms` is provided to `compile_to_pendo_aggregation`, literal `now()` is replaced by that integer. Otherwise it is preserved as the string `"now()"`. (source: `src/aggdsl/compiler.py:compile_pipeline`)
- `count` is a signed number. Negative values are accepted and preserved. (source: `tests/test_timeseries_negative_count.py`)
  - Example: `period=dayRange first=now() count=-30` means 30 days before now.
  - Example: `period=dayRange first=now() count=30` means 30 days after now.
- Period values are passed through as raw strings; no validation occurs. (source: `src/aggdsl/parser.py:parse`)

G. Validation & Errors
- Parser errors raise `DslParseError` with messages that indicate the failing construct. Common failure modes include:
  - Empty input -> `Empty DSL`. (source: `src/aggdsl/parser.py:parse`)
  - Missing `FROM` or `PIPELINE` -> `Missing FROM or PIPELINE`. (source: `src/aggdsl/parser.py:parse`)
  - Stage lines not starting with `|` -> `Expected pipeline stage starting with '|'`. (source: `src/aggdsl/parser.py:parse`)
  - Unknown stage keyword -> `Unknown stage: ...`. (source: `src/aggdsl/parser.py:_parse_stage`)
  - Invalid `TIMESERIES` fields (missing `period/first` or both `count` and `last`) -> explicit errors. (source: `src/aggdsl/parser.py:parse`)
  - `join fields [...]` empty -> `join fields [...] cannot be empty`. (source: `src/aggdsl/parser.py:_parse_stage`)
  - `limit` non-integer -> `limit must be an integer`. (source: `src/aggdsl/parser.py:_parse_stage`)
  - `group` malformed syntax -> `Group syntax: ...` or `Invalid aggregate expression`. (source: `src/aggdsl/parser.py:_parse_group`)
  - `merge` block missing `endmerge` -> `merge block missing 'endmerge'`. (source: `src/aggdsl/parser.py:_parse_merge_block`)
  - `spawn` block missing `| endspawn` -> `spawn block missing '| endspawn'`. (source: `src/aggdsl/parser.py:_parse_spawn_block`)
  - `raw`/`bulkExpand` invalid JSON -> `Invalid JSON in raw stage: ...`. (source: `src/aggdsl/parser.py:_parse_raw_json_object`)
- Compiler errors raise `CompileError`, including:
  - Unsupported request format (anything other than `object`). (source: `src/aggdsl/compiler.py:compile_to_pendo_aggregation_with_format`)
  - Unknown stage kind (if parser somehow emits an unsupported kind). (source: `src/aggdsl/compiler.py:_compile_stage`)
- CLI behavior: errors are printed to stderr with prefix `error:` and exit code 2. (source: `src/aggdsl/cli.py:main`)

H. Field notes & pitfalls (observed in practice)
- Filter language: the API rejects `in [...]`; use explicit equality chains with `||` (e.g., `field == "a" || field == "b"`).
- Session detail breakdown: `singleEvents` returned empty aggregates when grouped by `recordingSessionId`; `pageEvents` or `events` are safer for per-session aggregates (dead/error/rage/uTurn, pageId).

I. Examples (at least 5, increasing complexity)

Example 1: Minimal source + select
DSL:
```
FROM event([source=visitors])
| select { visitorId=visitorId, accountId=accountId }
```
JSON:
```json
{
  "response": {"location": "request", "mimeType": "application/json"},
  "request": {
    "pipeline": [
      {"source": {"visitors": {}}},
      {"select": {"visitorId": "visitorId", "accountId": "accountId"}}
    ]
  }
}
```
(sources: `src/aggdsl/parser.py:parse`, `src/aggdsl/compiler.py:compile_pipeline`)

Example 2: Time series + filter + group (list fields) + sort + limit
DSL:
```
FROM event([source=pageEvents,pageId="Mal8cGcc-XXqBEJ-c5C1L3frUz8",blacklist="apply"])
TIMESERIES period=dayRange first=1731769200000 count=180
| filter !isNull(parameters.parameter) && parameters.parameter != ""
| group by pageId,parameters.parameter fields { numEvents=sum(numEvents) }
| sort -numEvents
| limit 100
```
JSON:
```json
{
  "response": {"location": "request", "mimeType": "application/json"},
  "request": {
    "pipeline": [
      {
        "source": {
          "pageEvents": {"pageId": "Mal8cGcc-XXqBEJ-c5C1L3frUz8", "blacklist": "apply"},
          "timeSeries": {"period": "dayRange", "first": 1731769200000, "count": 180}
        }
      },
      {"filter": "!isNull(parameters.parameter) && parameters.parameter != \"\""},
      {"group": {"group": ["pageId", "parameters.parameter"], "fields": [{"numEvents": {"sum": "numEvents"}}]}},
      {"sort": ["-numEvents"]},
      {"limit": 100}
    ]
  }
}
```
(source: `tests/test_spec_example.py`)

Example 3: Group fields map form
DSL:
```
PIPELINE
| group by accountId,groupId fields map { mostRecentDay=max(day) }
```
JSON:
```json
{
  "response": {"location": "request", "mimeType": "application/json"},
  "request": {
    "pipeline": [
      {"group": {"group": ["accountId", "groupId"], "fields": {"mostRecentDay": {"max": "day"}}}}
    ]
  }
}
```
(source: `tests/test_group_fieldsmap.py`)

Example 4: Merge block with mappings
DSL:
```
PIPELINE
| merge fields [pageId] mappings { groupId=group.id }
FROM event([source=pages,appId=[]])
| filter !isNil(group.id)
| eval { pageId=id }
endmerge
```
JSON:
```json
{
  "response": {"location": "request", "mimeType": "application/json"},
  "request": {
    "pipeline": [
      {"merge": {"fields": ["pageId"], "mappings": {"groupId": "group.id"}, "pipeline": [
        {"source": {"pages": {"appId": []}}},
        {"filter": "!isNil(group.id)"},
        {"eval": {"pageId": "id"}}
      ]}}
    ]
  }
}
```
(source: `tests/test_merge_and_unmarshal.py`)

Example 5: Spawn + switch + limit
DSL:
```
PIPELINE
| spawn
branch
FROM event([source=pollEvents,guideId="g",pollId="p",blacklist="apply"])
TIMESERIES period=dayRange first=date(`firstDate`) last=date(`lastDate`)
|| identified visitorId
|| switch mapped from pollResponse { "1"=="aaa", "2"=="bbb" }
|| select { pollResponse1=mapped }
endbranch
| endspawn
| limit 10
```
JSON:
```json
{
  "response": {"location": "request", "mimeType": "application/json"},
  "request": {
    "pipeline": [
      {"spawn": [[
        {"source": {"pollEvents": {"guideId": "g", "pollId": "p", "blacklist": "apply"}, "timeSeries": {"period": "dayRange", "first": "date(`firstDate`)", "last": "date(`lastDate`)"}}},
        {"identified": "visitorId"},
        {"switch": {"mapped": {"pollResponse": [{"value": "1", "==": "aaa"}, {"value": "2", "==": "bbb"}]}}},
        {"select": {"pollResponse1": "mapped"}}
      ]]},
      {"limit": 10}
    ]
  }
}
```
(source: `tests/test_spawn_and_switch.py`)

Example 6: Top error-heavy sessions (events source)
DSL:
```
RESPONSE mimeType=application/json
REQUEST name="TopErrorSessionsLast30Days"
FROM event([source=events,appId=5767130939129856,blacklist="apply"])
TIMESERIES period=dayRange first=dateAdd(startOfPeriod("daily", now()), -30, "days") count=30
| filter !isNull(recordingSessionId)
| group by recordingSessionId fields { events=sum(numEvents), minutes=sum(numMinutes), errorClicks=sum(errorClickCount), deadClicks=sum(deadClickCount), rageClicks=sum(rageClickCount), uTurns=sum(uTurnCount) }
| eval { errorScore=errorClicks + rageClicks + deadClicks + uTurns }
| sort -errorScore
| limit 10
```

Example 7: Page-level friction for specific sessions (pageEvents + OR filter)
DSL:
```
RESPONSE mimeType=application/json
REQUEST name="ErrorSessionDetailsTop3"
FROM event([source=pageEvents,appId=5767130939129856,blacklist="apply"])
TIMESERIES period=dayRange first=dateAdd(startOfPeriod("daily", now()), -30, "days") count=30
| filter recordingSessionId == "CwXPili73WV555Zm" || recordingSessionId == "gKl6cyh9yr7trX3Z" || recordingSessionId == "SpsL1Xg6hFo7vlI3"
| group by recordingSessionId,pageId fields { events=sum(numEvents), errorClicks=sum(errorClickCount), deadClicks=sum(deadClickCount), rageClicks=sum(rageClickCount), uTurns=sum(uTurnCount) }
| sort -events
| limit 200
```

J. Downstream Agent Instructions (the part that will be used as a system prompt)
You MUST generate DSL or JSON that matches the exact structures below. Do not invent stages or fields.

Allowed grammar (DSL)
- Header:
  - Optional: `RESPONSE mimeType=<string>`
  - Optional: `REQUEST name="<string>"`
- Entry:
  - `FROM event([source=<sourceType>, key=value, ...])` optionally followed by `TIMESERIES period=<p> first=<v> count=<n>` OR `last=<v>`
  - OR `PIPELINE`
- Pipeline stages (each must start with `|`):
  - `filter <expr>`
  - `identified <field>`
  - `eval { out=expr, ... }`
  - `select { out=expr, ... }`
  - `join fields [field1,field2,...]`
  - `sort -field,+field,...`
  - `limit <non-negative int>`
  - `group by a,b fields { alias=agg(arg), ... }`
  - `group by a,b fields map { alias=agg(arg), ... }`
  - `merge fields [f1,f2] (optional: mappings { out=in })` then inner query, end with `endmerge`
  - `spawn` block with one or more `branch` blocks, end with `| endspawn`
    - Inside a branch, use `||` for stages
  - `switch <out> from <field> { "value"=="id", ... }`
  - `unmarshal { out=expr, ... }`
  - `unwind { field=list, index=idx, keepEmpty=True|False }`
  - `segment id="segmentId"` (also `segment { id=... }` or `segment segmentId`)
  - `bulkExpand { <json object> }`
  - `fork [ <json array> ]`
  - `sessionReplays { <json object> }`
  - `raw { <json object> }`

Stage catalog (JSON output)
- Source stage: `{ "source": { "<sourceType>": { ... }, "timeSeries": {"period": "...", "first": <int|string>, "count": <int> | "last": <int|string> }? } }`
- filter: `{ "filter": "<expr>" }`
- identified: `{ "identified": "<field>" }`
- eval: `{ "eval": { "out": "expr", ... } }`
- select: `{ "select": { "out": "expr", ... } }`
- join: `{ "join": { "fields": ["f1","f2"] } }`
- sort: `{ "sort": ["-f1", "+f2"] }`
- limit: `{ "limit": 100 }`
- group (list form): `{ "group": { "group": ["a","b"], "fields": [ {"alias": {"sum": "field"}} ] } }`
- group (map form): `{ "group": { "group": ["a","b"], "fields": { "alias": {"sum": "field"} } } }`
- merge: `{ "merge": { "fields": ["f1"], "mappings": {"out": "in"}?, "pipeline": [ ... ] } }`
- spawn: `{ "spawn": [ [ ...pipeline... ], [ ...pipeline... ] ] }`
- switch: `{ "switch": { "<out>": { "<field>": [ {"value": "...", "==": "..."} ] } } }`
- unmarshal: `{ "unmarshal": { "out": "expr" } }`
- unwind: `{ "unwind": { "field": "list", "index": "idx", "keepEmpty": true } }`
- segment: `{ "segment": { "id": "segmentId" } }`
- bulkExpand: `{ "bulkExpand": { ... } }`
- fork: `{ "fork": [ ... ] }`
- sessionReplays: `{ "sessionReplays": { ... } }`
- raw: `{ "<anyStageKey>": ... }` (passes through exactly)

Do/Don't
- DO keep expressions as raw strings; do not evaluate or rewrite them.
- DO use `TIMESERIES` only immediately after `FROM` and include exactly one of `count` or `last`.
- DO use `PIPELINE` if there is no source stage.
- DO use `||` inside spawn branches; use `|` inside merge blocks.
- DO ensure `switch` out and field identifiers follow the regex rules: out `[A-Za-z_][A-Za-z0-9_]*`, field `[A-Za-z_][A-Za-z0-9_.]*`.
- DO NOT invent stages beyond those listed; use `raw { ... }` for anything else.
- DO NOT quote brace-map keys unless you intend the quotes to be part of the JSON key.

Minimal valid examples
- DSL:
```
FROM event([source=visitors])
| select { visitorId=visitorId }
```
- JSON:
```json
{ "response": {"location": "request", "mimeType": "application/json"}, "request": {"pipeline": [{"source": {"visitors": {}}}, {"select": {"visitorId": "visitorId"}}]} }
```
