# aggDSL grammar (look things up here; don't read it all)

## Lines
`[RESPONSE mimeType=...]` `[REQUEST name="..."]` → entry (`FROM event([...])` | `PIPELINE`) →
`[TIMESERIES period=dayRange first=<ts> count=<±N> | last=<ts>]` → stages.
- Comments: whole-line `//` or `#` only.
- Bracket values: `"x"` string, bare `x` string, `-12` number, `true`/`false`, `[]`, `{...}`, `{{X}}` kept literally.
- `period`: `dayRange` `hourRange` `weekRange` `monthRange` `quarterRange`.
- `first`: `now()`, epoch-ms, `date(2025,1,1,0,0,0)`, `dateAdd(startOfPeriod("daily", now()), -30, "days")`.
- Common source params: `appId` (`-323232`, `[]` = all apps on object sources), `blacklist` (`apply`|`ignore`),
  `pageId` `featureId` `guideId` `pollId` `agentId`, `ignoreFrustration=only`.

## Source fields
Event sources (need TIMESERIES). All grouped ones have `visitorId accountId appId hour day week month quarter numEvents numMinutes userAgent tabId properties`, plus:
- `events`: `pageId firstTime lastTime country region recordingId recordingSessionId rageClickCount errorClickCount deadClickCount uTurnCount`
- `pageEvents`: `pageId` + click counts above · `featureEvents`: `featureId` + click counts · `trackEvents`: `trackTypeId`
- `singleEvents`: `recordingSessionId browserTime` (one row per event; grouping by `recordingSessionId` gives empty aggregates)
- `guideEvents`: `browserTime type guideId guideStepId guideSeenReason language url uiElementText`
- `pollEvents`: `browserTime type guideId pollId pollType pollResponse`
- `guidesSeen`: `guideId guideStepId firstSeenAt lastSeenAt seenCount lastState` · `pollsSeen`: `guideId pollId time pollResponse`
- `recordingMetadata`: `recordingId recordingSessionId startTime endTime minBrowserTime maxBrowserTime recordingSize recordingRrwebEventCount isBroken isSessionStart activityTimelineTimestamps recordingStartTime recordingEndTime` + inactivity fields
- `agenticEvents`: `eventId content browserTime` (needs `agentId` and `appId`)

Object sources (no TIMESERIES; usually inside `merge`): `pages`/`features`/`trackTypes`/`guides`:
`id appId name group (group.id, group.name) isCoreEvent createdAt`; `visitors`: `visitorId metadata.auto.* metadata.custom.*`;
`accounts`: `accountId metadata.*`; `groups` (product areas): `id name`.

## Stages
| DSL | JSON |
|---|---|
| `\| filter <expr>` | `{"filter":"<expr>"}` |
| `\| identified visitorId` | drops anonymous visitors |
| `\| eval { a=<expr> }` / `\| select { out=in }` | add fields / keep only these |
| `\| group by a,b fields { x=agg(f) }` | list form; `fields map {…}` → map form; `group by  fields {…}` (empty) = global rollup; may span lines |
| `\| sort -a,+b` | `-` desc, `+`/none asc |
| `\| limit N` | digits only |
| `\| join fields [a]` | `[]` allowed |
| `\| switch out from f { "1"=="a", "2"=="b" }` | value mapping |
| `\| unwind { field=list, index=i, keepEmpty=True }` | one row per list item |
| `\| unmarshal { out=expr }` | `{"unmarshal":{"out":"expr"}}` |
| `\| segment id="..."` | restrict to segment |
| `\| pes {json}` `\| sessionReplays {json}` `\| bulkExpand {json}` | PIPELINE-mode stages; strict JSON |
| `\| raw {json}` | verbatim (`reduce`, `accumulate`, `compute`, `fork` as JSON) |

## Blocks
```
| merge fields [k] mappings { outer=inner }   # join; body is a full query using |
FROM event([...])
| ...
endmerge
| spawn            # or | fork : union of branches
branch
FROM event([...])  # or PIPELINE
|| ...             # branch stages use ||
endbranch
| endspawn         # | endfork
```
- Inside a branch the header is `|| merge` / `|| spawn`; merge body still `|`; after `endmerge` back to `||`.
- `||` never becomes `|||`. A merge body cannot contain a branch (put the spawn outside).
- No match in merge → mapped field is null: "A not B" = `filter isNull(b)`, "A and B" = `!isNull(b)`.

## Functions
- Aggregators: `sum count(f)` (distinct non-null) `count(null)` (rows) `countIf(c) avg median min max first any list concat`;
  structured: `funnel({ items=[{'pageId':'x'},{'featureId':'y'}], maxDuration=0, uniqueVisitorFunnel=True })`,
  `inactivityPeriods({...})`, `accumulate({ fields={'count':'count'}, sort=['-steps'] })`.
- Expressions: `if(c,a,b) isNull isNil isEmpty contains(list|str, x) startsWith split toLowerCase toString len sortUnique listAverage listMedian`,
  `x[0]`, `== != < <= > >= && || ! + - * / %`. No `in`.
- `formatTime(layout, ts)`: Go layout built from `Mon Jan 2 15:04:05 MST 2006`
  (`2006` year, `01` month, `Jan` month name, `02` day, `15` hour, `04` minute, `05` second, `Mon` weekday).
  `"2006-01-02"` → `2025-03-14`; `"Jan 2"` → `Mar 14`.
