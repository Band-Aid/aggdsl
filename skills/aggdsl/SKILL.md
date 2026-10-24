---
name: aggdsl
description: >
  Write, fix, or review Pendo aggregation queries in aggDSL (`FROM event([source=...]) | group by ...`,
  `.dsl` files) — feature/page/event usage, visitors/accounts, funnels, retention, PES, session
  replays, frustration, product areas, cohorts. Use for any request that ends in a Pendo aggregation query.
---

# aggDSL

aggDSL compiles to a Pendo Aggregation API body. The compiler checks structure only; field names
and expressions pass through as opaque strings.

## Workflow (do exactly this)

1. Pick the closest pattern below and Read **only that file**. Copy it.
2. Change source/ids/fields. Replace every `{{PLACEHOLDER}}`; ask the user for ids you don't have.
3. Write the query to a `.dsl` file and run `python scripts/check.py q.dsl` (path relative to this
   skill). Fix what it reports and re-run until it prints `OK`. Its messages say how to fix each
   problem, so you don't need to read further docs to fix them.
4. Hand it over. Say "compiles cleanly", not "verified": a clean check can still return no rows
   (wrong appId or id, or a field not on that source).

Open `references/grammar.md` only when you need a stage, function, or source field that isn't in
the pattern. Never read the repo's `Pendo Aggregation * (Project Truth).md` files whole; `grep -n`
them for a specific term.

## Patterns (`patterns/<name>.dsl`)

| Question | Pattern |
|---|---|
| Top features / pages by usage | `top-features` |
| Raw event rows | `raw-feature-events` |
| Trend over time (daily), frustration counts | `daily-trend` |
| Which apps have data | `events-by-app` |
| Unique visitors (one number) | `unique-visitors` |
| Top accounts | `top-accounts` |
| Feature retention | `feature-retention` |
| Funnel / conversion | `funnel-simple` (full report: `funnel-full`) |
| PES / engagement score | `pes`, `pes-segment` |
| Limit to a segment | `segment-filter` |
| Product areas | `product-areas` |
| Cohort: did A, not B (or A and B) | `cohort-a-not-b` |
| Merge inside a spawn branch | `merge-in-branch` |
| Custom track events | `track-events` |
| Guide views / poll responses | `guide-views`, `poll-responses` |
| Session replay list | `replays-list` |
| Error / frustrated / broken sessions | `replays-top-error-sessions`, `replays-frustrated-sessions`, `replays-broken` |
| Watchable replay candidates | `replays-candidates` |
| Frustration by feature/page in sessions | `frustration-by-feature` |
| Events inside one session / session profile | `session-events`, `replay-profile` |
| AI agent prompts | `agent-prompts` |
| Something the DSL can't express | `raw-escape-hatch` |

## Shape

```
FROM event([source=<src>, appId=<id>, blacklist="apply"])
TIMESERIES period=dayRange first=now() count=-30
| filter <expr>
| group by <a,b> fields { x=sum(numEvents), n=count(visitorId) }
| eval { y=<expr> }
| sort -x
| limit 10
```

- One entry line: `FROM event([source=...])` or `PIPELINE`. `TIMESERIES` goes directly after
  `FROM`. Use either `count` or `last`, not both. `count` is signed: `-30` means the 30 days before `first`.
- `pes {...}` and `sessionReplays {...}` are stages, not sources. Write `PIPELINE` and then the stage.
- Source by what is counted: `featureEvents`/`featureId`, `pageEvents`/`pageId`, `events` (all
  activity, frustration, `recordingSessionId`), `trackEvents`/`trackTypeId`, `recordingMetadata`
  (replay sessions), `singleEvents` (individual rows), `guideEvents`/`pollEvents`. For names, merge
  in `features`/`pages`/`visitors`/`accounts`.
- Prefix rule: `|` = stage of an ordinary pipeline; `||` = stage inside a spawn/fork `branch`. A
  `merge` body is an independent query, so its stages use `|` even inside a branch. A block header
  takes its parent's prefix (`|| merge` inside a branch). See `merge-in-branch`.
- Quote string constants: `eval { t="page" }`. An unquoted or backticked value is a field reference.
- Anything unmodeled (`reduce`, `accumulate`, `compute`, ...) goes in `| raw {strict JSON}`.
