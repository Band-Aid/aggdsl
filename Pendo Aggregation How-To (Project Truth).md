# Pendo Aggregation How-To (Project Truth)

This guide shows engineers and analysts how to write valid Pendo Aggregation requests for this project, based strictly on the implemented DSL/parser/compiler. It covers the end-to-end flow from DSL to JSON, with step-by-step instructions and examples.

## 1) Decide your entry mode
You MUST choose exactly one of:

- `FROM event([...])` (source-first mode) with optional `TIMESERIES`.
- `PIPELINE` (pipeline-only mode) when you want to start with stages like `spawn`, `merge`, or `raw` without a source stage.

Example (source-first):
```
FROM event([source=visitors])
| select { visitorId=visitorId }
```

Example (pipeline-only):
```
PIPELINE
| raw {"limit": 10}
```

## 2) Define the source stage (if using FROM)
Use the required `source` key inside the bracket list. Any other key/value pairs are passed through directly.

Rules:
- `source=...` is REQUIRED.
- Quoted values are strings. Unquoted values are bare tokens.
- `[]` becomes an empty list.
- Integers become JSON numbers (including negatives).

Example:
```
FROM event([source=pageEvents,pageId="abc",blacklist="apply",appId=[]])
```

JSON emitted:
```json
{"source": {"pageEvents": {"pageId": "abc", "blacklist": "apply", "appId": []}}}
```

## 3) Add TIMESERIES when you need a time window
`TIMESERIES` must appear immediately after `FROM` and include `period` and `first`, plus exactly one of `count` or `last`.

Rules:
- `first` and `last` can be integers, `now()`, or any raw expression string.
- `count` can be negative and will be preserved.

Example (count form):
```
TIMESERIES period=dayRange first=1731769200000 count=180
```

Example (last form):
```
TIMESERIES period=dayRange first=now() last=date(2025, 1, 1, 12, 30, 00)
```

## 4) Build your pipeline stages
Each stage line starts with `|` (or `||` inside spawn branches). The DSL does not validate field names or expressions; they are emitted as raw strings.

### 4.1 Filter
```
| filter <expr>
```
Example:
```
| filter !isNull(parameters.parameter) && parameters.parameter != ""
```

### 4.2 Identified
```
| identified <field>
```
Example:
```
| identified visitorId
```

### 4.3 Eval
```
| eval { out=expr, ... }
```
Example:
```
| eval { day=startOfPeriod("dayRange", browserTime) }
```

### 4.4 Select
```
| select { out=expr, ... }
```
Example:
```
| select { accountId=accountId, visitorId=visitorId }
```

### 4.5 Join
```
| join fields [field1,field2]
```
Example:
```
| join fields [browserTime,visitorId]
```

### 4.6 Sort
```
| sort -field,+field
```
Example:
```
| sort -numEvents
```

### 4.7 Limit
```
| limit 100
```

### 4.8 Group (list form)
```
| group by a,b fields { alias=agg(arg), ... }
```
Example:
```
| group by accountId fields { numEvents=sum(numEvents), numResponses=count(null) }
```

### 4.9 Group (map form)
```
| group by a,b fields map { alias=agg(arg), ... }
```
Example:
```
| group by accountId fields map { mostRecentDay=max(day) }
```

### 4.10 Merge (block)
Merge defines an inner pipeline and joins on fields.

```
| merge fields [field1,field2] mappings { out=in }
FROM event([source=pages,appId=[]])
| filter !isNil(group.id)
| eval { pageId=id }
endmerge
```

Notes:
- `mappings` is optional.
- Inside merge blocks, pipeline stages use single `|`.

### 4.11 Spawn (block)
Spawn runs multiple branches in parallel.

```
| spawn
branch
FROM event([source=pollEvents,guideId="g",pollId="p",blacklist="apply"])
TIMESERIES period=dayRange first=date(`firstDate`) last=date(`lastDate`)
|| identified visitorId
|| switch mapped from pollResponse { "1"=="aaa", "2"=="bbb" }
|| select { pollResponse1=mapped }
endbranch
| endspawn
```

Notes:
- Inside `branch` blocks, stages MUST use `||`.
- Merge blocks inside spawn branches can use `|` for their internal lines.

### 4.12 Switch
```
| switch <out> from <field> { "value"=="id", ... }
```
Example:
```
| switch mapped from pollResponse { "1"=="aaa", "2"=="bbb" }
```

### 4.13 Unmarshal
```
| unmarshal { out=expr, ... }
```

### 4.14 Unwind
```
| unwind { field=list, index=listIndex, keepEmpty=True }
```

### 4.15 Segment
```
| segment id="segmentId"
```

### 4.16 BulkExpand
```
| bulkExpand {"account":{"account":"accountId"}}
```

### 4.17 Raw (escape hatch)
```
| raw {"someStage": {"any": "json"}}
```

## 5) Add optional headers
Headers must appear before `FROM` or `PIPELINE`:

- `RESPONSE mimeType=...` (default is `application/json`)
- `REQUEST name="..."` (optional request name)

Example:
```
RESPONSE mimeType=text/csv
REQUEST name="NPSResponses"
FROM event([source=pollEvents,guideId="g",pollId="p",blacklist="apply"])
| identified visitorId
| select { visitorId=visitorId, accountId=accountId }
```

## 6) Compile to JSON
Use the CLI to compile DSL to JSON:

```
aggdsl compile path/to/query.dsl
```

Or:
```
python -m aggdsl compile path/to/query.dsl
```

This outputs the JSON request body:
```json
{
  "response": {"location": "request", "mimeType": "application/json"},
  "request": {"pipeline": [ ... ]}
}
```

## 7) Validate common errors
If parsing fails, check these first:

- Missing `FROM` or `PIPELINE`.
- Stage lines missing the `|` prefix.
- `TIMESERIES` missing `period`, `first`, or both `count` and `last`.
- `join fields [...]` with an empty list.
- `limit` not an integer.
- `group` stage syntax errors.
- `merge` block missing `endmerge`.
- `spawn` block missing `| endspawn`.
- `raw`/`bulkExpand` JSON invalid.

## 8) Full example (end-to-end)
DSL:
```
RESPONSE mimeType=application/json
REQUEST name="ExampleAggregation"
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
    "name": "ExampleAggregation",
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
