## Session Replay Query Examples

### Example 1: List sessions with frustration metrics

Get the 50 most recent session replays with frustration indicators:

DSL:
```
REQUEST name="SessionReplaysWithFrustration"
PIPELINE
| sessionReplays { limit=50, frustration=[deadClicks,rageClicks,errorClicks] }
| sort -startTime
```

### Example 2: Get detailed events within a specific session

To retrieve individual events from a session replay with detailed event-level information, use the `singleEvents` source:

DSL:
```
REQUEST name="SessionReplayEventDetails"
FROM event([source=singleEvents])
| filter recordingSessionId == "gKl6cyh9yr7trX3Z"
| sort -browserTime
```

The `singleEvents` source provides event-level details for a recorded session, including:
- visitorId, accountId, appId
- hour, day, week, month, quarter (time bucketing)
- numEvents, numMinutes (aggregates per bucket)
- remoteIp, server, userAgent, tabId
- recordingSessionId (links to the session replay)
- properties (custom event properties)

This allows you to analyze individual events within a session replay with full event-level granularity.

### Example 3: Comprehensive session replay analysis with enrichment

Get session-level details enriched with frustration metrics and guide data using merges:

DSL:
```
REQUEST name="SessionReplayAnalysisEnriched"
FROM event([source=recordingMetadata,appId=-323232,blacklist=apply])
TIMESERIES period=dayRange first=now() count=30
| filter recordingSessionId != ""
| filter !isBroken
| group by visitorId,accountId,recordingSessionId fields { startTime=min(startTime), endTime=max(endTime), duration=max(endTime)-min(startTime), eventCount=sum(rrwebEventCount), rageClickCount=sum(rageClickCount), deadClickCount=sum(deadClickCount) }
| merge fields [visitorId,accountId,recordingSessionId] mappings { guideIds=guideIds, autoActivatedGuideCount=autoActivatedGuideCount }
FROM event([source=guideEvents,appId=-323232,blacklist=apply])
TIMESERIES period=dayRange first=now() count=30
|| filter recordingSessionId != ""
|| filter type == "guideSeen"
|| group by visitorId,accountId,recordingSessionId fields { guideIds=list(guideId), autoActivatedGuideCount=countIf(if(guideSeenReason=="auto",guideId,null)) }
endmerge
| sort -startTime
| limit 100
```

This approach:
1. Starts with `recordingMetadata` to get session-level metrics
2. Groups by session identifiers to aggregate data
3. Merges with `guideEvents` to add guide interaction data
4. Can include additional merges with `featureEvents`, `pollEvents`, etc.

### Example 4: Filter sessions by quality metrics

Find sessions with sufficient activity and valid recordings:

DSL:
```
REQUEST name="QualitySessionFilter"
FROM event([source=recordingMetadata,appId=-323232,blacklist=apply])
TIMESERIES period=dayRange first=now() count=30
| filter recordingSessionId != ""
| filter !isBroken
| filter rrwebEventCount > 2
| group by visitorId,accountId,recordingSessionId fields { eventCount=sum(rrwebEventCount), startTime=min(startTime), endTime=max(endTime) }
| eval { duration=endTime-startTime }
| filter duration >= 300
| sort -startTime
| limit 500
```

This ensures you're analyzing valid, complete sessions with meaningful activity.

### Example 5: Multi-source enrichment pattern

Merge session metadata with multiple event sources for comprehensive analysis:

DSL:
```
REQUEST name="SessionWithFeatureEvents"
FROM event([source=recordingMetadata,appId=-323232,blacklist=apply])
TIMESERIES period=dayRange first=now() count=30
| filter recordingSessionId != ""
| group by visitorId,accountId,recordingSessionId fields { sessionStart=min(startTime), sessionEnd=max(endTime), sessionEvents=sum(rrwebEventCount), rageClicks=sum(rageClickCount), deadClicks=sum(deadClickCount) }
| merge fields [visitorId,accountId,recordingSessionId] mappings { featureFrustration=frustration }
FROM event([source=featureEvents,appId=-323232,blacklist=apply,ignoreFrustration=only])
TIMESERIES period=dayRange first=now() count=30
|| filter recordingSessionId != ""
|| group by visitorId,accountId,recordingSessionId fields { frustration=sum(rageClickCount)+sum(deadClickCount)+sum(errorClickCount) }
endmerge
| sort -sessionStart
```

This pattern allows you to enrich session data with frustration counts from feature interactions.

### Example 6: Session data with visitor and account enrichment

Enrich session data with account information via bulkExpand:

DSL:
```
REQUEST name="SessionsWithAccountData"
FROM event([source=recordingMetadata,appId=-323232,blacklist=apply])
TIMESERIES period=dayRange first=now() count=30
| filter recordingSessionId != ""
| group by visitorId,accountId,recordingSessionId fields { startTime=min(startTime), endTime=max(endTime), events=sum(rrwebEventCount), rageClicks=sum(rageClickCount), deadClicks=sum(deadClickCount), errorClicks=sum(errorClickCount) }
| bulkExpand { account={ account=accountId } }
| eval { accountName=account.auto.name, accountPlan=account.metadata.custom.plan }
| select { visitorId=visitorId, accountId=accountId, accountName=accountName, recordingSessionId=recordingSessionId, startTime=startTime, endTime=endTime, events=events, rageClicks=rageClicks, deadClicks=deadClicks }
```

The `bulkExpand` stage joins with account data to enrich each session record.

## Key Patterns

**Start with recordingMetadata:**
- Use `recordingMetadata` as the base source for session-level analysis
- Always filter by `recordingSessionId != ""` to ensure valid sessions
- Filter by `!isBroken` to exclude corrupted recordings
- Group by `visitorId,accountId,recordingSessionId` to aggregate session metrics

**For individual event details:**
- Use `singleEvents` source when you need per-event granularity
- Filter by `recordingSessionId` to isolate events from a specific session
- Combine with time/property filters as needed

**For enrichment:**
- Use `merge` blocks to combine data from `featureEvents`, `guideEvents`, `pollEvents`
- Use `bulkExpand` to join with master data (accounts, apps)
- Chain multiple merges for comprehensive analysis

**Quality control:**
- Filter by `rrwebEventCount > 2` for meaningful sessions
- Filter by `duration >= 300` (milliseconds) for sessions of sufficient length
- Check `isSessionStart` flag to identify proper session boundaries
