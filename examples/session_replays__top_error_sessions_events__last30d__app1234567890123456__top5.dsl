// Top session replay error sessions (events-based) over the last 30 days for a single app.
// Focuses on frustration click counts grouped by recordingSessionId.
RESPONSE mimeType=application/json
REQUEST name="TopSRErrorSessionsLast30Days"
FROM event([source=events,blacklist="apply"])
TIMESERIES period=dayRange first=dateAdd(startOfPeriod("daily", now()), -30, "days") count=30
| filter appId == 1234567890123456
| filter !isNull(recordingSessionId)
| group by recordingSessionId fields { errorClicks=sum(errorClickCount), deadClicks=sum(deadClickCount), rageClicks=sum(rageClickCount), uTurns=sum(uTurnCount) }
| sort -errorClicks
| limit 5
