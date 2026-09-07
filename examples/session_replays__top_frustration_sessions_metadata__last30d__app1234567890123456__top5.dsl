// Top frustration-heavy sessions from recordingMetadata over the last 30 days for a single app.
// Returns totals and durations grouped by recordingSessionId.
RESPONSE mimeType=application/json
REQUEST name="TopSRFrustrationSessionsLast30Days"
FROM event([source=recordingMetadata,appId=1234567890123456,blacklist="ignore"])
TIMESERIES period=dayRange first=dateAdd(startOfPeriod("daily", now()), -30, "days") count=30
| filter recordingSessionId != ""
| group by recordingSessionId fields { startTime=min(startTime), endTime=max(endTime), rrwebEvents=max(rrwebEventCount), rageClicks=max(rageClickCount), deadClicks=max(deadClickCount), errorClicks=max(errorClickCount) }
| eval { totalErrors=rageClicks+deadClicks+errorClicks, durationMs=endTime-startTime }
| sort -totalErrors
| limit 5
