// Most frustrated replay sessions from recordingMetadata, with duration.
FROM event([source=recordingMetadata, appId={{APP_ID}}, blacklist="ignore"])
TIMESERIES period=dayRange first=now() count=-30
| filter recordingSessionId != ""
| group by recordingSessionId fields { startTime=min(startTime), endTime=max(endTime), rrwebEvents=max(rrwebEventCount), rageClicks=max(rageClickCount), deadClicks=max(deadClickCount), errorClicks=max(errorClickCount) }
| eval { totalErrors=rageClicks+deadClicks+errorClicks, durationMs=endTime-startTime }
| sort -totalErrors
| limit 5
