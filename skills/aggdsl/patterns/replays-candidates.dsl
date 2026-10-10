// Watchable replay candidates: not broken, >2 rrweb events, starts a session, >=300ms long; newest 100.
FROM event([source=recordingMetadata, appId={{APP_ID}}, blacklist="apply"])
TIMESERIES period=dayRange first=now() count=-30
| filter recordingSessionId != "" && !isBroken && rrwebEventCount > 2
| group by visitorId,accountId,recordingSessionId fields { startTime=min(startTime), endTime=max(endTime), minBrowserTime=min(minBrowserTime), eventCount=sum(rrwebEventCount), rageClickCount=sum(rageClickCount), deadClickCount=sum(deadClickCount), errorClickCount=sum(errorClickCount), uTurnCount=sum(uTurnCount), isSessionStart=sum(if(isSessionStart, 1, 0)) }
| filter isSessionStart > 0
| eval { duration=endTime - startTime }
| filter duration >= 300
| sort -minBrowserTime
| limit 100
