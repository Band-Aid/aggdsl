// Session replay candidate list for recent 30 days with basic quality filters applied.
// Excludes broken/short sessions, requires rrweb events, and returns top 100 by recency.
RESPONSE mimeType=application/json
REQUEST name="SessionReplayList"
FROM event([source=recordingMetadata,appId=-123456,blacklist="apply"])
TIMESERIES period=dayRange first=now() count=-31
| filter recordingSessionId != ""
| filter !isBroken
| filter rrwebEventCount > 2
| group by visitorId,accountId,recordingSessionId fields { startTime=min(startTime), endTime=max(endTime), minBrowserTime=min(minBrowserTime), maxBrowserTime=max(maxBrowserTime), eventCount=sum(rrwebEventCount), rageClickCount=sum(rageClickCount), deadClickCount=sum(deadClickCount), errorClickCount=sum(errorClickCount), uTurnCount=sum(uTurnCount), isSessionStart=sum(if(isSessionStart, 1, 0)) }
| filter isSessionStart > 0
| eval { duration=endTime - startTime }
| filter duration >= 300
| sort -minBrowserTime
| limit 100
| select { startTime=startTime, endTime=endTime, duration=duration, eventCount=eventCount, rageClickCount=rageClickCount, deadClickCount=deadClickCount, errorClickCount=errorClickCount, uTurnCount=uTurnCount, minBrowserTime=minBrowserTime, maxBrowserTime=maxBrowserTime }
