// Sessions with the most error clicks (events source), last 30 days.
FROM event([source=events, appId={{APP_ID}}, blacklist="apply"])
TIMESERIES period=dayRange first=now() count=-30
| filter !isNull(recordingSessionId)
| group by recordingSessionId fields { events=sum(numEvents), minutes=sum(numMinutes), errorClicks=sum(errorClickCount), deadClicks=sum(deadClickCount), rageClicks=sum(rageClickCount), uTurns=sum(uTurnCount) }
| sort -errorClicks
| limit 5
