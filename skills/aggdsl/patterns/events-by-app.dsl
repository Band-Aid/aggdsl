// Which apps produce data: top 10 appIds by events, last 30 days (no appId filter on purpose).
FROM event([source=events, blacklist="apply"])
TIMESERIES period=dayRange first=now() count=-30
| group by appId fields { events=sum(numEvents) }
| sort -events
| limit 10
