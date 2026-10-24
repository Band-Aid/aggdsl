// Diagnostic: top 5 daily event volumes over the last 30 days for a single app.
// Groups by day and returns event counts sorted by highest volume.
RESPONSE mimeType=application/json
REQUEST name="DiagEventsAppIdHasData"
FROM event([source=events,blacklist="apply"])
TIMESERIES period=dayRange first=dateAdd(startOfPeriod("daily", now()), -30, "days") count=30
| filter appId == 1234567890123456
| group by day fields { events=sum(numEvents) }
| sort -events
| limit 5
