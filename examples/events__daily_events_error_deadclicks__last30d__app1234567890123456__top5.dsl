// Diagnostic: daily events with error/dead clicks over the last 30 days for a single app.
// Breaks out totals by day and shows the top 5 days by volume.
RESPONSE mimeType=application/json
REQUEST name="DiagEventsAppSample"
FROM event([source=events,blacklist="apply"])
TIMESERIES period=dayRange first=dateAdd(startOfPeriod("daily", now()), -30, "days") count=30
| filter appId == 1234567890123456
| group by day fields { events=sum(numEvents), errorClicks=sum(errorClickCount), deadClicks=sum(deadClickCount) }
| sort -events
| limit 5
