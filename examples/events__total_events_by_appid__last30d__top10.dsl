// Diagnostic timeseries across appIds: top 10 by total events over the last 30 days.
// Useful for spotting which apps are producing data in this window.
RESPONSE mimeType=application/json
REQUEST name="DiagEventsNumericTimeSeries"
FROM event([source=events,blacklist="apply"])
TIMESERIES period=dayRange first=dateAdd(startOfPeriod("daily", now()), -30, "days") count=30
| group by appId fields { events=sum(numEvents) }
| sort -events
| limit 10
