// Diagnostic sample of singleEvents for one recording session over the last 30 days.
// Provides recent events for an example session ordered by browserTime.
RESPONSE mimeType=application/json
REQUEST name="DiagSingleEventsForTopSession"
FROM event([source=singleEvents,blacklist="apply"])
TIMESERIES period=dayRange first=dateAdd(startOfPeriod("daily", now()), -30, "days") count=30
| filter recordingSessionId == "session_example_1"
| sort -browserTime
| limit 20
