// Diagnostic detail rows for a specific recording session (example) over the last 30 days.
// Pulls timing and frustration metrics for the last 30 days, limited to 20 rows.
RESPONSE mimeType=application/json
REQUEST name="DiagRecordingMetadataRowsForSession"
FROM event([source=recordingMetadata,appId=1234567890123456,blacklist="ignore"])
TIMESERIES period=dayRange first=dateAdd(startOfPeriod("daily", now()), -30, "days") count=30
| filter recordingSessionId == "session_example_1"
| select { startTime=startTime, endTime=endTime, recordingStartTime=recordingStartTime, recordingEndTime=recordingEndTime, deadClickCount=deadClickCount, rageClickCount=rageClickCount, errorClickCount=errorClickCount, rrwebEventCount=rrwebEventCount }
| sort -recordingStartTime
| limit 20
