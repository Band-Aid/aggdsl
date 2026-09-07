// Diagnostic: top recording sessions for a single app over the last 30 days.
// Counts recordings per session to confirm data flow.
RESPONSE mimeType=application/json
REQUEST name="DiagRecordingMetadataAppSample"
FROM event([source=recordingMetadata,appId=1234567890123456,blacklist="ignore"])
TIMESERIES period=dayRange first=dateAdd(startOfPeriod("daily", now()), -30, "days") count=30
| group by recordingSessionId fields { recordings=count(null) }
| sort -recordings
| limit 5
