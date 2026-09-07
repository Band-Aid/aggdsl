// Diagnostic: top recording sessions (any app) by count and total size over the last 30 days.
// Uses a placeholder appId to validate recordingMetadata availability.
RESPONSE mimeType=application/json
REQUEST name="DiagRecordingMetadataAny"
FROM event([source=recordingMetadata,appId=-123456,blacklist="ignore"])
TIMESERIES period=dayRange first=1765682904280 count=30
| group by recordingSessionId fields { recordings=count(null), size=sum(recordingSize) }
| sort -recordings
| limit 5
