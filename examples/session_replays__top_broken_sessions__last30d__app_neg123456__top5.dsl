// Top broken session replays over the last 30 days (example appId).
// Ranks sessions by broken recording count and includes size/time spans.
RESPONSE mimeType=application/json
REQUEST name="TopBrokenReplaySessionsLast30Days"
FROM event([source=recordingMetadata,appId=-123456,blacklist="ignore"])
TIMESERIES period=dayRange first=dateAdd(startOfPeriod("daily", now()), -30, "days") count=30
| filter isBroken
| group by recordingSessionId fields { brokenRecordings=count(null), totalSize=sum(recordingSize), minStart=min(recordingStartTime), maxEnd=max(recordingEndTime) }
| sort -brokenRecordings
| limit 5
