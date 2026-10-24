// Broken recordings: per-session counts. For a broken/ok split, group by isBroken instead.
FROM event([source=recordingMetadata, appId={{APP_ID}}, blacklist="ignore"])
TIMESERIES period=dayRange first=now() count=-30
| filter isBroken
| group by recordingSessionId fields { brokenRecordings=count(null), totalSize=sum(recordingSize), minStart=min(recordingStartTime), maxEnd=max(recordingEndTime) }
| sort -brokenRecordings
| limit 5
