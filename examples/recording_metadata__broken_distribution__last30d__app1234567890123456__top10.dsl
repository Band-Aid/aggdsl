// Diagnostic: distribution of broken vs non-broken recordings over the last 30 days.
// Summarizes counts, total size, and rrweb events grouped by isBroken flag.
RESPONSE mimeType=application/json
REQUEST name="DiagRecordingMetadataBrokenDistribution"
FROM event([source=recordingMetadata,appId=1234567890123456,blacklist="ignore"])
TIMESERIES period=dayRange first=dateAdd(startOfPeriod("daily", now()), -30, "days") count=30
| group by isBroken fields { recordings=count(null), totalSize=sum(recordingSize), rrwebEvents=sum(recordingRrwebEventCount) }
| sort -recordings
| limit 10
