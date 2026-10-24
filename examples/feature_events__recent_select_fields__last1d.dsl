// Recent feature events (last 1 day) selecting a few non-identifying fields.
RESPONSE mimeType=application/json
FROM event([source=featureEvents])
TIMESERIES period=dayRange first=now() count=-1
| select { featureId=featureId, browserTime=browserTime }
