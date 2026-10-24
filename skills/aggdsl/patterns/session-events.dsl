// Individual events inside one replay session (singleEvents: rows only, do not group by recordingSessionId).
FROM event([source=singleEvents, appId={{APP_ID}}, blacklist="apply"])
TIMESERIES period=dayRange first=now() count=-30
| filter recordingSessionId == "{{SESSION_ID}}"
| sort -browserTime
| limit 20
