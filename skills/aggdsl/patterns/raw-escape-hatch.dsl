// Anything the DSL does not model goes through `| raw {json}` verbatim (here: a fork written as JSON).
FROM event([source=recordingMetadata, appId={{APP_ID}}, blacklist="ignore"])
TIMESERIES period=dayRange first=now() count=-7
| filter !isBroken
| raw {"fork":[[{"group":{"group":["recordingSessionId"],"fields":[{"size":{"sum":"recordingSize"}}]}}],[{"group":{"group":["visitorId"],"fields":[{"sessions":{"count":"recordingSessionId"}}]}}]]}
