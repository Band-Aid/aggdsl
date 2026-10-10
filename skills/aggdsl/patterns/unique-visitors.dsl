// Unique identified visitors, last 7 days (single-number answer via raw reduce).
FROM event([source=events, appId={{APP_ID}}, blacklist="apply"])
TIMESERIES period=dayRange first=now() count=-7
| identified visitorId
| raw {"reduce": {"uniqueVisitors": {"count": "visitorId"}}}
