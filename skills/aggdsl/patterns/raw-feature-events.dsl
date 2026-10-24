// Raw feature event rows, last 1 day, selected fields only.
FROM event([source=featureEvents, appId={{APP_ID}}, blacklist="apply"])
TIMESERIES period=dayRange first=now() count=-1
| select { featureId=featureId, visitorId=visitorId, browserTime=browserTime }
| sort -browserTime
| limit 1000
