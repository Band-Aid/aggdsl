// Restrict any event query to a segment: add `| segment id=...` right after the source.
FROM event([source=featureEvents, appId={{APP_ID}}, blacklist="apply"])
TIMESERIES period=dayRange first=now() count=-30
| segment id="{{SEGMENT_ID}}"
| group by featureId fields { totalEvents=sum(numEvents), visitors=count(visitorId) }
| sort -totalEvents
| limit 10
