// Top custom pendo.track() events, last 30 days.
FROM event([source=trackEvents, appId={{APP_ID}}, blacklist="apply"])
TIMESERIES period=dayRange first=now() count=-30
| group by trackTypeId fields { totalEvents=sum(numEvents), visitors=count(visitorId) }
| sort -totalEvents
| limit 20
