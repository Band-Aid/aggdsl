// Top 10 features by events, last 30 days. For pages: source=pageEvents, pageId.
FROM event([source=featureEvents, appId={{APP_ID}}, blacklist="apply"])
TIMESERIES period=dayRange first=now() count=-30
| group by featureId fields { featureName=any(featureName), totalEvents=sum(numEvents), visitors=count(visitorId) }
| sort -totalEvents
| limit 10
