// Top 10 features by event volume over the last 30 days (example appId placeholder).
FROM event([source=featureEvents,appId=-123456])
TIMESERIES period=dayRange first=now() count=-30
| group by featureId fields {
    featureName = any(featureName),
    totalEvents = sum(numEvents)
  }
| sort -totalEvents
| limit 10
