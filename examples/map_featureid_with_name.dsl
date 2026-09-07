FROM event([source=featureEvents, appId=-323232])
TIMESERIES period=dayRange first=now() count=-7
| group by featureId fields {
    visitors=count(visitorId),
    events=sum(numEvents)
  }
| merge fields [featureId] mappings { featureName=name }
FROM event([source=features])
| eval { featureId=id }
| select { featureId=featureId, name=name }
endmerge
| limit 5
