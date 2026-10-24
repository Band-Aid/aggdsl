// Top 5 features by retention (% visitors active on 2+ days in 30d), names merged in.
FROM event([source=featureEvents, appId={{APP_ID}}, blacklist="apply"])
TIMESERIES period=dayRange first=now() count=-30
| filter !isNull(featureId)
| group by featureId,visitorId fields { activeDays=count(day) }
| eval { retained=if(activeDays > 1, 1, 0) }
| group by featureId fields { totalUsers=count(visitorId), retainedUsers=sum(retained) }
| eval { retentionRate=retainedUsers / totalUsers }
| merge fields [featureId]
FROM event([source=features, appId={{APP_ID}}])
| select { featureId=id, featureName=name }
endmerge
| filter !isNull(featureName)
| sort -retentionRate,-retainedUsers
| limit 5
