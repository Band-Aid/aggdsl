// Top 5 features by 30-day retention rate for appId {{APP_ID}}.
// Retention definition: % of visitors with activity on 2+ distinct days in the last 30 days.
// Data source: featureEvents and features metadata
// Output: Top 5 features with totalUsers, retainedUsers, retentionRate, and featureName
REQUEST name="FeatureRetention_Top5_Last30d_App323232"
RESPONSE mimeType=application/json
FROM event([source=featureEvents,appId={{APP_ID}},blacklist="apply"])
TIMESERIES period=dayRange first=now() count=-30
| filter !isNull(featureId)
| group by featureId,visitorId fields { activeDays=count(day) }
| eval { retained=if(activeDays > 1, 1, 0) }
| group by featureId fields { totalUsers=count(visitorId), retainedUsers=sum(retained) }
| eval { retentionRate=retainedUsers / totalUsers }
| merge fields [featureId]
FROM event([source=features,appId={{APP_ID}}])
| select { featureId=id, featureName=name }
endmerge
| filter !isNull(featureName) && !contains(featureName, "Nav") && !contains(featureName, "Super")
| sort -retainedUsers,-retentionRate
| limit 5
