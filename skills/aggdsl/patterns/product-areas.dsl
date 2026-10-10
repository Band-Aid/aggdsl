// Usage per product area (page + feature rows), last 7 days: spawn with a merge inside each branch.
PIPELINE
| spawn
branch
FROM event([source=pageEvents, appId={{APP_ID}}, blacklist="apply"])
TIMESERIES period=dayRange first=now() count=-7
|| merge fields [pageId] mappings { productAreaId=groupId, productAreaName=groupName }
FROM event([source=pages, appId=[]])
| filter !isNil(group.id)
| eval { pageId=id, groupId=group.id, groupName=group.name }
endmerge
|| group by productAreaId,productAreaName fields { totalEvents=sum(numEvents), totalMinutes=sum(numMinutes) }
|| eval { resultType="page" }
endbranch
branch
FROM event([source=featureEvents, appId={{APP_ID}}, blacklist="apply"])
TIMESERIES period=dayRange first=now() count=-7
|| merge fields [featureId] mappings { productAreaId=groupId, productAreaName=groupName }
FROM event([source=features, appId=[]])
| filter !isNil(group.id)
| eval { featureId=id, groupId=group.id, groupName=group.name }
endmerge
|| group by productAreaId,productAreaName fields { totalEvents=sum(numEvents), totalMinutes=sum(numMinutes) }
|| eval { resultType="feature" }
endbranch
| endspawn
| sort -totalEvents
