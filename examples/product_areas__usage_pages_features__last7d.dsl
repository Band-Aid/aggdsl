// Product area usage for pages and features over the last 7 days, via spawn branches.
// Each branch merges to product areas and surfaces total events/minutes for comparison.
REQUEST name="ProductAreas_PageAndFeatureUsage"
RESPONSE mimeType=application/json
PIPELINE
| spawn

branch
FROM event([source=pageEvents,blacklist="apply"])
TIMESERIES period=dayRange first=now() count=-7
|| merge fields [pageId] mappings { productAreaId=groupId, productAreaName=groupName }
FROM event([source=pages,appId=[]])
| filter !isNil(group.id)
| eval { pageId=id, groupId=group.id, groupName=group.name }
endmerge
|| group by productAreaId,productAreaName fields { totalEvents=sum(numEvents), totalMinutes=sum(numMinutes) }
|| eval { resultType=`page` }
|| sort -totalEvents
endbranch

branch
FROM event([source=featureEvents,blacklist="apply"])
TIMESERIES period=dayRange first=now() count=-7
|| merge fields [featureId] mappings { productAreaId=groupId, productAreaName=groupName }
FROM event([source=features,appId=[]])
| filter !isNil(group.id)
| eval { featureId=id, groupId=group.id, groupName=group.name }
endmerge
|| group by productAreaId,productAreaName fields { totalEvents=sum(numEvents), totalMinutes=sum(numMinutes) }
|| eval { resultType=`feature` }
|| sort -totalEvents
endbranch

| endspawn
