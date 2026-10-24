// Visitors who viewed page A but never page B (30d). For "A and B" use !isNull(pageBEvents).
FROM event([source=pageEvents, pageId="{{PAGE_A_ID}}", appId={{APP_ID}}, blacklist="apply"])
TIMESERIES period=dayRange first=now() count=-30
| identified visitorId
| group by visitorId fields { pageAEvents=sum(numEvents) }
| merge fields [visitorId] mappings { pageBEvents=pageBEvents }
FROM event([source=pageEvents, pageId="{{PAGE_B_ID}}", appId={{APP_ID}}, blacklist="apply"])
TIMESERIES period=dayRange first=now() count=-30
| identified visitorId
| group by visitorId fields { pageBEvents=sum(numEvents) }
endmerge
| filter isNull(pageBEvents)
| raw {"reduce": {"visitorsWhoNeverVisitedB": {"count": "visitorId"}}}
