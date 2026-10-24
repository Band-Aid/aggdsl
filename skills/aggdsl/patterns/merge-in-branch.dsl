// Prefix reference: merge inside a spawn branch. Header ||, merge body |, after endmerge ||.
PIPELINE
| spawn
branch
FROM event([source=pageEvents, pageId="{{PAGE_A_ID}}", appId={{APP_ID}}])
TIMESERIES period=dayRange first=now() count=-30
|| identified visitorId
|| group by visitorId fields { pageAEvents=sum(numEvents) }
|| merge fields [visitorId] mappings { pageBEvents=pageBEvents }
FROM event([source=pageEvents, pageId="{{PAGE_B_ID}}", appId={{APP_ID}}])
TIMESERIES period=dayRange first=now() count=-30
| identified visitorId
| group by visitorId fields { pageBEvents=sum(numEvents) }
endmerge
|| filter isNull(pageBEvents)
endbranch
| endspawn
| limit 100
