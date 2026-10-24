FROM event([source=pageEvents, pageId="PAGE_A_ID", appId=-323232])
TIMESERIES period=dayRange first=dateAdd(startOfPeriod("daily", now()), -30, "days") count=30
| identified visitorId
| group by visitorId fields { pageAEvents=sum(numEvents) }
| merge fields [visitorId] mappings { pageBEvents=pageBEvents }
FROM event([source=pageEvents, pageId="PAGE_B_ID", appId=-323232])
TIMESERIES period=dayRange first=dateAdd(startOfPeriod("daily", now()), -30, "days") count=30
| identified visitorId
| group by visitorId fields { pageBEvents=sum(numEvents) }
endmerge
| filter isNull(pageBEvents)
| raw {
    "reduce": {
      "visitorsWhoNeverVisitedB": {"count": "visitorId"}
    }
  }
