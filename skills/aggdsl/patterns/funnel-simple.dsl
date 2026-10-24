// Two-step funnel (page -> feature), visitors per furthest step, last 90 days.
FROM event([source=singleEvents, appId=[{{APP_ID}}]])
TIMESERIES period=dayRange first=now() count=-90
| group by visitorId fields { funnel=funnel({ maxDuration=0, items=[{'pageId': '{{PAGE_ID}}'}, {'featureId': '{{FEATURE_ID}}'}], uniqueVisitorFunnel=True, onlyMatchedEvents=True }) }
| unwind { field=funnel }
| select { visitorId=visitorId, steps=funnel.steps, times=funnel.times }
| group by steps fields { visitors=count(null) }
| sort -steps
| raw {"accumulate":{"visitors":"visitors"}}
