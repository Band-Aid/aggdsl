// Guide views/visitors for one guide, last 30 days.
FROM event([source=guideEvents, guideId="{{GUIDE_ID}}", appId={{APP_ID}}, blacklist="apply"])
TIMESERIES period=dayRange first=now() count=-30
| group by type fields { events=count(null), visitors=count(visitorId) }
