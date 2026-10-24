// Top 20 accounts by time in app, last 30 days.
FROM event([source=events, appId={{APP_ID}}, blacklist="apply"])
TIMESERIES period=dayRange first=now() count=-30
| filter !isNull(accountId) && accountId != ""
| group by accountId fields { minutes=sum(numMinutes), events=sum(numEvents), visitors=count(visitorId) }
| sort -minutes
| limit 20
