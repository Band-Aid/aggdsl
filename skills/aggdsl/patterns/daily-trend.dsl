// Daily events + frustration clicks, last 30 full days, with a readable date label.
FROM event([source=events, appId={{APP_ID}}, blacklist="apply"])
TIMESERIES period=dayRange first=dateAdd(startOfPeriod("daily", now()), -30, "days") count=30
| group by day fields { events=sum(numEvents), minutes=sum(numMinutes), errorClicks=sum(errorClickCount), deadClicks=sum(deadClickCount), rageClicks=sum(rageClickCount) }
| eval { dayLabel=formatTime("2006-01-02", day) }
| sort day
