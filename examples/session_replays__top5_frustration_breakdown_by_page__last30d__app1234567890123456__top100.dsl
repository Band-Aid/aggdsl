// Frustration breakdown by page for a fixed set of example sessions (single-app example).
// Filters to selected sessions and sums frustration signals per page.
RESPONSE mimeType=application/json
REQUEST name="SRTop5FrustrationBreakdownByPage"
FROM event([source=pageEvents,appId=1234567890123456,blacklist="apply",ignoreFrustration=only])
TIMESERIES period=dayRange first=dateAdd(startOfPeriod("daily", now()), -30, "days") count=30
| filter recordingSessionId != ""
| filter contains(["session_example_1","session_example_3","session_example_4","session_example_5","session_example_6"], recordingSessionId)
| group by recordingSessionId,pageId fields { deadClicks=sum(deadClickCount), rageClicks=sum(rageClickCount), errorClicks=sum(errorClickCount), events=sum(numEvents) }
| eval { totalErrors=deadClicks+rageClicks+errorClicks }
| sort -totalErrors
| limit 100
