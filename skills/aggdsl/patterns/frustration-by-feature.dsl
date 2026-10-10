// Frustration per feature for chosen sessions. For pages: source=pageEvents and group by pageId.
FROM event([source=featureEvents, appId={{APP_ID}}, blacklist="apply", ignoreFrustration=only])
TIMESERIES period=dayRange first=now() count=-30
| filter contains(["{{SESSION_ID_1}}", "{{SESSION_ID_2}}"], recordingSessionId)
| group by recordingSessionId,featureId fields { deadClicks=sum(deadClickCount), rageClicks=sum(rageClickCount), errorClicks=sum(errorClickCount), events=sum(numEvents) }
| eval { totalErrors=deadClicks+rageClicks+errorClicks }
| sort -totalErrors
| limit 100
