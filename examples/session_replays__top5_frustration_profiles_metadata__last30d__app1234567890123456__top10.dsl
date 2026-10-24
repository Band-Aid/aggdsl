// Profile-style rollup for a fixed set of example sessions (single-app example).
// Returns timing, inactivity periods, and frustration counts while omitting identifiers.
RESPONSE mimeType=application/json
REQUEST name="SRTop5FrustrationProfiles"
FROM event([source=recordingMetadata,appId=1234567890123456,blacklist="ignore"])
TIMESERIES period=dayRange first=dateAdd(startOfPeriod("daily", now()), -30, "days") count=30
| filter recordingSessionId != ""
| filter contains(["session_example_1","session_example_3","session_example_4","session_example_5","session_example_6"], recordingSessionId)
| group by visitorId,accountId,recordingSessionId fields {
	appId=first(appId),
	startTime=min(startTime),
	endTime=max(endTime),
	rrwebEvents=max(rrwebEventCount),
	deadClicks=max(deadClickCount),
	rageClicks=max(rageClickCount),
	errorClicks=max(errorClickCount),
	totalSize=sum(recordingSize),
	inactivityPeriods=inactivityPeriods({
		recordingFirstActiveTs=recordingFirstActiveTs,
		recordingLastActiveTs=recordingLastActiveTs,
		recordingInactivityPeriodStartTimes=recordingInactivityPeriodStartTimes,
		recordingInactivityPeriodEndTimes=recordingInactivityPeriodEndTimes,
		recordingStartTime=recordingStartTime,
		recordingEndTime=recordingEndTime,
		recordingLastMobileState=recordingLastMobileState,
		appId=appId
	})
 }
| eval { totalErrors=deadClicks+rageClicks+errorClicks, durationMs=endTime-startTime }
| sort -totalErrors
| limit 10
| select { startTime=startTime, endTime=endTime, durationMs=durationMs, rrwebEvents=rrwebEvents, deadClicks=deadClicks, rageClicks=rageClicks, errorClicks=errorClicks, totalErrors=totalErrors, totalSize=totalSize, inactivityPeriods=inactivityPeriods }
