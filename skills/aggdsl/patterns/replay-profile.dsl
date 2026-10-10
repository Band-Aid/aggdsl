// Per-session profile with the structured inactivityPeriods aggregator (multiline group).
FROM event([source=recordingMetadata, appId={{APP_ID}}, blacklist="ignore"])
TIMESERIES period=dayRange first=now() count=-30
| filter recordingSessionId == "{{SESSION_ID}}"
| group by visitorId,accountId,recordingSessionId fields {
    startTime=min(startTime),
    endTime=max(endTime),
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
