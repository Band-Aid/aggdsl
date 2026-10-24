// Aggregation of recordingMetadata for a specific visitor/session using structured fork branches.
// Produces summary, chunk, and timeline branches (example IDs).
RESPONSE mimeType=application/json
REQUEST name="ReplayRecordingAggregation"
FROM event([source=recordingMetadata,appId=1234567890123456,blacklist="ignore"])
TIMESERIES period=dayRange first=1767577688130 last=1767578042618
| filter !isBroken
| filter visitorId == "visitor_example_1"
| filter contains(["session_example_2"], recordingSessionId)
| filter isSessionStart || recordingRrwebEventCount > 2

| fork
branch
PIPELINE
|| group by visitorId fields {
 appId=first(appId),
   startTime=min(startTime),
   endTime=max(endTime),
   minBrowserTime=min(minBrowserTime),
   maxBrowserTime=max(maxBrowserTime),
   inactivityPeriods=inactivityPeriods({
     recordingFirstActiveTs=recordingFirstActiveTs,
     recordingLastActiveTs=recordingLastActiveTs,
     recordingInactivityPeriodStartTimes=recordingInactivityPeriodStartTimes,
     recordingInactivityPeriodEndTimes=recordingInactivityPeriodEndTimes,
     recordingStartTime=recordingStartTime,
     recordingEndTime=recordingEndTime,
     recordingLastMobileState=recordingLastMobileState,
     appId=appId
   }),
   recordingSize=sum(recordingSize)
 }
|| select { startTime=startTime, endTime=endTime, minBrowserTime=minBrowserTime, maxBrowserTime=maxBrowserTime, recordingSize=recordingSize, inactivityPeriods=inactivityPeriods }
endbranch
branch
PIPELINE
|| group by recordingId,recordingSessionId fields { chunkSize=sum(recordingSize),
chunkStartTime=min(recordingStartTime),
chunkEndTime=max(recordingEndTime) }
|| sort chunkStartTime
|| select { chunkSize=chunkSize, chunkStartTime=chunkStartTime, chunkEndTime=chunkEndTime }
endbranch
branch
PIPELINE
|| group by visitorId,accountId,recordingSessionId fields { activityTimelineTimestamps=concat(activityTimelineTimestamps) }
|| eval { activityTimelineTimestamps=sortUnique(activityTimelineTimestamps) }
|| select { activityTimelineTimestamps=activityTimelineTimestamps }
endbranch
| endfork
