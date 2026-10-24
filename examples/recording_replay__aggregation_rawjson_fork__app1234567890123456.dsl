// Aggregation of recordingMetadata for a specific visitor/session using a raw JSON fork definition.
// Produces summary, chunk, and timeline branches (example IDs).
RESPONSE mimeType=application/json
REQUEST name="ReplayRecordingAggregation"
FROM event([source=recordingMetadata,appId=1234567890123456,blacklist="ignore"])
TIMESERIES period=dayRange first=1767577688130 last=1767578042618
| filter !isBroken
| filter visitorId == "visitor_example_1"
| filter contains(["session_example_2"], recordingSessionId)
| filter isSessionStart || recordingRrwebEventCount > 2
| raw {"fork":[[{"group":{"group":["visitorId"],"fields":[{"appId":{"first":"appId"}},{"accountId":{"first":"accountId"}},{"visitorId":{"first":"visitorId"}},{"tabId":{"first":"tabId"}},{"startTime":{"min":"startTime"}},{"endTime":{"max":"endTime"}},{"minBrowserTime":{"min":"minBrowserTime"}},{"maxBrowserTime":{"max":"maxBrowserTime"}},{"recordingIds":{"list":"recordingId"}},{"inactivityPeriods":{"inactivityPeriods":{"recordingFirstActiveTs":"recordingFirstActiveTs","recordingLastActiveTs":"recordingLastActiveTs","recordingInactivityPeriodStartTimes":"recordingInactivityPeriodStartTimes","recordingInactivityPeriodEndTimes":"recordingInactivityPeriodEndTimes","recordingStartTime":"recordingStartTime","recordingEndTime":"recordingEndTime","recordingLastMobileState":"recordingLastMobileState","appId":"appId"}}},{"recordingSize":{"sum":"recordingSize"}}]}}],[{"group":{"group":["recordingId","recordingSessionId"],"fields":[{"chunkSize":{"sum":"recordingSize"}},{"chunkStartTime":{"min":"recordingStartTime"}},{"chunkEndTime":{"max":"recordingEndTime"}}]}},{"sort":["chunkStartTime"]}],[{"group":{"group":["visitorId","accountId","recordingSessionId"],"fields":[{"activityTimelineTimestamps":{"concat":"activityTimelineTimestamps"}}]}},{"eval":{"activityTimelineTimestamps":"sortUnique(activityTimelineTimestamps)"}}]]}
