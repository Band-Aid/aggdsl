// NPS-style poll responses mapped to labels with switch, 2024 calendar year.
PIPELINE
| spawn
branch
FROM event([source=pollEvents, guideId="{{GUIDE_ID}}", pollId="{{POLL_ID}}", blacklist="apply"])
TIMESERIES period=dayRange first=date(2024, 1, 1, 0, 0, 0) last=date(2024, 12, 31, 23, 59, 59)
|| filter pollResponse != ""
|| switch mappedPollResponse from pollResponse { "1"=="promoter", "2"=="passive", "3"=="detractor" }
|| group by mappedPollResponse fields { numEvents=sum(numEvents) }
endbranch
| endspawn
