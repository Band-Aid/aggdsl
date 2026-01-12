from __future__ import annotations


from aggdsl import compile_to_pendo_aggregation, parse


def test_group_allows_object_argument_for_aggregate() -> None:
    dsl = """\
PIPELINE
| group by visitorId fields { inactivityPeriods=inactivityPeriods({ recordingFirstActiveTs=recordingFirstActiveTs, recordingLastActiveTs=recordingLastActiveTs, recordingInactivityPeriodStartTimes=recordingInactivityPeriodStartTimes, recordingInactivityPeriodEndTimes=recordingInactivityPeriodEndTimes, recordingStartTime=recordingStartTime, recordingEndTime=recordingEndTime, recordingLastMobileState=recordingLastMobileState, appId=appId }) }
"""

    body = compile_to_pendo_aggregation(parse(dsl))
    assert body["request"]["pipeline"][0] == {
        "group": {
            "group": ["visitorId"],
            "fields": [
                {
                    "inactivityPeriods": {
                        "inactivityPeriods": {
                            "recordingFirstActiveTs": "recordingFirstActiveTs",
                            "recordingLastActiveTs": "recordingLastActiveTs",
                            "recordingInactivityPeriodStartTimes": "recordingInactivityPeriodStartTimes",
                            "recordingInactivityPeriodEndTimes": "recordingInactivityPeriodEndTimes",
                            "recordingStartTime": "recordingStartTime",
                            "recordingEndTime": "recordingEndTime",
                            "recordingLastMobileState": "recordingLastMobileState",
                            "appId": "appId",
                        }
                    }
                }
            ],
        }
    }
