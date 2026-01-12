from __future__ import annotations


from aggdsl import compile_to_pendo_aggregation, parse


def test_group_fields_block_can_span_multiple_lines_inside_fork_branch() -> None:
    dsl = """\
PIPELINE
| fork
branch
PIPELINE
|| group by visitorId fields {
||   inactivityPeriods=inactivityPeriods({
||     recordingStartTime=recordingStartTime,
||     recordingEndTime=recordingEndTime,
||     appId=appId
||   })
|| }
endbranch
| endfork
"""

    body = compile_to_pendo_aggregation(parse(dsl))
    fork_stage = body["request"]["pipeline"][0]
    assert "fork" in fork_stage
    assert fork_stage["fork"][0][0] == {
        "group": {
            "group": ["visitorId"],
            "fields": [
                {
                    "inactivityPeriods": {
                        "inactivityPeriods": {
                            "recordingStartTime": "recordingStartTime",
                            "recordingEndTime": "recordingEndTime",
                            "appId": "appId",
                        }
                    }
                }
            ],
        }
    }
