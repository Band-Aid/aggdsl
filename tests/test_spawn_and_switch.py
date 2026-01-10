from __future__ import annotations

from aggdsl import compile_to_pendo_aggregation, parse


def test_spawn_with_switch_compiles() -> None:
    dsl = """\
RESPONSE mimeType=text/csv
REQUEST name="NPSResponses"
PIPELINE
| spawn
branch
FROM event([source=pollEvents,guideId="g",pollId="p",blacklist="apply"])
TIMESERIES period=dayRange first=date(`firstDate`) last=date(`lastDate`)
|| identified visitorId
|| switch mapped from pollResponse { "1"=="aaa", "2"=="bbb" }
|| select { pollResponse1=mapped }
endbranch
| endspawn
| limit 10
"""

    q = parse(dsl)
    body = compile_to_pendo_aggregation(q)

    assert body["response"]["mimeType"] == "text/csv"
    assert body["request"]["name"] == "NPSResponses"

    pipeline = body["request"]["pipeline"]
    assert "spawn" in pipeline[0]
    branch0 = pipeline[0]["spawn"][0]
    assert branch0[0]["source"]["pollEvents"]["guideId"] == "g"
    assert branch0[1] == {"identified": "visitorId"}
    assert branch0[2] == {"switch": {"mapped": {"pollResponse": [{"value": "1", "==": "aaa"}, {"value": "2", "==": "bbb"}]}}}


def test_spawn_accepts_pipe_prefixed_branch_markers() -> None:
    from aggdsl.compiler import compile_to_pendo_aggregation_with_format
    from aggdsl.parser import parse

    dsl = """\
RESPONSE mimeType=application/json
REQUEST name="ExampleSpawnSwitch"
PIPELINE
| spawn
| branch
FROM event([source=pollEvents,guideId="g",pollId="p",blacklist="apply"])
TIMESERIES period=dayRange first=date(`firstDate`) last=date(`lastDate`)
|| identified visitorId
|| switch mapped from pollResponse { "1"=="aaa", "2"=="bbb" }
| endbranch
| endspawn
"""

    q = parse(dsl)
    body = compile_to_pendo_aggregation_with_format(q, request_format="object")
    pipeline = body["request"]["pipeline"]

    assert "spawn" in pipeline[0]
    assert len(pipeline[0]["spawn"]) == 1
