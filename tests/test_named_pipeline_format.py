from __future__ import annotations

from aggdsl import compile_to_pendo_aggregation, parse


def test_named_pipeline_format_and_csv_response() -> None:
    dsl = """\
RESPONSE mimeType=text/csv
REQUEST name="NPSResponses"
FROM event([source=pollEvents,guideId="g",pollId="p",blacklist="apply"])
TIMESERIES period=dayRange first=date(`firstDate`) last=date(`lastDate`)
| identified visitorId
| eval { time1=browserTime }
| join fields [browserTime,visitorId]
| select { visitorId=visitorId, accountId=accountId }
| raw {"limit": 10}
"""

    q = parse(dsl)
    body = compile_to_pendo_aggregation(q, now_ms=123)

    assert body["response"]["mimeType"] == "text/csv"
    assert isinstance(body["request"], dict)
    assert body["request"]["name"] == "NPSResponses"

    pipeline = body["request"]["pipeline"]
    assert pipeline[0]["source"]["pollEvents"]["guideId"] == "g"

    ts = pipeline[0]["source"]["timeSeries"]
    assert ts == {
        "period": "dayRange",
        "first": "date(`firstDate`)",
        "last": "date(`lastDate`)",
    }

    assert pipeline[1] == {"identified": "visitorId"}
    assert pipeline[-1] == {"limit": 10}


def test_join_allows_empty_fields_list() -> None:
    dsl = """\
PIPELINE
| join fields []
"""

    body = compile_to_pendo_aggregation(parse(dsl))
    assert body["request"]["pipeline"] == [{"join": {"fields": []}}]
