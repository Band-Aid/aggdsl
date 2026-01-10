from __future__ import annotations

from aggdsl import compile_to_pendo_aggregation, parse


def test_pipeline_mode_can_start_with_spawn_raw() -> None:
    dsl = """\
RESPONSE mimeType=text/csv
REQUEST name="NPSResponses"
PIPELINE
| raw {"spawn": [[{"source": {"pollEvents": {"guideId": "g", "pollId": "p", "blacklist": "apply"}, "timeSeries": {"period": "dayRange", "first": "date(`firstDate`)", "last": "date(`lastDate`)"}}}]]}
| eval { realTime=formatTime(`2006-01-02`, browserTime) }
| join fields [browserTime,visitorId]
"""

    q = parse(dsl)
    body = compile_to_pendo_aggregation(q, now_ms=123)

    assert body["response"]["mimeType"] == "text/csv"
    assert body["request"]["name"] == "NPSResponses"
    pipeline = body["request"]["pipeline"]
    assert "spawn" in pipeline[0]
    assert pipeline[1] == {"eval": {"realTime": "formatTime(`2006-01-02`, browserTime)"}}
    assert pipeline[2] == {"join": {"fields": ["browserTime", "visitorId"]}}
