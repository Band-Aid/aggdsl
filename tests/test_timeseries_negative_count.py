from __future__ import annotations

from aggdsl import compile_to_pendo_aggregation, parse


def test_timeseries_negative_count_is_preserved() -> None:
    dsl = """\
FROM event([source=pageEvents,pageId=\"x\",blacklist=\"apply\"])
TIMESERIES period=dayRange first=now() count=-180
| limit 1
"""

    q = parse(dsl)
    body = compile_to_pendo_aggregation(q, now_ms=1_000_000_000_000)
    ts = body["request"]["pipeline"][0]["source"]["timeSeries"]

    assert ts["first"] == 1_000_000_000_000
    assert ts["count"] == -180
