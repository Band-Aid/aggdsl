from __future__ import annotations

from aggdsl import compile_to_pendo_aggregation, parse


def test_timeseries_last_expression_with_spaces_parses() -> None:
    dsl = """\
FROM event([source=pollEvents,guideId="g",pollId="p",blacklist="apply"])
TIMESERIES period=dayRange first=now() last=date(2025, 1, 1, 12, 30, 00)
| limit 1
"""

    q = parse(dsl)
    body = compile_to_pendo_aggregation(q, now_ms=123)
    ts = body["request"]["pipeline"][0]["source"]["timeSeries"]

    assert ts["first"] == 123
    assert ts["last"] == "date(2025, 1, 1, 12, 30, 00)"
