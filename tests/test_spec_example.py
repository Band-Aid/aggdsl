from __future__ import annotations

from aggdsl import compile_to_pendo_aggregation, parse


def test_compiles_spec_json_example() -> None:
    dsl = """\
FROM event([source=pageEvents,pageId=\"Mal8cGcc-XXqBEJ-c5C1L3frUz8\",blacklist=\"apply\"])
TIMESERIES period=dayRange first=1731769200000 count=180
| filter !isNull(parameters.parameter) && parameters.parameter != \"\"
| group by pageId,parameters.parameter fields { numEvents=sum(numEvents) }
| sort -numEvents
| limit 100
"""

    q = parse(dsl)
    body = compile_to_pendo_aggregation(q)

    assert body == {
        "response": {"location": "request", "mimeType": "application/json"},
        "request": {
            "pipeline": [
                {
                    "source": {
                        "pageEvents": {
                            "pageId": "Mal8cGcc-XXqBEJ-c5C1L3frUz8",
                            "blacklist": "apply",
                        },
                        "timeSeries": {
                            "period": "dayRange",
                            "first": 1731769200000,
                            "count": 180,
                        },
                    }
                },
                {"filter": '!isNull(parameters.parameter) && parameters.parameter != ""'},
                {
                    "group": {
                        "group": ["pageId", "parameters.parameter"],
                        "fields": [{"numEvents": {"sum": "numEvents"}}],
                    }
                },
                {"sort": ["-numEvents"]},
                {"limit": 100},
            ]
        },
    }
