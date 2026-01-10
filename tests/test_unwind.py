from __future__ import annotations

from aggdsl import compile_to_pendo_aggregation, decompile_pendo_aggregation_to_dsl, parse


def test_unwind_compiles() -> None:
    dsl = """\
FROM event([source=visitors])
| select { visitorId=visitorId, list=metadata.agent.list }
| unwind { field=list, index=listIndex }
"""

    body = compile_to_pendo_aggregation(parse(dsl))
    pipeline = body["request"]["pipeline"]

    assert pipeline[2] == {"unwind": {"field": "list", "index": "listIndex"}}


def test_decompile_unwind_round_trip() -> None:
    body = {
        "response": {"location": "request", "mimeType": "application/json"},
        "request": {
            "name": "examples of list checks and manipulation",
            "pipeline": [
                {"source": {"visitors": {}}},
                {"select": {"visitorId": "visitorId", "list": "metadata.agent.list"}},
                {"unwind": {"field": "list", "index": "listIndex"}},
            ],
        },
    }

    dsl = decompile_pendo_aggregation_to_dsl(body)
    assert "| unwind" in dsl

    body2 = compile_to_pendo_aggregation(parse(dsl))
    assert body2 == body
