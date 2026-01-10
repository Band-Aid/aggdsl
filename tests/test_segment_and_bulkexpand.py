from aggdsl import compile_to_pendo_aggregation, decompile_pendo_aggregation_to_dsl, parse


def test_segment_and_bulkexpand_compile() -> None:
    dsl = """\
FROM event([source=visitors])
| identified visitorId
| eval { accountId=metadata.auto.accountids }
| unwind { field=accountId, keepEmpty=True }
| bulkExpand {"account":{"account":"accountId"}}
| segment id="segment12345"
| select { visitorId=visitorId, accountId=accountId }
"""

    body = compile_to_pendo_aggregation(parse(dsl))
    pipeline = body["request"]["pipeline"]

    assert pipeline[0] == {"source": {"visitors": {}}}
    assert pipeline[3] == {"unwind": {"field": "accountId", "keepEmpty": True}}
    assert pipeline[4] == {"bulkExpand": {"account": {"account": "accountId"}}}
    assert pipeline[5] == {"segment": {"id": "segment12345"}}


def test_decompile_segment_and_bulkexpand_round_trip() -> None:
    body = {
        "response": {"location": "request", "mimeType": "application/json"},
        "request": {
            "name": "SegBulk",
            "pipeline": [
                {"source": {"visitors": {}}},
                {"identified": "visitorId"},
                {"eval": {"accountId": "metadata.auto.accountids"}},
                {"unwind": {"field": "accountId", "keepEmpty": True}},
                {"bulkExpand": {"account": {"account": "accountId"}}},
                {"segment": {"id": "segment12345"}},
                {"select": {"visitorId": "visitorId", "accountId": "accountId"}},
            ],
        },
    }

    dsl = decompile_pendo_aggregation_to_dsl(body)
    assert "| raw {\"bulkExpand\"" not in dsl
    assert "| raw {\"segment\"" not in dsl
    assert "bulkExpand" in dsl
    assert "segment" in dsl

    body2 = compile_to_pendo_aggregation(parse(dsl))
    assert body2 == body
