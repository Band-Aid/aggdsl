from aggdsl import compile_to_pendo_aggregation, decompile_pendo_aggregation_to_dsl, parse


def test_group_fieldsmap_compiles_to_object_map() -> None:
    dsl = "\n".join(
        [
            "PIPELINE",
            "| group by accountId,groupId fields map { mostRecentDay=max(day) }",
            "",
        ]
    )

    body = compile_to_pendo_aggregation(parse(dsl))
    assert body["request"]["pipeline"][0] == {
        "group": {
            "group": ["accountId", "groupId"],
            "fields": {"mostRecentDay": {"max": "day"}},
        }
    }


def test_decompile_group_object_fields_to_fieldsmap_round_trip() -> None:
    body = {
        "response": {"location": "request", "mimeType": "application/json"},
        "request": {
            "pipeline": [
                {
                    "group": {
                        "group": ["accountId"],
                        "fields": {"groups": {"list": "groups"}},
                    }
                }
            ]
        },
    }

    dsl = decompile_pendo_aggregation_to_dsl(body)
    assert " fields map {" in dsl

    body2 = compile_to_pendo_aggregation(parse(dsl))
    assert body2 == body
