import json

from aggdsl import compile_to_pendo_aggregation, decompile_pendo_aggregation_to_dsl, parse


def test_merge_stage_compiles() -> None:
    dsl = "\n".join(
        [
            "PIPELINE",
            "| merge fields [pageId] mappings { groupId=group.id }",
            "FROM event([source=pages,appId=[]])",
            "| filter !isNil(group.id)",
            "| eval { pageId=id }",
            "endmerge",
            "",
        ]
    )

    body = compile_to_pendo_aggregation(parse(dsl))
    pipeline = body["request"]["pipeline"]

    assert pipeline[0] == {
        "merge": {
            "fields": ["pageId"],
            "mappings": {"groupId": "group.id"},
            "pipeline": [
                {"source": {"pages": {"appId": []}}},
                {"filter": "!isNil(group.id)"},
                {"eval": {"pageId": "id"}},
            ],
        }
    }


def test_merge_stage_compiles_with_legacy_double_chevron() -> None:
    dsl = "\n".join(
        [
            "PIPELINE",
            "| merge fields [pageId] mappings { groupId=group.id }",
            "FROM event([source=pages,appId=[]])",
            ">> filter !isNil(group.id)",
            ">> eval { pageId=id }",
            "endmerge",
            "",
        ]
    )

    body = compile_to_pendo_aggregation(parse(dsl))
    pipeline = body["request"]["pipeline"]

    assert pipeline[0]["merge"]["pipeline"][1] == {"filter": "!isNil(group.id)"}


def test_merge_stage_without_mappings_compiles() -> None:
    dsl = "\n".join(
        [
            "PIPELINE",
            "| merge fields [accountId]",
            "FROM event([source=visitors])",
            "| identified visitorId",
            "endmerge",
            "",
        ]
    )

    body = compile_to_pendo_aggregation(parse(dsl))
    merge_stage = body["request"]["pipeline"][0]["merge"]

    assert merge_stage["fields"] == ["accountId"]
    assert "mappings" not in merge_stage
    assert merge_stage["pipeline"][0] == {"source": {"visitors": {}}}


def test_decompile_merge_without_mappings_round_trip() -> None:
    body = {
        "response": {"location": "request", "mimeType": "application/json"},
        "request": {
            "name": "MergeNoMappings",
            "pipeline": [
                {
                    "merge": {
                        "fields": ["accountId"],
                        "pipeline": [
                            {"source": {"visitors": {}}},
                            {"identified": "visitorId"},
                        ],
                    }
                }
            ],
        },
    }

    dsl = decompile_pendo_aggregation_to_dsl(body)
    assert "merge fields [accountId]" in dsl
    assert "mappings" not in dsl

    body2 = compile_to_pendo_aggregation(parse(dsl))
    assert body2 == body


def test_unmarshal_stage_compiles() -> None:
    dsl = "\n".join(
        [
            "PIPELINE",
            "| unmarshal { groups=string }",
            "",
        ]
    )

    body = compile_to_pendo_aggregation(parse(dsl))
    assert body["request"]["pipeline"][0] == {"unmarshal": {"groups": "string"}}


def test_decompile_merge_and_unmarshal_round_trip() -> None:
    body = {
        "response": {"location": "request", "mimeType": "application/json"},
        "request": {
            "name": "MergeUnmarshal",
            "pipeline": [
                {
                    "merge": {
                        "fields": ["pageId"],
                        "mappings": {"groupId": "group.id"},
                        "pipeline": [
                            {
                                "source": {
                                    "pages": {"appId": []}
                                }
                            },
                            {"filter": "!isNil(group.id)"},
                            {"eval": {"pageId": "id"}},
                        ],
                    }
                },
                {"unmarshal": {"groups": "string"}},
            ],
        },
    }

    dsl = decompile_pendo_aggregation_to_dsl(body)
    # Ensure we emitted first-class stages, not raw fallbacks.
    assert " raw {\"merge\"" not in dsl
    assert "merge fields" in dsl
    assert "unmarshal" in dsl

    body2 = compile_to_pendo_aggregation(parse(dsl))
    assert body2 == body
