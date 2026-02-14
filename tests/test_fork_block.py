from __future__ import annotations


from aggdsl import compile_to_pendo_aggregation, decompile_pendo_aggregation_to_dsl, parse


def test_fork_block_compiles_to_fork_pipelines() -> None:
    dsl = """\
RESPONSE mimeType=application/json
REQUEST name="ForkBlockExample"
FROM event([source=recordingMetadata,appId=123,blacklist="ignore"])
TIMESERIES period=dayRange first=now() count=1
| fork
branch
PIPELINE
|| filter recordingId != null
|| limit 1
endbranch
branch
PIPELINE
|| filter isBroken == true
|| limit 2
endbranch
| endfork
"""

    q = parse(dsl)
    body = compile_to_pendo_aggregation(q)
    pipeline = body["request"]["pipeline"]

    assert pipeline[0]["source"]["recordingMetadata"]["appId"] == 123
    assert "fork" in pipeline[1]
    assert len(pipeline[1]["fork"]) == 2
    assert pipeline[1]["fork"][0][0] == {"filter": "recordingId != null"}
    assert pipeline[1]["fork"][0][1] == {"limit": 1}


def test_decompile_prefers_fork_block_when_possible() -> None:
    body = {
        "response": {"location": "request", "mimeType": "application/json"},
        "request": {
            "pipeline": [
                {"source": {"events": {}, "timeSeries": {"period": "dayRange", "first": "now()", "count": 1}}},
                {"fork": [[{"filter": "a==1"}], [{"filter": "b==2"}]]},
            ]
        },
    }

    dsl = decompile_pendo_aggregation_to_dsl(body)
    # Sanity: fork block markers exist.
    assert "| fork" in dsl
    assert "| endfork" in dsl
    # Roundtrip compile preserves the fork structure.
    q2 = parse(dsl)
    body2 = compile_to_pendo_aggregation(q2)
    assert body2["request"]["pipeline"][1] == body["request"]["pipeline"][1]


def test_fork_branch_can_contain_nested_fork_block() -> None:
    dsl = """\
PIPELINE
| fork
branch
PIPELINE
|| fork
branch
PIPELINE
|| group by steps fields { count=count(null) }
endbranch
|| endfork
endbranch
| endfork
"""

    body = compile_to_pendo_aggregation(parse(dsl))
    pipeline = body["request"]["pipeline"]

    assert "fork" in pipeline[0]
    outer_branch0 = pipeline[0]["fork"][0]
    assert outer_branch0[0] == {
        "fork": [
            [
                {
                    "group": {
                        "group": ["steps"],
                        "fields": [{"count": {"count": None}}],
                    }
                }
            ]
        ]
    }


def test_fork_branch_allows_omitted_pipeline_when_starting_with_stage() -> None:
    dsl = """\
PIPELINE
| fork
branch
|| fork
branch
|| limit 1
endbranch
|| endfork
endbranch
| endfork
"""

    body = compile_to_pendo_aggregation(parse(dsl))
    pipeline = body["request"]["pipeline"]

    assert pipeline[0] == {"fork": [[{"fork": [[{"limit": 1}]]}]]}
