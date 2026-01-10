import json

from aggdsl import compile_to_pendo_aggregation, decompile_pendo_aggregation_to_dsl, parse


def test_decompile_round_trip_spawn_switch_unicode() -> None:
    dsl_in = "\n".join(
        [
            "RESPONSE mimeType=application/json",
            'REQUEST name="RoundTrip"',
            "PIPELINE",
            "| spawn",
            "branch",
            "PIPELINE",
            "|| select { ID=visitorId, ハローPoll1=pollResponse1 }",
            "|| switch mappedpollResponse from pollResponse { \"1\"==\"abc\", \"2\"==\"def\" }",
            "endbranch",
            "| endspawn",
            "| join fields [browserTime,visitorId]",
            "| limit 10",
            "",
        ]
    )

    body1 = compile_to_pendo_aggregation(parse(dsl_in))

    dsl_out = decompile_pendo_aggregation_to_dsl(body1)
    body2 = compile_to_pendo_aggregation(parse(dsl_out))

    assert body2 == body1


def test_decompile_accepts_legacy_request_array() -> None:
    body = {
        "request": [
            {"filter": "x == 1"},
            {"limit": 1},
        ]
    }

    dsl_out = decompile_pendo_aggregation_to_dsl(body)
    body2 = compile_to_pendo_aggregation(parse(dsl_out))

    assert body2["request"]["pipeline"] == body["request"]


def test_decompile_accepts_pipeline_array_input() -> None:
    pipeline = [{"filter": "x == 1"}, {"limit": 1}]
    dsl_out = decompile_pendo_aggregation_to_dsl(pipeline)
    body2 = compile_to_pendo_aggregation(parse(dsl_out))

    assert body2["request"]["pipeline"] == pipeline
