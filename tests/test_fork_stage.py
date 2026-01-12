from aggdsl import compile_to_pendo_aggregation, parse


def test_fork_stage_parses_and_compiles_multiline_json_array() -> None:
    dsl = "\n".join(
        [
            "PIPELINE",
            "| fork [",
            "  [ {\"limit\": 1} ],",
            "  [ {\"limit\": 2} ]",
            "]",
            "",
        ]
    )

    body = compile_to_pendo_aggregation(parse(dsl))
    assert body["request"]["pipeline"] == [{"fork": [[{"limit": 1}], [{"limit": 2}]]}]
