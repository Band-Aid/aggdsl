from __future__ import annotations

from aggdsl import compile_to_pendo_aggregation, parse


def test_pes_stage_parses_and_compiles_multiline_json_object() -> None:
    dsl = """\
PIPELINE
| pes {
  "appId": -123456,
  "firstDay": "now()",
  "dayCount": -30,
  "blacklist": "apply",
  "segment": { "id": "SEGMENT_ID_EXAMPLE" }
}
"""

    body = compile_to_pendo_aggregation(parse(dsl))
    assert body["request"]["pipeline"][0] == {
        "pes": {
            "appId": -123456,
            "firstDay": "now()",
            "dayCount": -30,
            "blacklist": "apply",
            "segment": {"id": "SEGMENT_ID_EXAMPLE"},
        }
    }
