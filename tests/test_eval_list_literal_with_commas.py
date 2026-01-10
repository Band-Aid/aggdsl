from __future__ import annotations

from aggdsl import compile_to_pendo_aggregation, parse


def test_eval_list_literal_with_commas_parses() -> None:
    dsl = """\
RESPONSE mimeType=application/json
REQUEST name="examples of list checks and manipulation"
FROM event([source=visitors])
| eval { inputs.listTwo=[8,5,1,1,2,1+2] }
| eval { outputs.index_firstItem=inputs.listTwo[0], outputs.sublist=inputs.listTwo[3:5] }
"""

    body = compile_to_pendo_aggregation(parse(dsl))
    pipeline = body["request"]["pipeline"]

    assert pipeline[1]["eval"]["inputs.listTwo"] == "[8,5,1,1,2,1+2]"
    assert pipeline[2]["eval"]["outputs.index_firstItem"] == "inputs.listTwo[0]"
    assert pipeline[2]["eval"]["outputs.sublist"] == "inputs.listTwo[3:5]"
