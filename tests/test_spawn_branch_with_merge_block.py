from __future__ import annotations

from aggdsl import compile_to_pendo_aggregation, parse


def test_spawn_branch_allows_nested_merge_block_with_pipe_stages() -> None:
    dsl = """\
PIPELINE
| spawn
branch
PIPELINE
|| merge fields [accountId]
FROM event([source=visitors])
| identified visitorId
endmerge
|| limit 1
endbranch
| endspawn
"""

    body = compile_to_pendo_aggregation(parse(dsl))
    pipeline = body["request"]["pipeline"]

    assert len(pipeline) == 1
    assert "spawn" in pipeline[0]

    branch0 = pipeline[0]["spawn"][0]
    assert branch0[0]["merge"]["fields"] == ["accountId"]
    assert branch0[0]["merge"]["pipeline"][0] == {"source": {"visitors": {}}}
    assert branch0[0]["merge"]["pipeline"][1] == {"identified": "visitorId"}
    assert branch0[1] == {"limit": 1}
