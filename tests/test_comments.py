from __future__ import annotations

from aggdsl import compile_to_pendo_aggregation, parse


def test_leading_double_slash_comment_lines_are_ignored() -> None:
    dsl = """\
// leading comment line
FROM event([source=visitors])
| filter foo
"""

    body = compile_to_pendo_aggregation(parse(dsl))

    assert body["request"]["pipeline"] == [
        {"source": {"visitors": {}}},
        {"filter": "foo"},
    ]
