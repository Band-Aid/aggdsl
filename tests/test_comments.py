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


def test_indented_double_slash_comment_lines_are_ignored() -> None:
    """Test that indented // comment lines are stripped."""
    dsl = """\
FROM event([source=visitors])
    // indented comment line
| filter foo
"""

    body = compile_to_pendo_aggregation(parse(dsl))

    assert body["request"]["pipeline"] == [
        {"source": {"visitors": {}}},
        {"filter": "foo"},
    ]


def test_inline_double_slash_is_not_stripped() -> None:
    """Test that inline // is treated as content, not a comment."""
    dsl = """\
FROM event([source=visitors])
| filter url == "https://example.com"
"""

    body = compile_to_pendo_aggregation(parse(dsl))

    assert body["request"]["pipeline"] == [
        {"source": {"visitors": {}}},
        {"filter": 'url == "https://example.com"'},
    ]
