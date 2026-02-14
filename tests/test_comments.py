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
    """Test that whitespace-indented // comments are stripped."""
    dsl = """\
FROM event([source=visitors])
    // indented comment between stages
| filter foo
  // another indented comment
| limit 10
"""

    body = compile_to_pendo_aggregation(parse(dsl))

    assert body["request"]["pipeline"] == [
        {"source": {"visitors": {}}},
        {"filter": "foo"},
        {"limit": 10},
    ]


def test_comments_between_stages() -> None:
    """Test that // comments can appear between stages and headers."""
    dsl = """\
// comment before FROM
FROM event([source=pageEvents])
// comment after FROM, before TIMESERIES
TIMESERIES period=dayRange first=1234567890000 count=30
// comment after TIMESERIES, before filter
| filter foo
// comment between filter stages
| filter bar
// final comment
"""

    body = compile_to_pendo_aggregation(parse(dsl))

    assert body["request"]["pipeline"] == [
        {
            "source": {
                "pageEvents": {},
                "timeSeries": {
                    "period": "dayRange",
                    "first": 1234567890000,
                    "count": 30,
                },
            }
        },
        {"filter": "foo"},
        {"filter": "bar"},
    ]


def test_inline_double_slash_remains_as_content() -> None:
    """Test that inline // sequences are NOT treated as comments."""
    dsl = """\
FROM event([source=visitors])
| filter url == "https://example.com"
"""

    body = compile_to_pendo_aggregation(parse(dsl))

    assert body["request"]["pipeline"] == [
        {"source": {"visitors": {}}},
        {"filter": 'url == "https://example.com"'},
    ]
