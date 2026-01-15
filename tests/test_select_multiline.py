from aggdsl.compiler import compile_pipeline
from aggdsl.parser import parse


def test_select_multiline_top_level() -> None:
    dsl = """
PIPELINE
| select {
  a=b,
  c=d
}
"""
    q = parse(dsl)
    pipeline = compile_pipeline(q)

    assert pipeline == [{"select": {"a": "b", "c": "d"}}]


def test_select_multiline_inside_fork_branch() -> None:
    # Continuation lines inside a fork branch may be plain (no leading ||)
    # as long as the opening `|| select {` begins a multiline brace stage.
    dsl = """
PIPELINE
| fork
branch
PIPELINE
|| select {
  a=b,
  c=d
}
endbranch
branch
PIPELINE
|| select { x=y }
endbranch
| endfork
"""

    q = parse(dsl)
    pipeline = compile_pipeline(q)

    assert pipeline == [
        {
            "fork": [
                [{"select": {"a": "b", "c": "d"}}],
                [{"select": {"x": "y"}}],
            ]
        }
    ]
