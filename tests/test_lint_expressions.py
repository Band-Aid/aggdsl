from __future__ import annotations

import pytest

from aggdsl import lint_expressions
from aggdsl.cli import main

HEAD = "FROM event([source=agenticEvents, appId=-1])\n"


def test_valid_expression_has_no_warnings() -> None:
    assert lint_expressions(HEAD + "| filter isNil(conversationId) || isEmpty(conversationId)\n") == []


@pytest.mark.parametrize(
    "stage, needle",
    [
        ("| filter isBroken(conversationId)", "isBroken"),
        ("| filter fooBar(x)", "unknown function `fooBar()`"),
        ("| eval { n=count(x) }", "aggregator"),
        ("| filter (isNil(a)", "unbalanced"),
        ("| filter a = 1", "use `==`"),
        ("| filter isNil(a) ||", "dangling"),
    ],
)
def test_bad_expression_is_flagged_with_line_number(stage: str, needle: str) -> None:
    warnings = lint_expressions(HEAD + stage + "\n")
    assert len(warnings) == 1 and warnings[0].startswith("line 2:") and needle in warnings[0]


def test_string_literals_and_comments_are_ignored() -> None:
    assert lint_expressions(HEAD + '// | filter bogus(x)\n| filter content == "bogus(x"\n') == []


def test_group_allows_aggregators() -> None:
    assert lint_expressions(HEAD + "| group by day fields { e=sum(numEvents), n=count(null) }\n") == []


def test_cli_compile_lint_exit_code(tmp_path, capsys) -> None:
    bad = tmp_path / "bad.dsl"
    bad.write_text(HEAD + "| filter isBroken(x)\n")
    assert main(["compile", str(bad)]) == 0
    assert main(["compile", str(bad), "--lint"]) == 1
    assert "isBroken" in capsys.readouterr().err
    good = tmp_path / "good.dsl"
    good.write_text(HEAD + "| filter isNil(x)\n")
    assert main(["compile", str(good), "--lint"]) == 0
