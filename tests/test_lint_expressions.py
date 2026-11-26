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
        ("| eval { x=if(a) }", "`if()` called with 1"),
        ("| eval { x=if(a,1,2,3) }", "`if()` called with 4"),
        ("| filter isNil(a, b)", "`isNil()` called with 2"),
        ("| filter isNil()", "`isNil()` called with 0"),
        ('| filter isNull("a")', "takes a field name"),
        ("| filter isEmpty(0)", "takes a field name"),
        ("| group by day fields { n=avg() }", "`avg()` called with 0"),
        ("| group by day fields { n=count(a, b) }", "`count()` called with 2"),
        ("| group by day fields { n=first(a, b) }", "`first()` called with 2"),
        ("| group by day fields { n=minutesActiveCount(x) }", "only for the field `minutesActive`"),
        ("| group by day fields { n=minutesActiveCount() }", "`minutesActiveCount()` called with 0"),
        ("| eval { n=minutesActiveCount(minutesActive) }", "aggregator"),
        ('| eval { x=formatTime("yyyy-MM-dd", t) }', "not a Go time layout"),
        ('| eval { x=formatTime("", t) }', "not a Go time layout"),
        ("| filter a === 1", "`===`"),
        ("| filter a <> 1", "`<>`"),
        ("| filter a & b", "single `&`"),
        ("| filter a | b", "single `|`"),
        ("| filter a ^ 2 > 1", "not operators"),
        ("| eval { x=a ? 1 : 2 }", "not operators"),
        ("| filter a == 1 and b == 2", "`and`/`or`/`not`"),
        ("| filter not a", "`and`/`or`/`not`"),
        ("| filter a in [1, 2]", "no `in` operator"),
        ("| filter contains(a)", "`contains()` called with 1"),
        ("| eval { x=split(a) }", "`split()` called with 1"),
        ("| eval { x=toLowerCase(a, b) }", "`toLowerCase()` called with 2"),
        ("| eval { x=listInRange(l, 1) }", "`listInRange()` called with 2"),
        ("| eval { x=now(1) }", "`now()` called with 1"),
        ("| eval { x=date() }", "`date()` called with 0"),
        ("| eval { x=date(1,2,3,4,5,6,7,8) }", "`date()` called with 8"),
        ("| eval { x=list() }", "`list()` called with 0"),
        ('| eval { x=startOfPeriod("hourly", t) }', "period \"hourly\" is not one of"),
        ("| eval { x=startOfPeriod(1, t) }", "period must be a string"),
        ("| eval { x=startOfPeriod(t) }", "`startOfPeriod()` called with 1"),
        ('| eval { x=dateAdd(t, 1, "day") }', 'interval "day" is not one of'),
        ('| eval { x=dateAdd(t, 1) }', "`dateAdd()` called with 2"),
        ("| eval { x=formatTime(t) }", "`formatTime()` called with 1"),
        ("| filter identified() ", "`identified()` called with 0"),
        ("| eval { x=parentAccountId(a, b) }", "`parentAccountId()` called with 2"),
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


def test_documented_signatures_accepted() -> None:
    ok = (
        "| eval { a=if(x == 1, 'y') }\n"
        "| eval { a=if(isNil(x), 0, if(isEmpty(y), 1, 2)) }\n"
        "| filter isNull(x) && isString(y) && isList(z) && isBoolean(b) && isNumber(n)\n"
    )
    assert lint_expressions(HEAD + ok) == []


def test_remaining_documented_signatures_accepted() -> None:
    ok = (
        "| eval { a=contains(split(p, \"/\"), \"x\"), b=startsWith(toLowerCase(p), \"a,b\") }\n"
        "| eval { l=list(a, b, c), n=len(l), s=sum(reverse(sortUnique(listInRange(l, 1, 9)))) }\n"
        "| eval { d=date(\"2022-01-26T10:30:00\"), e=date(2021, 4, 7), f=date(2021) }\n"
        "| eval { w=startOfPeriod(\"weekly\", now()), z=dateAdd(w, -1, \"months\") }\n"
        "| eval { t=formatTime(\"2006-01-02\", w), u=toString(n) }\n"
        "| filter identified(visitorId) && parentAccountId(accountId) == accountId\n"
        "| eval { x=l[0], y=l[1:3], z=l[-1] }\n"
    )
    assert lint_expressions(HEAD + ok) == []


def test_documented_field_aggregators_accepted() -> None:
    ok = (
        "| group by day fields { a=avg(x), c=count(null), d=count(v), f=first(x), l=list(x), m=median(x),"
        " mx=max(x), mn=mean(x), lo=min(x), s=sum(x), u=minutesActiveCount(minutesActive),"
        " ci=countIf(x == 1) }\n"
    )
    assert lint_expressions(HEAD + ok) == []


def test_format_time_accepts_go_layouts() -> None:
    ok = (
        '| eval { a=formatTime("2006-01-02", t), b=formatTime("2006-01-02T15:04", t),'
        ' c=formatTime("It\'s 2006, January 2", t), d=formatTime("Jan 2", t), e=formatTime(fmt, t) }\n'
    )
    assert lint_expressions(HEAD + ok) == []


def test_documented_operators_accepted() -> None:
    ok = (
        "| filter a == 1 && b != 2 || !c\n"
        "| filter a < 1 && a <= 2 && a > 0 && a >= 0\n"
        '| eval { x=a + b - c * d / e % f, y=a == "and", z="a|b" }\n'
        "| filter contains(list(a, b), 1)\n"
    )
    assert lint_expressions(HEAD + ok) == []
