from __future__ import annotations

import pytest

from aggdsl import compile_to_pendo_aggregation, decompile_pendo_aggregation_to_dsl, lint_expressions, parse
from aggdsl.parser import DslParseError

HEAD = "FROM event([source=events, appId=-1])\n"
RANGES = '| switch size from numEvents { "small" < 10, "medium" >= 10 && < 20, "large" >= 20 && < 50 }\n'

# Example from the Pendo API docs (Aggregation > switch).
DOCS_SWITCH = {
    "switch": {
        "size": {
            "numEvents": [
                {"value": "small", "<": 10},
                {"value": "medium", ">=": 10, "<": 20},
                {"value": "large", ">=": 20, "<": 50},
            ]
        }
    }
}


def _pipeline(dsl: str) -> list:
    return compile_to_pendo_aggregation(parse(dsl))["request"]["pipeline"]


def test_range_cases_compile_to_documented_json() -> None:
    assert _pipeline(HEAD + RANGES)[1] == DOCS_SWITCH


def test_range_cases_round_trip() -> None:
    body = compile_to_pendo_aggregation(parse(HEAD + RANGES))
    dsl = decompile_pendo_aggregation_to_dsl(body)
    assert '"medium" >= 10 && < 20' in dsl
    assert compile_to_pendo_aggregation(parse(dsl)) == body


def test_float_and_negative_bounds() -> None:
    p = _pipeline(HEAD + '| switch s from f { "neg" < -1.5, "pos" > 0 }\n')
    assert p[1]["switch"]["s"]["f"] == [{"value": "neg", "<": -1.5}, {"value": "pos", ">": 0}]


def test_equality_cases_unchanged() -> None:
    p = _pipeline(HEAD + '| switch m from f { "1"=="aaa", "2"=="bbb" }\n')
    assert p[1]["switch"]["m"]["f"] == [{"value": "1", "==": "aaa"}, {"value": "2", "==": "bbb"}]


@pytest.mark.parametrize("case", ['"a" >= x', '"a" >= 1 && >= 2', '"a" >= 1 &&', '"a" 10', '"a" ~ 10'])
def test_bad_range_case_is_rejected(case: str) -> None:
    with pytest.raises(DslParseError, match="Invalid switch case"):
        parse(HEAD + "| switch s from f { " + case + " }\n")


def test_unmodelled_switch_case_decompiles_to_raw() -> None:
    body = {"request": {"pipeline": [{"source": {"events": {"appId": -1}}},
            {"switch": {"s": {"f": [{"value": "a", "==": "b", "<": 3}]}}}]}}
    assert "raw" in decompile_pendo_aggregation_to_dsl(body)


def test_raw_switch_still_works() -> None:
    raw = '| raw {"switch":{"size":{"numEvents":[{"value":"small","<":10}]}}}\n'
    assert _pipeline(HEAD + raw)[1] == {"switch": {"size": {"numEvents": [{"value": "small", "<": 10}]}}}


def test_linter_accepts_range_switch() -> None:
    assert lint_expressions(HEAD + RANGES) == []
