"""Lint for aggDSL expressions (filter/eval/select/unmarshal/switch/group).

The parser treats expressions as opaque strings, so these checks cover what the
Pendo API rejects only at run time. Function names come from the Pendo API docs
(Aggregation > Expressions).
"""
from __future__ import annotations

import re

# Functions the Pendo aggregation expression language defines (Pendo API docs,
# Aggregation > Expressions). Anything else called in an expression is rejected
# by the API at run time, so lint flags it.
_EXPR_FUNCS = frozenset(
    "if isBoolean isNumber isString isList isNil isNull isEmpty "
    "contains split startsWith toLowerCase toString "
    "len list listInRange reverse sortUnique sum "
    "now date startOfPeriod dateAdd formatTime "
    "identified childAccountId parentAccountId objectValues".split()
)
# Aggregators are only valid as the function of a `group ... fields { }` entry.
_AGG_FUNCS = frozenset(
    "sum count countIf avg mean median min max first any list concat "
    "listAverage listMedian funnel inactivityPeriods accumulate".split()
)
_EXPR_STAGE = re.compile(r"^\s*\|{1,2}\s*(filter|eval|select|unmarshal|switch|group)\b(.*)$")
_STRINGS = re.compile(r"\"(?:[^\"\\]|\\.)*\"|'(?:[^'\\]|\\.)*'|`[^`]*`")
_CALL = re.compile(r"(?<![\w.])([A-Za-z_]\w*)\s*\(")
_SINGLE_EQ = re.compile(r"(?<![=!<>])=(?!=)")
def _balanced(expr: str) -> bool:
    depth = []
    pairs = {")": "(", "]": "["}
    for ch in expr:
        if ch in "([":
            depth.append(ch)
        elif ch in pairs and (not depth or depth.pop() != pairs[ch]):
            return False
    return not depth


def lint_expression(stage: str, body: str) -> list[str]:
    """Check one single-line stage's expression against the documented language."""
    bare = _STRINGS.sub('""', body)
    out = []
    allowed = _EXPR_FUNCS | _AGG_FUNCS if stage == "group" else _EXPR_FUNCS
    for name in dict.fromkeys(_CALL.findall(bare)):
        if name in allowed:
            continue
        if name in _AGG_FUNCS:
            out.append(f"`{name}()` is an aggregator, only valid inside `group ... fields {{ }}`")
        elif name == "isBroken":
            out.append("`isBroken` is not an expression function; for replay sessions it is a field: `filter !isBroken`")
        elif name == "in":
            continue  # reported by the `in [...]` rule
        else:
            out.append(f"unknown function `{name}()`; not in the Pendo expression function list (see _EXPR_FUNCS in aggdsl/lint.py)")
    if stage == "filter":
        if not _balanced(bare):
            out.append("unbalanced parentheses/brackets in filter expression")
        if _SINGLE_EQ.search(bare):
            out.append("`=` in a filter is not comparison; use `==`")
        if re.search(r"&&\s*$|\|\|\s*$|^\s*(?:&&|\|\|)", bare.strip()):
            out.append("filter expression has a dangling `&&`/`||`")
    return out


def lint_expressions(text: str) -> list[str]:
    """Return "line N: message" warnings for every single-line expression stage."""
    out = []
    for i, line in enumerate(text.splitlines(), 1):
        s = line.strip()
        if s.startswith("//") or s.startswith("#"):
            continue
        m = _EXPR_STAGE.match(line)
        if m:
            out += [f"line {i}: {msg}" for msg in lint_expression(m.group(1), m.group(2))]
    return out
