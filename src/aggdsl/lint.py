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
    "minutesActiveCount listAverage listMedian funnel inactivityPeriods accumulate".split()
)
_EXPR_STAGE = re.compile(r"^\s*\|{1,2}\s*(filter|eval|select|unmarshal|switch|group)\b(.*)$")
_STRINGS = re.compile(r"\"(?:[^\"\\]|\\.)*\"|'(?:[^'\\]|\\.)*'|`[^`]*`")
_CALL = re.compile(r"(?<![\w.])([A-Za-z_]\w*)\s*\(")
_SINGLE_EQ = re.compile(r"(?<![=!<>])=(?!=)")
# Documented signatures (Pendo API docs, Aggregation > Expressions):
# name -> (min args, max args or None for variadic, usage). `if` takes an optional ifFalse.
_SIGNATURES = {
    "if": (2, 3, "if(expression, ifTrue[, ifFalse])"),
    "isBoolean": (1, 1, "isBoolean(operand)"),
    "isNumber": (1, 1, "isNumber(operand)"),
    "isString": (1, 1, "isString(operand)"),
    "isList": (1, 1, "isList(operand)"),
    "isNil": (1, 1, "isNil(operand)"),
    "isNull": (1, 1, "isNull(fieldName)"),
    "isEmpty": (1, 1, "isEmpty(fieldName)"),
    "contains": (2, 2, "contains(haystack, needle) | contains(myList, item)"),
    "split": (2, 2, "split(myString, separator)"),
    "startsWith": (2, 2, "startsWith(haystack, needle)"),
    "toLowerCase": (1, 1, "toLowerCase(myString)"),
    "toString": (1, 1, "toString(myNumber)"),
    "len": (1, 1, "len(myList)"),
    "list": (1, None, "list(field1, field2, fieldN)"),
    "listInRange": (3, 3, "listInRange(myList, minValue, maxValue)"),
    "reverse": (1, 1, "reverse(myList)"),
    "sortUnique": (1, 1, "sortUnique(myList)"),
    "sum": (1, 1, "sum(myList)"),
    "now": (0, 0, "now()"),
    "date": (1, 7, "date(dateString) | date(year, mon, day, hour, min, sec, nanosec)"),
    "startOfPeriod": (2, 2, "startOfPeriod(period, timestamp)"),
    "dateAdd": (3, 3, "dateAdd(timestamp, num, interval)"),
    "formatTime": (2, 2, "formatTime(formatString, timestamp)"),
    "identified": (1, 1, "identified(visitorId)"),
    "childAccountId": (1, 1, "childAccountId(accountId)"),
    "parentAccountId": (1, 1, "parentAccountId(accountId)"),
}
# Field aggregators (Pendo API docs, Aggregation > Field aggregators), valid only in `group`.
# countIf is not listed: the docs give no argument form for it.
_AGG_SIGNATURES = {
    n: (1, 1, f"{n}(field)") for n in "avg count first list median max mean min sum".split()
}
_AGG_SIGNATURES["minutesActiveCount"] = (1, 1, "minutesActiveCount(minutesActive)")
# isNull/isEmpty take a field name, not a literal value.
_FIELD_ARG = frozenset({"isNull", "isEmpty"})
# (function, arg index) -> allowed string values, per the docs.
_ENUM_ARGS = {
    ("startOfPeriod", 0): ("period", ("daily", "weekly", "monthly")),
    ("dateAdd", 2): ("interval", ("days", "weeks", "months", "years")),
}
_LITERAL = re.compile(r"""^(?:"[^"]*"|'[^']*'|-?\d+(?:\.\d+)?|true|false|null|nil)$""", re.I)


# formatTime's format string must be a Go time layout: it has to contain at least one
# element of the reference time `Mon Jan 2 15:04:05 MST 2006` ("yyyy-MM-dd" has none).
_GO_LAYOUT = re.compile(
    r"2006|Monday|Mon|January|Jan|MST|PM|pm|Z07|0[1-6]|15|[1-6]|_2"
)


def _mask_strings(body: str) -> str:
    """Same-length copy of body with string contents blanked, so offsets match the original."""
    return _STRINGS.sub(lambda m: m.group(0)[0] + "x" * (len(m.group(0)) - 2) + m.group(0)[-1], body)


def _call_args(masked: str, orig: str, open_idx: int) -> list[str] | None:
    """Top-level arguments (original text) of the call whose '(' is at open_idx; None if unclosed."""
    depth, args, cur = 0, [], []
    for i in range(open_idx, len(masked)):
        ch = masked[i]
        if ch in "([{":
            depth += 1
            if depth == 1:
                continue
        elif ch in ")]}":
            depth -= 1
            if depth == 0:
                arg = "".join(cur).strip()
                if arg or args:
                    args.append(arg)
                return args
        elif ch == "," and depth == 1:
            args.append("".join(cur).strip())
            cur = []
            continue
        cur.append(orig[i])
    return None


def _check_signatures(body: str, stage: str = "") -> list[str]:
    masked = _mask_strings(body)
    out = []
    for m in _CALL.finditer(masked):
        name = m.group(1)
        # In `group`, an aggregator name uses its aggregator signature (e.g. 1-arg `list`/`sum`).
        sigs = _AGG_SIGNATURES if stage == "group" and name in _AGG_SIGNATURES else _SIGNATURES
        if name not in sigs:
            continue
        args = _call_args(masked, body, m.end() - 1)
        if args is None:
            continue  # unbalanced; reported separately for filters
        lo, hi, usage = sigs[name]
        if len(args) < lo or (hi is not None and len(args) > hi) or "" in args:
            out.append(f"`{name}()` called with {len(args)} argument(s); documented signature is `{usage}`")
            continue
        if name == "minutesActiveCount" and args[0] != "minutesActive":
            out.append(f"`{name}()` is only for the field `minutesActive`, got `{args[0]}`")
        if name in _FIELD_ARG and _LITERAL.match(args[0]):
            out.append(f"`{name}()` takes a field name, not the literal `{args[0]}`; signature `{usage}`")
        if name == "formatTime" and args[0][:1] in "\"'" and args[0][-1:] == args[0][:1]:
            if not _GO_LAYOUT.search(args[0][1:-1]):
                out.append(
                    f"`formatTime()` format {args[0]} is not a Go time layout; build it from "
                    "`Mon Jan 2 15:04:05 MST 2006` (e.g. \"2006-01-02\"), not yyyy-MM-dd"
                )
        for (fn, idx), (label, allowed) in _ENUM_ARGS.items():
            if fn != name:
                continue
            arg = args[idx]
            if arg[:1] in "\"'" and arg[-1:] == arg[:1] and arg[1:-1] not in allowed:
                out.append(f"`{name}()` {label} {arg} is not one of {', '.join(allowed)}")
            elif re.fullmatch(r"-?\d+(?:\.\d+)?", arg):
                out.append(f"`{name}()` {label} must be a string, one of {', '.join(allowed)}")
    return out


# Operators the expression language defines (Pendo API docs, Aggregation > Expressions > Operators):
# == != < <= > >= && || ! + - * / %. Common operators from other languages are not among them.
_BAD_OPERATORS = (
    (re.compile(r"[=!]==|<>"), "`===`, `!==` and `<>` are not operators; use `==` / `!=`"),
    (re.compile(r"(?<!&)&(?!&)"), "single `&` is not an operator; use `&&`"),
    (re.compile(r"(?<!\|)\|(?!\|)"), "single `|` is not an operator; use `||`"),
    (re.compile(r"\*\*|\^|=>|\?"), "`**`, `^`, `=>` and `?:` are not operators; the language has `== != < <= > >= && || ! + - * / %` and `if(c, a, b)`"),
    (re.compile(r"\b(?:and|or|not)\b", re.I), "`and`/`or`/`not` are not operators; use `&&`, `||`, `!`"),
    (re.compile(r"\bin\s*[\[(]|\bnot\s+in\b", re.I), "there is no `in` operator; chain `==` with `||`, or use `contains(list, x)`"),
)


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
            continue  # reported by the operator rules
        else:
            out.append(f"unknown function `{name}()`; not in the Pendo expression function list (see _EXPR_FUNCS in aggdsl/lint.py)")
    out += _check_signatures(body, stage)
    if stage != "switch":  # switch bodies are `"label" >= 10 && < 20` case lists
        for pattern, message in _BAD_OPERATORS:
            if pattern.search(bare):
                out.append(message)
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
