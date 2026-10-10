#!/usr/bin/env python3
"""Compile-check and lint aggDSL files. Output is deliberately terse.

    python scripts/check.py query.dsl [more.dsl ...]   # "OK" or errors/warnings
    python scripts/check.py - < query.dsl               # read stdin
    python scripts/check.py query.dsl --body            # also print the JSON body
    python scripts/check.py query.dsl --now-ms 1735689600000

Exit 0 = every file compiled with no lint warnings; 1 = lint warnings only;
2 = a compile error. Lint warnings are run-time failures the compiler cannot
see, so treat them as errors.
"""
from __future__ import annotations

import argparse
import importlib
import json
import os
import re
import sys


def _load_aggdsl():
    # Prefer a repo checkout (src/aggdsl in an ancestor) so the repo stays
    # authoritative, then an installed package, then the copy bundled into the
    # packaged skill under scripts/vendor/.
    here = os.path.dirname(os.path.abspath(__file__))
    cur = here
    while True:
        cand = os.path.join(cur, "src")
        if os.path.isdir(os.path.join(cand, "aggdsl")):
            sys.path.insert(0, cand)
            break
        parent = os.path.dirname(cur)
        if parent == cur:
            break
        cur = parent
    try:
        return importlib.import_module("aggdsl")
    except ModuleNotFoundError:
        pass
    sys.path.insert(0, os.path.join(here, "vendor"))
    try:
        return importlib.import_module("aggdsl")
    except ModuleNotFoundError:
        sys.stderr.write("error: aggdsl compiler not found (skill installed without scripts/vendor/aggdsl)\n")
        sys.exit(2)


# (pattern, message). Each message says how to fix, so a warning teaches the
# rule only when it is broken instead of the skill spending tokens up front.
_LINE_RULES = [
    (re.compile(r"\bin\s*\["),
     'no `in` operator; use `f == "a" || f == "b"` or `contains(["a","b"], f)`'),
    (re.compile(r"now\(\)\s*[-+]\s*\d"),
     'now() is epoch-ms, so now()-30 is 30ms; use `count=-30` or dateAdd(startOfPeriod("daily", now()), -30, "days")'),
    (re.compile(r"^\s*\|\|\|"),
     "bars never stack; nested branches still use `||`"),
    (re.compile(r"\{\{[A-Z0-9_]+\}\}"),
     "unfilled {{PLACEHOLDER}}; substitute the real id, ask the user if unknown (inside JSON stages appId is an unquoted number)"),
    (re.compile(r"=\s*`[^`]*`"),
     'backticks are a field reference; quote string constants: x="page"'),
    (re.compile(r"\S\s+//\s"),
     "no inline comments; move `//` to its own line"),
]
# Compile errors whose message does not say how to fix them.
_HINTS = [
    ("Unknown stage: |", "`||` is only for spawn/fork branch stages; unmodeled stages go in `| raw {json}`"),
    ("Group syntax", "form is `| group by a,b fields { x=sum(f) }`; no inline comments"),
    ("Invalid JSON", "JSON stages need strict JSON: double-quoted keys/strings, unquoted numbers"),
    ("Missing FROM or PIPELINE", "first non-header line must be FROM event([source=...]) or PIPELINE"),
]
_NEEDS_APP = re.compile(r"source=(events|recordingMetadata|agenticEvents|singleEvents)\b")
_TIME_GROUP = re.compile(r"group by\s+([\w,]*\b(?:hour|day|week|month|quarter)\b[\w,]*)\s+fields")
_FUTURE = re.compile(r"first=now\(\)\s+count=(\d+)")


def lint(text: str) -> list[str]:
    out = []
    lines = text.splitlines()
    for i, line in enumerate(lines, 1):
        s = line.strip()
        if s.startswith("//") or s.startswith("#"):
            continue
        for rx, msg in _LINE_RULES:
            if rx.search(line):
                out.append(f"line {i}: {msg}")
        if _NEEDS_APP.search(line) and "appId" not in text:
            out.append(f"line {i}: this source returns nothing without appId=<id> in the brackets")
        m = _FUTURE.search(line)
        if m:
            out.append(f"line {i}: first=now() count={m.group(1)} is a FUTURE window; use count=-{m.group(1)}")
        if _TIME_GROUP.search(line) and "formatTime" not in text:
            out.append(f'line {i}: grouped by a time bucket; add `| eval {{ label=formatTime("2006-01-02", day) }}`')
    return out


def main() -> int:
    ap = argparse.ArgumentParser(description="Compile-check and lint aggDSL.")
    ap.add_argument("paths", nargs="+", help=".dsl files, or - for stdin")
    ap.add_argument("--body", action="store_true", help="print the compiled JSON body")
    ap.add_argument("--now-ms", type=int, default=None, help="substitute now() with this epoch-ms")
    args = ap.parse_args()

    aggdsl = _load_aggdsl()
    worst = 0
    multi = len(args.paths) > 1
    for path in args.paths:
        tag = f"{path}: " if multi else ""
        try:
            text = sys.stdin.read() if path == "-" else open(path, encoding="utf-8").read()
            body = aggdsl.compile_to_pendo_aggregation(aggdsl.parse(text), now_ms=args.now_ms)
        except Exception as exc:  # DslParseError names the offending line
            hint = next((h for k, h in _HINTS if k in str(exc)), None)
            print(f"{tag}COMPILE ERROR: {exc}" + (f" -> {hint}" if hint else ""))
            worst = 2
            continue
        warnings = lint(text)
        for w in warnings:
            print(f"{tag}LINT {w}")
        if warnings:
            worst = max(worst, 1)
        elif not multi or not args.body:
            print(f"{tag}OK")
        if args.body:
            print(json.dumps(body, separators=(",", ":"), ensure_ascii=False))
    return worst


if __name__ == "__main__":
    sys.exit(main())
