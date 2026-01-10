from __future__ import annotations

import argparse
import json
import sys
from collections import Counter
from typing import Any, Iterable


def _load_json(path: str | None, *, use_stdin: bool) -> Any:
    if use_stdin:
        return json.load(sys.stdin)
    if path is None:
        raise SystemExit("error: provide a result JSON file or --stdin")
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def _extract_rows(obj: Any) -> list[dict[str, Any]]:
    """Best-effort extraction of tabular rows from common Pendo response shapes."""
    if isinstance(obj, list):
        if all(isinstance(x, dict) for x in obj):
            return obj  # type: ignore[return-value]
        return []

    if not isinstance(obj, dict):
        return []

    for key in ("results", "result", "data", "rows"):
        v = obj.get(key)
        if isinstance(v, list) and all(isinstance(x, dict) for x in v):
            return v  # type: ignore[return-value]

    return []


def _infer_numeric_fields(rows: Iterable[dict[str, Any]]) -> list[str]:
    counts: Counter[str] = Counter()
    for r in rows:
        for k, v in r.items():
            if isinstance(v, (int, float)):
                counts[k] += 1
    return [k for k, _ in counts.most_common()]


def _infer_categorical_fields(rows: Iterable[dict[str, Any]]) -> list[str]:
    counts: Counter[str] = Counter()
    for r in rows:
        for k, v in r.items():
            if isinstance(v, str):
                counts[k] += 1
    return [k for k, _ in counts.most_common()]


def _summarize(rows: list[dict[str, Any]]) -> str:
    if not rows:
        return "No tabular rows found in result.\n"

    numeric = _infer_numeric_fields(rows)
    cats = _infer_categorical_fields(rows)

    out: list[str] = []
    out.append(f"Rows: {len(rows)}")
    out.append(f"Columns: {sorted(set().union(*[r.keys() for r in rows]))}")

    if cats:
        c = cats[0]
        out.append(f"Top values for '{c}':")
        freq = Counter(str(r.get(c, "")) for r in rows)
        for val, n in freq.most_common(10):
            out.append(f"- {val}: {n}")

    if numeric:
        n = numeric[0]
        vals = [r.get(n) for r in rows if isinstance(r.get(n), (int, float))]
        if vals:
            out.append(f"Numeric '{n}': min={min(vals)} max={max(vals)}")

    return "\n".join(out) + "\n"


def _vega_spec(rows: list[dict[str, Any]], *, x: str, y: str, title: str | None) -> dict[str, Any]:
    return {
        "$schema": "https://vega.github.io/schema/vega-lite/v5.json",
        "description": "Generated from Pendo Aggregation results",
        "title": title or f"{y} by {x}",
        "data": {"values": rows},
        "mark": {"type": "bar", "tooltip": True},
        "encoding": {
            "x": {"field": x, "type": "nominal", "sort": "-y"},
            "y": {"field": y, "type": "quantitative"},
        },
    }


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description="Summarize and chart Pendo Aggregation results")
    p.add_argument("path", nargs="?", help="Path to result JSON")
    p.add_argument("--stdin", action="store_true", help="Read JSON from stdin")
    p.add_argument("--summary", action="store_true", help="Print a short text summary")
    p.add_argument("--vega", action="store_true", help="Output a Vega-Lite JSON spec")
    p.add_argument("--x", help="Field name for x axis")
    p.add_argument("--y", help="Field name for y axis")
    p.add_argument("--title", help="Chart title")
    args = p.parse_args(argv)

    obj = _load_json(args.path, use_stdin=args.stdin)
    rows = _extract_rows(obj)

    if args.summary:
        sys.stdout.write(_summarize(rows))

    if args.vega:
        if not rows:
            print("error: no rows available for charting", file=sys.stderr)
            return 2

        x = args.x
        y = args.y
        if not x:
            cats = _infer_categorical_fields(rows)
            x = cats[0] if cats else None
        if not y:
            nums = _infer_numeric_fields(rows)
            y = nums[0] if nums else None

        if not x or not y:
            print("error: could not infer --x/--y; pass them explicitly", file=sys.stderr)
            return 2

        json.dump(_vega_spec(rows, x=x, y=y, title=args.title), sys.stdout, indent=2, ensure_ascii=False)
        sys.stdout.write("\n")

    if not args.summary and not args.vega:
        # Default to summary.
        sys.stdout.write(_summarize(rows))

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
