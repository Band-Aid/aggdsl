from __future__ import annotations

import argparse
import json
import sys
import time
from typing import Any

from aggdsl import compile_to_pendo_aggregation, parse


def compile_dsl_text(dsl: str, *, resolve_now: bool) -> dict[str, Any]:
    q = parse(dsl)
    now_ms = int(time.time() * 1000) if resolve_now else None
    return compile_to_pendo_aggregation(q, now_ms=now_ms)


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description="Compile aggdsl DSL to a Pendo Aggregation JSON body")
    p.add_argument("path", nargs="?", help="Path to .dsl file")
    p.add_argument("--stdin", action="store_true", help="Read DSL from stdin")
    p.add_argument(
        "--keep-now",
        action="store_true",
        help="Do not resolve now() to epoch-ms (leave as 'now()' in output)",
    )
    p.add_argument("--pretty", action="store_true", help="Pretty-print JSON")
    args = p.parse_args(argv)

    if not args.stdin and not args.path:
        print("error: provide a DSL file or --stdin", file=sys.stderr)
        return 2

    try:
        if args.stdin:
            dsl = sys.stdin.read()
        else:
            with open(args.path, "r", encoding="utf-8") as f:
                dsl = f.read()

        body = compile_dsl_text(dsl, resolve_now=not args.keep_now)

        if args.pretty:
            json.dump(body, sys.stdout, indent=2, ensure_ascii=False)
        else:
            json.dump(body, sys.stdout, ensure_ascii=False)
        sys.stdout.write("\n")
        return 0
    except Exception as e:
        print(f"error: {e}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
