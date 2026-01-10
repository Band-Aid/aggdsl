from __future__ import annotations

import argparse
import json
import sys

from .compiler import compile_to_pendo_aggregation
from .decompiler import decompile_pendo_aggregation_to_dsl
from .parser import DslParseError, parse


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="aggdsl")
    sub = parser.add_subparsers(dest="cmd", required=True)

    compile_p = sub.add_parser("compile", help="Compile DSL to Pendo Aggregation JSON")
    compile_p.add_argument("path", help="Path to a .dsl file")
    compile_p.add_argument(
        "--now-ms",
        type=int,
        default=None,
        help="Override current time in epoch ms (useful for deterministic compilation)",
    )

    decompile_p = sub.add_parser(
        "decompile", help="Translate Pendo Aggregation JSON to aggdsl DSL"
    )
    decompile_p.add_argument("path", help="Path to a .json file")

    args = parser.parse_args(argv)

    if args.cmd == "compile":
        try:
            with open(args.path, "r", encoding="utf-8") as f:
                dsl = f.read()
            q = parse(dsl)
            body = compile_to_pendo_aggregation(q, now_ms=args.now_ms)
            json.dump(body, sys.stdout, indent=2, sort_keys=False, ensure_ascii=False)
            sys.stdout.write("\n")
            return 0
        except (OSError, DslParseError, ValueError) as e:
            print(f"error: {e}", file=sys.stderr)
            return 2

    if args.cmd == "decompile":
        try:
            with open(args.path, "r", encoding="utf-8") as f:
                body = json.load(f)
            dsl = decompile_pendo_aggregation_to_dsl(body)
            sys.stdout.write(dsl)
            return 0
        except (OSError, json.JSONDecodeError, ValueError) as e:
            print(f"error: {e}", file=sys.stderr)
            return 2

    return 1
