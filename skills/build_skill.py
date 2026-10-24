#!/usr/bin/env python3
"""Package skills/aggdsl into dist/aggdsl.zip for upload as a Claude skill.

The compiler is copied from src/aggdsl at build time (into scripts/vendor/),
so the packaged skill always checks queries with the current parser instead
of a hand-copied snapshot that drifts.
"""
from __future__ import annotations

import pathlib
import shutil

ROOT = pathlib.Path(__file__).resolve().parent.parent
SKILL = ROOT / "skills" / "aggdsl"
DIST = ROOT / "dist"


def main() -> None:
    out = DIST / "aggdsl"
    shutil.rmtree(out, ignore_errors=True)
    shutil.copytree(SKILL, out, ignore=shutil.ignore_patterns("__pycache__"))
    shutil.copytree(
        ROOT / "src" / "aggdsl",
        out / "scripts" / "vendor" / "aggdsl",
        ignore=shutil.ignore_patterns("__pycache__", "*.json"),
    )
    archive = shutil.make_archive(str(DIST / "aggdsl"), "zip", DIST, "aggdsl")
    print(archive)


if __name__ == "__main__":
    main()
