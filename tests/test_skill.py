"""Keep skills/aggdsl honest: every pattern compiles, the checker's lint
rules fire, and the prefix rules SKILL.md teaches match the parser."""
from __future__ import annotations

import pathlib
import re
import subprocess
import sys

import pytest

from aggdsl import parse

SKILL = pathlib.Path(__file__).resolve().parent.parent / "skills" / "aggdsl"
CHECK = SKILL / "scripts" / "check.py"
PATTERNS = sorted((SKILL / "patterns").glob("*.dsl"))


def check(text: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run([sys.executable, str(CHECK), "-"], input=text, capture_output=True, text=True)


@pytest.mark.parametrize("path", PATTERNS, ids=lambda p: p.stem)
def test_pattern_compiles_and_only_warns_about_placeholders(path: pathlib.Path) -> None:
    result = check(path.read_text())
    assert result.returncode in (0, 1), result.stdout
    for line in result.stdout.splitlines():
        assert line == "OK" or "PLACEHOLDER" in line, line


def test_every_pattern_in_skill_table_exists() -> None:
    text = (SKILL / "SKILL.md").read_text()
    stems = {p.stem for p in PATTERNS}
    table = text.split("## Patterns", 1)[1].split("## Shape", 1)[0]
    named = set(re.findall(r"`([a-z][a-z-]+)`", table))
    assert named - stems == set()
    assert stems - named == set(), "pattern not reachable from SKILL.md"


@pytest.mark.parametrize(
    "dsl, needle",
    [
        ('FROM event([source=featureEvents, appId=-1])\n| filter featureId in ["a"]\n', "no `in`"),
        ("FROM event([source=events, appId=-1])\n| filter day > now() - 30\n", "epoch-ms"),
        ("FROM event([source=events])\n| limit 5\n", "appId"),
        ("FROM event([source=events, appId=-1])\nTIMESERIES period=dayRange first=now() count=30\n", "FUTURE"),
        ("FROM event([source=pageEvents, appId=-1])\n| eval { t=`page` }\n", "backticks"),
        ("FROM event([source=pageEvents, appId={{APP_ID}}])\n", "PLACEHOLDER"),
        ("FROM event([source=events, appId=-1])\n| group by day fields { e=sum(numEvents) }\n", "formatTime"),
    ],
)
def test_lint_rules(dsl: str, needle: str) -> None:
    result = check(dsl)
    assert result.returncode == 1
    assert needle in result.stdout


def test_compile_error_gets_fix_hint() -> None:
    result = check("FROM event([source=events, appId=-1])\n|| limit 5\n")
    assert result.returncode == 2
    assert "spawn/fork branch" in result.stdout


BRANCH = "PIPELINE\n| spawn\nbranch\nFROM event([source=pageEvents, appId=-1])\n{body}\nendbranch\n| endspawn\n"
MERGE = "FROM event([source=pages, appId=[]])\n| eval {{ pageId=id }}\nendmerge"


@pytest.mark.parametrize(
    "dsl, ok",
    [
        ("FROM event([source=events, appId=-1])\n| limit 1", True),
        ("FROM event([source=events, appId=-1])\n|| limit 1", False),
        (BRANCH.format(body="|| limit 1"), True),
        (BRANCH.format(body="| limit 1"), False),
        ("FROM event([source=pageEvents, appId=-1])\n| merge fields [pageId]\n" + MERGE.format(), True),
        ("FROM event([source=pageEvents, appId=-1])\n| merge fields [pageId]\n" + MERGE.format().replace("| eval", "|| eval"), False),
        (BRANCH.format(body="|| merge fields [pageId]\n" + MERGE.format() + "\n|| limit 1"), True),
        (BRANCH.format(body="| merge fields [pageId]\n" + MERGE.format() + "\n|| limit 1"), False),
        (BRANCH.format(body="||| limit 1"), False),
    ],
)
def test_prefix_rules(dsl: str, ok: bool) -> None:
    if ok:
        parse(dsl)
    else:
        with pytest.raises(Exception):
            parse(dsl)
