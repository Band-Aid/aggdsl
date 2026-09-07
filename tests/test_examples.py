from __future__ import annotations

import json
from pathlib import Path

import pytest

from aggdsl import compile_to_pendo_aggregation, parse


EXAMPLES_DIR = Path(__file__).parents[1] / "examples"
EXAMPLE_FILES = sorted(EXAMPLES_DIR.glob("*.dsl"))


def example_id(path: Path) -> str:
    return path.name


@pytest.mark.parametrize("path", EXAMPLE_FILES, ids=example_id)
def test_example_compiles(path: Path) -> None:
    query = parse(path.read_text(encoding="utf-8"))
    body = compile_to_pendo_aggregation(query)

    assert body["request"]["pipeline"]


def test_example_index_references_existing_files() -> None:
    index = json.loads((EXAMPLES_DIR / "index.json").read_text(encoding="utf-8"))

    referenced = {
        filename
        for category in index["categories"].values()
        for filename in category["examples"]
    }
    missing = sorted(filename for filename in referenced if not (EXAMPLES_DIR / filename).is_file())

    assert not missing
