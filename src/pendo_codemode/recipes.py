"""Saved programs. A recipe is a program body that reads its inputs from `params`.

Recipes turn a program that worked into one line the next time, so the cost
of writing it is paid once and the library grows from use.
"""
from __future__ import annotations

from pathlib import Path
import re

_NAME = re.compile(r"^[a-z][A-Za-z0-9]{2,40}$")


class RecipeStore:
    def __init__(self, directory: str | Path) -> None:
        self._dir = Path(directory)

    def all(self) -> dict[str, dict[str, str]]:
        found = {}
        for path in sorted(self._dir.glob("*.js")) if self._dir.is_dir() else []:
            if not _NAME.match(path.stem):
                continue
            text = path.read_text(encoding="utf-8")
            first, _, body = text.partition("\n")
            described = first.startswith("// ")
            found[path.stem] = {"description": first[3:].strip() if described else "", "code": body if described else text}
        return found

    def code(self) -> dict[str, str]:
        return {name: entry["code"] for name, entry in self.all().items()}

    def save(self, name: str, description: str, code: str) -> None:
        if not _NAME.match(name or ""):
            raise ValueError("recipe name must be camelCase letters and digits, 3-41 characters")
        self._dir.mkdir(parents=True, exist_ok=True)
        summary = " ".join(str(description or "").split())[:200]
        (self._dir / f"{name}.js").write_text(f"// {summary}\n{code}", encoding="utf-8")

    def index(self) -> str:
        return "\n".join(f"- recipes.{name}(params)  // {entry['description']}" for name, entry in self.all().items())
