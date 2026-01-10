from __future__ import annotations

import os
from pathlib import Path


def load_dotenv(path: str | os.PathLike[str] = ".env") -> bool:
    """Load KEY=VALUE pairs from a .env-like file into os.environ.

    - Does not override existing environment variables.
    - Supports simple quoted values using single or double quotes.
    - Ignores blank lines and lines starting with '#'.

    Returns True if a file was found and parsed.
    """
    p = Path(path)
    if not p.exists() or not p.is_file():
        return False

    for raw in p.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        if "=" not in line:
            continue
        k, v = line.split("=", 1)
        key = k.strip()
        val = v.strip()
        if not key:
            continue
        if (val.startswith('"') and val.endswith('"')) or (val.startswith("'") and val.endswith("'")):
            val = val[1:-1]
        os.environ.setdefault(key, val)

    return True
