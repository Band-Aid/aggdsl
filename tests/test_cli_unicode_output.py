import os
import subprocess
import sys
from pathlib import Path


def test_cli_outputs_unicode_unescaped(tmp_path: Path) -> None:
    dsl = (
        'RESPONSE mimeType=application/json\n'
        'REQUEST name="UnicodeKeys"\n'
        'PIPELINE\n'
        '| select { "ID"=visitorId, "コード"=accountId }\n'
    )

    dsl_path = tmp_path / "unicode.dsl"
    dsl_path.write_text(dsl, encoding="utf-8")

    repo_root = Path(__file__).resolve().parents[1]
    env = dict(os.environ)
    env["PYTHONUTF8"] = "1"
    env["PYTHONPATH"] = str(repo_root / "src")

    res = subprocess.run(
        [sys.executable, "-m", "aggdsl", "compile", str(dsl_path)],
        cwd=str(repo_root),
        env=env,
        check=True,
        capture_output=True,
        text=True,
        encoding="utf-8",
    )

    assert "ID" in res.stdout
    assert "\\u30b3\\u30fc\\u30c9" not in res.stdout
