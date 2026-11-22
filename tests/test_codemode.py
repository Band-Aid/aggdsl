from __future__ import annotations

import json
import shutil
import time

import pytest

from aggdsl.codemode import CodeModeError, Limits, PendoBackend, execute_code


pytestmark = pytest.mark.skipif(not shutil.which("node"), reason="Node.js is required")


class FakeBackend:
    def __init__(self):
        self.calls = []

    def run(self, dsl: str, *, timeout_seconds: float):
        self.calls.append(dsl)
        return {"rows": [{"feature": dsl, "visitors": 4 if dsl == "before" else 9}]}


def test_two_aggregations_in_one_code_execution():
    backend = FakeBackend()
    code = """
const before = await pendo.aggdsl.run('before');
const after = await pendo.aggdsl.run('after');
return {
  before: before.rows[0].visitors,
  after: after.rows[0].visitors,
  growth: after.rows[0].visitors - before.rows[0].visitors
};
"""
    result = execute_code(code, backend=backend)
    assert result["calls"] == 2
    assert result["result"] == {"before": 4, "after": 9, "growth": 5}
    assert backend.calls == ["before", "after"]


def test_promise_all_multiple_aggregations():
    result = execute_code("""
const [a, b] = await Promise.all([
  pendo.aggdsl.run('first'), pendo.aggdsl.run('second')
]);
return [a.rows[0].feature, b.rows[0].feature];
""", backend=FakeBackend())
    assert result["calls"] == 2
    assert result["result"] == ["first", "second"]


def test_can_aggregate_locally_without_any_api_calls():
    result = execute_code("return [2, 3, 5].reduce((a, b) => a + b, 0);", backend=FakeBackend())
    assert result["result"] == 10
    assert result["calls"] == 0


def test_rejects_call_limit():
    with pytest.raises(CodeModeError, match="Maximum aggregation calls exceeded"):
        execute_code("""
for (let i = 0; i < 4; i++) await pendo.aggdsl.run('query');
return 'unreachable';
""", backend=FakeBackend(), limits=Limits(max_calls=3))


def test_rejects_large_code():
    with pytest.raises(CodeModeError, match="size limit"):
        execute_code("return '123456789';", backend=FakeBackend(), limits=Limits(max_code_bytes=4))


def test_rejects_large_result():
    with pytest.raises(CodeModeError, match="result too large"):
        execute_code("return 'a'.repeat(5000);", backend=FakeBackend(), limits=Limits(max_output_bytes=100))


def test_invalid_script_returns_error():
    with pytest.raises(CodeModeError, match="JavaScript error"):
        execute_code("throw new Error('bad query');", backend=FakeBackend())


def test_infinite_loop_is_bounded():
    start = time.monotonic()
    with pytest.raises(CodeModeError, match="JavaScript error|timed out"):
        execute_code("while (true) {}", backend=FakeBackend(), limits=Limits(timeout_seconds=2))
    assert time.monotonic() - start < 4


def test_aggregation_backend_compiles_without_network(monkeypatch):
    monkeypatch.delenv("PENDO_API_KEY", raising=False)
    monkeypatch.delenv("PENDO_INTEGRATION_KEY", raising=False)
    dsl = """
FROM event([source=featureEvents, appId=123, blacklist="apply"])
TIMESERIES period=dayRange first=now() count=-7
| group by featureId fields { events=sum(numEvents) }
| limit 3
"""
    compiled = PendoBackend(dry_run=True).run(dsl, timeout_seconds=1)
    assert compiled["executed"] is False
    assert compiled["compiled"]["request"]["pipeline"]
    assert compiled["compiled"]["response"]["mimeType"] == "application/json"


def test_requires_json_response_type():
    dsl = """
RESPONSE mimeType=text/csv
FROM event([source=featureEvents, appId=123, blacklist="apply"])
TIMESERIES period=dayRange first=now() count=-7
| limit 3
"""
    with pytest.raises(CodeModeError, match="requires RESPONSE"):
        PendoBackend(dry_run=True).run(dsl, timeout_seconds=1)
