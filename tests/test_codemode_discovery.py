"""Code Mode discovery and orchestration regression tests.

Mocks represent the documented Pendo endpoints; these tests never reach Pendo.
"""
from __future__ import annotations

import json
import shutil
import threading
import time
from urllib.parse import parse_qs, urlsplit

import pytest

from aggdsl.codemode import CodeModeError, Limits, execute_code
from aggdsl.codemode_catalog import (
    DiscoveryError, PendoCatalog, help_topic, validate_dsl,
)

VALID_DSL = """
FROM event([source=featureEvents, appId=123456, blacklist="apply"])
TIMESERIES period=dayRange first=now() count=-7
| group by featureId fields { clicks=sum(numEvents) }
| sort -clicks
| limit 10
"""


def test_validate_new_dsl_compiles_without_catalog_or_pendo_key():
    response = validate_dsl(VALID_DSL, include_body=True)
    assert response["valid"] is True
    assert response["compiled"]["request"]["pipeline"]
    assert response["note"].startswith("Compile-only")


def test_invalid_dsl_returns_actionable_diagnostics_not_false_success():
    response = validate_dsl("THIS IS NOT AGGDSL")
    assert response["valid"] is False
    assert response["errors"]


def test_dsl_warns_on_future_and_unresolved_placeholder():
    response = validate_dsl("""
FROM event([source=featureEvents, appId={{APP_ID}}])
TIMESERIES period=dayRange first=now() count=7
| limit 5
""")
    # The parser may reject the unresolved placeholder outright, which is also
    # valid feedback; it must never be claimed successfully executable.
    assert not response["valid"] or response["warnings"]


def test_help_topics_are_small_and_specific():
    assert "catalog.search" in help_topic("workflow")["text"]
    assert "TIMESERIES" in help_topic("syntax")["text"]
    with pytest.raises(DiscoveryError, match="Unknown help topic"):
        help_topic("imaginary")


def test_catalog_search_filters_and_returns_grounded_fields(monkeypatch):
    observed = []

    def transport(url, timeout, cap):
        observed.append((url, timeout, cap))
        return [
            {"id": "F-1", "name": "Checkout Button", "appId": 123456,
             "pageId": "P1", "irrelevantSecret": "do-not-return"},
            {"id": "F-2", "name": "Account Settings", "appId": 123456},
        ]

    catalog = PendoCatalog(transport=transport)
    result = catalog.search(kind="features", query="checkout", limit=1)
    assert result["totalMatches"] == 1
    assert result["items"] == [{
        "id": "F-1", "name": "Checkout Button", "appId": "123456", "pageId": "P1"
    }]
    parsed = urlsplit(observed[0][0])
    assert parsed.path == "/api/v1/feature"
    assert parse_qs(parsed.query) == {"expand": ["*"]}
    # Repeat a search without re-downloading the full same resource.
    catalog.search(kind="features", query="settings")
    assert len(observed) == 1


def test_catalog_scopes_to_known_app_and_does_not_guess_ids():
    urls = []

    def transport(url, timeout, cap):
        urls.append(url)
        return [{"id": "P-01", "name": "Billing Page", "appId": 88}]

    catalog = PendoCatalog(transport=transport)
    response = catalog.search(kind="pages", query="billing", app_id="88")
    assert response["items"][0]["id"] == "P-01"
    assert parse_qs(urlsplit(urls[0]).query) == {"appId": ["88"]}
    with pytest.raises(DiscoveryError, match="appId"):
        catalog.search(kind="pages", app_id="../../admin")
    with pytest.raises(DiscoveryError, match="Unsupported"):
        catalog.search(kind="unsafe_endpoint", query="whatever")


def test_guide_discovery_returns_no_guide_content():
    def transport(url, timeout, cap):
        assert "summarizeContent=true" in url
        return [{
            "guideId": "GUIDE-01", "guideName": "Welcome Guide", "appIds": [7],
            "guideContent": "<secret>private content</secret>",
        }]

    item = PendoCatalog(transport=transport).search(kind="guides", query="welcome")["items"][0]
    assert item["id"] == "GUIDE-01"
    assert item["appIds"] == ["7"]
    assert "guideContent" not in item


def test_apps_returns_observed_only_not_fake_full_inventory():
    def transport(url, timeout, cap):
        path = urlsplit(url).path
        if path.endswith("/page"):
            return [{"id": "P-1", "appId": 4}]
        if path.endswith("/feature"):
            return [{"id": "F-1", "appId": 4}, {"id": "F-2", "appId": 6}]
        if path.endswith("/tracktype"):
            return [{"id": "T-1", "appId": 6}]
        raise AssertionError(path)

    found = PendoCatalog(transport=transport).apps()
    assert [app["appId"] for app in found["apps"]] == ["4", "6"]
    assert found["apps"][0]["name"] is None
    assert found["completeness"] == "observed-on-tagged-entities-only"


def test_rejects_non_pendo_catalog_host(monkeypatch):
    monkeypatch.setenv("PENDO_API_BASE_URL", "https://evil.example.com")
    with pytest.raises(DiscoveryError, match="HTTPS Pendo"):
        PendoCatalog(transport=lambda *_: []).search(kind="pages", query="test")


class FakeBackend:
    def __init__(self):
        self.dsl = []

    def run(self, dsl, *, timeout_seconds):
        self.dsl.append(dsl)
        return {"rows": [{"feature": "checkout", "total": 12}]}


class FakeCatalog:
    def __init__(self):
        self.queries = []

    def search(self, *, kind, query, app_id, limit, timeout_seconds):
        self.queries.append((kind, query, app_id, limit))
        return {"items": [{"id": "FEATURE-123", "name": "Checkout", "appId": "123456"}]}

    def apps(self, *, timeout_seconds):
        return {"apps": [{"appId": "123456", "name": None}],
                "completeness": "observed-on-tagged-entities-only"}


@pytest.mark.skipif(not shutil.which("node"), reason="Node.js required")
def test_ai_can_discover_validate_and_execute_unknown_query_in_one_call():
    code = """
const discovered = await pendo.catalog.search({type:'features',query:'checkout',limit:5});
const id = discovered.items[0].id;
const appId = Number(discovered.items[0].appId);
const dsl = \`
FROM event([source=featureEvents, appId=\${appId}, featureId="\${id}", blacklist="apply"])
TIMESERIES period=dayRange first=now() count=-7
| group by featureId fields { clicks=sum(numEvents) }
| limit 5
\`;
const checked = await pendo.aggdsl.validate(dsl);
if (!checked.valid) throw new Error(JSON.stringify(checked));
const data = await pendo.aggdsl.run(dsl);
return {feature:discovered.items[0].name, total:data.rows[0].total, valid:checked.valid};
"""
    backend = FakeBackend()
    catalog = FakeCatalog()
    result = execute_code(code, backend=backend, catalog=catalog)
    assert result["calls"] == 3
    assert result["aggregation_calls"] == 1
    assert result["discovery_calls"] == 1
    assert result["result"] == {"feature": "Checkout", "total": 12, "valid": True}
    assert 'featureId="FEATURE-123"' in backend.dsl[0]


@pytest.mark.skipif(not shutil.which("node"), reason="Node.js required")
def test_catalog_apps_and_help_are_composable_in_one_call():
    result = execute_code("""
const [a, h] = await Promise.all([pendo.catalog.apps(), pendo.help('workflow')]);
return {apps:a.apps.length, hasDocs:h.text.includes('catalog.search')};
""", backend=FakeBackend(), catalog=FakeCatalog())
    assert result["result"] == {"apps": 1, "hasDocs": True}
    assert result["calls"] == 2


@pytest.mark.skipif(not shutil.which("node"), reason="Node.js required")
def test_independent_aggregations_overlap_in_time():
    class CoordinatedBackend:
        def __init__(self):
            self.gate = threading.Barrier(2, timeout=3)

        def run(self, dsl, *, timeout_seconds):
            self.gate.wait()
            return {"which": dsl}

    result = execute_code("""
const x = await Promise.all([pendo.aggdsl.run('A'),pendo.aggdsl.run('B')]);
return x.map(v => v.which);
""", backend=CoordinatedBackend(), limits=Limits(timeout_seconds=6))
    assert result["result"] == ["A", "B"]
    assert result["aggregation_calls"] == 2


@pytest.mark.skipif(not shutil.which("node"), reason="Node.js required")
def test_discovery_budget_is_enforced():
    code = """
for (let i = 0; i < 3; i++) await pendo.catalog.apps();
return 1;
"""
    with pytest.raises(CodeModeError, match="Maximum catalog calls exceeded"):
        execute_code(code, backend=FakeBackend(), catalog=FakeCatalog(),
                     limits=Limits(max_discovery_calls=2))


@pytest.mark.skipif(not shutil.which("node"), reason="Node.js required")
def test_unawaited_pendo_call_is_rejected():
    with pytest.raises(CodeModeError, match="Await all pending"):
        execute_code("pendo.aggdsl.run('A'); return 1;", backend=FakeBackend())
