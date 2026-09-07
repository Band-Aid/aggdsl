from __future__ import annotations

from typing import Any

from tools.pendo import lookup_names


def test_fetch_entities_requests_expanded_catalogue(monkeypatch) -> None:
    captured: dict[str, Any] = {}

    class FakeResponse:
        def __enter__(self):
            return self

        def __exit__(self, *args) -> None:
            return None

        def read(self) -> bytes:
            return b'[{"id":"f1","name":"Search"}]'

    def fake_urlopen(request, timeout):
        captured["url"] = request.full_url
        captured["api_key"] = request.get_header("X-pendo-integration-key")
        captured["timeout"] = timeout
        return FakeResponse()

    monkeypatch.setenv("PENDO_API_KEY", "test-key")
    monkeypatch.setattr(lookup_names.urllib.request, "urlopen", fake_urlopen)

    assert lookup_names._fetch_entities("feature") == [{"id": "f1", "name": "Search"}]
    assert captured == {
        "url": "https://app.pendo.io/api/v1/feature?expand=%2A",
        "api_key": "test-key",
        "timeout": 30,
    }


def test_lookup_names_uses_expanded_catalogue_and_falls_back_to_id(monkeypatch) -> None:
    seen: list[str] = []

    def fake_fetch(resource: str) -> list[dict[str, Any]]:
        seen.append(resource)
        return [
            {"id": "f1", "name": "Search", "appId": 7},
            {"id": "f2", "name": "Other app", "appId": 8},
        ]

    monkeypatch.setattr(lookup_names, "_fetch_entities", fake_fetch)

    assert lookup_names.lookup_features(["f1", "f2", "missing"], app_id="7") == {
        "f1": "Search",
        "f2": "f2",
        "missing": "missing",
    }
    assert seen == ["feature"]


def test_enrichment_recurses_and_preserves_id_fields(monkeypatch) -> None:
    monkeypatch.setattr(
        lookup_names,
        "lookup_features",
        lambda ids, app_id=None: {entity_id: f"Feature {entity_id}" for entity_id in ids},
    )
    monkeypatch.setattr(
        lookup_names,
        "lookup_pages",
        lambda ids, app_id=None: {entity_id: f"Page {entity_id}" for entity_id in ids},
    )
    result = {
        "results": [
            {"featureId": "f1", "nested": {"page_id": "p1"}},
            {"feature": "f2", "page": "p2"},
        ]
    }

    enriched = lookup_names.enrich_aggregation_results(result)

    assert enriched == {
        "results": [
            {
                "featureId": "f1",
                "featureName": "Feature f1",
                "nested": {"page_id": "p1", "page_name": "Page p1"},
            },
            {
                "feature": "f2",
                "featureName": "Feature f2",
                "page": "p2",
                "pageName": "Page p2",
            },
        ]
    }
    assert result["results"][0] == {"featureId": "f1", "nested": {"page_id": "p1"}}


def test_page_lookup_still_runs_when_feature_lookup_fails(monkeypatch, capsys) -> None:
    monkeypatch.setattr(
        lookup_names,
        "lookup_features",
        lambda ids, app_id=None: (_ for _ in ()).throw(RuntimeError("feature API unavailable")),
    )
    monkeypatch.setattr(
        lookup_names,
        "lookup_pages",
        lambda ids, app_id=None: {"p1": "Home"},
    )

    enriched = lookup_names.enrich_aggregation_results({"featureId": "f1", "pageId": "p1"})

    assert enriched == {"featureId": "f1", "pageId": "p1", "pageName": "Home"}
    assert "feature API unavailable" in capsys.readouterr().err
