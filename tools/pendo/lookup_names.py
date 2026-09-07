#!/usr/bin/env python3
"""Enrich Pendo aggregation results with feature and page names."""
from __future__ import annotations

import argparse
import json
import os
import sys
import urllib.error
import urllib.parse
import urllib.request
from typing import Any


FEATURE_ID_FIELDS = {
    "featureId": "featureName",
    "feature_id": "feature_name",
    "feature": "featureName",
}
PAGE_ID_FIELDS = {
    "pageId": "pageName",
    "page_id": "page_name",
    "page": "pageName",
}


def get_api_key() -> str:
    """Return the configured Pendo integration key."""
    api_key = os.environ.get("PENDO_API_KEY") or os.environ.get("PENDO_INTEGRATION_KEY")
    if not api_key:
        raise ValueError("PENDO_API_KEY or PENDO_INTEGRATION_KEY environment variable not set")
    return api_key


def _fetch_entities(resource: str) -> list[dict[str, Any]]:
    """Fetch one entity catalogue across every app in the subscription."""
    base_url = os.environ.get("PENDO_API_BASE_URL", "https://app.pendo.io").rstrip("/")
    query = urllib.parse.urlencode({"expand": "*"})
    request = urllib.request.Request(
        f"{base_url}/api/v1/{resource}?{query}",
        headers={
            "Accept": "application/json",
            "x-pendo-integration-key": get_api_key(),
        },
    )
    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            payload = json.loads(response.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"Pendo {resource} lookup failed ({exc.code}): {detail}") from exc

    if not isinstance(payload, list):
        raise ValueError(f"Unexpected Pendo {resource} response: expected a list")
    return [item for item in payload if isinstance(item, dict)]


def _lookup_names(
    resource: str,
    entity_ids: list[str],
    app_id: str | None = None,
) -> dict[str, str]:
    if not entity_ids:
        return {}

    wanted = set(entity_ids)
    names: dict[str, str] = {}
    for entity in _fetch_entities(resource):
        entity_id = entity.get("id")
        if entity_id not in wanted:
            continue
        # `expand=*` is essential for multi-app subscriptions. When the caller
        # supplies an app, narrow the expanded catalogue locally as well.
        entity_app_id = entity.get("appId")
        if app_id is not None and str(entity_app_id) != str(app_id):
            continue
        names[str(entity_id)] = str(entity.get("name") or entity_id)

    # Keep aggregation rows usable even when an ID was deleted or inaccessible.
    return {entity_id: names.get(entity_id, entity_id) for entity_id in entity_ids}


def lookup_features(feature_ids: list[str], app_id: str | None = None) -> dict[str, str]:
    """Map feature IDs to names, falling back to the ID when not found."""
    return _lookup_names("feature", feature_ids, app_id)


def lookup_pages(page_ids: list[str], app_id: str | None = None) -> dict[str, str]:
    """Map page IDs to names, falling back to the ID when not found."""
    return _lookup_names("page", page_ids, app_id)


def _collect_ids(value: Any, feature_ids: set[str], page_ids: set[str]) -> None:
    if isinstance(value, dict):
        for key, child in value.items():
            if key in FEATURE_ID_FIELDS and child is not None:
                feature_ids.add(str(child))
            elif key in PAGE_ID_FIELDS and child is not None:
                page_ids.add(str(child))
            else:
                _collect_ids(child, feature_ids, page_ids)
    elif isinstance(value, list):
        for child in value:
            _collect_ids(child, feature_ids, page_ids)


def _add_names(
    value: Any,
    feature_names: dict[str, str],
    page_names: dict[str, str],
) -> Any:
    if isinstance(value, list):
        return [_add_names(child, feature_names, page_names) for child in value]
    if not isinstance(value, dict):
        return value

    enriched: dict[str, Any] = {}
    for key, child in value.items():
        enriched[key] = _add_names(child, feature_names, page_names)
        if key in FEATURE_ID_FIELDS and child is not None:
            entity_id = str(child)
            if entity_id in feature_names:
                enriched[FEATURE_ID_FIELDS[key]] = feature_names[entity_id]
        elif key in PAGE_ID_FIELDS and child is not None:
            entity_id = str(child)
            if entity_id in page_names:
                enriched[PAGE_ID_FIELDS[key]] = page_names[entity_id]
    return enriched


def enrich_aggregation_results(data: Any, app_id: str | None = None) -> Any:
    """Return a copy of an aggregation response enriched with entity names."""
    feature_ids: set[str] = set()
    page_ids: set[str] = set()
    _collect_ids(data, feature_ids, page_ids)

    feature_names: dict[str, str] = {}
    page_names: dict[str, str] = {}
    if feature_ids:
        try:
            feature_names = lookup_features(sorted(feature_ids), app_id)
        except Exception as exc:
            print(f"Warning: failed to look up feature names: {exc}", file=sys.stderr)
    if page_ids:
        try:
            page_names = lookup_pages(sorted(page_ids), app_id)
        except Exception as exc:
            print(f"Warning: failed to look up page names: {exc}", file=sys.stderr)

    return _add_names(data, feature_names, page_names)


def _load_json(path: str) -> Any:
    if path == "-":
        return json.load(sys.stdin)
    with open(path, "r", encoding="utf-8") as handle:
        return json.load(handle)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Add feature and page names to a Pendo aggregation response",
    )
    parser.add_argument("path", help="Aggregation result JSON file, or - for stdin")
    parser.add_argument("--app-id", help="Optionally restrict matches to one Pendo app")
    args = parser.parse_args(argv)

    try:
        enriched = enrich_aggregation_results(_load_json(args.path), args.app_id)
    except (OSError, ValueError, json.JSONDecodeError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2

    json.dump(enriched, sys.stdout, indent=2, ensure_ascii=False)
    sys.stdout.write("\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
