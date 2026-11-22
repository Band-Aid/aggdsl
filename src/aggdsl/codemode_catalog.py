"""Read-only Pendo catalog discovery and just-in-time aggDSL guidance.

Only documented Pendo v1 GET endpoints are used. The API key stays on the
Python side of the Code Mode bridge and never enters JavaScript.
"""
from __future__ import annotations

import json
import os
from pathlib import Path
import re
import time
import urllib.error
import urllib.parse
import urllib.request
from typing import Any, Callable

from aggdsl import compile_to_pendo_aggregation, parse


class DiscoveryError(ValueError):
    """Invalid discovery arguments or a failed Pendo catalog request."""


# Supported and documented read-only Pendo API resources. Never interpolate
# arbitrary user strings into a URL path.
_RESOURCES = {
    "pages": "page",
    "features": "feature",
    "trackTypes": "tracktype",
    "guides": "guide",
}
_GUIDE_NAMES = {
    "workflow": (
        "Discover with pendo.catalog.search({type:'features',query:'checkout'}) "
        "or pendo.catalog.apps(); inspect pendo.help('syntax'|'sources'|'examples'); "
        "call pendo.aggdsl.validate(dsl) and correct errors; then "
        "await pendo.aggdsl.run(dsl). Return a small summary, not raw visitor data. "
        "Only actual returned entity IDs should be used in queries."
    ),
    "syntax": (
        "aggDSL structure: FROM event([source=featureEvents,appId=123,blacklist=\"apply\"])\n"
        "TIMESERIES period=dayRange first=now() count=-30\n"
        "| filter <expression>\n"
        "| group by featureId fields { clicks=sum(numEvents), visitors=count(visitorId) }\n"
        "| sort -clicks\n"
        "| limit 10\n"
        "Use PIPELINE instead of FROM for stages with no direct event source. "
        "Quoted values are constants. count=-30 means the past 30 days. "
        "Compilation only checks syntax, NOT whether a Pendo field or app ID exists."
    ),
    "sources": (
        "Common event sources: featureEvents (featureId), pageEvents (pageId), "
        "trackEvents (trackTypeId), guideEvents (guideId), pollEvents, "
        "events (often requires appId), singleEvents, recordingMetadata. "
        "Use catalog.search to resolve entity IDs. App IDs can be observed through "
        "catalog.apps(); it is NOT a comprehensive application inventory. "
        "Metadata/event-property field availability cannot be inferred reliably "
        "from this catalog, so do not invent field names."
    ),
    "examples": (
        "Top 10 features for an observed app (replace ID):\n"
        "FROM event([source=featureEvents,appId=123,blacklist=\"apply\"])\n"
        "TIMESERIES period=dayRange first=now() count=-30\n"
        "| group by featureId fields { clicks=sum(numEvents) }\n"
        "| sort -clicks\n"
        "| limit 10\n"
        "Read-only JavaScript orchestration:\n"
        "const x = await pendo.aggdsl.run(dsl); return x;\n"
        "Use pendo.aggdsl.validate(dsl) to inspect parser errors before executing."
    ),
    "troubleshooting": (
        "If compile fails, check FROM/PIPELINE, TIMESERIES placement, source names, "
        "quotation, required appId, operator spelling, and the group syntax. "
        "A successful compile does not prove that a field exists or that data is present. "
        "If an API request fails, inspect its error message, revise the DSL and "
        "revalidate; do not silently use invented values. For multi-app "
        "aggregation, Pendo documents appId=expandAppIds(\"*\"). "
        "API read latency is not improved by Code Mode itself."
    ),
}


def help_topic(topic: str = "workflow") -> dict[str, str]:
    """Load only the requested slice of the repo's DSL knowledge.

    Grammar and patterns are shipped with the installed wheel so the agent
    need not fetch unrelated docs or rely on pre-trained syntax guesses.
    """
    if not isinstance(topic, str):
        raise DiscoveryError("Help topic must be a string")
    root = Path(__file__).resolve().parent
    source_skills = root.parents[1] / "skills" / "aggdsl"
    if topic == "grammar":
        file = root / "codemode_grammar.md"
        if not file.is_file():
            file = source_skills / "references" / "grammar.md"
        if not file.is_file():
            raise DiscoveryError("Grammar reference not bundled in this installation.")
        return {"topic": topic, "text": file.read_text(encoding="utf-8")[:10_000]}
    patterns_dir = root / "codemode_patterns"
    if not patterns_dir.is_dir():
        patterns_dir = source_skills / "patterns"
    if topic == "patterns":
        names = sorted(p.stem for p in patterns_dir.glob("*.dsl")) if patterns_dir.is_dir() else []
        return {"topic": topic, "text": "Available patterns: " + ", ".join(names)}
    if topic.startswith("pattern:"):
        name = topic.removeprefix("pattern:")
        if not re.fullmatch(r"[a-z][a-z0-9-]{1,60}", name):
            raise DiscoveryError("Invalid pattern name")
        file = patterns_dir / (name + ".dsl")
        if not file.is_file():
            raise DiscoveryError("Unknown pattern. Call pendo.help('patterns') first.")
        return {"topic": topic, "text": file.read_text(encoding="utf-8")[:10_000]}
    if topic not in _GUIDE_NAMES:
        raise DiscoveryError(
            "Unknown help topic; choose "
            + ", ".join(_GUIDE_NAMES)
            + ", grammar, patterns or pattern:<name>"
        )
    return {"topic": topic, "text": _GUIDE_NAMES[topic]}


def validate_dsl(dsl: str, *, include_body: bool = False) -> dict[str, Any]:
    """Compile and report actionable diagnostics without contacting Pendo."""
    if not isinstance(dsl, str) or not dsl.strip() or len(dsl.encode("utf-8")) > 20_000:
        return {"valid": False, "errors": ["Provide a non-empty aggDSL query of at most 20 KB."]}
    try:
        body = compile_to_pendo_aggregation(parse(dsl), now_ms=int(time.time() * 1000))
    except Exception as exc:
        return {
            "valid": False,
            "errors": [str(exc)[:500]],
            "hint": "Check pendo.help('syntax') and pendo.help('examples').",
        }
    if body.get("response", {}).get("mimeType") != "application/json":
        return {"valid": False, "errors": ["Code Mode requires RESPONSE mimeType=application/json."]}
    stages = body.get("request", {}).get("pipeline")
    if not isinstance(stages, list) or not stages:
        return {"valid": False, "errors": ["A nonempty aggregation pipeline is required."]}
    warnings: list[str] = []
    if "{{" in dsl and "}}" in dsl:
        warnings.append("Unresolved {{PLACEHOLDER}}. Supply a real ID.")
    if re.search(r"\bsource=(events|singleEvents|recordingMetadata|agenticEvents)\b", dsl) and "appId" not in dsl:
        warnings.append("This event source usually requires appId in its source arguments.")
    if re.search(r"\bfirst=now\(\)\s+count=[1-9]\d*", dsl):
        warnings.append("Positive count from now() requests a future window; use a negative count for history.")
    result: dict[str, Any] = {
        "valid": True,
        "warnings": warnings,
        "stages": len(stages),
        "note": "Compile-only validation; Pendo field names and data availability are not verified.",
    }
    if include_body:
        encoded = json.dumps(body, ensure_ascii=False)
        if len(encoded.encode("utf-8")) > 32_000:
            return {"valid": False, "errors": ["Compiled query is too large to inspect."]}
        result["compiled"] = body
    return result


def _base_url() -> str:
    base = os.environ.get("PENDO_API_BASE_URL", "https://app.pendo.io").rstrip("/")
    parsed = urllib.parse.urlsplit(base)
    # Public Pendo hosts only; no arbitrary URLs, userinfo, paths or local hosts.
    if (parsed.scheme != "https" or not parsed.hostname or
            not parsed.hostname.endswith(".pendo.io") or
            parsed.username is not None or parsed.password is not None or
            parsed.path or parsed.query or parsed.fragment or parsed.port is not None):
        raise DiscoveryError("PENDO_API_BASE_URL must be an HTTPS Pendo hostname.")
    return base



class NoRedirect(urllib.request.HTTPRedirectHandler):
    """Do not follow redirects with the subscription-wide integration key."""

    def redirect_request(self, req: Any, fp: Any, code: int, msg: str,
                         headers: Any, newurl: str) -> None:
        return None


class PendoCatalog:
    """Search a limited catalog of non-personal Pendo definitions.

    Application IDs returned by apps() are observed on tagged entities, not
    guaranteed to represent every application in a subscription.
    """

    def __init__(
        self,
        *,
        transport: Callable[[str, float, int], Any] | None = None,
        max_response_bytes: int = 2_000_000,
    ) -> None:
        self.transport = transport
        self.max_response_bytes = max_response_bytes
        self._cache: dict[tuple[str, str | None], tuple[float, list[dict[str, Any]]]] = {}

    def _fetch(self, kind: str, app_id: str | None, timeout_seconds: float) -> list[dict[str, Any]]:
        if kind not in _RESOURCES:
            raise DiscoveryError("Unsupported catalog type. Use " + ", ".join(_RESOURCES))
        cache_key = kind, app_id
        previous = self._cache.get(cache_key)
        if previous and time.monotonic() - previous[0] < 90:
            return previous[1]
        parameters = {"appId": app_id} if app_id is not None else {"expand": "*"}
        if kind == "guides":
            parameters["summarizeContent"] = "true"
        url = _base_url() + "/api/v1/" + _RESOURCES[kind] + "?" + urllib.parse.urlencode(parameters)
        if self.transport is not None:
            value = self.transport(url, min(timeout_seconds, 12.0), self.max_response_bytes)
        else:
            key = os.environ.get("PENDO_INTEGRATION_KEY") or os.environ.get("PENDO_API_KEY")
            if not key:
                raise DiscoveryError("Set PENDO_INTEGRATION_KEY on the Python host for discovery.")
            request = urllib.request.Request(
                url, method="GET",
                headers={"Accept": "application/json", "x-pendo-integration-key": key},
            )
            try:
                with urllib.request.build_opener(NoRedirect()).open(request, timeout=max(0.1, min(timeout_seconds, 12.0))) as response:
                    raw = response.read(self.max_response_bytes + 1)
                if len(raw) > self.max_response_bytes:
                    raise DiscoveryError("Catalog too large; use a specific appId or a higher server cap.")
                value = json.loads(raw)
            except urllib.error.HTTPError as exc:
                raise DiscoveryError(f"Pendo catalog HTTP {exc.code} for {kind}.") from exc
            except urllib.error.URLError as exc:
                raise DiscoveryError(f"Pendo catalog connection failed for {kind}.") from exc
            except (ValueError, UnicodeDecodeError) as exc:
                raise DiscoveryError(f"Pendo catalog returned invalid JSON for {kind}.") from exc
        if not isinstance(value, list):
            raise DiscoveryError(f"Unexpected Pendo {kind} catalog shape: expected a list.")
        result = [item for item in value if isinstance(item, dict)]
        self._cache[cache_key] = time.monotonic(), result
        return result

    @staticmethod
    def _summary(item: dict[str, Any], kind: str) -> dict[str, Any]:
        guidelike = kind == "guides"
        item_id = item.get("guideId", item.get("id")) if guidelike else item.get("id")
        name = item.get("guideName", item.get("name")) if guidelike else item.get("name")
        app_ids = item.get("appIds") if guidelike else None
        result: dict[str, Any] = {
            "id": str(item_id) if item_id is not None else None,
            "name": str(name)[:200] if name is not None else None,
        }
        if item.get("appId") is not None:
            result["appId"] = str(item["appId"])
        if isinstance(app_ids, list):
            result["appIds"] = [str(v) for v in app_ids[:20]]
        description = item.get("guideDescription") if guidelike else item.get("description")
        if isinstance(description, str) and description:
            result["description"] = description[:240]
        if kind == "features" and item.get("pageId") is not None:
            result["pageId"] = str(item["pageId"])
        if kind == "trackTypes" and isinstance(item.get("eventPropertyConfigurations"), list):
            # Discovery of declared property names only, never event payloads.
            result["eventProperties"] = [
                str(prop.get("name"))[:80]
                for prop in item["eventPropertyConfigurations"][:20]
                if isinstance(prop, dict) and prop.get("name")
            ]
        return result

    def search(
        self, *,
        kind: str, query: str = "", app_id: str | None = None,
        limit: int = 20, timeout_seconds: float = 10.0,
    ) -> dict[str, Any]:
        if not isinstance(query, str) or len(query) > 120:
            raise DiscoveryError("Catalog query must be a string of at most 120 characters.")
        if app_id is not None and (not isinstance(app_id, str) or
                                   len(app_id) > 50 or not re.fullmatch(r"-?\d+", app_id)):
            raise DiscoveryError("appId must be a numeric Pendo application ID string.")
        if type(limit) is not int or not 1 <= limit <= 50:
            raise DiscoveryError("Catalog limit must be 1-50.")
        rows = self._fetch(kind, app_id, timeout_seconds)
        summaries = (self._summary(row, kind) for row in rows)
        needle = query.casefold().strip()
        matched = [
            entry for entry in summaries if
            not needle or needle in (entry.get("name") or "").casefold() or
            needle in (entry.get("id") or "").casefold() or
            needle in (entry.get("description") or "").casefold()
        ]
        return {
            "type": kind, "scope": app_id if app_id is not None else "all-visible-apps",
            "items": matched[:limit], "totalMatches": len(matched),
            "truncated": len(matched) > limit,
        }

    def apps(self, *, timeout_seconds: float = 10.0) -> dict[str, Any]:
        """Derive App IDs from tagged resources; does not promise an exhaustive app list."""
        counts: dict[str, dict[str, int]] = {}
        for kind in ("pages", "features", "trackTypes"):
            rows = self._fetch(kind, None, timeout_seconds)
            for row in rows:
                if row.get("appId") is None:
                    continue
                app_id = str(row["appId"])
                counts.setdefault(app_id, {"pages": 0, "features": 0, "trackTypes": 0})[kind] += 1
        return {
            "apps": [
                {"appId": app_id, "name": None, "observedEntities": counts[app_id]}
                for app_id in sorted(counts)
            ],
            "completeness": "observed-on-tagged-entities-only",
            "note": "Apps with no returned pages/features/track types are not listed. Names require an external app inventory.",
        }
