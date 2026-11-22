"""A stand-in Pendo MCP server with synthetic data, for tests and offline demos.

Tool names follow the public Pendo MCP tool list, but the argument and result
shapes here are SIMPLIFIED INVENTIONS, not Pendo's real schemas. Against the
real server, Code Mode reads the real schemas from `tools/list`.

Run as an MCP stdio server:  python -m pendo_codemode.mock_pendo
"""
from __future__ import annotations

import hashlib
from typing import Any

from .mcp_wire import serve_stdio, text_result

APPS = [
    {"id": "-323232", "name": "Acme Web", "platform": "web"},
    {"id": "5551001", "name": "Acme Mobile", "platform": "ios"},
]
_AREAS = ["Onboarding", "Reporting", "Collaboration", "Admin"]
_FEATURE_NAMES = [
    "Create Dashboard", "Export CSV", "Invite Teammate", "Share Link", "Filter Results", "Save View",
    "Open Settings", "Add Widget", "Schedule Report", "Search", "Bulk Edit", "Connect Integration",
]
_PLANS = ["free", "team", "enterprise"]


def _n(*parts: Any, mod: int) -> int:
    """Deterministic pseudo-random integer in [0, mod)."""
    digest = hashlib.sha256("|".join(map(str, parts)).encode()).digest()
    return int.from_bytes(digest[:4], "big") % mod


FEATURES = [
    {
        "id": f"feat-{app['id'].lstrip('-')[:3]}-{index:02d}",
        "name": name,
        "appId": app["id"],
        "type": "Feature",
        "productArea": _AREAS[index % len(_AREAS)],
        "description": f"Synthetic feature '{name}' in {app['name']}; tagged for demo purposes only.",
    }
    for app in APPS
    for index, name in enumerate(_FEATURE_NAMES)
]
ACCOUNTS = [
    {
        "accountId": f"acct-{index:03d}",
        "name": f"Example Co {index:03d}",
        "plan": _PLANS[_n("plan", index, mod=3)],
        "arr": 5000 * (1 + _n("arr", index, mod=40)),
        "csm": f"csm-{_n('csm', index, mod=5)}",
    }
    for index in range(40)
]
SEGMENTS = [{"id": f"seg-{i}", "name": name} for i, name in enumerate(["Paying", "Trial", "Power users"])]


def _activity(args: dict[str, Any]) -> dict[str, Any]:
    app_id = args.get("appId")
    wanted = set(args.get("entityIds") or [])
    group_by = args.get("groupBy") or ["entity"]
    days = max(1, min(int(args.get("days", 30)), 180))
    features = [f for f in FEATURES if f["appId"] == app_id and (not wanted or f["id"] in wanted)]
    totals: dict[tuple, dict[str, Any]] = {}
    for feature in features:
        for account in ACCOUNTS:
            if _n("uses", feature["id"], account["accountId"], mod=4) == 0:
                continue  # this account does not use this feature
            events = days * (1 + _n("ev", feature["id"], account["accountId"], mod=20))
            visitors = 1 + _n("vis", feature["id"], account["accountId"], mod=12)
            key_fields = {}
            if "entity" in group_by:
                key_fields["featureId"] = feature["id"]
            if "account" in group_by:
                key_fields["accountId"] = account["accountId"]
            row = totals.setdefault(tuple(key_fields.items()), {**key_fields, "numEvents": 0, "uniqueVisitors": 0})
            row["numEvents"] += events
            row["uniqueVisitors"] += visitors
    rows = sorted(totals.values(), key=lambda r: -r["numEvents"])
    limit = args.get("limit")
    return {"appId": app_id, "days": days, "rows": rows[: int(limit)] if limit else rows}


def _search(args: dict[str, Any]) -> dict[str, Any]:
    needle = str(args.get("substring") or "").lower()
    app_id = args.get("appId")
    found = [f for f in FEATURES if (not app_id or f["appId"] == app_id) and needle in f["name"].lower()]
    return {"entities": found}


def _accounts(args: dict[str, Any]) -> dict[str, Any]:
    wanted = set(args.get("accountIds") or [])
    found = [a for a in ACCOUNTS if not wanted or a["accountId"] in wanted]
    return {"accounts": found[: int(args.get("limit") or len(found))]}


def _obj(properties: dict[str, Any], required: list[str] | None = None) -> dict[str, Any]:
    return {"type": "object", "properties": properties, "required": required or []}


_APP = {"type": "string", "description": "Application ID from list_all_applications"}
_READ = {"readOnlyHint": True}

TOOLS: list[tuple[dict[str, Any], Any]] = [
    (
        {
            "name": "list_all_applications",
            "description": "List the applications in the Pendo subscription.",
            "inputSchema": _obj({}),
            "annotations": _READ,
        },
        lambda args: {"applications": APPS},
    ),
    (
        {
            "name": "searchEntities",
            "description": "Search tagged features by name. Returns ids to use with activityQuery.",
            "inputSchema": _obj(
                {
                    "appId": _APP,
                    "itemType": {"type": "string", "enum": ["Feature"]},
                    "substring": {"type": "string", "description": "Case-insensitive name filter"},
                }
            ),
            "annotations": _READ,
        },
        _search,
    ),
    (
        {
            "name": "activityQuery",
            "description": "Aggregate feature usage (events and visitors) for one application.",
            "inputSchema": _obj(
                {
                    "appId": _APP,
                    "entityIds": {"type": "array", "items": {"type": "string"}, "description": "Feature ids; omit for all"},
                    "groupBy": {
                        "type": "array",
                        "items": {"type": "string", "enum": ["entity", "account"]},
                        "description": "Row grain; default ['entity']",
                    },
                    "days": {"type": "integer", "description": "Look-back window in days; default 30"},
                    "limit": {"type": "integer"},
                },
                ["appId"],
            ),
            "annotations": _READ,
        },
        _activity,
    ),
    (
        {
            "name": "accountQuery",
            "description": "Look up accounts and their metadata (plan, ARR, CSM).",
            "inputSchema": _obj(
                {"accountIds": {"type": "array", "items": {"type": "string"}}, "limit": {"type": "integer"}}
            ),
            "annotations": _READ,
        },
        _accounts,
    ),
    (
        {
            "name": "segmentList",
            "description": "List saved segments.",
            "inputSchema": _obj({}),
            "annotations": _READ,
        },
        lambda args: {"segments": SEGMENTS},
    ),
    (
        {
            "name": "createSegment",
            "description": "Create a segment (write tool; hidden from Code Mode unless explicitly allowed).",
            "inputSchema": _obj({"name": {"type": "string"}}, ["name"]),
            "annotations": {"readOnlyHint": False},
        },
        lambda args: {"created": {"id": "seg-new", "name": args.get("name")}},
    ),
]
_HANDLERS = {spec["name"]: handler for spec, handler in TOOLS}


def list_tools() -> list[dict[str, Any]]:
    return [spec for spec, _handler in TOOLS]


def call_tool(name: str, arguments: dict[str, Any]) -> dict[str, Any]:
    handler = _HANDLERS.get(name)
    if handler is None:
        return text_result(f"Unknown tool: {name}", is_error=True)
    try:
        return text_result(handler(arguments))
    except (TypeError, ValueError) as exc:
        return text_result(f"Invalid arguments for {name}: {exc}", is_error=True)


def main() -> None:
    serve_stdio(name="mock-pendo", list_tools=list_tools, call_tool=call_tool)


if __name__ == "__main__":
    main()
