"""A stand-in Pendo MCP server with synthetic data, for tests and offline demos.

The tool names, argument schemas and result shapes follow the real Pendo MCP
server for the handful of tools modelled here. Descriptions are condensed, only
part of each tool's behaviour is implemented, and all data is synthetic.
`createSegment` is a placeholder write tool with a made-up schema, and the
`readOnlyHint` annotations are an assumption about the real server.

Run as an MCP stdio server:  python -m pendo_codemode.mock_pendo
"""
from __future__ import annotations

from datetime import date, timedelta
import hashlib
from typing import Any

from .mcp_wire import serve_stdio, text_result

SUB_ID = "mock-sub"
APPS = [-323232, 5475190817226752]
_AREAS = [{"id": f"pa-{i}", "name": name} for i, name in enumerate(["Onboarding", "Reporting", "Admin"])]


def _n(*parts: Any, mod: int) -> int:
    """Deterministic pseudo-random integer in [0, mod)."""
    digest = hashlib.sha256("|".join(map(str, parts)).encode()).digest()
    return int.from_bytes(digest[:4], "big") % mod


def _entities(kind: str, names: list[str]) -> list[dict[str, Any]]:
    out = []
    for index, name in enumerate(names):
        entity: dict[str, Any] = {"id": f"{kind}-{index:02d}", "name": name, "appId": APPS[index % 2], "type": kind}
        if kind != "trackEvent" and index % 3:
            entity["productArea"] = _AREAS[index % len(_AREAS)]
        out.append(entity)
    return out


ENTITIES = (
    _entities("feature", ["Add New Button", "Export CSV", "Invite Teammate", "Share Link", "Filter Results",
                          "Save View", "Open Settings", "Add Widget", "Schedule Report", "Bulk Edit"])
    + _entities("page", ["Dashboard", "Reports", "Settings", "Expense Detail", "Trips", "Admin Console"])
    + _entities("trackEvent", ["Report Exported", "Trip Booked", "Receipt Uploaded", "Login Failed"])
)
ACCOUNTS = [f"Example Co {index:02d}" for index in range(30)]
GUIDES = [
    {
        "id": f"guide-{index:02d}",
        "name": name,
        "appId": APPS[index % 2],
        "state": "public" if index % 4 else "draft",
        "launchMethod": ["auto", "badge", "embed", "launcher"][index % 4],
        "steps": 1 + index % 3,
    }
    for index, name in enumerate(["Welcome tour", "New: scheduled reports", "Invite your team", "NPS survey",
                                  "Export tips", "Release notes", "Trial ending banner", "Setup checklist",
                                  "Bulk edit walkthrough", "Resource center", "Policy update", "Mobile app promo"])
]
SEGMENTS = [{"id": f"seg-{i}", "name": name} for i, name in enumerate(["Paying Customers", "Trial", "Power users"])]


# --- synthetic measurements ---------------------------------------------------


def _window(range_: Any, before: tuple[date, date] | None = None) -> tuple[date, date]:
    range_ = range_ or {}
    if range_.get("type") == "absolute":
        return date.fromisoformat(range_["startDate"]), date.fromisoformat(range_["endDate"])
    days = int(range_.get("lastNDays") or 30)
    end = (before[0] if before else date(2026, 11, 22)) - timedelta(days=1)
    return end - timedelta(days=days - 1), end


def _meta(window: tuple[date, date]) -> dict[str, Any]:
    return {"dateRange": {"startDate": window[0].isoformat(), "endDate": window[1].isoformat()}}


def _usage(entity: dict[str, Any], account: str, window: tuple[date, date]) -> tuple[int, int]:
    """(events, visitors) one account produced on one entity in the window."""
    if _n("uses", entity["id"], account, mod=3) == 0:
        return 0, 0
    days = (window[1] - window[0]).days + 1
    # A per-period factor so that comparing two periods shows movement.
    drift = 60 + _n("drift", entity["id"], window[0].toordinal() // 30, mod=90)
    events = days * (1 + _n("ev", entity["id"], account, mod=40)) * drift // 100
    return events, 1 + _n("vis", entity["id"], account, mod=15)


def _entity_totals(entity: dict[str, Any], window: tuple[date, date]) -> dict[str, Any]:
    per_account = [_usage(entity, account, window) for account in ACCOUNTS]
    row: dict[str, Any] = {
        "appId": entity["appId"],
        "entityId": entity["id"],
        "entityName": entity["name"],
        "totalEvents": sum(e for e, _v in per_account),
        "uniqueAccounts": sum(1 for e, _v in per_account if e),
        "uniqueVisitors": sum(v for _e, v in per_account),
    }
    if entity["type"] != "trackEvent":
        row["totalRageClickCount"] = _n("rage", entity["id"], mod=40)
        row["totalDeadClickCount"] = _n("dead", entity["id"], mod=25)
        row["totalErrorClickCount"] = _n("err", entity["id"], mod=10)
    return row


def _sorted(rows: list[dict[str, Any]], key: str, order: str | None) -> list[dict[str, Any]]:
    return sorted(rows, key=lambda r: r.get(key) or 0, reverse=order != "asc")


# --- tool handlers -------------------------------------------------------------


def _list_countables(args: dict[str, Any]) -> list[dict[str, Any]]:
    found = [e for e in ENTITIES if e["type"] == args["type"]]
    if args.get("appId"):
        found = [e for e in found if str(e["appId"]) == args["appId"]]
    if args.get("search"):
        found = [e for e in found if args["search"].lower() in e["name"].lower()]
    offset = int(args.get("offset") or 0)
    page = found[offset : offset + int(args.get("limit") or 50)]
    return [{k: v for k, v in e.items() if k not in {"type", "appId"}} for e in page]


def _aggregate_entity_usage(args: dict[str, Any]) -> dict[str, Any]:
    scope = args.get("productArea") or []
    if scope and isinstance(scope[0], dict):
        wanted = {(entry["entityType"], i) for entry in scope for i in entry["ids"]}
        entities = [e for e in ENTITIES if (e["type"], e["id"]) in wanted]
    else:
        if not args.get("entityType"):
            raise ValueError("entityType is required for an unscoped ranking")
        entities = [e for e in ENTITIES if e["type"] == args["entityType"]]
    if args.get("appId"):
        entities = [e for e in entities if str(e["appId"]) == args["appId"]]
    window = _window(args["dateRange"])
    meta = _meta(window)
    rows = [_entity_totals(e, window) for e in entities]
    sort_by = args.get("sortBy") or "totalEvents"
    if args.get("compareToDateRange"):
        prior_window = _window(args["compareToDateRange"], before=window)
        meta["compareToDateRange"] = _meta(prior_window)["dateRange"]
        compared = []
        for entity, current in zip(entities, rows):
            prior = _entity_totals(entity, prior_window)
            row = {k: current[k] for k in ("appId", "entityId", "entityName")}
            for metric in ("totalEvents", "uniqueVisitors", "uniqueAccounts"):
                now, then = current[metric], prior[metric]
                row[f"current_{metric}"], row[f"prior_{metric}"] = now, then
                row[f"change_{metric}"] = now - then
                row[f"pctChange_{metric}"] = round(100 * (now - then) / then, 1) if then else None
            compared.append(row)
        rows = compared
        sort_by = args.get("sortBy") or "change_totalEvents"
    else:
        rows = [r for r in rows if r["totalEvents"] or args.get("includeUnused")]
    summary = {"totalEvents": sum(r.get("totalEvents", r.get("current_totalEvents", 0)) for r in rows)}
    return {"meta": meta, "summary": summary, "rows": _sorted(rows, sort_by, args.get("sortOrder"))[: int(args.get("limit") or 10)]}


def _entity_usage(args: dict[str, Any]) -> dict[str, Any]:
    entity = next((e for e in ENTITIES if e["id"] == args["entityId"] and e["type"] == args["entityType"]), None)
    if entity is None:
        raise ValueError(f"no {args['entityType']} with id {args['entityId']}")
    if args.get("groupBy", "visitor") != "account":
        raise ValueError("the mock only implements groupBy='account'")
    window = _window(args["dateRange"])
    rows = []
    for account in ACCOUNTS:
        events, visitors = _usage(entity, account, window)
        if events and args.get("accountId") in (None, account):
            rows.append({"accountId": account, "daysActive": min(26, 1 + events // 40), "events": events,
                         "numVisitors": visitors, "timeOnEntityInMinutes": events})
    summary = {"totalNumAccounts": len(rows), "totalNumEvents": sum(r["events"] for r in rows),
               "totalNumVisitors": sum(r["numVisitors"] for r in rows)}
    return {"meta": _meta(window), "summary": summary,
            "rows": _sorted(rows, args.get("sortBy") or "events", args.get("sortOrder"))[: int(args.get("limit") or 10)]}


def _app_usage(args: dict[str, Any]) -> dict[str, Any]:
    if args.get("groupBy") != "account":
        raise ValueError("the mock only implements groupBy='account'")
    window = _window(args["dateRange"])
    rows = []
    for account in ACCOUNTS:
        if args.get("accountId") not in (None, account):
            continue
        usage = [_usage(e, account, window) for e in ENTITIES]
        events = sum(e for e, _v in usage)
        seconds = events * 45
        rows.append({"accountId": account, "daysActive": min(26, 1 + events // 400),
                     "numVisitors": max((v for _e, v in usage), default=0), "totalEvents": events,
                     "totalRageClickCount": _n("arage", account, mod=30),
                     "totalTime": {"seconds": seconds, "display": f"{seconds // 3600}h {seconds % 3600 // 60}m"}})
    rows = _sorted(rows, args.get("sortBy") or "totalEvents", args.get("sortOrder"))
    offset, limit = int(args.get("offset") or 0), int(args.get("limit") or 10)
    meta = {**_meta(window), "offset": offset, "hasMore": offset + limit < len(rows), "rowCapReached": False}
    summary = {"totalNumAccounts": len(rows), "totalNumEvents": sum(r["totalEvents"] for r in rows)}
    return {"meta": meta, "summary": summary, "rows": rows[offset : offset + limit]}


def _list_accounts(args: dict[str, Any]) -> dict[str, Any]:
    limit = int(args.get("limit") or 10)
    if args.get("mode") == "search":
        needle = str(args.get("search") or "").lower()
        rows = [{"accountId": a, "name": a, "relevance": 1 if a.lower() == needle else 0.9}
                for a in ACCOUNTS if needle and needle in a.lower()]
        return {"summary": {"numAccounts": len(rows[:limit])}, "rows": _sorted(rows, "relevance", None)[:limit]}
    return {"summary": {"numAccounts": len(ACCOUNTS)}, "rows": [{"accountId": a, "name": a} for a in ACCOUNTS[:limit]]}


def _guide_numbers(guide: dict[str, Any], account: str | None = None) -> dict[str, int]:
    scale = 1 if account is None else 0 if _n("gacct", guide["id"], account, mod=2) else 1
    seed = (guide["id"], account or "")
    visitors = scale * (20 + _n("gv", *seed, mod=400)) // (1 if account is None else 12)
    views = visitors * (1 + _n("gviews", *seed, mod=6))
    return {"visitors": visitors, "views": views, "engaged": visitors * _n("geng", guide["id"], mod=90) // 100,
            "completions": views * _n("gdone", guide["id"], mod=50) // 100,
            "dismissals": views * _n("gdis", guide["id"], mod=70) // 100}


def _guide_filter(args: dict[str, Any]) -> list[dict[str, Any]]:
    state = {"inactive": "disabled"}.get(args.get("status"), args.get("status"))
    return [g for g in GUIDES if (not state or g["state"] == state)
            and (not args.get("appId") or str(g["appId"]) == args["appId"])]


def _aggregate_guide_metrics(args: dict[str, Any]) -> dict[str, Any]:
    rows = []
    for guide in _guide_filter(args):
        n = _guide_numbers(guide, args.get("accountId"))
        if n["views"]:
            rows.append({"appId": guide["appId"], "entityId": guide["id"], "entityName": guide["name"],
                         "totalCompletions": n["completions"], "totalDismissals": n["dismissals"],
                         "totalViews": n["views"], "uniqueAccounts": min(len(ACCOUNTS), 1 + n["visitors"] // 9),
                         "uniqueVisitors": n["visitors"], "viewsPerUser": n["views"] / n["visitors"]})
    meta = {**_meta(_window(args["dateRange"])), "totalGuideCount": len(rows)}
    return {"meta": meta, "rows": _sorted(rows, args.get("sortBy") or "totalViews", args.get("sortOrder"))[: int(args.get("limit") or 10)]}


def _guide_effectiveness(args: dict[str, Any]) -> dict[str, Any]:
    rows = []
    for guide in _guide_filter(args):
        n = _guide_numbers(guide)
        block: dict[str, Any] = {"engagementRate": round(100 * n["engaged"] / n["visitors"], 1),
                                 "firstTimeViews": n["visitors"], "totalViews": n["views"], "totalVisitors": n["visitors"]}
        if guide["steps"] > 1:  # the real tool omits completionRate for single-step guides
            block["completionRate"] = round(100 * n["completions"] / n["views"], 1)
        rows.append({"entityId": guide["id"], "entityName": guide["name"], "launchMethod": guide["launchMethod"],
                     "state": guide["state"], "segments": {"everyone": block}})
    sort = args.get("sortBy") or {}
    metric = sort.get("metric") or "totalVisitors"
    rows.sort(key=lambda r: r["segments"]["everyone"].get(metric) or 0, reverse=sort.get("direction") != "asc")
    meta = {**_meta(_window(args.get("dateRange"))), "guideCount": len(rows), "segments": [{"id": "everyone", "label": "Everyone"}]}
    summary = {"everyone": {"totalViews": sum(r["segments"]["everyone"]["totalViews"] for r in rows)}}
    return {"meta": meta, "summary": summary, "rows": rows[: int(args.get("limit") or 20)]}


def _segment_list(args: dict[str, Any]) -> dict[str, Any]:
    needle = str(args.get("substring") or "").lower()
    found = [s for s in SEGMENTS if needle in s["name"].lower()]
    return {"totalCount": len(found), "offset": 0, "returned": len(found), "segments": found}


# --- schemas (argument names, types and enums as on the real server) -------------

_S, _N, _B = {"type": "string"}, {"type": "number"}, {"type": "boolean"}


def _d(schema: dict[str, Any], description: str) -> dict[str, Any]:
    return {**schema, "description": description}


def _enum(*values: str) -> dict[str, Any]:
    return {"type": "string", "enum": list(values)}


_SUB = _d(_S, "Subscription ID that owns the data. Required for all queries.")
_QUERY = _d(_S, "Optional short summary (under 20 words) of the Pendo task this call addresses, for usage analytics.")
_APP = _d(_S, "Optional application ID to filter by. If omitted, includes all applications the user can access.")
_ORDER = _d(_enum("asc", "desc"), "'desc' (default, highest first) or 'asc'.")
_SEGMENT = _d(_S, "A saved segment ID, or the pipeline returned by buildPendoSegment passed verbatim.")
_DATE_RANGE = {
    "type": "object",
    "description": "Time window. Rolling: type='relative' with lastNDays. Fixed: type='absolute' with startDate and endDate.",
    "properties": {
        "type": _d(_enum("absolute", "relative"), "'absolute' or 'relative'."),
        "lastNDays": _d({"type": "integer", "minimum": 1}, "Complete days to look back (excludes today); default 30."),
        "startDate": _d(_S, "YYYY-MM-DD, inclusive. Required when type='absolute'."),
        "endDate": _d(_S, "YYYY-MM-DD, inclusive. Required when type='absolute'."),
    },
}
_PRODUCT_AREA = {
    "type": "array",
    "description": "Scope to saved product area ids, ['*'] for any saved area, or inline entities grouped by type.",
    "items": {"anyOf": [_S, {"type": "object", "required": ["entityType", "ids"], "properties": {
        "entityType": _enum("page", "feature", "trackEvent", "guide"), "ids": {"type": "array", "items": _S}}}]},
}
_ENTITY_TYPE = _d(_enum("page", "feature", "trackEvent"), "The kind of entity: 'page', 'feature', or 'trackEvent'.")
_GROUP_BY = _d(_enum("visitor", "account"), "'visitor' (default) or 'account'.")
_STRINGS = {"type": "array", "items": _S}
_ACTIVATION = _enum("auto", "api", "badge", "dom", "embed", "launcher", "page", "feature", "form", "track")


def _tool(name: str, description: str, properties: dict[str, Any], required: list[str], *, write: bool = False) -> dict[str, Any]:
    return {
        "name": name,
        "description": description,
        "inputSchema": {"type": "object", "properties": {**properties, "userQuery": _QUERY}, "required": required},
        "annotations": {"readOnlyHint": not write},
    }


TOOLS: list[tuple[dict[str, Any], Any]] = [
    (_tool("listSubscriptions",
           "Returns the id and name of every Pendo subscription this user can use through this server.\n\n"
           "RETURNS:\n- Array of subscriptions with: subscriptionId, subscriptionName", {}, []),
     lambda args: [{"subscriptionId": SUB_ID, "subscriptionName": "Mock subscription"}]),
    (_tool("listPagesFeaturesAndEvents",
           "Find, search or list pages, features, or track events by name and return their entity IDs.\n\n"
           "USE FOR: Resolving a named page, feature, or track event to its ID before calling entityUsage.\n\n"
           "RETURNS:\n- An array, one item per entity: id, name, description, eventPropertyNames. Features include "
           "pageId. Pages and features include productArea {id, name} when assigned to one.",
           {"subId": _SUB, "type": _d(_ENTITY_TYPE, "The kind of entity to list."), "appId": _APP,
            "limit": _d(_N, "Maximum number of results to return (default 50, max 500)."),
            "offset": _d(_N, "Number of matching results to skip."),
            "search": _d(_S, "Search query. Must be used together with searchType."),
            "searchType": _d(_enum("semantic", "fuzzy", "substring"), "How to search. Requires search.")},
           ["type", "subId"]),
     _list_countables),
    (_tool("aggregateEntityUsage",
           "Rank pages, features, or track events by aggregate usage over a date range, or measure a named set of "
           "them as one population. Entities are identified by ID, never by name. Supply compareToDateRange to rank "
           "by how usage CHANGED between two periods.\n\n"
           "USE FOR: Ranking or comparing entities by usage (top pages by views, most-clicked features).\n\n"
           "RETURNS:\n- meta.dateRange: resolved startDate and endDate; meta.compareToDateRange when comparing\n"
           "- summary: the whole scoped set as one deduplicated population; never add rows together instead\n"
           "- rows: one per entity with entityId, entityName, appId (a number), totalEvents, uniqueVisitors, "
           "uniqueAccounts (pages/features also frustration counts). In comparison mode the headline metrics are "
           "split into current_<m>, prior_<m>, change_<m> and pctChange_<m> (null when prior is 0).",
           {"subId": _SUB, "dateRange": _DATE_RANGE,
            "compareToDateRange": {**_DATE_RANGE, "description": "Optional baseline period. A relative baseline is the N days immediately before dateRange."},
            "entityType": _ENTITY_TYPE, "appId": _APP, "productArea": _PRODUCT_AREA,
            "includeUnused": _d(_B, "Also include entities with zero usage in the window."),
            "limit": _d(_N, "Maximum number of entity rows to return (1-200, default 10)."),
            "sortBy": _d(_S, "Defaults to 'totalEvents'; when comparing use a prefixed column such as 'change_totalEvents'."),
            "sortOrder": _ORDER, "segmentPipeline": _SEGMENT},
           ["subId", "dateRange"]),
     _aggregate_entity_usage),
    (_tool("entityUsage",
           "Get usage analytics for one known page, feature, or track event ID over a date range.\n\n"
           "USE FOR: 'How many people used it' totals and top-N visitor or account questions for one known entity.\n\n"
           "RETURNS:\n- summary: totalNumEvents, totalNumVisitors, totalNumAccounts\n"
           "- rows: events, timeOnEntityInMinutes, daysActive per visitorId or accountId; per-account rows also "
           "include numVisitors",
           {"subId": _SUB, "entityType": _ENTITY_TYPE, "entityId": _d(_S, "The Pendo page, feature, or track event ID."),
            "dateRange": _DATE_RANGE, "groupBy": _GROUP_BY,
            "accountId": _d(_S, "Optional Pendo account id to filter both summary and rows to."),
            "limit": _d(_N, "Maximum number of rows to return (1-200, default 10)."),
            "sortBy": _d(_S, "Field to sort rows by; default 'events'."), "sortOrder": _ORDER,
            "segmentPipeline": _SEGMENT},
           ["subId", "entityType", "entityId", "dateRange"]),
     _entity_usage),
    (_tool("appUsage",
           "Get per-visitor or per-account app usage metrics for a date range.\n\n"
           "WORKFLOW: To get more rows than limit allows, call again with offset increased by limit while "
           "meta.hasMore is true.\n\n"
           "RETURNS:\n- meta.hasMore: whether more rows follow this page\n"
           "- summary: totalNumVisitors or totalNumAccounts, totalNumEvents\n"
           "- rows: accountId or visitorId, daysActive, totalTime {seconds, display}, totalEvents, frustration "
           "totals; per-account rows also include numVisitors",
           {"subId": _SUB, "dateRange": _DATE_RANGE, "appId": _APP, "groupBy": _GROUP_BY,
            "accountId": _d(_S, "Optional Pendo account id to filter the usage to."),
            "limit": _d(_N, "Maximum number of rows to return (1-200, default 10)."),
            "offset": _d(_N, "Number of rows to skip before returning (0-10000)."),
            "productArea": _PRODUCT_AREA, "sortBy": _d(_S, "Field to sort rows by."), "sortOrder": _ORDER,
            "segmentPipeline": _SEGMENT},
           ["subId", "dateRange"]),
     _app_usage),
    (_tool("listAccounts",
           "List the accounts that match a segment, or fuzzy-search account display names or IDs.\n\n"
           "RETURNS:\n- summary: numAccounts\n- rows: accountId, name; search rows also include relevance",
           {"subId": _SUB, "mode": _d(_enum("segment", "search"), "'segment' (default) or 'search'."),
            "search": _d(_S, "Account search query. Required when mode is 'search'."),
            "limit": _d(_N, "Maximum number of account rows to return (1-200, default 10)."),
            "select": _d(_STRINGS, "Account metadata fields to return, from accountMetadataSchema."),
            "segmentPipeline": _SEGMENT},
           ["subId"]),
     _list_accounts),
    (_tool("aggregateGuideMetrics",
           "Rank guides by aggregate usage over a date range, returning one row per guide.\n\n"
           "RETURNS:\n- meta.totalGuideCount: total distinct guides with activity before limit\n"
           "- rows: one per guide with entityId, entityName, appId, totalViews, totalCompletions, totalDismissals, "
           "uniqueVisitors, uniqueAccounts, viewsPerUser",
           {"subId": _SUB, "dateRange": _DATE_RANGE, "appId": _APP,
            "accountId": _d(_S, "Optional Pendo account ID; restricts results to activity from that account."),
            "status": _enum("public", "staged", "scheduled", "draft", "pendingReview", "inactive"),
            "activation": _ACTIVATION, "limit": _d(_N, "Maximum number of guide rows to return (1-200, default 10)."),
            "productArea": _PRODUCT_AREA,
            "sortBy": _enum("totalViews", "totalCompletions", "totalDismissals", "uniqueVisitors", "uniqueAccounts", "viewsPerUser"),
            "sortOrder": _ORDER, "segmentPipeline": _SEGMENT},
           ["subId", "dateRange"]),
     _aggregate_guide_metrics),
    (_tool("guideEffectivenessMetrics",
           "Dashboard-consistent, visitor-weighted Guide Effectiveness metrics for every guide.\n\n"
           "RETURNS:\n- meta: resolved dateRange, the segments compared, and guideCount\n"
           "- summary: per-segment overview keyed by segment id ('everyone' when no segments are given)\n"
           "- rows: one per guide with entityId, entityName, state, launchMethod, and segments.<segmentId> blocks: "
           "engagementRate, totalViews, totalVisitors, firstTimeViews. completionRate is absent for single-step "
           "guides; goalAdoptionRate is absent for guides without a conversion goal",
           {"subId": _SUB, "dateRange": _DATE_RANGE, "appId": _APP, "status": _enum("public", "staged", "draft", "disabled"),
            "activation": _d(_S, "Optional launch-method filter, e.g. 'auto', 'badge'."),
            "limit": _d(_N, "Maximum number of guide rows to return (1-200, default 20)."),
            "productArea": _PRODUCT_AREA,
            "sortBy": {"type": "object", "properties": {
                "metric": _enum("totalViews", "totalVisitors", "engagementRate", "completionRate", "goalAdoptionRate"),
                "direction": _enum("asc", "desc")}}},
           ["subId"]),
     _guide_effectiveness),
    (_tool("segmentList",
           "List or search the subscription's segments.\n\nRETURNS:\n- totalCount, offset, returned, and segments (id, name)",
           {"subId": _SUB, "substring": _d(_S, "Only return segments whose name or id contains this value."),
            "offset": _d(_N, "Number of matching results to skip.")},
           ["subId"]),
     _segment_list),
    (_tool("createSegment", "Create a segment (placeholder write tool with a made-up schema).",
           {"subId": _SUB, "name": _S}, ["subId", "name"], write=True),
     lambda args: {"created": {"id": "seg-new", "name": args["name"]}}),
]
_HANDLERS = {spec["name"]: (spec, handler) for spec, handler in TOOLS}
_JSON_TYPES = {"string": str, "number": (int, float), "integer": int, "boolean": bool, "array": list, "object": dict}


def _problems(value: Any, schema: dict[str, Any], path: str) -> list[str]:
    """Check a value against the subset of JSON Schema used above."""
    if "anyOf" in schema:
        return [] if any(not _problems(value, option, path) for option in schema["anyOf"]) else [f"{path}: no variant matches"]
    kind = schema.get("type")
    if kind and (not isinstance(value, _JSON_TYPES[kind]) or (kind != "boolean" and isinstance(value, bool))):
        return [f"{path}: expected {kind}"]
    if "enum" in schema and value not in schema["enum"]:
        return [f"{path}: must be one of {schema['enum']}"]
    found: list[str] = []
    if kind == "object":
        properties = schema.get("properties") or {}
        found += [f"{path}.{key}: is required" for key in schema.get("required") or [] if key not in value]
        for key, item in value.items():
            found += _problems(item, properties[key], f"{path}.{key}") if key in properties else [f"{path}.{key}: unknown argument"]
    if kind == "array" and "items" in schema:
        for index, item in enumerate(value):
            found += _problems(item, schema["items"], f"{path}[{index}]")
    return found


def list_tools() -> list[dict[str, Any]]:
    return [spec for spec, _handler in TOOLS]


def call_tool(name: str, arguments: dict[str, Any]) -> dict[str, Any]:
    if name not in _HANDLERS:
        return text_result(f"Unknown tool: {name}", is_error=True)
    spec, handler = _HANDLERS[name]
    problems = _problems(arguments, spec["inputSchema"], "args")
    if problems:
        return text_result(f"Invalid arguments for {name}: {'; '.join(problems[:5])}", is_error=True)
    try:
        value = handler(arguments)
    except (KeyError, TypeError, ValueError) as exc:
        return text_result(f"{name} failed: {exc}", is_error=True)
    # Like the real server, results are JSON text; a top-level array has no structuredContent.
    return text_result(value)


def main() -> None:
    serve_stdio(name="mock-pendo", list_tools=list_tools, call_tool=call_tool)


if __name__ == "__main__":
    main()
