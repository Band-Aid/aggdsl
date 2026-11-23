"""The high-level `lib` functions: what they need upstream and how they are documented.

The raw tools are many, change often and publish no result schema. These
functions are few, compose the raw tools for common questions, and return
shapes this package defines, so their one-line documentation is exact.
"""
from __future__ import annotations

from pathlib import Path

# name -> (tools it calls, one-line signature shown to the model)
FUNCTIONS: dict[str, tuple[tuple[str, ...], str]] = {
    "find": (
        ("listPagesFeaturesAndEvents",),
        "lib.find(query, {appId?, types?: page|feature|trackEvent[], limit?, match?: auto|substring|semantic}) → "
        "[{type, id, name, pageId?}]  // by name; falls back to meaning when no name matches",
    ),
    "frustration": (
        ("aggregateEntityUsage", "getEntity"),
        "lib.frustration({appId?, days?, top?}) → {dateRange, totals: {rage, dead, error}, "
        "elements: [{signal, name, count, selectors[], page}], pages: [{name, urlRules?[], rage, dead, uTurns}]}",
    ),
    "funnel": (
        ("queryFunnel",),
        "lib.funnel(steps, {appId?, days?}) → {visitors[], keptPct[], overallPct, medianBetween[], medianTotal}  "
        "// steps: 2-3 of {type, id}, e.g. items from lib.find",
    ),
    "visitors": (
        ("entityUsage",),
        "lib.visitors({type, id}, {days?}) → {ids[], total, capped}  // at most 200 visitor ids",
    ),
    "cohorts": (
        ("entityUsage",),
        "lib.cohorts({among, did, behaviours, days?}) → {completers, others, behaviours: [{name, completersPct, "
        "othersPct}], capped}  // of visitors who did `among`: those who also did `did` vs those who did not",
    ),
    "guidePolls": (
        ("listGuides", "guidePollResponses"),
        "lib.guidePolls({appId?, days?}) → [{guide, state, respondents, polls: [{question, answers: {response: "
        "count}}]}]  // free-text answers are counted, not returned",
    ),
}


def available(tool_names: set[str]) -> list[str]:
    """Signatures of the functions whose upstream tools all exist."""
    return [doc for needs, doc in FUNCTIONS.values() if set(needs) <= tool_names]


def source() -> str:
    return Path(__file__).with_name("stdlib.js").read_text(encoding="utf-8")
