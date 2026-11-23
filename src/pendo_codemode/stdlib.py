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
        "[{type, id, name, pageId?}]  // name match against a catalog fetched once (1 call per type, then free); "
        "when no name matches, a search by meaning (1 call per type)",
    ),
    "catalog": (
        ("listPagesFeaturesAndEvents",),
        "lib.catalog({appId?, types?}) → [{type, id, name, pageId?}]  // every entity, kept in state.catalog; "
        "filter it in code rather than returning it",
    ),
    "frustration": (
        ("aggregateEntityUsage", "getEntity"),
        "lib.frustration({appId?, days?, top?}) → {dateRange, totals: {rage, dead, error}, "
        "elements: [{signal, name, count, selectors[], page}], pages: [{name, urlRules?[], rage, dead, uTurns}]}  "
        "// totals and elements count clicks on tagged features only; pages count every click, so they are larger. "
        "4 calls + 1 per element",
    ),
    "funnel": (
        ("queryFunnel",),
        "lib.funnel(steps, {appId?, days?}) → {visitors[], keptPct[], overallPct, medianBetween[], medianTotal}  "
        "// unique visitors, in order; steps: 2-3 of {type, id}, e.g. items from lib.find. 1 call",
    ),
    "visitors": (
        ("entityUsage",),
        "lib.visitors({type, id}, {days?}) → {ids[], total, capped}  // at most 200 visitor ids. 1 call",
    ),
    "cohorts": (
        ("entityUsage",),
        "lib.cohorts({among, did, behaviours: [{type, id, name?}], days?}) → {completers, others, behaviours: "
        "[{name, completersPct, othersPct}], capped}  // of visitors who did `among`: those who also did `did` vs "
        "those who did not, in any order. 2 calls + 1 per behaviour",
    ),
    "guidePolls": (
        ("listGuides", "guidePollResponses"),
        "lib.guidePolls({appId?, days?}) → [{guide, state, respondents, polls: [{question, answers: {response: "
        "count}}]}]  // every guide that has a non-NPS poll, including ones with no responses; free-text answers "
        "are counted, not returned. 1 call + 1 per guide with a poll",
    ),
}


def available(tool_names: set[str]) -> list[str]:
    """Signatures of the functions whose upstream tools all exist."""
    return [doc for needs, doc in FUNCTIONS.values() if set(needs) <= tool_names]


def source() -> str:
    return Path(__file__).with_name("stdlib.js").read_text(encoding="utf-8")
