from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Literal


BlacklistBehavior = Literal["apply", "ignore", "only"]


@dataclass(frozen=True)
class TimeSeries:
    period: str
    # `first` may be an epoch-ms int, or a string expression (e.g. "date(`firstDate`)").
    first: int | str
    # Exactly one of `count` or `last` should be set.
    count: int | None = None
    last: int | str | None = None


@dataclass(frozen=True)
class EventSource:
    source_type: str  # e.g. pageEvents, featureEvents, trackEvents
    params: dict[str, Any]


@dataclass(frozen=True)
class Stage:
    kind: str
    payload: Any


@dataclass(frozen=True)
class Query:
    event_source: EventSource | None
    time_series: TimeSeries | None
    stages: list[Stage]
    response_mime_type: str = "application/json"
    request_name: str | None = None
