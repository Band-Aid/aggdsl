from __future__ import annotations

from aggdsl import compile_to_pendo_aggregation, parse


def test_response_location_is_ignored_and_always_request() -> None:
    dsl = """\
RESPONSE location=somewhere mimeType=text/csv
PIPELINE
| limit 1
"""

    body = compile_to_pendo_aggregation(parse(dsl))

    # location is fixed by the compiler regardless of the header.
    assert body["response"]["location"] == "request"
    assert body["response"]["mimeType"] == "text/csv"
