from __future__ import annotations

from aggdsl import compile_to_pendo_aggregation, parse


def test_from_event_supports_moustache_placeholder_param() -> None:
    dsl = """\
FROM event([source=pageEvents,appId={{APP_ID}},blacklist="apply"])
| limit 1
"""

    body = compile_to_pendo_aggregation(parse(dsl))
    assert body["request"]["pipeline"][0] == {
        "source": {
            "pageEvents": {
                "appId": "{{APP_ID}}",
                "blacklist": "apply",
            }
        }
    }
