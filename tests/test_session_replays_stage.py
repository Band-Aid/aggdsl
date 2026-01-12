import json

from aggdsl import compile_to_pendo_aggregation, decompile_pendo_aggregation_to_dsl, parse


def test_session_replays_stage_compiles() -> None:
    dsl = "\n".join(
        [
            "RESPONSE mimeType=application/json",
            'REQUEST name="SessionReplaysWithFrustration"',
            "PIPELINE",
            "| sessionReplays {\n"
            '  "appId": -323232,\n'
            '  "dayCount": -30,\n'
            '  "firstDay": "now()",\n'
            '  "limit": 50,\n'
            '  "blacklist": "apply",\n'
            '  "includeFrustrationMetrics": true,\n'
            '  "minDuration": 300,\n'
            '  "sortBy": "-minBrowserTime"\n'
            "}",
            "",
        ]
    )

    body = compile_to_pendo_aggregation(parse(dsl))
    assert body["request"]["name"] == "SessionReplaysWithFrustration"
    assert body["request"]["pipeline"] == [
        {
            "sessionReplays": {
                "appId": -323232,
                "dayCount": -30,
                "firstDay": "now()",
                "limit": 50,
                "blacklist": "apply",
                "includeFrustrationMetrics": True,
                "minDuration": 300,
                "sortBy": "-minBrowserTime",
            }
        }
    ]


def test_session_replays_stage_roundtrip() -> None:
    body1 = {
        "response": {"location": "request", "mimeType": "application/json"},
        "request": {
            "name": "SessionReplaysWithFrustration",
            "pipeline": [
                {
                    "sessionReplays": {
                        "appId": -323232,
                        "dayCount": -30,
                        "firstDay": "now()",
                        "limit": 50,
                        "blacklist": "apply",
                        "includeFrustrationMetrics": True,
                        "minDuration": 300,
                        "sortBy": "-minBrowserTime",
                    }
                }
            ],
        },
    }

    dsl_out = decompile_pendo_aggregation_to_dsl(body1)
    body2 = compile_to_pendo_aggregation(parse(dsl_out))

    # Exact JSON equality is expected for this simple request.
    assert json.dumps(body2, sort_keys=True) == json.dumps(body1, sort_keys=True)
