from aggdsl import decompile_pendo_aggregation_to_dsl


def test_decompile_source_with_null_event_config() -> None:
    body = {
        "response": {"location": "request", "mimeType": "application/json"},
        "request": {
            "name": "",
            "pipeline": [
                {
                    "source": {
                        "featureEvents": None,
                        "timeSeries": {
                            "period": "dayRange",
                            "first": "now()",
                            "count": -1,
                        },
                    }
                }
            ],
        },
    }

    dsl = decompile_pendo_aggregation_to_dsl(body)
    assert 'REQUEST name=""' in dsl
    assert "FROM event([source=featureEvents])" in dsl
    assert "TIMESERIES period=dayRange first=now() count=-1" in dsl
