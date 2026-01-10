from __future__ import annotations

import argparse
import json
import sys
from typing import Any


class ValidationError(ValueError):
    pass


def validate_aggregation_body(body: Any) -> None:
    if not isinstance(body, dict):
        raise ValidationError("Body must be a JSON object")

    response = body.get("response")
    if not isinstance(response, dict):
        raise ValidationError("Missing/invalid 'response' object")

    # Pendo accepts requests where response.location is omitted; when present,
    # we expect the canonical value.
    if "location" in response and response.get("location") != "request":
        raise ValidationError("response.location must be 'request' when provided")

    mime_type = response.get("mimeType")
    if not isinstance(mime_type, str) or not mime_type:
        raise ValidationError("response.mimeType must be a non-empty string")

    request = body.get("request")
    if not isinstance(request, dict):
        raise ValidationError("request must be an object containing pipeline")

    if "pipeline" not in request:
        raise ValidationError("request.pipeline is required")

    pipeline = request.get("pipeline")
    if not isinstance(pipeline, list):
        raise ValidationError("request.pipeline must be a list")

    for idx, stage in enumerate(pipeline):
        if not isinstance(stage, dict) or not stage:
            raise ValidationError(f"pipeline[{idx}] must be a non-empty object")


def _load_json(path: str | None, *, use_stdin: bool) -> Any:
    if use_stdin:
        return json.load(sys.stdin)
    if path is None:
        raise SystemExit("error: provide a JSON file or --stdin")
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description="Validate a Pendo Aggregation request JSON body")
    p.add_argument("path", nargs="?", help="Path to aggregation JSON body")
    p.add_argument("--stdin", action="store_true", help="Read JSON from stdin")
    args = p.parse_args(argv)

    try:
        body = _load_json(args.path, use_stdin=args.stdin)
        validate_aggregation_body(body)
        return 0
    except ValidationError as e:
        print(f"error: {e}", file=sys.stderr)
        return 2
    except Exception as e:
        print(f"error: {e}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
