// PES for one segment with explicit core events (adoption config). Segment ids: tools/pendo/lookup_segments.py.
PIPELINE
| pes {"appId": "{{APP_ID}}", "firstDay": "now()", "dayCount": -30, "blacklist": "apply", "segment": {"id": "{{SEGMENT_ID}}"}, "config": {"adoption": {"userBase": "visitors", "events": [{"kind": "feature", "id": "{{FEATURE_ID}}"}, {"kind": "page", "id": "{{PAGE_ID}}"}]}}}
