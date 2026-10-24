// PES score for a specific segment (core events) over the last 30 days (example IDs).
// Uses explicit adoption config for selected feature/page IDs.
RESPONSE mimeType=application/json
REQUEST name="PES segment 30d core events (example)"
PIPELINE
| pes {"appId":-123456,"firstDay":"now()","dayCount":-30,"blacklist":"apply","segment":{"id":"SEGMENT_ID_EXAMPLE"},"config":{"adoption":{"userBase":"visitors","events":[{"kind":"feature","id":"FEATURE_ID_1"},{"kind":"feature","id":"FEATURE_ID_2"},{"kind":"feature","id":"FEATURE_ID_3"},{"kind":"feature","id":"FEATURE_ID_4"},{"kind":"feature","id":"FEATURE_ID_5"},{"kind":"page","id":"PAGE_ID_1"},{"kind":"page","id":"PAGE_ID_2"}]}}}
