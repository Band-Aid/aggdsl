// Basic sessionReplays API call over the last 30 days with frustration metrics (example appId).
RESPONSE mimeType=application/json
REQUEST name="SessionReplayList"
PIPELINE
| sessionReplays {
  "appId": -123456,
  "blacklist": "apply",
  "dayCount": -30,
  "firstDay": "now()",
  "limit": 100,
  "includeFrustrationMetrics": true
}
