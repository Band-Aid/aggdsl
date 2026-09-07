// SessionReplays API query including frustration metrics with stricter filters (example appId).
RESPONSE mimeType=application/json
REQUEST name="SessionReplaysWithFrustration"
PIPELINE
| sessionReplays {
  "appId": -123456,
  "dayCount": -30,
  "firstDay": "now()",
  "limit": 50,
  "blacklist": "apply",
  "includeFrustrationMetrics": true,
  "minDuration": 300,
  "sortBy": "-minBrowserTime"
}
