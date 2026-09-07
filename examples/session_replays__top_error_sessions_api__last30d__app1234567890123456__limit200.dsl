// SessionReplays API call for top error sessions with frustration metrics (single-app example).
// Pulls last 30 days, limited to 200 sessions.
RESPONSE mimeType=application/json
REQUEST name="TopSRErrorsLast30Days"
PIPELINE
| sessionReplays {
  "appId": 1234567890123456,
  "blacklist": "apply",
  "dayCount": -30,
  "firstDay": "now()",
  "limit": 200,
  "includeFrustrationMetrics": true
}
