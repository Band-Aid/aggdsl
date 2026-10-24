// Session replay list via the dedicated API (PIPELINE + stage). Add "minDuration": 300, "sortBy": "-minBrowserTime" to filter/sort.
PIPELINE
| sessionReplays {"appId": "{{APP_ID}}", "blacklist": "apply", "firstDay": "now()", "dayCount": -30, "limit": 100, "includeFrustrationMetrics": true}
