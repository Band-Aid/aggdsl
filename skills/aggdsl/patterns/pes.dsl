// Product Engagement Score, last 30 days. PES is a stage: PIPELINE mode, no FROM.
PIPELINE
| pes {"appId": "{{APP_ID}}", "firstDay": "now()", "dayCount": -30, "blacklist": "apply"}
