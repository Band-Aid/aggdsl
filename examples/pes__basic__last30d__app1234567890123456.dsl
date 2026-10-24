// Basic PES aggregation for a single app over the last 30 days.
PIPELINE
| pes {"appId":1234567890123456,"firstDay":"now()","dayCount":-30}
