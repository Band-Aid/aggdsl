// Query: Calculate Product Engagement Score (PES) for the last 30 days
// Data Source: PES pipeline aggregation
// Output: Product Engagement Score metrics including stickiness, growth, and adoption

PIPELINE
| pes {"appId":"{{APP_ID}}","firstDay":"now()","dayCount":-30}
