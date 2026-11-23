# Pendo Code Mode recipes

Each file is the body of one `execute` call, written against the real Pendo MCP
tool names, arguments and result shapes. `subId` is left out on purpose: start
the server with `--default subId=<your subscription id>`.

| Recipe | Question | Tool calls it replaces |
| --- | --- | --- |
| `feature_adoption_by_account.js` | Which accounts drive our most-used features, and how concentrated is each? | 6 |
| `usage_movers.js` | What grew and what dropped versus the previous period, across pages, features and track events? | 6 |
| `guide_audit.js` | Which live guides reach many people but are ignored or dismissed? | 2, with a few hundred rows |
| `account_brief.js` | How is one account using the product? | 10 |
| `guide_poll_responses.js` | What did people answer in every guide poll? | 1 + one per guide with a poll |

```bash
pendo-code run --mock examples/pendo_code_mode/usage_movers.js
```
