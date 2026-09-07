// Agentic prompt events over the last 30 days (prompt content + timing).
// Sorts newest first; omits personal identifiers from the selected fields.
RESPONSE mimeType=application/json
REQUEST name="AiAgentPromptsAggregation"
FROM event([source=agenticEvents,blacklist=apply,appId=1234567890123456,agentId="AGENT_ID_EXAMPLE"])
TIMESERIES period=dayRange first="dateAdd(startOfPeriod(\"daily\", now()), -30, \"days\")" count=30
| sort -browserTime
| limit 1000
| select { content=content, browserTime=browserTime }
