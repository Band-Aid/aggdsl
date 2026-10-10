// AI agent prompts (content + time), newest first. agenticEvents needs agentId AND appId.
FROM event([source=agenticEvents, appId={{APP_ID}}, agentId="{{AGENT_ID}}", blacklist="apply"])
TIMESERIES period=dayRange first=now() count=-30
| sort -browserTime
| limit 1000
| select { content=content, browserTime=browserTime }
