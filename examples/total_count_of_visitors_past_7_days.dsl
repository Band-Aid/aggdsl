FROM event([source=events,appId=-323232,blacklist="apply"])
TIMESERIES period=dayRange first=now() count=-60
| identified visitorId
| raw {
    "reduce": {
      "uniqueVisitors": {"count": "visitorId"}
    }
  }
