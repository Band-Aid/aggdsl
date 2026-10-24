// Full funnel report (rates, avg/median time, per-step visitor rows) via nested forks. Rarely needed: start from funnel-simple.dsl.
FROM event([source=singleEvents,appId=[{{APP_ID}}]])
TIMESERIES period=dayRange first=now() count=-90
| group by visitorId fields { funnel=funnel({ maxDuration=0, items=[{'pageId': '{{PAGE_ID}}'}, {'featureId': '{{FEATURE_ID}}'}], uniqueVisitorFunnel=True, onlyMatchedEvents=True }) }
| unwind { field=funnel }
| select { rowType="visitors", times=funnel.times, start=funnel.start, steps=funnel.steps, visitorId=visitorId, accountIds=funnel.accountIds, recordingIds=funnel.recordingIds, recordingSessionIds=funnel.recordingSessionIds, tabIds=funnel.tabIds, conversationId=funnel.conversationIds, eventIds=funnel.eventIds, content=funnel.contents }
| fork
branch
|| fork
branch
|| group by steps fields { count=count(null) }
|| sort -steps
|| raw {"accumulate":{"count":"count"}}
|| group by  fields { counts=list(count), furthestStep=max(steps) }
|| eval { percentCompleted=if(furthestStep != 2, 0, (counts[0]/counts[len(counts) - 1]) * 100) }
|| select { percentCompleted=percentCompleted }
endbranch
branch
PIPELINE
|| raw {"compute":{"durations":{"diff":"times"}}}
|| filter len(durations) == 1
|| group by  fields { averageTimes=listAverage(durations), medianTimes=listMedian(durations) }
|| fork
branch
PIPELINE
|| unwind { field=averageTimes }
|| group by  fields { averageTimeToCompletion=sum(averageTimes) }
endbranch
branch
PIPELINE
|| unwind { field=medianTimes }
|| group by  fields { medianTimeToCompletion=sum(medianTimes) }
endbranch
|| endfork
|| join fields []
endbranch
|| endfork
|| join fields []
|| eval { rowType="wholeFunnelMetrics" }
endbranch
branch
|| fork
branch
|| group by steps fields { count=count(null) }
|| group by  fields { groupSteps=accumulate({ fields={'count': 'count'}, sort=['-steps'] }) }
|| eval { percentCompleted=if(groupSteps[0].steps != 2, 0, (groupSteps[0].count/groupSteps[len(groupSteps) - 1].count) * 100) }
endbranch
branch
|| raw {"compute":{"durations":{"diff":"times"}}}
|| filter len(durations) == 1
|| group by  fields { averageTimes=listAverage(durations), medianTimes=listMedian(durations) }
|| fork
branch
|| unwind { field=averageTimes }
|| group by  fields { averageTimeToCompletion=sum(averageTimes) }
endbranch
branch
|| unwind { field=medianTimes }
|| group by  fields { medianTimeToCompletion=sum(medianTimes) }
endbranch
|| endfork
endbranch
branch
|| raw {"compute":{"durations":{"diff":"times"}}}
|| group by  fields { averageTimes=listAverage(durations), medianTimes=listMedian(durations) }
endbranch
|| endfork
|| join fields []
|| eval { rowType="groupInfo" }
endbranch
branch
|| filter steps == 1
|| limit 10000
endbranch
branch
|| filter steps == 2
|| limit 10000
endbranch
| endfork
