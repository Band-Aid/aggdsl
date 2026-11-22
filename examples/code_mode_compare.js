// Replace 123456 with a real Pendo app ID before running against Pendo.
const dsl = (count) => `
FROM event([source=featureEvents, appId=123456, blacklist="apply"])
TIMESERIES period=dayRange first=now() count=${count}
| group by featureId fields { clicks=sum(numEvents) }
| sort -clicks
| limit 5
`;
const [last7Days, last30Days] = await Promise.all([
  pendo.aggdsl.run(dsl(-7)),
  pendo.aggdsl.run(dsl(-30))
]);
return { last7Days, last30Days };
