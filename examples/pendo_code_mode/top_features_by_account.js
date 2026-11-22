// Which enterprise accounts drive the three most-used features, across every app?
// Written against the bundled mock (`pendo-code run --mock ...`); the real Pendo MCP
// server has different argument shapes: read them with `pendo-code tools`.
const { applications } = await pendo.list_all_applications();

// One usage query per app, in parallel.
const usage = await Promise.all(
  applications.map((app) => pendo.activityQuery({ appId: app.id, groupBy: ['entity'], days: 30 }))
);
const top = usage
  .flatMap((u) => u.rows.map((row) => ({ ...row, appId: u.appId })))
  .sort((a, b) => b.numEvents - a.numEvents)
  .slice(0, 3);

// Names, per-account breakdowns and account metadata: all independent, so fetch together.
const [catalogs, breakdowns, { accounts }] = await Promise.all([
  Promise.all(applications.map((app) => pendo.searchEntities({ appId: app.id, itemType: 'Feature' }))),
  Promise.all(
    top.map((f) =>
      pendo.activityQuery({ appId: f.appId, entityIds: [f.featureId], groupBy: ['account'], days: 30 })
    )
  ),
  pendo.accountQuery()
]);
const featureName = new Map(catalogs.flatMap((c) => c.entities).map((e) => [e.id, e.name]));
const appName = new Map(applications.map((a) => [a.id, a.name]));
const account = new Map(accounts.map((a) => [a.accountId, a]));

return top.map((f, i) => ({
  feature: featureName.get(f.featureId),
  app: appName.get(f.appId),
  events: f.numEvents,
  topEnterpriseAccounts: breakdowns[i].rows
    .filter((row) => account.get(row.accountId)?.plan === 'enterprise')
    .slice(0, 3)
    .map((row) => ({ name: account.get(row.accountId).name, arr: account.get(row.accountId).arr, events: row.numEvents }))
}));
