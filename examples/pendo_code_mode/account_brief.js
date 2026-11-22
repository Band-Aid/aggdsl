// How is one account using the product? Activity, adoption of the top features, and guides seen.
const ACCOUNT = 'Example Co 07'; // name or id; resolved with a fuzzy search
const TOP_FEATURES = 5;
const dateRange = { type: 'relative', lastNDays: 30 };

const found = await pendo.listAccounts({ mode: 'search', search: ACCOUNT, limit: 3 });
if (!found.rows.length) return { error: `No account matches "${ACCOUNT}"` };
const account = found.rows[0];
const accountId = account.accountId;

const [activity, everyone, topFeatures, guides] = await Promise.all([
  pendo.appUsage({ dateRange, groupBy: 'account', accountId, limit: 1 }),
  pendo.appUsage({ dateRange, groupBy: 'account', limit: 1 }),
  pendo.aggregateEntityUsage({ entityType: 'feature', dateRange, limit: TOP_FEATURES }),
  pendo.aggregateGuideMetrics({ dateRange, accountId, limit: 5 })
]);

// aggregateEntityUsage has no account filter, so ask entityUsage once per top feature.
const perFeature = await Promise.all(
  topFeatures.rows.map((feature) =>
    pendo.entityUsage({ entityType: 'feature', entityId: feature.entityId, dateRange, groupBy: 'account', accountId, limit: 1 })
  )
);

const row = activity.rows[0];
return {
  account: account.name,
  otherMatches: found.rows.slice(1).map((r) => r.name),
  dateRange: activity.meta.dateRange,
  activity: row
    ? { daysActive: row.daysActive, visitors: row.numVisitors, events: row.totalEvents, time: row.totalTime.display }
    : null,
  shareOfAllEventsPct:
    row && everyone.summary.totalNumEvents
      ? Math.round((1000 * row.totalEvents) / everyone.summary.totalNumEvents) / 10
      : null,
  topFeatureAdoption: topFeatures.rows.map((feature, i) => ({
    feature: feature.entityName,
    used: perFeature[i].summary.totalNumEvents > 0,
    clicks: perFeature[i].summary.totalNumEvents,
    visitors: perFeature[i].summary.totalNumVisitors
  })),
  guidesSeen: guides.rows.map((g) => ({ guide: g.entityName, views: g.totalViews, visitors: g.uniqueVisitors }))
};
