// Which accounts drive the most-used features, and how concentrated is each feature's usage?
const TOP_FEATURES = 5;
const TOP_ACCOUNTS = 3;
const dateRange = { type: 'relative', lastNDays: 30 };

const ranking = await pendo.aggregateEntityUsage({ entityType: 'feature', dateRange, limit: TOP_FEATURES });

// One per-account breakdown per feature, all at once.
const breakdowns = await Promise.all(
  ranking.rows.map((feature) =>
    pendo.entityUsage({
      entityType: 'feature',
      entityId: feature.entityId,
      dateRange,
      groupBy: 'account',
      limit: TOP_ACCOUNTS
    })
  )
);

const pct = (part, whole) => (whole ? Math.round((1000 * part) / whole) / 10 : null);
return {
  dateRange: ranking.meta.dateRange,
  features: ranking.rows.map((feature, i) => {
    const { summary, rows } = breakdowns[i];
    return {
      feature: feature.entityName,
      clicks: feature.totalEvents,
      accounts: summary.totalNumAccounts,
      // Share of clicks from the single largest account: high means one customer carries the feature.
      topAccountSharePct: pct(rows[0]?.events ?? 0, summary.totalNumEvents),
      topAccounts: rows.map((row) => ({ account: row.accountId, clicks: row.events, visitors: row.numVisitors }))
    };
  })
};
