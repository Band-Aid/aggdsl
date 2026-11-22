// What grew and what dropped the most versus the previous period, across pages, features and track events?
const DAYS = 30;
const KEEP = 5;
const dateRange = { type: 'relative', lastNDays: DAYS };
const types = ['page', 'feature', 'trackEvent'];

// Six rankings (three entity types, both directions) in parallel; the server does the comparison.
const ranked = (entityType, sortOrder) =>
  pendo.aggregateEntityUsage({
    entityType,
    dateRange,
    compareToDateRange: dateRange, // a relative baseline is the N days immediately before dateRange
    sortBy: 'change_totalEvents',
    sortOrder,
    limit: KEEP
  });
const results = await Promise.all(types.flatMap((type) => [ranked(type, 'desc'), ranked(type, 'asc')]));

const rows = results.flatMap((result, i) =>
  result.rows.map((row) => ({
    type: types[Math.floor(i / 2)],
    name: row.entityName,
    change: row.change_totalEvents,
    // null when there was no activity in the baseline
    pctChange: row.pctChange_totalEvents == null ? null : Math.round(row.pctChange_totalEvents * 10) / 10,
    now: row.current_totalEvents,
    before: row.prior_totalEvents
  }))
);
// The same entity can appear in both directions when a type has few entities.
const unique = [...new Map(rows.map((row) => [`${row.type}:${row.name}`, row])).values()];
const byChange = unique.sort((a, b) => b.change - a.change);

return {
  current: results[0].meta.dateRange,
  baseline: results[0].meta.compareToDateRange,
  gainers: byChange.filter((row) => row.change > 0).slice(0, KEEP),
  decliners: byChange.filter((row) => row.change < 0).slice(-KEEP).reverse()
};
