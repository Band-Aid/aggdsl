// Which live guides reach many people but are ignored or dismissed?
const MIN_VISITORS = 50;
const KEEP = 5;
const dateRange = { type: 'relative', lastNDays: 30 };

// Two full tables (up to 200 guides each) that never need to enter the conversation.
const [effectiveness, usage] = await Promise.all([
  pendo.guideEffectivenessMetrics({ dateRange, status: 'public', limit: 200 }),
  pendo.aggregateGuideMetrics({ dateRange, status: 'public', limit: 200 })
]);
const usageById = new Map(usage.rows.map((row) => [row.entityId, row]));

const guides = effectiveness.rows
  .map((row) => {
    const everyone = row.segments.everyone;
    const raw = usageById.get(row.entityId);
    return {
      guide: row.entityName,
      launch: row.launchMethod,
      visitors: everyone.totalVisitors,
      engagementPct: everyone.engagementRate,
      completionPct: everyone.completionRate ?? null, // absent for single-step guides
      dismissPct: raw && raw.totalViews ? Math.round((1000 * raw.totalDismissals) / raw.totalViews) / 10 : null,
      viewsPerVisitor: raw ? Math.round(raw.viewsPerUser * 10) / 10 : null
    };
  })
  .filter((guide) => guide.visitors >= MIN_VISITORS);

return {
  dateRange: effectiveness.meta.dateRange,
  publicGuides: effectiveness.meta.guideCount,
  audited: guides.length,
  lowestEngagement: [...guides].sort((a, b) => a.engagementPct - b.engagementPct).slice(0, KEEP),
  mostDismissed: [...guides].sort((a, b) => (b.dismissPct ?? 0) - (a.dismissPct ?? 0)).slice(0, KEEP),
  // Shown repeatedly to the same people: a sign of a guide that keeps reappearing.
  mostRepeated: [...guides].sort((a, b) => (b.viewsPerVisitor ?? 0) - (a.viewsPerVisitor ?? 0)).slice(0, KEEP)
};
