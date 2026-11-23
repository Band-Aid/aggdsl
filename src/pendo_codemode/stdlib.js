// High-level Pendo functions available to programs as `lib`. Each one makes the
// several tool calls a common question needs and returns a small, fixed-shape
// result, so a program does not have to know the raw tools' result shapes.
const lib = (() => {
  const range = (days) => ({ type: 'relative', lastNDays: days });
  const pct = (part, whole) => (whole ? Math.round((1000 * part) / whole) / 10 : null);
  const app = (appId) => (appId ? { appId: String(appId) } : {});
  const kindOf = (entity) => entity.kind || entity.type;

  async function find(query, { appId, types = ['page', 'feature', 'trackEvent'], limit = 8, match = 'auto' } = {}) {
    const run = (searchType) =>
      Promise.all(
        types.map((type) =>
          pendo
            .listPagesFeaturesAndEvents({ ...app(appId), type, search: query, searchType, limit })
            .then((rows) => rows.map((e) => ({ type, id: e.id, name: e.name, ...(e.pageId ? { pageId: e.pageId } : {}) })))
        )
      ).then((lists) => lists.flat());
    if (match !== 'auto') return run(match);
    const byName = await run('substring');
    return byName.length ? byName : run('semantic');
  }

  async function frustration({ appId, days = 30, top = 3 } = {}) {
    const base = { ...app(appId), dateRange: range(days) };
    const rank = (entityType, sortBy) => pendo.aggregateEntityUsage({ ...base, entityType, sortBy, limit: 5 });
    const [rage, dead, error, pages] = await Promise.all([
      rank('feature', 'totalRageClickCount'),
      rank('feature', 'totalDeadClickCount'),
      rank('feature', 'totalErrorClickCount'),
      rank('page', 'totalUTurnCount')
    ]);
    const signals = [
      ['rage', 'totalRageClickCount', rage],
      ['dead', 'totalDeadClickCount', dead],
      ['error', 'totalErrorClickCount', error]
    ];
    const picks = signals.flatMap(([signal, metric, data]) =>
      data.rows.filter((row) => row[metric] > 0).slice(0, top).map((row) => ({ signal, count: row[metric], row }))
    );
    const ids = [...new Set(picks.map((p) => p.row.entityId))];
    const worst = pages.rows[0];
    const [details, page] = await Promise.all([
      Promise.all(ids.map((entityId) => pendo.getEntity({ entityType: 'feature', entityId }))),
      worst ? pendo.getEntity({ entityType: 'page', entityId: worst.entityId }) : null
    ]);
    const byId = new Map(details.map((d) => [d.id, d]));
    return {
      dateRange: rage.meta.dateRange,
      totals: Object.fromEntries(signals.map(([signal, metric, data]) => [signal, data.summary[metric] ?? 0])),
      elements: picks.map((p) => {
        const d = byId.get(p.row.entityId) || {};
        return { signal: p.signal, name: p.row.entityName, count: p.count, selectors: d.elementPathRules || [], page: d.pageName || null };
      }),
      pages: pages.rows.slice(0, top).map((row, i) => ({
        name: row.entityName,
        ...(i === 0 && page ? { urlRules: page.rules || [] } : {}),
        rage: row.totalRageClickCount ?? 0,
        dead: row.totalDeadClickCount ?? 0,
        uTurns: row.totalUTurnCount ?? 0
      }))
    };
  }

  async function funnel(steps, { appId, days = 30 } = {}) {
    const result = await pendo.queryFunnel({
      ...app(appId),
      dateRange: range(days),
      steps: steps.map((step) => ({ kind: kindOf(step), id: step.id }))
    });
    const s = result.summary;
    return {
      visitors: s.steps.map((step) => step.visitors),
      keptPct: s.steps.slice(1).map((step) => pct(1 - step.dropOffFromPrevious, 1)),
      overallPct: pct(s.overallConversion, 1),
      medianBetween: s.steps.slice(0, -1).map((step) => (step.medianTimeToNextStep ? step.medianTimeToNextStep.display : null)),
      medianTotal: s.medianTimeToCompletion ? s.medianTimeToCompletion.display : null
    };
  }

  async function visitors(entity, { days = 30 } = {}) {
    const usage = await pendo.entityUsage({
      entityType: kindOf(entity), entityId: entity.id, dateRange: range(days), groupBy: 'visitor', limit: 200
    });
    const total = usage.summary.totalNumVisitors;
    return { ids: usage.rows.map((row) => row.visitorId), total, capped: total > usage.rows.length };
  }

  // Of the visitors who did `among`, compare those who also did `did` with those who did not.
  async function cohorts({ among, did, behaviours = [], days = 30 }) {
    const [all, done, ...each] = await Promise.all([among, did, ...behaviours].map((e) => visitors(e, { days })));
    const doneSet = new Set(done.ids);
    const completers = all.ids.filter((id) => doneSet.has(id));
    const others = all.ids.filter((id) => !doneSet.has(id));
    return {
      completers: completers.length,
      others: others.length,
      behaviours: behaviours.map((b, i) => {
        const set = new Set(each[i].ids);
        return {
          name: b.name || b.id,
          completersPct: pct(completers.filter((id) => set.has(id)).length, completers.length),
          othersPct: pct(others.filter((id) => set.has(id)).length, others.length)
        };
      }),
      capped: [all, done, ...each].some((v) => v.capped)
    };
  }

  async function guidePolls({ appId, days = 30 } = {}) {
    const guides = await pendo.listGuides({ ...app(appId), expand: true, limit: 500, offset: 0 });
    const withPolls = guides.filter((g) => (g.steps || []).some((s) => (s.pollIds || s.polls || []).length));
    const responses = await Promise.all(
      withPolls.map((g) => pendo.guidePollResponses({ guideId: g.id, dateRange: range(days), limit: 200 }).catch((e) => ({ error: e.message })))
    );
    return withPolls.map((g, i) => {
      const r = responses[i];
      if (r.error) return { guide: g.name, state: g.state, error: r.error };
      return {
        guide: g.name,
        state: g.state,
        respondents: r.rows.length,
        polls: r.summary.polls.map((poll) => ({
          question: poll.question,
          // Free-text answers are counted, never returned.
          answers: poll.freeText
            ? { freeText: r.rows.filter((row) => row[poll.pollId] != null).length }
            : Object.fromEntries((poll.distribution || []).map((d) => [d.response, d.count]))
        }))
      };
    });
  }

  return Object.freeze({ find, frustration, funnel, visitors, cohorts, guidePolls });
})();
