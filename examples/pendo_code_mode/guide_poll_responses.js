// Pull poll responses: find every guide that has a poll and summarise the answers.
const DAYS = 30;
const dateRange = { type: 'relative', lastNDays: DAYS };

// Expanded listing includes each guide's polls; keep only guides that have one.
const guides = (await pendo.listGuides({ expand: true, limit: 200 })).filter((g) => (g.polls ?? []).length);
state.guidesWithPolls = guides.map((g) => ({ id: g.id, name: g.name, polls: g.polls.map((p) => p.id) }));

const responses = await Promise.all(
  guides.map((g) => pendo.guidePollResponses({ guideId: g.id, dateRange, limit: 200 }).catch((e) => ({ error: e.message })))
);
return guides.map((guide, i) => {
  const r = responses[i];
  if (r.error) return { guide: guide.name, error: r.error };
  return {
    guide: guide.name,
    state: guide.state,
    respondents: r.rows.length,
    polls: r.summary.polls.map((poll) => ({
      question: poll.question,
      // Free-text polls have no distribution; report how many answered and leave the text out.
      answers: poll.freeText
        ? { freeText: r.rows.filter((row) => row[poll.pollId] != null).length }
        : Object.fromEntries(poll.distribution.map((d) => [d.response, d.count]))
    }))
  };
});
