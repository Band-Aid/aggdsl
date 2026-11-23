// Pull poll responses: find every guide that has a poll and summarise the answers.
const dateRange = { type: 'relative', lastNDays: 30 };

// Poll ids hang off a guide's steps, and only in the expanded listing.
const guides = (await pendo.listGuides({ expand: true, limit: 500 })).filter((g) =>
  (g.steps ?? []).some((s) => (s.pollIds ?? []).length)
);
state.guidesWithPolls = guides.map((g) => g.name);

// One response query per guide, in parallel; only counts leave the sandbox.
const responses = await Promise.all(
  guides.map((g) => pendo.guidePollResponses({ guideId: g.id, dateRange, limit: 200 }))
);
return guides.map((guide, i) => ({
  guide: guide.name,
  state: guide.state,
  respondents: responses[i].rows.length,
  polls: responses[i].summary.polls.map((poll) => ({
    question: poll.question,
    // Free-text polls have no distribution: count the answers and leave the text out.
    answers: poll.freeText
      ? { freeText: responses[i].rows.filter((row) => row[poll.pollId] != null).length }
      : Object.fromEntries(poll.distribution.map((d) => [d.response, d.count]))
  }))
}));
