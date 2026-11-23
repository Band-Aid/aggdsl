// Pull poll responses for every guide that has a poll. One high-level call does the lookup and the joins.
const polls = await lib.guidePolls({ days: 30 });
state.guidesWithPolls = polls.map((p) => p.guide);
return polls;
