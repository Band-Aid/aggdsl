// Discovery-first example: no hard-coded Pendo Feature or App IDs.
// Change the search phrase to match a unique tagged feature in your subscription.
// Requires PENDO_INTEGRATION_KEY, including in --dry-run mode for catalog access.
const matches = await pendo.catalog.search({
  type: 'features', query: 'checkout', limit: 10
});
if (matches.items.length !== 1) {
  return {
    status: 'needs-disambiguation',
    matches: matches.items,
    note: 'Use a more specific name or scope by appId; do not guess.'
  };
}
const feature = matches.items[0];
if (!feature.appId) {
  return { status: 'missing-app-id', feature };
}
const dsl = [
  'FROM event([source=featureEvents, appId=' + feature.appId +
    ', featureId="' + feature.id + '", blacklist="apply"])',
  'TIMESERIES period=dayRange first=now() count=-30',
  '| group by featureId fields { clicks=sum(numEvents) }',
  '| limit 1'
].join('\n');
const validation = await pendo.aggdsl.validate(dsl);
if (!validation.valid || validation.warnings.length) {
  return { status: 'needs-query-review', validation, dsl };
}
const result = await pendo.aggdsl.run(dsl);
return {
  status: 'executed',
  feature: { id: feature.id, name: feature.name, appId: feature.appId },
  aggregation: result
};
