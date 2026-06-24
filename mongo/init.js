// Inicialización de MongoDB para GDELT Analytics
db = db.getSiblingDB('gdelt_analytics');

db.createUser({
  user: 'gdelt_user',
  pwd: 'gdelt_read',
  roles: [{ role: 'readWrite', db: 'gdelt_analytics' }]
});

// Colecciones de resultados (datos pre-calculados por Spark)
const collections = [
  'conflict_heatmap',
  'top_countries_events',
  'tone_sources_correlation',
  'cameo_by_region',
  'actor_interaction_matrix',
  'media_coverage',
  'sentiment_trend',
  'conflict_country_pairs',
  'escalation_events',
  'religion_conflict_clusters',
  'gkg_themes_by_continent',
  'top_organizations',
  'tone_lag_analysis',
  'diplomatic_conflict_graph',
  'source_diversity_index',
  'ethnic_conflict_frequency',
  'breaking_news',
  'quadclass_timeline',
  'hourly_event_density',
  'pipeline_metadata',
];

collections.forEach(function(col) {
  db.createCollection(col);
});

// Índices para consultas del dashboard
db.conflict_heatmap.createIndex({ event_date: 1, country_code: 1 });
db.top_countries_events.createIndex({ event_date: 1, rank: 1 });
db.sentiment_trend.createIndex({ country_code: 1, event_date: 1 });
db.top_organizations.createIndex({ event_date: 1, rank: 1 });
db.pipeline_metadata.createIndex({ last_run: -1 });

print('GDELT Analytics DB inicializada correctamente');
