CREATE TABLE search_synonyms (
  tenant_id STRING(64) NOT NULL,
  locale STRING(16) NOT NULL,
  term STRING(128) NOT NULL,
  synonyms ARRAY<STRING(128)>,
  weight FLOAT64,
  updated_at TIMESTAMP OPTIONS (allow_commit_timestamp = true)
) PRIMARY KEY (tenant_id, locale, term)
