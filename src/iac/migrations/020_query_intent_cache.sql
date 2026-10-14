CREATE TABLE query_intent_cache (
  query_hash STRING(64) NOT NULL,
  query_text STRING(512),
  intent_json JSON,
  source STRING(16),
  created_at TIMESTAMP OPTIONS (allow_commit_timestamp = true)
) PRIMARY KEY (query_hash)
