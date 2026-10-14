CREATE TABLE user_session_events (
  tenant_id STRING(64) NOT NULL,
  session_id STRING(128) NOT NULL,
  event_id STRING(64) NOT NULL,
  product_id STRING(1024) NOT NULL,
  event_type STRING(32) NOT NULL,
  created_at TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp = true)
) PRIMARY KEY (tenant_id, session_id, event_id)
