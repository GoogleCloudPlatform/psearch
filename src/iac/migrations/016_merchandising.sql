ALTER TABLE products ADD COLUMN clicks INT64;
ALTER TABLE products ADD COLUMN orders INT64;
ALTER TABLE products ADD COLUMN discount_pct FLOAT64;
ALTER TABLE products ADD COLUMN popularity_score FLOAT64;
ALTER TABLE products ADD COLUMN margin_pct FLOAT64;
ALTER TABLE products ADD COLUMN ctr FLOAT64;
ALTER TABLE products ADD COLUMN conversion_rate FLOAT64;
ALTER TABLE products ADD COLUMN recency_score FLOAT64;
CREATE TABLE merchandising_rules (
  tenant_id STRING(64) NOT NULL,
  rule_id STRING(64) NOT NULL,
  name STRING(256),
  query_pattern STRING(256),
  target_brand STRING(256),
  target_category STRING(256),
  boost_multiplier FLOAT64,
  pin_product_ids ARRAY<STRING(1024)>,
  bury_product_ids ARRAY<STRING(1024)>,
  active BOOL,
  updated_at TIMESTAMP OPTIONS (allow_commit_timestamp = true)
) PRIMARY KEY (tenant_id, rule_id)
