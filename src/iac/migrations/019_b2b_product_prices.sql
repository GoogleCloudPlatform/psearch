CREATE TABLE product_prices (
  product_id STRING(1024) NOT NULL,
  price_table_id STRING(64) NOT NULL,
  tenant_id STRING(64) NOT NULL,
  price FLOAT64 NOT NULL,
  currency_code STRING(8),
  min_quantity INT64,
  is_exclusive BOOL
) PRIMARY KEY (product_id, price_table_id),
  INTERLEAVE IN PARENT products ON DELETE CASCADE
