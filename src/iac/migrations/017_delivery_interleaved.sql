CREATE TABLE skus (
  product_id STRING(1024) NOT NULL,
  sku_id STRING(128) NOT NULL,
  seller_id STRING(128),
  attributes JSON
) PRIMARY KEY (product_id, sku_id),
  INTERLEAVE IN PARENT products ON DELETE CASCADE;
CREATE TABLE regional_inventory (
  product_id STRING(1024) NOT NULL,
  sku_id STRING(128) NOT NULL,
  region_code STRING(16) NOT NULL,
  zip_prefix_start STRING(8),
  zip_prefix_end STRING(8),
  cd_id STRING(64),
  stock INT64,
  sla_hours INT64,
  shipping_cost FLOAT64,
  free_shipping BOOL
) PRIMARY KEY (product_id, sku_id, region_code),
  INTERLEAVE IN PARENT skus ON DELETE CASCADE
