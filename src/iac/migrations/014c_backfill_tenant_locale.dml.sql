-- data backfill (run AFTER 014_tenant_locale.sql). product_data uses snake_case 'tenant_id'.
UPDATE products SET
  tenant_id = IFNULL(JSON_VALUE(product_data, '$.tenant_id'), 'default'),
  locale = JSON_VALUE(product_data, '$.locale')
WHERE TRUE
