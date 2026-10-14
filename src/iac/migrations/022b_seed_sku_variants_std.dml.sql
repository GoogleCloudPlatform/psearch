-- Issue #32 seed, step 1 (run AFTER 022_sku_variants.sql): the existing <id>-std SKU becomes the first variant.
-- Mirrors services.DefaultVariants(); regional_inventory keeps referencing the -std SKUs.
UPDATE skus SET
  color = CASE sku_id WHEN 'prod-001-std' THEN 'preto' WHEN 'prod-002-std' THEN 'preto' WHEN 'prod-003-std' THEN 'preto' WHEN 'prod-004-std' THEN 'preto' WHEN 'prod-005-std' THEN 'prata' WHEN 'prod-006-std' THEN 'prata' WHEN 'prod-007-std' THEN 'prata' WHEN 'prod-008-std' THEN 'preto' WHEN 'prod-009-std' THEN 'preto' WHEN 'prod-010-std' THEN 'prata' END,
  size = CASE sku_id WHEN 'prod-001-std' THEN '38' WHEN 'prod-002-std' THEN '38' WHEN 'prod-003-std' THEN '38' WHEN 'prod-004-std' THEN '38' WHEN 'prod-005-std' THEN NULL WHEN 'prod-006-std' THEN NULL WHEN 'prod-007-std' THEN NULL WHEN 'prod-008-std' THEN NULL WHEN 'prod-009-std' THEN NULL WHEN 'prod-010-std' THEN NULL END,
  stock = CASE sku_id WHEN 'prod-001-std' THEN 0 WHEN 'prod-002-std' THEN 7 WHEN 'prod-003-std' THEN 5 WHEN 'prod-004-std' THEN 0 WHEN 'prod-005-std' THEN 1 WHEN 'prod-006-std' THEN 8 WHEN 'prod-007-std' THEN 6 WHEN 'prod-008-std' THEN 4 WHEN 'prod-009-std' THEN 2 WHEN 'prod-010-std' THEN 0 END,
  is_available = TRUE
WHERE sku_id IN ('prod-001-std', 'prod-002-std', 'prod-003-std', 'prod-004-std', 'prod-005-std', 'prod-006-std', 'prod-007-std', 'prod-008-std', 'prod-009-std', 'prod-010-std')

