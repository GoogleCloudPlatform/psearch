-- Issue #8 seed (run AFTER 017_delivery_interleaved.sql). Mirrors services.DefaultInventory().
-- Statement 1 (skus):
INSERT OR UPDATE INTO skus (product_id, sku_id, seller_id) VALUES
  ('prod-001', 'prod-001-std', 'seller-psearch'),
  ('prod-002', 'prod-002-std', 'seller-psearch'),
  ('prod-003', 'prod-003-std', 'seller-marketplace-nike'),
  ('prod-004', 'prod-004-std', 'seller-psearch'),
  ('prod-005', 'prod-005-std', 'seller-psearch'),
  ('prod-006', 'prod-006-std', 'seller-marketplace-acer'),
  ('prod-007', 'prod-007-std', 'seller-psearch'),
  ('prod-008', 'prod-008-std', 'seller-psearch'),
  ('prod-009', 'prod-009-std', 'seller-marketplace-sony'),
  ('prod-010', 'prod-010-std', 'seller-psearch'),
  ('prod-011', 'prod-011-std', 'seller-psearch'),
  ('prod-012', 'prod-012-std', 'seller-marketplace-lg'),
  ('prod-013', 'prod-013-std', 'seller-psearch'),
  ('prod-014', 'prod-014-std', 'seller-psearch'),
  ('prod-015', 'prod-015-std', 'seller-marketplace-keychron')
