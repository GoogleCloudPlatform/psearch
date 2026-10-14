-- Issue #3 data backfill (run AFTER 012_availability_visibility_channels.sql):
-- gcloud spanner databases execute-sql psearch-db --instance=psearch-instance --project=<PROJECT_ID> --sql="$(cat 012b_backfill_visibility_channels.dml.sql | grep -v '^--')"
UPDATE products SET
  is_visible = TRUE,
  channels = CASE product_id
    WHEN 'prod-001' THEN ['web', 'mobile', 'store']
    WHEN 'prod-002' THEN ['web', 'mobile']
    WHEN 'prod-003' THEN ['web', 'mobile', 'store']
    WHEN 'prod-004' THEN ['web']
    WHEN 'prod-005' THEN ['web', 'mobile']
    WHEN 'prod-006' THEN ['web', 'mobile', 'store']
    WHEN 'prod-007' THEN ['web', 'mobile', 'store']
    WHEN 'prod-008' THEN ['web', 'mobile', 'store']
    WHEN 'prod-009' THEN ['web', 'mobile']
    WHEN 'prod-010' THEN ['web', 'mobile', 'store']
    WHEN 'prod-011' THEN ['web', 'store']
    WHEN 'prod-012' THEN ['web']
    WHEN 'prod-013' THEN ['web', 'mobile', 'store']
    WHEN 'prod-014' THEN ['web', 'mobile', 'store']
    WHEN 'prod-015' THEN ['web', 'mobile']
    ELSE ['web', 'mobile', 'store']
  END
WHERE TRUE
