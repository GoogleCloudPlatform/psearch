-- aggregate top search queries from analytics and export them
-- into Spanner autocomplete_suggestions (n-gram indexed). Schedule daily.
-- Expects psearch_dataset.search_query_logs(tenant_id, locale, query, clicked BOOL, ts TIMESTAMP).
EXPORT DATA OPTIONS (
  uri = 'https://spanner.googleapis.com/projects/${PROJECT_ID}/instances/psearch-instance/databases/psearch-db',
  format = 'CLOUD_SPANNER',
  spanner_options = """{ "table": "autocomplete_suggestions" }"""
) AS
SELECT
  IFNULL(tenant_id, 'default') AS tenant_id,
  IFNULL(locale, 'pt-BR') AS locale,
  LOWER(TRIM(query)) AS suggestion_text,
  REGEXP_REPLACE(NORMALIZE(LOWER(TRIM(query)), NFD), r'\p{M}', '') AS suggestion_folded,
  COUNT(*) AS search_count,
  SAFE_DIVIDE(COUNTIF(clicked), COUNT(*)) AS ctr_score,
  CURRENT_TIMESTAMP() AS updated_at
FROM `psearch_dataset.search_query_logs`
WHERE ts >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 DAY)
  AND LENGTH(TRIM(query)) BETWEEN 2 AND 256
GROUP BY 1, 2, 3
HAVING COUNT(*) >= 3
QUALIFY ROW_NUMBER() OVER (PARTITION BY tenant_id, locale ORDER BY ctr_score DESC, search_count DESC) <= 50000
