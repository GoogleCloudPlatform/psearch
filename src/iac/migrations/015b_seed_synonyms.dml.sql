-- seed (run AFTER 015_search_synonyms.sql) via gcloud spanner databases execute-sql.
-- Terms/synonyms are lowercased and accent-folded (matches SynonymService normalization).
INSERT OR UPDATE INTO search_synonyms (tenant_id, locale, term, synonyms, weight, updated_at) VALUES
  ('default', 'pt-BR', 'tenis', ['calcado', 'sapato esportivo', 'sneaker', 'running shoes'], 0.3, PENDING_COMMIT_TIMESTAMP()),
  ('default', 'pt-BR', 'notebook', ['laptop', 'computador portatil'], 0.3, PENDING_COMMIT_TIMESTAMP()),
  ('default', 'pt-BR', 'geladeira', ['refrigerador', 'refrigerator', 'fridge'], 0.3, PENDING_COMMIT_TIMESTAMP()),
  ('default', 'pt-BR', 'fone', ['headphones', 'fone de ouvido'], 0.3, PENDING_COMMIT_TIMESTAMP()),
  ('default', 'en-US', 'sneakers', ['shoes', 'running shoes', 'tenis'], 0.3, PENDING_COMMIT_TIMESTAMP()),
  ('default', 'en-US', 'laptop', ['notebook', 'computer'], 0.3, PENDING_COMMIT_TIMESTAMP()),
  ('default', 'en-US', 'fridge', ['refrigerator', 'geladeira'], 0.3, PENDING_COMMIT_TIMESTAMP()),
  ('default', 'en-US', 'tv', ['television', 'smart tv'], 0.3, PENDING_COMMIT_TIMESTAMP())
