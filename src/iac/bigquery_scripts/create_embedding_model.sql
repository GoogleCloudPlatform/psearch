-- Issue #11 / ADR-001: gemini-embedding-2 (GA 2026-04-22) replaces text-multilingual-embedding-002
-- (retires 2027-04-01). Vectors are requested at 1024 dims and written to products.embedding_v2.
CREATE OR REPLACE MODEL psearch.embedding_model
REMOTE WITH CONNECTION `projects/${YOUR_PROJECT_ID}/locations/${YOUR_REGION}/connections/${YOUR_CONNECTION_ID}`
OPTIONS (
  ENDPOINT = "gemini-embedding-2"
);