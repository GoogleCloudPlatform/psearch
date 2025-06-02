CREATE OR REPLACE MODEL psearch.embedding_model
REMOTE WITH CONNECTION `projects/${YOUR_PROJECT_ID}/locations/${YOUR_REGION}/connections/${YOUR_CONNECTION_ID}`
OPTIONS (
  ENDPOINT = "text-multilingual-embedding-002"
);