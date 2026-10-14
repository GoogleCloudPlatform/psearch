CREATE TABLE embedding_cache (
    content_sha256 STRING(64) NOT NULL,
    model_id STRING(128) NOT NULL,
    dimensions INT64 NOT NULL,
    embedding ARRAY<FLOAT32>,
    created_at TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp=true)
) PRIMARY KEY (content_sha256, model_id);
