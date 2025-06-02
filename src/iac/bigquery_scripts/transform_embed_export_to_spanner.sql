-- 
-- Copyright 2025 Google LLC
-- 
-- Licensed under the Apache License, Version 2.0 (the "License");
-- you may not use this file except in compliance with the License.
-- You may obtain a copy of the License at
-- 
--     https://www.apache.org/licenses/LICENSE-2.0
-- 
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.

-- Transforms raw product data, generates embeddings from product description, and exports directly to Cloud Spanner
-- in a single BigQuery SQL statement.

CREATE OR REPLACE PROCEDURE psearch.transform_and_export_to_spanner(
  in_project_id STRING,
  in_spanner_instance STRING,
  in_spanner_database STRING
)
BEGIN
  DECLARE dynamic_columns_for_json STRING;
  DECLARE final_query STRING;

  -- 1. Get all column names from the raw table (psearch.products)
  -- and format them as raw.column_name for the STRUCT
  SET dynamic_columns_for_json = (
    SELECT
      STRING_AGG(CONCAT('raw.', column_name), ', ' ORDER BY ordinal_position)
    FROM
      psearch.INFORMATION_SCHEMA.COLUMNS
    WHERE
      table_schema = 'psearch' AND table_name = 'products'
  );

  -- If no columns are found (should not happen for an existing table), handle it
  IF dynamic_columns_for_json IS NULL THEN
    SET dynamic_columns_for_json = '';
  END IF;

  -- 2. Construct the main query using the dynamic column list
  SET final_query = FORMAT("""
EXPORT DATA OPTIONS (
  uri="https://spanner.googleapis.com/projects/%s/instances/%s/databases/%s",
  format='CLOUD_SPANNER',
  spanner_options='{"table": "products", "priority": "HIGH"}' -- Note: Using single quotes for JSON string for easier formatting
)
AS (
  WITH
    transformed_data AS (
      SELECT
        raw.id AS product_id,
        raw.name AS title,
        TO_JSON(STRUCT(
          %s
        )) AS product_data_json,
        raw.description AS content_to_embed
      FROM
        psearch.products AS raw
    ),
    embeddings_generated AS (
      SELECT
        ml_generate_text_embedding_result.product_id,
        td.title,
        td.product_data_json,
        ml_generate_text_embedding_result.text_embedding AS embedding_array
      FROM
        ML.GENERATE_TEXT_EMBEDDING (
          MODEL psearch.embedding_model,
          (
            SELECT
              product_id,
              content_to_embed AS content
            FROM
              transformed_data
          ),
          STRUCT(TRUE AS flatten_json_output)
        ) AS ml_generate_text_embedding_result
      JOIN
        transformed_data AS td
      ON
        ml_generate_text_embedding_result.product_id = td.product_id
    )
  SELECT
    eg.product_id,
    eg.title,
    eg.product_data_json AS product_data,
    eg.embedding_array AS embedding
  FROM
    embeddings_generated eg
);
""", in_project_id, in_spanner_instance, in_spanner_database, dynamic_columns_for_json);

  -- For debugging:
  SELECT final_query;

  -- 3. Execute the dynamically constructed query
  EXECUTE IMMEDIATE final_query;

END;