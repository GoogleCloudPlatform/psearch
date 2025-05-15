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

EXPORT DATA OPTIONS (
  uri="https://spanner.googleapis.com/projects/${YOUR_PROJECT_ID}/instances/${YOUR_SPANNER_INSTANCE}/databases/${YOUR_SPANNER_DATABASE}",
  format='CLOUD_SPANNER',
  spanner_options="""{
    "table": "products",
    "priority": "HIGH"
  }"""
)
AS (
  WITH
    transformed_data AS (
      SELECT
        raw.id AS product_id,
        raw.name AS title,
        TO_JSON(STRUCT(
          raw.name,  
          raw.id,  
          raw.type,  
          raw.primaryProductId, 
          raw.collectionMemberIds, 
          raw.gtin,  
          raw.categories,  
          raw.title,  
          raw.brands,  
          raw.description,  
          raw.languageCode,  
          raw.attributes,  
          raw.tags, 
          raw.priceInfo, 
          raw.rating,
          raw.expireTime,
          raw.ttl,
          raw.availableTime,
          raw.availability,
          raw.availableQuantity,
          raw.fulfillmentInfo,
          raw.uri,
          raw.images, 
          raw.audience,
          raw.colorInfo,
          raw.sizes,
          raw.materials,
          raw.patterns,
          raw.conditions,
          raw.retrievableFields, 
          raw.publishTime, 
          raw.promotions, 
          raw.attributes, 
          raw.priceInfo,
          raw.images
        )) AS product_data_json,
        raw.description AS content_to_embed
      FROM
        `psearch.products` AS raw
    ),
    embeddings_generated AS (
      SELECT
        ml_generate_text_embedding_result.product_id,
        td.title,
        td.product_data_json,
        ml_generate_text_embedding_result.text_embedding AS embedding_array
      FROM
        ML.GENERATE_TEXT_EMBEDDING (
          MODEL `psearch.embedding_model`,
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
