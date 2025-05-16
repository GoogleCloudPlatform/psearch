#
# Copyright 2025 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import logging
import json
import re
from typing import Dict, Any, Optional, List, Tuple

from google.genai.types import GenerateContentConfig, FinishReason

from ..common.client_utils import GenAIClient
from ..common.schema_utils import SchemaLoader # For destination schema if needed

logger = logging.getLogger(__name__)

class SemanticEnhancer:
    """
    Refines an existing SQL query by attempting to semantically map critical fields
    using a sample of source data. Corresponds to Step 3 of the multi-step SQL strategy.
    """

    def __init__(self, project_id: str, location: str, model_name: Optional[str] = None):
        """
        Initializes the SemanticEnhancer.

        Args:
            project_id: The Google Cloud Project ID.
            location: The GCP region (e.g., us-central1).
            model_name: Optional. The Gemini model name.
        """
        self.genai_client = GenAIClient(project_id, location, model_name)
        self.default_destination_schema = SchemaLoader.get_destination_schema()

    def _construct_prompt(
        self,
        current_sql_query: str,
        source_table_name: str,
        source_schema_fields: List[str],
        source_data_sample_json: str,
        destination_schema: Dict[str, Any],
        critical_fields_to_refine: List[str]
    ) -> str:
        """Constructs the prompt for semantic SQL enhancement."""
        
        formatted_destination_schema = json.dumps(destination_schema, indent=2)
        formatted_source_fields = ", ".join(f"`{field}`" for field in source_schema_fields)

        # Ensure source_data_sample_json is indeed a string; if it's already parsed, dump it back.
        # This was in the original SQLTransformationService, good practice.
        if not isinstance(source_data_sample_json, str):
            try:
                source_data_sample_json = json.dumps(source_data_sample_json, indent=2)
            except TypeError as e:
                logger.warning(f"Could not serialize source_data_sample to JSON string: {e}. Using as is.")
                source_data_sample_json = str(source_data_sample_json)


        prompt = rf"""You are a data mapping expert specializing in BigQuery GoogleSQL transformations.
Your task is to refine an existing BigQuery SQL query by improving the mappings for a specific list of critical destination fields.
You will be given the original SQL, source table name, source schema fields, a sample of source data (as a JSON string), the destination schema, and a list of critical fields to refine.

ORIGINAL SQL QUERY:
```sql
{current_sql_query}
```

SOURCE TABLE NAME: `{source_table_name}`
SOURCE SCHEMA FIELDS (available columns in source): [{formatted_source_fields}]
SOURCE DATA SAMPLE (first 3 rows, JSON array string):
```json
{source_data_sample_json}
```
DESTINATION SCHEMA (target structure):
```json
{formatted_destination_schema}
```
CRITICAL DESTINATION FIELDS TO REFINE: {critical_fields_to_refine}

INSTRUCTIONS:
1. For each field listed in CRITICAL DESTINATION FIELDS TO REFINE:
   a. Examine its current mapping in the ORIGINAL SQL QUERY.
   b. If the current mapping is `NULL` or a generic default (like `0`, `""`, `[]`), analyze the SOURCE SCHEMA FIELDS and the SOURCE DATA SAMPLE.
   c. Identify the source field from SOURCE SCHEMA FIELDS that is the best semantic match for the critical destination field, based on its name and the content observed in the SOURCE DATA SAMPLE.
      - Example: If a critical destination field is 'product_name', and the source has 'title' or 'item_description' with relevant text in the sample, choose the best one.
      - Example: If a critical destination field is 'unique_identifier', and the source has 'sku' or 'article_id' with unique-looking values in the sample, choose the best one.
   d. Update the `SELECT` expression for this critical field in the ORIGINAL SQL QUERY to use the identified semantic match.
      - The new expression MUST be valid BigQuery GoogleSQL.
      - Ensure type compatibility with the destination field's type defined in DESTINATION SCHEMA. Use `SAFE_CAST(source.field AS DESTINATION_TYPE)` if necessary.
      - Add a comment explaining the semantic mapping: `-- Semantically mapped [destination_field] from source.[chosen_source_field] based on data sample.`
   e. If, after reviewing the data sample and source schema, no confident semantic match can be made for a critical field, leave its original mapping from the ORIGINAL SQL QUERY as is (e.g., `NULL`), but add a comment: `-- No confident semantic match found for [destination_field] in source data sample.`
2. PRESERVATION: Preserve all other mappings, JOINs, WHERE clauses, and the overall structure of the ORIGINAL SQL QUERY. Only modify the `SELECT` expressions for the fields listed in CRITICAL DESTINATION FIELDS TO REFINE.
3. OUTPUT: Your response MUST be only the complete, modified BigQuery GoogleSQL script. Do not include any explanatory text, markdown formatting, or anything else outside the SQL script itself.

Ensure the final output is a single, valid, and executable BigQuery GoogleSQL query.
"""
        return prompt

    def _apply_programmatic_fixes(self, sql_query: str) -> str:
        """Applies programmatic fixes to the SQL query, e.g., for formatting.
           (Similar to InitialSQLGenerator._apply_programmatic_fixes)
        """
        if not sql_query:
            return ""
        
        sql_query = re.sub(
            r"CREATE\s+OR\s+REPLACE\s+TABLE\s*`([^`]+)`\s*AS",
            lambda m: f"CREATE OR REPLACE TABLE `{m.group(1)}` AS",
            sql_query, count=1, flags=re.IGNORECASE
        )
        sql_query = re.sub(
            r"(CREATE\s+OR\s+REPLACE\s+TABLE)\s+(?=`)" ,
            r"\1 ",
            sql_query, count=1, flags=re.IGNORECASE
        )
        sql_query = re.sub(
            r"(?<=`)\s+(AS\s+SELECT)",
            r" \1",
            sql_query, count=1, flags=re.IGNORECASE
        )
        if sql_query.startswith("``"):
            sql_query = sql_query[2:]
        elif sql_query.startswith("`") and not sql_query.startswith("```"):
            sql_query = sql_query[1:]
        return sql_query

    def enhance_sql(
        self,
        current_sql_query: str,
        source_table_name: str,
        source_schema_fields: List[str],
        source_data_sample_json: str, # Expecting a JSON string
        critical_fields_to_refine: List[str],
        destination_schema: Optional[Dict[str, Any]] = None
    ) -> Tuple[str, Optional[str]]: # Returns (potentially_refined_sql, error_message)
        """
        Refines an SQL query by semantically mapping critical fields.

        Args:
            current_sql_query: The initial SQL query.
            source_table_name: Name of the source BigQuery table.
            source_schema_fields: List of field names in the source schema.
            source_data_sample_json: A JSON string of source data sample.
            critical_fields_to_refine: List of destination field names for semantic review.
            destination_schema: The JSON schema of the destination. Uses default if None.

        Returns:
            A tuple: (refined_sql_query_or_original_on_error, error_message_if_any)
            The first element is the refined SQL, or the original SQL if enhancement fails.
        """
        logger.info(f"Starting semantic enhancement for SQL query targeting table {source_table_name} for fields: {critical_fields_to_refine}")

        current_destination_schema = destination_schema or self.default_destination_schema
        if not current_destination_schema:
            err_msg = "No destination schema provided for semantic enhancement and no default schema loaded."
            logger.error(err_msg)
            return current_sql_query, err_msg # Return original query on error

        prompt = self._construct_prompt(
            current_sql_query,
            source_table_name,
            source_schema_fields,
            source_data_sample_json,
            current_destination_schema,
            critical_fields_to_refine
        )

        generation_config = GenerateContentConfig(
            temperature=0.2, # Lower temperature for more deterministic changes
            max_output_tokens=32768, # SQL can be long
            top_p=0.95,
            top_k=40
        )

        text_response, _, error_message, finish_reason = self.genai_client.generate_content(
            prompt_text=prompt,
            generation_config_override=generation_config,
            tools=None # Expecting direct SQL output
        )

        if error_message:
            logger.error(f"Semantic SQL enhancement GenAI call failed: {error_message}")
            return current_sql_query, error_message # Return original query

        if not text_response:
            err_msg = "No text response received from GenAI for semantic SQL enhancement."
            logger.error(err_msg)
            return current_sql_query, err_msg # Return original query

        refined_sql_query = GenAIClient.extract_sql_from_text(text_response)

        if not refined_sql_query:
            err_msg = f"Could not extract SQL from GenAI response for semantic enhancement. Finish Reason: {finish_reason.name if finish_reason else 'UNKNOWN'}. Response text: {text_response[:500]}..."
            logger.error(err_msg)
            # Fallback as in original code
            if text_response.strip().upper().startswith(("CREATE OR REPLACE TABLE", "SELECT")):
                logger.warning("Using raw text_response as refined SQL query due to extraction failure.")
                refined_sql_query = text_response.strip()
            else:
                return current_sql_query, err_msg # Return original query

        refined_sql_query = self._apply_programmatic_fixes(refined_sql_query)
        
        if not (refined_sql_query.upper().startswith("CREATE OR REPLACE TABLE") or refined_sql_query.upper().startswith("SELECT")):
            err_msg = f"Semantically enhanced SQL content after fixes does not appear to be a valid SQL query: {refined_sql_query[:200]}..."
            logger.error(err_msg)
            return current_sql_query, err_msg # Return original query

        logger.info(f"Successfully performed semantic enhancement on SQL query for table {source_table_name}.")
        return refined_sql_query, None


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    project = os.environ.get("GOOGLE_CLOUD_PROJECT")
    if not project:
        logger.error("GOOGLE_CLOUD_PROJECT environment variable not set. Skipping SemanticEnhancer example.")
    else:
        enhancer = SemanticEnhancer(project_id=project, location="us-central1")

        mock_initial_sql = """CREATE OR REPLACE TABLE `project.dataset.target` AS SELECT
  source.ProductID AS id,
  NULL AS name, -- Defaulted name to NULL as no direct source match found.
  STRUCT(
    NULL AS price, -- Defaulted price to NULL as no direct source match found.
    "USD" AS currencyCode
  ) AS priceInfo,
  source.FullDescription AS description
FROM `project.dataset.source` AS source;"""
        mock_source_table = "project.dataset.source"
        mock_source_fields = ["ProductID", "ItemName", "Cost", "FullDescription", "VendorName"]
        mock_data_sample = json.dumps([
            {"ProductID": "SKU123", "ItemName": "Cool Gadget", "Cost": 19.99, "FullDescription": "A very cool gadget.", "VendorName": "GadgetsRUs"},
            {"ProductID": "SKU456", "ItemName": "Awesome Widget", "Cost": 29.99, "FullDescription": "An awesome widget for all your needs.", "VendorName": "WidgetsInc"}
        ])
        mock_critical_fields = ["name", "priceInfo.price"] # 'id' and 'description' are already mapped

        # Assuming default schema is loaded by SchemaLoader and is appropriate
        if not enhancer.default_destination_schema:
            logger.error("Default destination schema not loaded. Ensure schema.json exists. Skipping example.")
        else:
            logger.info(f"Using destination schema with fields: {[f.get('name') for f in enhancer.default_destination_schema.get('fields', [])]}")

            refined_sql, error = enhancer.enhance_sql(
                current_sql_query=mock_initial_sql,
                source_table_name=mock_source_table,
                source_schema_fields=mock_source_fields,
                source_data_sample_json=mock_data_sample,
                critical_fields_to_refine=mock_critical_fields
            )

            if error:
                logger.error(f"Error enhancing SQL: {error}")
                logger.info("Original SQL (returned due to error):")
                logger.info(f"\n{refined_sql}\n")
            elif refined_sql:
                logger.info("Successfully enhanced SQL:")
                logger.info(f"\n{refined_sql}\n")
            else:
                logger.error("Enhancement failed without specific error, or SQL was empty.")
