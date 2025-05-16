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
from ..common.schema_utils import SchemaLoader # To get default schema if not provided

logger = logging.getLogger(__name__)

class InitialSQLGenerator:
    """
    Generates the initial syntactically-focused SQL transformation script.
    Corresponds to Step 1 of the multi-step SQL generation strategy.
    """

    def __init__(self, project_id: str, location: str, model_name: Optional[str] = None):
        """
        Initializes the InitialSQLGenerator.

        Args:
            project_id: The Google Cloud Project ID.
            location: The GCP region (e.g., us-central1).
            model_name: Optional. The Gemini model name.
        """
        self.genai_client = GenAIClient(project_id, location, model_name)
        # Default destination schema can be loaded if not provided in generate method
        self.default_destination_schema = SchemaLoader.get_destination_schema()

    def _construct_prompt(
        self,
        source_table_name: str,
        destination_table_name: str,
        source_schema_fields: List[str],
        destination_schema: Dict[str, Any]
    ) -> str:
        """Constructs the prompt for initial SQL generation."""
        
        formatted_destination_schema = json.dumps(destination_schema, indent=2)
        formatted_source_fields = ", ".join(f"`{field}`" for field in source_schema_fields) # Add backticks for clarity

        prompt = rf"""You are an expert GoogleSQL engineer specializing in BigQuery transformations.
Your primary goal is to generate a syntactically valid and executable BigQuery GoogleSQL script.
This script will transform data from a source table to a destination table, precisely matching the destination schema structure.
Focus on syntactic correctness for BigQuery and complete schema coverage. Do NOT perform semantic guessing or complex logic at this stage.

SOURCE TABLE NAME: `{source_table_name}`
SOURCE SCHEMA FIELDS (available columns in source): [{formatted_source_fields}]
DESTINATION TABLE NAME: `{destination_table_name}`
DESTINATION SCHEMA (target structure):
```json
{formatted_destination_schema}
```

MANDATORY BigQuery GoogleSQL SYNTAX AND FORMATTING:
1. The script MUST start exactly with `CREATE OR REPLACE TABLE \`{destination_table_name}\` AS SELECT ...`.
   - There MUST be exactly one space after `TABLE` and before the first backtick (`\``).
   - There MUST be exactly one space after the closing backtick (`\``) of the table name and before `AS`.
   - Example of CORRECT start: `CREATE OR REPLACE TABLE \`my_project.my_dataset.my_table\` AS SELECT`
   - Example of INCORRECT start: `CREATE OR REPLACE TABLE\`my_project.my_dataset.my_table\`AS SELECT`
2. All BigQuery GoogleSQL keywords (SELECT, FROM, WHERE, AND, OR, AS, CAST, STRUCT, IFNULL, SAFE_CAST, etc.) MUST be surrounded by single spaces.
3. Use BigQuery-specific functions and data types (e.g., `SAFE_CAST` for robust type conversions, `TIMESTAMP`, `DATE`, `GEOGRAPHY`, `NUMERIC`, `STRUCT`, `ARRAY`).
4. Do NOT use backticks around nested field references (e.g., `source.priceInfo.cost` is correct, NOT `source.\`priceInfo\`.\`cost\``).

MAPPING AND DEFAULTING RULES:
1. Direct Name Mapping: If a destination field name (from DESTINATION SCHEMA) matches a source field name in SOURCE SCHEMA FIELDS (case-insensitive), map it directly.
   Example: `source.someField AS someField`

2. Basic Type-Correct Defaults: For any field in the DESTINATION SCHEMA that does not have a direct case-insensitive name match in SOURCE SCHEMA FIELDS:
   - Apply a basic, BigQuery type-correct default value. Examples:
     - STRING, TIMESTAMP, DATE, GEOGRAPHY: `NULL`
     - INT64, NUMERIC, FLOAT64, BIGNUMERIC: `0`
     - ARRAY: `[]` (an empty array)
     - BOOL: `FALSE`
     - STRUCT: A `STRUCT()` constructor with all its sub-fields also set to their respective basic defaults (e.g., `STRUCT(field1 AS NULL, field2 AS 0)`).
   - Add a comment for each defaulted field: `-- Defaulted [destination_field_name] to [default_value] as no direct source match found.`

3. Type Compatibility: Ensure type compatibility when mapping fields. Use SAFE_CAST where needed.
   Example: `SAFE_CAST(source.price_string AS FLOAT64) AS price`

4. Complete Coverage: Ensure EVERY field defined in the DESTINATION SCHEMA is present in the SELECT statement of your generated query.

Your response MUST be only the complete BigQuery GoogleSQL script. Do not include any explanatory text, markdown formatting, or anything else outside the SQL script itself.
"""
        return prompt

    def _apply_programmatic_fixes(self, sql_query: str) -> str:
        """Applies programmatic fixes to the generated SQL query, e.g., for formatting."""
        if not sql_query:
            return ""
            
        # Fix for CREATE OR REPLACE TABLE formatting (taken from original SQLTransformationService)
        # Ensure one space after TABLE and before backtick, and one space after backtick and before AS
        # Regex to find `CREATE OR REPLACE TABLE \`...\` AS` with potentially wrong spacing
        # Corrected: TABLE `name` AS
        sql_query = re.sub(
            r"CREATE\s+OR\s+REPLACE\s+TABLE\s*`([^`]+)`\s*AS",
            lambda m: f"CREATE OR REPLACE TABLE `{m.group(1)}` AS", # Ensures correct spacing around backticks
            sql_query, count=1, flags=re.IGNORECASE
        )
        # Further ensure single space after TABLE if backtick immediately follows
        sql_query = re.sub(
            r"(CREATE\s+OR\s+REPLACE\s+TABLE)\s+(?=`)" , # Positive lookahead for backtick
            r"\1 ", # Ensures single space
            sql_query, count=1, flags=re.IGNORECASE
        )
        # Further ensure single space after closing backtick if AS immediately follows
        sql_query = re.sub(
            r"(?<=`)\s+(AS\s+SELECT)", # Positive lookbehind for backtick
            r" \1", # Ensures single space
            sql_query, count=1, flags=re.IGNORECASE
        )

        # Specific fix for leading double backticks or single backtick if not markdown
        if sql_query.startswith("``"):
            sql_query = sql_query[2:]
        elif sql_query.startswith("`") and not sql_query.startswith("```"): # Avoid stripping markdown
            sql_query = sql_query[1:]
            
        return sql_query

    def generate(
        self,
        source_table_name: str,
        destination_table_name: str,
        source_schema_fields: List[str],
        destination_schema: Optional[Dict[str, Any]] = None
    ) -> Tuple[Optional[str], Optional[str]]:
        """
        Generates the initial SQL transformation script.

        Args:
            source_table_name: The BigQuery source table ID (project.dataset.table).
            destination_table_name: The BigQuery destination table ID (project.dataset.table).
            source_schema_fields: A list of field names available in the source table.
            destination_schema: The JSON schema of the destination table. If None, uses default.

        Returns:
            A tuple containing:
            - sql_query (Optional[str]): The generated SQL query.
            - error_message (Optional[str]): An error message if generation failed.
        """
        current_destination_schema = destination_schema or self.default_destination_schema
        if not current_destination_schema:
            err_msg = "No destination schema provided and no default schema loaded."
            logger.error(err_msg)
            return None, err_msg
        
        logger.info(f"Generating initial SQL transformation from '{source_table_name}' to '{destination_table_name}'")

        prompt = self._construct_prompt(
            source_table_name,
            destination_table_name,
            source_schema_fields,
            current_destination_schema
        )

        # Configure for direct text output, no function calling for this step
        # Max output tokens might need to be high for complex schemas.
        generation_config = GenerateContentConfig(
            temperature=0.2, # Low temperature for deterministic output
            max_output_tokens=32768, # Increased from 8192, SQL can be long
            top_p=0.95,
            top_k=40
        )

        text_response, _, error_message, finish_reason = self.genai_client.generate_content(
            prompt_text=prompt,
            generation_config_override=generation_config,
            tools=None # No tools for initial generation, expect direct SQL
        )

        if error_message:
            logger.error(f"Initial SQL generation failed: {error_message}")
            return None, error_message
        
        if not text_response:
            # This case should ideally be caught by error_message from generate_content
            err_msg = "No text response received from GenAI for initial SQL generation."
            logger.error(err_msg)
            return None, err_msg

        # The plan is to get direct SQL, but GenAIClient.extract_sql_from_text handles potential markdown
        sql_query = GenAIClient.extract_sql_from_text(text_response)

        if not sql_query:
            err_msg = f"Could not extract SQL from GenAI response. Finish Reason: {finish_reason.name if finish_reason else 'UNKNOWN'}. Response text: {text_response[:500]}..."
            logger.error(err_msg)
            # Fallback: Try to use the text_response directly if it looks like SQL but extract_sql_from_text failed
            # This handles cases where extract_sql_from_text is too strict or model doesn't use markdown
            if text_response.strip().upper().startswith(("CREATE OR REPLACE TABLE", "SELECT")):
                logger.warning("Using raw text_response as SQL query due to extraction failure.")
                sql_query = text_response.strip()
            else:
                return None, err_msg
        
        # Apply programmatic fixes
        sql_query = self._apply_programmatic_fixes(sql_query)

        if not (sql_query.upper().startswith("CREATE OR REPLACE TABLE") or sql_query.upper().startswith("SELECT")):
            err_msg = f"Final SQL content after fixes does not appear to be a valid SQL query: {sql_query[:200]}..."
            logger.error(err_msg)
            return None, err_msg
            
        logger.info(f"Initial SQL transformation generated successfully for '{destination_table_name}'.")
        # logger.debug(f"Generated SQL: \n{sql_query}")
        return sql_query, None


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    # This example requires GOOGLE_APPLICATION_CREDENTIALS and GOOGLE_CLOUD_PROJECT to be set
    project = os.environ.get("GOOGLE_CLOUD_PROJECT")
    if not project:
        logger.error("GOOGLE_CLOUD_PROJECT environment variable not set. Skipping InitialSQLGenerator example.")
    else:
        generator = InitialSQLGenerator(project_id=project, location="us-central1")
        
        # Mock data for testing
        mock_source_table = f"{project}.my_dataset.source_products"
        mock_dest_table = f"{project}.my_dataset.destination_products_catalog"
        mock_source_fields = ["product_ID", "productName", "PriceAmount", "description_text", "stockQty", "categories_list", "isAvailable"]
        
        # Use the default schema loaded by SchemaLoader or provide one
        # For this example, we rely on the default schema.json being present
        # in ../../schema.json relative to this file's location.
        # Ensure you have a schema.json file there for this example to run.
        # Example schema.json content:
        # {
        #   "fields": [
        #     { "name": "id", "type": "STRING", "mode": "REQUIRED" },
        #     { "name": "name", "type": "STRING" },
        #     { "name": "priceInfo", "type": "RECORD", "fields": [
        #       { "name": "price", "type": "FLOAT64" },
        #       { "name": "currencyCode", "type": "STRING" }
        #     ]},
        #     { "name": "description", "type": "STRING" },
        #     { "name": "categories", "type": "STRING", "mode": "REPEATED" },
        #     { "name": "available", "type": "BOOLEAN" },
        #     { "name": "quantity", "type": "INTEGER" }
        #   ]
        # }
        
        # Check if default schema loaded
        if not generator.default_destination_schema:
            logger.error("Default destination schema not loaded. Ensure schema.json exists at the expected location (e.g., src/psearch/gen_ai/services/schema.json). Skipping example.")
        else:
            logger.info(f"Using destination schema with fields: {[f.get('name') for f in generator.default_destination_schema.get('fields', [])]}")
            
            sql, error = generator.generate(
                mock_source_table,
                mock_dest_table,
                mock_source_fields
                # destination_schema will use the default loaded by SchemaLoader
            )

            if error:
                logger.error(f"Error generating SQL: {error}")
            elif sql:
                logger.info("Successfully generated SQL:")
                logger.info(f"\n{sql}\n")
            else:
                logger.error("Generation failed without a specific error message, or SQL was empty.")
