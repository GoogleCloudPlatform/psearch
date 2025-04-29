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
import os
from google import genai
from google.genai import types
from typing import Dict, Any, Optional, List, Union

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class SQLTransformationService:
    """Service for generating SQL transformation scripts using GenAI."""
    
    def __init__(self, project_id: str, location: str):
        """Initialize the service with GCP project details.
        
        Args:
            project_id: The Google Cloud Project ID
            location: The GCP region (e.g., us-central1)
        """
        self.project_id = project_id
        self.location = location
        
        # Initialize GenAI client
        self.client = genai.Client(
            vertexai=True,
            project=project_id,
            location=location,
        )
        
        # Set model name for Gemini
        self.model = "gemini-2.5-pro-preview-03-25"
        
        logger.info(f"SQL Transformation Service initialized: {project_id}/{location}")
    
    def generate_sql_transformation(
        self,
        source_table: str,
        destination_table: str,
        destination_schema: Dict[str, Any]
    ) -> str:
        """Generate a SQL transformation script to map data from source to destination schema.
        
        Args:
            source_table: The BigQuery source table ID (project.dataset.table)
            destination_table: The BigQuery destination table ID (project.dataset.table)
            destination_schema: The JSON schema of the destination table
            
        Returns:
            A SQL script that transforms data from the source table to match the destination schema
        """
        logger.info(f"Generating SQL transformation from {source_table} to {destination_table}")
        
        # Format the schema for better readability in the prompt
        formatted_schema = json.dumps(destination_schema, indent=2)
        
        # Construct the prompt for Gemini
        prompt = f"""You are an expert BigQuery SQL engineer. Your task is to generate a BigQuery SQL script that transforms data from a source table to a destination table, precisely matching the destination schema structure and data types.

**Inputs:**

1.  **Source Table:** `{source_table}`
2.  **Destination Table:** `{destination_table}`
3.  **Destination Schema:**
    ```sql
    {formatted_schema}
    ```
    *(This schema definition accurately represents the target structure, including nested fields, arrays, and data types like STRING, INT64, FLOAT64, BOOL, TIMESTAMP, DATE, STRUCT, ARRAY, etc.)*

**Instructions:**

1.  **Generate a BigQuery SQL script** using Google Standard SQL syntax.
2.  **Start the script** with `CREATE OR REPLACE TABLE \`{destination_table}\` AS`.
3.  **Select data** from the source table: `FROM \`{source_table}\``.
4.  **Map Source Columns to Destination Columns:** For each column defined in the `{formatted_schema}`, select the corresponding data from the `{source_table}`.
5.  **Apply Transformations:**
    *   Perform necessary **type conversions** using `CAST` (e.g., `CAST(source_column AS STRING)`).
    *   Handle **NULL values** gracefully. Use `IFNULL()` or `COALESCE()` to provide default values appropriate for the destination type (e.g., `IFNULL(source_string_column, "")`, `IFNULL(source_array_column, [])`, `IFNULL(source_numeric_column, 0)`).
    *   Construct **STRUCTs** and **ARRAYs** as required by the destination schema. Use functions like `STRUCT()`, `ARRAY()`, `[ ]`, `SPLIT()`, etc.
        *   *Example:* To create an array from a single value: `[IFNULL(categories, "N/A")]`
        *   *Example:* To create a complex STRUCT: `STRUCT('npm' as key, STRUCT([CAST(id as STRING)] as text, ARRAY<FLOAT64>[]) as value)`
        *   *Example:* To create an array from a delimited string: `SPLIT(tags, ",")`
        *   *Example:* To create an array of STRUCTs for images: `[STRUCT(IFNULL(uri,"default_url") as uri, 800 as height, 500 as width)]`
    *   Use built-in functions for specific needs, like `FORMAT_DATETIME` for timestamps (e.g., `FORMAT_DATETIME("%FT%R:%E2SZ", CURRENT_DATETIME())`).
    *   Consider if temporary functions (like `FirstN` in the example) are needed for complex array manipulations, but prefer standard SQL functions if possible. If needed, include the `CREATE TEMP FUNCTION` statement *before* the `CREATE OR REPLACE TABLE` statement. *(Self-correction: The prompt requests ONLY the SQL script starting with CREATE OR REPLACE. Let's modify this instruction slightly)* If a temporary function is absolutely necessary for a complex transformation not easily done otherwise, include its definition *within the prompt context* but ensure the final *output* starts strictly with `CREATE OR REPLACE TABLE`. *Better:* Avoid temp functions in the generated script unless explicitly stated they are required and allowed outside the main `CREATE TABLE AS SELECT`. Assume standard SQL is sufficient.
6.  **Handle Missing Source Fields:** If a field required by the destination schema **does not exist** in the source table `{source_table}`, **set its value to `NULL`** in the `SELECT` statement for that column. **Do not fail the script.** Ensure the generated `SELECT` statement includes *all* columns listed in the destination schema, providing `NULL` for those completely missing in the source.
7.  **Strict Schema Adherence:** Only include columns that exist in the `{formatted_schema}` in the final `SELECT` list. The names and order in the `SELECT` list should ideally match the destination schema for clarity.
8.  **Best Practices:**
    *   Use clear aliases if needed for complex expressions.
    *   Add brief comments (`-- comment`) only for non-obvious transformations.
    *   Format the SQL for readability (indentation, line breaks).
    *   **Ensure proper spacing in SQL syntax.** For example:
        * `CREATE OR REPLACE TABLE `products_psearch.psearch` AS` (with spaces between keywords and backticked identifiers)
        * NOT `CREATE OR REPLACE TABLE`products_psearch.psearch`AS` (which has no spaces)
    *   **Do NOT use backticks around nested field references** from the source table. For example:
        * CORRECT: `source.priceInfo.cost` (no backticks around the path)
        * INCORRECT: ``source.priceInfo.cost`` (with backticks around the entire path)
    *   Use source table alias consistently, e.g., `source.field_name` rather than just `field_name` to avoid ambiguity.
9.  **WHERE Clause:** Include a `WHERE` clause *only* if it is absolutely necessary for the core transformation logic (e.g., filtering for specific record types *before* transformation). Generally, avoid it.

**Output Requirements:**

*   **Return ONLY the final BigQuery SQL script.**
*   **Do NOT include any explanations, markdown formatting (like ```sql), introductory phrases ("Here is the script..."), or concluding remarks.**
*   The script must start *exactly* with `CREATE OR REPLACE TABLE \`{destination_table}\` AS`.
*   The script must end *exactly* with the final semicolon of the `SELECT` statement.
"""
        
        try:
            # Generate the SQL script using Gemini
            # Create content structure for prompt
            contents = [types.Content(role="user", parts=[types.Part.from_text(text=prompt)])]
            
            # Set up generation configuration
            generate_content_config = types.GenerateContentConfig(
                temperature=0.2,  # Low temperature for more deterministic output
                max_output_tokens=8192,  # Allow for longer SQL scripts
                top_p=0.95,
                top_k=40,
            )
            
            # Call Gemini model using the new SDK
            response = self.client.models.generate_content(
                model=self.model,
                contents=contents,
                config=generate_content_config,
            )
            
            # Extract and clean the SQL script
            sql_script = response.text.strip()
            
            # Remove any markdown code block indicators that might be in the response
            if sql_script.startswith("```"):
                # Find the first and last occurrence of ```
                first_marker = sql_script.find("```")
                last_marker = sql_script.rfind("```")
                
                if first_marker != last_marker:  # Both markers exist
                    # Extract content between markers
                    content_start = sql_script.find("\n", first_marker) + 1
                    content_end = last_marker
                    sql_script = sql_script[content_start:content_end].strip()
            
            # Check for duplicate CREATE statements
            create_pattern = "CREATE\\s+OR\\s+REPLACE\\s+TABLE"
            import re
            matches = re.findall(create_pattern, sql_script, re.IGNORECASE)
            
            if len(matches) > 1:
                # More than one CREATE statement found, keep only the first one
                logger.warning(f"Found {len(matches)} CREATE statements in generated SQL. Normalizing...")
                
                # Find the position of the second CREATE statement
                first_pos = sql_script.upper().find("CREATE OR REPLACE TABLE")
                second_pos = sql_script.upper().find("CREATE OR REPLACE TABLE", first_pos + 1)
                
                if second_pos > 0:
                    # Extract the part after the second CREATE statement
                    after_second_create = sql_script[second_pos + len("CREATE OR REPLACE TABLE"):].strip()
                    # Find the first SELECT statement after the second CREATE
                    select_pos = after_second_create.upper().find("SELECT")
                    
                    if select_pos >= 0:
                        # Reconstruct SQL with only one CREATE statement
                        sql_script = sql_script[:first_pos] + "CREATE OR REPLACE TABLE " + destination_table + " AS\n" + after_second_create[select_pos:]
            
            # Ensure the SQL has a CREATE statement
            if not re.search(create_pattern, sql_script, re.IGNORECASE):
                logger.warning("Generated SQL does not contain CREATE OR REPLACE TABLE. Adding prefix.")
                sql_script = f"CREATE OR REPLACE TABLE `{destination_table}` AS\n{sql_script}"
            
            # Final cleanup - ensure destination table is properly quoted
            if "`" not in destination_table and destination_table in sql_script:
                sql_script = sql_script.replace(destination_table, f"`{destination_table}`")
            
            # Additional post-processing to fix common formatting issues
            import re
            
            # Fix spacing issues between backticks and SQL keywords
            sql_script = re.sub(r'TABLE`', 'TABLE `', sql_script)
            sql_script = re.sub(r'`AS', '` AS', sql_script)
            sql_script = re.sub(r'FROM`', 'FROM `', sql_script)
            
            # Fix issue with missing space after a backtick
            sql_script = re.sub(r'`([A-Za-z])', '` \\1', sql_script)
            
            # Remove backticks around nested field references
            # This pattern finds backticked field paths with dots (e.g. `source.field.nested`)
            sql_script = re.sub(r'`([a-zA-Z0-9_]+\.[a-zA-Z0-9_\.]+)`', r'\1', sql_script)
            
            # Log the post-processing
            logger.info("Applied SQL post-processing to fix formatting issues")
            logger.info(f"SQL transformation generated successfully: {len(sql_script)} characters")
            logger.debug(f"Generated SQL: {sql_script[:200]}...")
            
            return sql_script
            
        except Exception as e:
            logger.error(f"Error generating SQL transformation: {str(e)}")
            raise Exception(f"Failed to generate SQL transformation: {str(e)}")
    
    def refine_sql_script(self, sql_script: str, error_message: str) -> str:
        """Refine an SQL script that has errors based on the error message.
        
        Args:
            sql_script: The original SQL script that failed
            error_message: The error message returned by BigQuery
            
        Returns:
            A refined SQL script that addresses the errors
        """
        logger.info(f"Refining SQL script based on error: {error_message[:100]}...")
        
        # Detect specific error patterns for tailored handling
        tailored_instructions = ""
        
        # Handle 'Invalid field reference' errors with special attention
        if "Invalid field reference" in error_message and "Field does not exist" in error_message:
            # Extract the field name from the error message if possible
            import re
            field_match = re.search(r"Invalid field reference '([^']+)'", error_message)
            missing_field = field_match.group(1) if field_match else "unknown field"
            
            tailored_instructions = f"""
For this specific 'Invalid field reference' error with missing field '{missing_field}':

1. REPLACE the UNNEST operation on the missing field with a static default value:
   - For array fields: Replace ARRAY(SELECT ... FROM UNNEST({missing_field})) with ARRAY(SELECT CAST('Default' AS STRING))
   - For nested fields: Add explicit handling with comments explaining the default value usage

2. If the missing field is part of a STRUCT definition:
   - Keep the field name in the output but provide a default value that matches the expected type
   - Add a comment indicating this is a default value for a field not in the source

3. For all related fields in the same section (like 'colors' if 'colorFamilies' is missing):
   - Use IFNULL to handle potential nulls: IFNULL(field_name, ['Default'])
   - Wrap UNNEST operations in safety checks to prevent errors

EXAMPLE FIX for colorFamilies error:
```
-- Original problematic code:
STRUCT(
    ARRAY(SELECT CAST(colorFamily AS STRING) FROM UNNEST(colorFamilies) AS colorFamily) AS colorFamilies,
    ARRAY(SELECT CAST(color AS STRING) FROM UNNEST(colors) AS color) AS colors
) AS colorInfo

-- Fixed code:
STRUCT(
    -- Default values for colorFamilies which doesn't exist in source
    ARRAY(SELECT CAST('Default Color' AS STRING)) AS colorFamilies,
    -- Handle colors with NULL checks in case it also doesn't exist
    ARRAY(SELECT CAST(IFNULL(color, 'Unknown') AS STRING) FROM UNNEST(IFNULL(colors, ['Default'])) AS color) AS colors
) AS colorInfo
```
"""

        # Construct the refinement prompt with enhanced guidance and any tailored instructions
        prompt = f"""You are an expert SQL engineer. Fix the following BigQuery SQL script based on the error message.

ERROR MESSAGE:
{error_message}

ORIGINAL SQL SCRIPT:
{sql_script}

{tailored_instructions}

For error messages about "Invalid syntax near X" or "Column not found":
1. Check if the referenced column exists in the source table
2. Verify that all field names and paths in nested STRUCT declarations are valid
3. If a nested field is referenced (like 'attribute.value.text'), ensure all parts of the path exist
4. Fix any issues with UNNEST operations on arrays that might not exist
5. If a field doesn't exist in source, replace with NULL values or appropriate defaults
6. Pay special attention to nested structs and arrays that might require default values

For other syntax errors:
1. Check for proper quoting of table names with backticks
2. Ensure correct CAST operations for each data type
3. Verify that all parentheses are properly balanced
4. Check for missing commas or extra commas in SELECT statements

Please provide a refined version of the SQL script that fixes the issues mentioned in the error message. 
Return ONLY the corrected SQL without any additional explanations, prefixes or suffixes.
The script must start with CREATE OR REPLACE TABLE and be ready to execute in BigQuery.
"""
        
        try:
            # Generate the refined SQL script
            # Create content structure for prompt
            contents = [types.Content(role="user", parts=[types.Part.from_text(text=prompt)])]
            
            # Set up generation configuration
            generate_content_config = types.GenerateContentConfig(
                temperature=0.2,
                max_output_tokens=8192,
                top_p=0.95,
                top_k=40,
            )
            
            # Call Gemini model using the new SDK
            response = self.client.models.generate_content(
                model=self.model,
                contents=contents,
                config=generate_content_config,
            )
            
            # Extract and clean the refined SQL script
            refined_sql = response.text.strip()
            
            # Validation
            if not refined_sql.upper().startswith("CREATE OR REPLACE TABLE"):
                logger.warning("Refined SQL does not start with CREATE OR REPLACE TABLE. Adding prefix.")
                # Extract destination table from original script
                import re
                table_match = re.search(r"CREATE OR REPLACE TABLE\s+`([^`]+)`", sql_script)
                destination_table = table_match.group(1) if table_match else "destination_table"
                refined_sql = f"CREATE OR REPLACE TABLE `{destination_table}` AS\n{refined_sql}"
            
            # Apply the same post-processing to the refined SQL as we do for generated SQL
            import re
            
            # Fix spacing issues between backticks and SQL keywords
            refined_sql = re.sub(r'TABLE`', 'TABLE `', refined_sql)
            refined_sql = re.sub(r'`AS', '` AS', refined_sql)
            refined_sql = re.sub(r'FROM`', 'FROM `', refined_sql)
            
            # Fix issue with missing space after a backtick
            refined_sql = re.sub(r'`([A-Za-z])', '` \\1', refined_sql)
            
            # Remove backticks around nested field references
            refined_sql = re.sub(r'`([a-zA-Z0-9_]+\.[a-zA-Z0-9_\.]+)`', r'\1', refined_sql)
            
            # Log the post-processing
            logger.info("Applied SQL post-processing to refined SQL script")
            logger.info(f"SQL script refined successfully: {len(refined_sql)} characters")
            return refined_sql
            
        except Exception as e:
            logger.error(f"Error refining SQL script: {str(e)}")
            raise Exception(f"Failed to refine SQL script: {str(e)}")
