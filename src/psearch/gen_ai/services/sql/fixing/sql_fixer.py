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
from typing import Dict, Any, Optional, Tuple

from google.genai.types import GenerateContentConfig, FinishReason, FunctionCall

from ..common.client_utils import GenAIClient
from ..common.prompt_utils import SQL_FIX_TOOL, SQL_FIX_OUTPUT_SCHEMA # Using the tool and schema

logger = logging.getLogger(__name__)

class SQLFixer:
    """
    Attempts to fix SQL scripts based on BigQuery error messages using GenAI.
    Corresponds to Step 5 of the multi-step SQL strategy.
    Logic is derived from SQLTransformationService.refine_sql_script.
    """

    def __init__(self, project_id: str, location: str, model_name: Optional[str] = None):
        """
        Initializes the SQLFixer.

        Args:
            project_id: The Google Cloud Project ID.
            location: The GCP region (e.g., us-central1).
            model_name: Optional. The Gemini model name.
        """
        self.genai_client = GenAIClient(project_id, location, model_name)

    def _construct_prompt(self, sql_script: str, error_message: str) -> str:
        """Constructs the prompt for the SQL fixing task."""
        prompt = rf"""You are an expert SQL engineer. Fix the following BigQuery GoogleSQL script based on the error message.

ERROR MESSAGE:
{error_message}

ORIGINAL SQL SCRIPT:
```sql
{sql_script}
```

SPECIFIC GUIDANCE FOR COMMON ERRORS:
1. For "Invalid field reference" or "Unrecognized name" errors - check if the field exists in the source. If not, provide an appropriate default value (NULL for most types, empty array [] for ARRAY, 0 for NUMERIC/INT64, FALSE for BOOL, STRUCT() for STRUCT). Ensure the alias in the SELECT statement matches the destination schema.
2. For "Syntax error" - carefully check for proper backtick formatting around table and field names (only where necessary), spacing between SQL keywords and identifiers, and correct use of commas and parentheses.
3. For nested field errors (e.g., accessing a field in a STRUCT that might be NULL) - ensure all parts of the path exist and add appropriate IFNULL or SAFE navigation (e.g., SAFE.field_name).
4. Ensure all table references are correctly formatted (e.g., \`project.dataset.table\`).
5. The fixed SQL script MUST be a complete and executable BigQuery GoogleSQL query.

Your response MUST be ONLY a call to the `{SQL_FIX_OUTPUT_SCHEMA.name}` function. Do NOT include any other explanatory text, conversational pleasantries, or markdown formatting.
"""
        return prompt
    
    def _apply_programmatic_fixes(self, sql_query: str) -> str:
        """Applies programmatic fixes to the SQL query, (e.g., for formatting).
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


    def fix_sql(self, sql_script_to_fix: str, error_message: str) -> Tuple[Optional[str], Optional[str]]:
        """
        Attempts to fix the provided SQL script based on the error message.

        Args:
            sql_script_to_fix: The SQL script that has an error.
            error_message: The error message from BigQuery.

        Returns:
            A tuple containing:
            - fixed_sql_query (Optional[str]): The potentially fixed SQL query.
            - error_msg (Optional[str]): An error message if the fixing process itself failed.
        """
        logger.info(f"Attempting to fix SQL script based on error: {error_message[:150]}...")

        prompt = self._construct_prompt(sql_script_to_fix, error_message)

        generation_config = GenerateContentConfig(
            temperature=0.2, # Low temperature for more deterministic fixes
            max_output_tokens=32768, # SQL can be long
            top_p=0.95,
            top_k=40
        )

        text_resp, func_call_resp, gen_err_msg, finish_reason = self.genai_client.generate_content(
            prompt_text=prompt,
            generation_config_override=generation_config,
            tools=[SQL_FIX_TOOL] # Use the defined tool for structured output
        )

        if gen_err_msg:
            logger.error(f"SQL fixing GenAI call failed: {gen_err_msg}")
            return None, gen_err_msg

        fixed_sql: Optional[str] = None

        if func_call_resp and func_call_resp.name == SQL_FIX_OUTPUT_SCHEMA.name:
            logger.info(f"Received function call: {func_call_resp.name}")
            args = GenAIClient.parse_function_call_args(func_call_resp, SQL_FIX_OUTPUT_SCHEMA.name)
            if args and "fixed_sql" in args:
                fixed_sql = args["fixed_sql"]
                changes_made = args.get("changes", [])
                reasoning = args.get("reasoning", "No reasoning provided.")
                logger.info(f"SQL fix suggested. Reasoning: {reasoning}. Changes: {changes_made}")
            else:
                logger.warning(f"Could not extract 'fixed_sql' from function call args: {args}")
        
        # Fallback to text response if function call didn't yield SQL
        # (This logic is from the original SQLTransformationService.refine_sql_script's fallback)
        if not fixed_sql and text_resp:
            logger.warning("No valid function call for SQL fix, attempting to extract SQL from text response.")
            extracted_sql_from_text = GenAIClient.extract_sql_from_text(text_resp)
            if extracted_sql_from_text:
                logger.info("Using SQL extracted directly from text response as a fallback.")
                fixed_sql = extracted_sql_from_text
            else:
                # If text_resp is not SQL-like after extraction, it might be an error message or refusal.
                # Check if the raw text_resp itself looks like SQL (e.g. model ignored function calling)
                if text_resp.strip().upper().startswith(("CREATE OR REPLACE TABLE", "SELECT")):
                    logger.info("Raw text response appears to be SQL. Using it directly.")
                    fixed_sql = text_resp.strip()
                else:
                    err_msg = f"SQL Fix: No function call and text response does not appear to be SQL. Text: {text_resp[:200]}"
                    logger.error(err_msg)
                    return None, err_msg
        
        if not fixed_sql:
            err_msg = f"Failed to obtain fixed SQL. Finish reason: {finish_reason.name if finish_reason else 'UNKNOWN'}."
            logger.error(err_msg)
            return None, err_msg

        fixed_sql = self._apply_programmatic_fixes(fixed_sql) # Apply formatting fixes

        if not (fixed_sql.upper().startswith("CREATE OR REPLACE TABLE") or fixed_sql.upper().startswith("SELECT")):
            err_msg = f"Final fixed SQL content after programmatic fixes does not appear to be a valid SQL query: {fixed_sql[:200]}..."
            logger.error(err_msg)
            return None, err_msg # Or return the 'fixed_sql' and let validator catch it? For now, error.

        logger.info("SQL script refined successfully by SQLFixer.")
        return fixed_sql, None

    def _construct_simple_fix_prompt(self, sql_script: str, error_message: str) -> str:
        """Constructs a very simple prompt for a last-chance SQL fix."""
        prompt = rf"""The following BigQuery GoogleSQL query failed with an error.
Please provide a corrected version of the SQL query.
Output ONLY the complete, corrected BigQuery GoogleSQL query. Do not include any other text or explanations.

FAILED SQL QUERY:
```sql
{sql_script}
```

BIGQUERY ERROR MESSAGE:
```
{error_message}
```

CORRECTED SQL QUERY:
"""
        return prompt

    def simple_fix_sql(self, sql_script_to_fix: str, error_message: str) -> Tuple[Optional[str], Optional[str]]:
        """
        Attempts a simple, direct fix of the SQL script based on the error message,
        expecting direct SQL text output from the model.
        Uses the specified model (gemini-2.5-pro-preview-05-06 by default via GenAIClient).

        Args:
            sql_script_to_fix: The SQL script that has an error.
            error_message: The error message from BigQuery.

        Returns:
            A tuple containing:
            - fixed_sql_query (Optional[str]): The potentially fixed SQL query.
            - error_msg (Optional[str]): An error message if the fixing process itself failed.
        """
        logger.info(f"Attempting simple fix for SQL script based on error: {error_message[:150]}...")

        prompt = self._construct_simple_fix_prompt(sql_script_to_fix, error_message)

        # Use a specific configuration for direct text output, possibly higher token limit
        generation_config = GenerateContentConfig(
            temperature=0.1, # Very deterministic
            max_output_tokens=32768, 
            top_p=0.95, # Standard top_p
            top_k=40   # Standard top_k
        )

        text_resp, _, gen_err_msg, finish_reason = self.genai_client.generate_content(
            prompt_text=prompt,
            generation_config_override=generation_config,
            tools=None # No tools, expect direct SQL
        )

        if gen_err_msg:
            logger.error(f"Simple SQL fix GenAI call failed: {gen_err_msg}")
            return None, gen_err_msg

        if not text_resp:
            err_msg = f"Simple SQL fix: No text response received. Finish reason: {finish_reason.name if finish_reason else 'UNKNOWN'}."
            logger.error(err_msg)
            return None, err_msg

        # Extract SQL (handles potential markdown) and apply programmatic fixes
        fixed_sql = GenAIClient.extract_sql_from_text(text_resp)
        
        if not fixed_sql:
            # If extract_sql_from_text returns None, it means the response didn't look like SQL.
            # Let's check if the raw text_resp itself might be the SQL if the model didn't use markdown.
            if text_resp.strip().upper().startswith(("CREATE OR REPLACE TABLE", "SELECT")):
                logger.warning("Simple SQL fix: extract_sql_from_text failed, but raw response looks like SQL. Using raw response.")
                fixed_sql = text_resp.strip()
            else:
                err_msg = f"Simple SQL fix: Could not extract valid SQL from GenAI response. Preview: {text_resp[:200]}"
                logger.error(err_msg)
                return None, err_msg
        
        fixed_sql = self._apply_programmatic_fixes(fixed_sql)

        if not (fixed_sql.upper().startswith("CREATE OR REPLACE TABLE") or fixed_sql.upper().startswith("SELECT")):
            err_msg = f"Final 'simple fixed' SQL content after programmatic fixes does not appear to be a valid SQL query: {fixed_sql[:200]}..."
            logger.error(err_msg)
            return None, err_msg

        logger.info("Simple SQL fix attempt completed.")
        return fixed_sql, None


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    project = os.environ.get("GOOGLE_CLOUD_PROJECT")
    if not project:
        logger.error("GOOGLE_CLOUD_PROJECT environment variable not set. Skipping SQLFixer example.")
    else:
        fixer = SQLFixer(project_id=project, location="us-central1")

        print("\n--- Testing SQL Fixer ---")
        # Example from SQLValidator test
        # Error: "Syntax error: Expected end of input but got keyword AS [at 1:19]" (example error)
        # A more realistic error for this SQL would be "Unrecognized name: non_existent_column"
        error_sql = "SELECT non_existent_column FROM `bigquery-public-data.samples.shakespeare` AS t1 LIMIT 1;"
        # Let's assume BigQuery error was: "Unrecognized name: non_existent_column [at 1:8]"
        bq_error_message = "Unrecognized name: non_existent_column [at 1:8]"
        
        logger.info(f"Attempting to fix SQL:\n{error_sql}\nBased on error: {bq_error_message}")
        
        fixed_sql, error = fixer.fix_sql(error_sql, bq_error_message)

        if error:
            logger.error(f"Error fixing SQL: {error}")
        elif fixed_sql:
            logger.info("Successfully fixed SQL (suggestion):")
            logger.info(f"\n{fixed_sql}\n")
            
            # For a real test, you'd validate this 'fixed_sql' again.
            # from ..validation.sql_validator import SQLValidator
            # validator = SQLValidator(project_id=project)
            # validation_result = validator.validate_sql_dry_run(fixed_sql)
            # logger.info(f"Validation of fixed SQL: {json.dumps(validation_result, indent=2)}")
        else:
            logger.error("Fixing process failed to produce SQL without a specific error message.")
