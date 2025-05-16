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
import re # For parsing error messages
from typing import Dict, Any

from google.cloud import bigquery
from google.api_core.exceptions import BadRequest

logger = logging.getLogger(__name__)

class SQLValidator:
    """
    Validates SQL queries using BigQuery dry runs.
    Corresponds to Step 4 of the multi-step SQL strategy.
    """

    def __init__(self, project_id: str):
        """
        Initializes the SQLValidator.

        Args:
            project_id: The Google Cloud Project ID.
        """
        self.project_id = project_id
        try:
            self.bigquery_client = bigquery.Client(project=project_id)
            logger.info(f"BigQuery client initialized for project: {project_id}")
        except Exception as e:
            logger.error(f"Failed to initialize BigQuery client for project {project_id}: {str(e)}")
            raise

    def validate_sql_dry_run(self, sql_script: str, timeout_seconds: int = 30) -> Dict[str, Any]:
        """
        Performs a dry run of a SQL query to validate it against BigQuery.
        (Logic from SQLFixService.validate_sql)

        Args:
            sql_script: The SQL script to validate.
            timeout_seconds: Timeout for the dry run in seconds.

        Returns:
            A dictionary containing validation results:
            {
                "valid": bool,
                "message": Optional[str], // Success message or general error
                "error_message": Optional[str], // Specific BigQuery error if invalid
                "details": Optional[Dict[str, Any]] // Additional details like estimated bytes or parsed error info
            }
        """
        if not sql_script or not sql_script.strip():
            logger.warning("SQL script is empty. Validation cannot be performed.")
            return {
                "valid": False,
                "message": "SQL script is empty.",
                "error_message": "SQL script is empty.",
                "details": None
            }
            
        logger.info(f"Validating SQL with dry run (timeout: {timeout_seconds}s)...")
        # logger.debug(f"SQL to validate:\n{sql_script[:1000]}...") # Log start of SQL

        try:
            job_config = bigquery.QueryJobConfig(
                dry_run=True,
                use_query_cache=False # Important for validation
            )
            
            query_job = self.bigquery_client.query(
                sql_script,
                job_config=job_config,
                timeout=timeout_seconds # bigquery.Client.query timeout is in seconds
            )
            
            # If no exception, the query is syntactically and semantically valid (as far as dry run can tell)
            estimated_bytes = query_job.total_bytes_processed if query_job.total_bytes_processed is not None else 0
            success_message = f"SQL syntax validated successfully (Estimated bytes: {estimated_bytes:,})"
            logger.info(success_message)
            return {
                "valid": True,
                "message": success_message,
                "error_message": None,
                "details": {
                    "estimated_bytes_processed": estimated_bytes,
                    "job_id": query_job.job_id,
                    "location": query_job.location,
                }
            }
            
        except BadRequest as e:
            # BigQuery syntax, semantic errors, or other request issues
            error_message_str = str(e)
            logger.warning(f"SQL validation failed (BadRequest): {error_message_str}")
            
            # Attempt to parse more details from the error message
            error_details: Dict[str, Any] = {"raw_error": error_message_str}
            # Example: "Invalid field name "source.productName" [at 3:7]"
            match_field_error = re.search(r"Invalid field name \"([^\"]+)\"(?: \[at (\d+:\d+)\])?", error_message_str, re.IGNORECASE)
            if match_field_error:
                error_details["invalid_field"] = match_field_error.group(1)
                if match_field_error.group(2):
                    error_details["error_location"] = match_field_error.group(2)
            
            # Example: "Unrecognized name: some_column [at 1:23]"
            match_unrecognized_name = re.search(r"Unrecognized name: ([a-zA-Z0-9_.]+)(?: \[at (\d+:\d+)\])?", error_message_str)
            if match_unrecognized_name:
                error_details["unrecognized_name"] = match_unrecognized_name.group(1)
                if match_unrecognized_name.group(2):
                    error_details["error_location"] = match_unrecognized_name.group(2)

            # Example: "Syntax error: Expected end of input but got keyword AS [at 5:1]"
            match_syntax_error = re.search(r"Syntax error: ([^\[]+)(?:\[at (\d+:\d+)\])?", error_message_str)
            if match_syntax_error:
                error_details["syntax_error_message"] = match_syntax_error.group(1).strip()
                if match_syntax_error.group(2):
                    error_details["error_location"] = match_syntax_error.group(2)

            return {
                "valid": False,
                "message": "SQL validation failed.",
                "error_message": error_message_str,
                "details": error_details
            }
        
        except Exception as e:
            # Other unexpected errors (network issues, timeouts not caught by BadRequest, etc.)
            error_message_str = str(e)
            error_type = type(e).__name__
            logger.error(f"Unexpected error during SQL dry run ({error_type}): {error_message_str}", exc_info=True)
            return {
                "valid": False,
                "message": f"An unexpected error occurred during SQL validation: {error_type}.",
                "error_message": error_message_str,
                "details": {"error_type": error_type}
            }

if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    project = os.environ.get("GOOGLE_CLOUD_PROJECT")
    if not project:
        logger.error("GOOGLE_CLOUD_PROJECT environment variable not set. Skipping SQLValidator example.")
    else:
        validator = SQLValidator(project_id=project)

        print("\n--- Testing Valid SQL ---")
        valid_sql = "SELECT 1 AS one, 'test' AS two;"
        result_valid = validator.validate_sql_dry_run(valid_sql)
        print(f"Validation result for valid SQL: {json.dumps(result_valid, indent=2)}")

        print("\n--- Testing Invalid SQL (Syntax Error) ---")
        invalid_sql_syntax = "SELECT FROM table_that_does_not_exist;" # Syntax error before table existence check
        result_invalid_syntax = validator.validate_sql_dry_run(invalid_sql_syntax)
        print(f"Validation result for invalid syntax SQL: {json.dumps(result_invalid_syntax, indent=2)}")
        
        print("\n--- Testing Invalid SQL (Unrecognized Name) ---")
        # Assuming 'my_dataset.my_source_table' exists for this to primarily fail on 'non_existent_column'
        # If the table doesn't exist, the error might be different.
        # For a more reliable unrecognized name test, use a valid table.
        # Let's use a public dataset table for a more robust test.
        # This query is valid if the column exists, invalid if not.
        # This will test the "Unrecognized name" parsing.
        invalid_sql_field = "SELECT non_existent_column FROM `bigquery-public-data.samples.shakespeare` LIMIT 1;"
        result_invalid_field = validator.validate_sql_dry_run(invalid_sql_field)
        print(f"Validation result for non-existent column: {json.dumps(result_invalid_field, indent=2)}")

        print("\n--- Testing Empty SQL ---")
        empty_sql = "   "
        result_empty = validator.validate_sql_dry_run(empty_sql)
        print(f"Validation result for empty SQL: {json.dumps(result_empty, indent=2)}")
