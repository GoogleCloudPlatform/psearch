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
import os
import json # For example usage
from typing import Dict, Any, Optional

# Import new modular components
from .sql.validation.sql_validator import SQLValidator
from .sql.fixing.sql_fixer import SQLFixer
from .sql.analysis.diff_analyzer import DiffAnalyzer

# Configure logging - ensure this is configured at a higher level (e.g., main.py or service entry)
# If not, uncomment and configure here. For now, assume it's handled.
# logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class SQLFixService:
    """
    Service for validating and fixing SQL errors using refactored components.
    """
    
    def __init__(self, project_id: str, location: str = "us-central1", model_name: Optional[str] = None, use_genai_for_diff_analysis: bool = True):
        """
        Initialize the SQL Fix service with refactored components.
        
        Args:
            project_id: The Google Cloud Project ID.
            location: The GCP region (e.g., us-central1).
            model_name: Optional. The Gemini model name for fixer and analyzer.
            use_genai_for_diff_analysis: Whether the DiffAnalyzer should use GenAI.
        """
        self.project_id = project_id
        self.location = location
        self.model_name = model_name # Will be passed to components that need it

        self.validator = SQLValidator(project_id=self.project_id)
        self.fixer = SQLFixer(
            project_id=self.project_id,
            location=self.location,
            model_name=self.model_name
        )
        self.diff_analyzer = DiffAnalyzer(
            project_id=self.project_id if use_genai_for_diff_analysis else None,
            location=self.location if use_genai_for_diff_analysis else None,
            model_name=self.model_name if use_genai_for_diff_analysis else None,
            use_genai_for_analysis=use_genai_for_diff_analysis
        )
        
        logger.info(f"SQL Fix Service initialized: {project_id}/{location}. GenAI for diff analysis: {use_genai_for_diff_analysis}")

    def validate_sql(self, sql_script: str, timeout_seconds: int = 30) -> Dict[str, Any]:
        """
        Perform a dry run of a SQL query to validate it using the SQLValidator component.
        
        Args:
            sql_script: The SQL script to validate.
            timeout_seconds: Timeout for the dry run in seconds.
            
        Returns:
            A dictionary containing validation results from SQLValidator.
        """
        logger.info(f"SQLFixService: Validating SQL (timeout: {timeout_seconds}s).")
        return self.validator.validate_sql_dry_run(sql_script, timeout_seconds)
    
    def generate_sql_fix(self, 
                         original_sql: str, # Kept for context, though fixer might not directly use it
                         current_sql_that_failed: str,
                         error_message: str) -> Dict[str, Any]:
        """
        Generate a fixed SQL script using the SQLFixer component and analyze differences.
        
        Args:
            original_sql: The original SQL that started the fix process (for context/diff).
            current_sql_that_failed: The current SQL version that failed validation.
            error_message: The error message from the failed validation.
            
        Returns:
            A dictionary containing the suggested fixed SQL, diff analysis, and success status.
        """
        logger.info(f"SQLFixService: Generating SQL fix for error: {error_message[:100]}...")
        
        suggested_sql, fix_err_msg = self.fixer.fix_sql(current_sql_that_failed, error_message)
        
        if fix_err_msg or not suggested_sql:
            logger.error(f"SQLFixer failed to generate a fix: {fix_err_msg or 'No SQL returned'}")
            return {
                "success": False,
                "error": f"SQLFixer component failed: {fix_err_msg or 'No SQL returned'}",
                "suggested_sql": None,
                "analysis": None
            }
        
        logger.info("SQLFixer provided a suggested SQL. Analyzing differences...")
        # Analyze differences between the script that failed and the new suggestion
        analysis = self.diff_analyzer.analyze_sql_differences(current_sql_that_failed, suggested_sql)
        
        return {
            "success": True,
            "suggested_sql": suggested_sql,
            "analysis": analysis # Contains diff_text and other analysis fields
        }

# Example of how this refactored service might be used:
if __name__ == '__main__':
    # Ensure GOOGLE_APPLICATION_CREDENTIALS and GOOGLE_CLOUD_PROJECT are set
    logging.basicConfig(level=logging.DEBUG) 
    
    gcp_project_id = os.environ.get("GOOGLE_CLOUD_PROJECT")
    if not gcp_project_id:
        logger.error("GOOGLE_CLOUD_PROJECT environment variable is not set. Cannot run example.")
    else:
        # Initialize with GenAI for diff analysis
        fix_service_with_genai_diff = SQLFixService(
            project_id=gcp_project_id, 
            location="us-central1",
            use_genai_for_diff_analysis=True
        )
        
        # Initialize without GenAI for diff analysis (basic diff only)
        fix_service_basic_diff = SQLFixService(
            project_id=gcp_project_id,
            location="us-central1",
            use_genai_for_diff_analysis=False
        )

        test_original_sql = "SELECT name, price FROM products_source_table WHERE category = 'electronics';"
        test_failed_sql = "SELECT name, price FROM products_source_table WHER category = 'electronics';" # Typo: WHER
        test_error_message = "Syntax error: Expected keyword OR or AND but got identifier \"category\" at [1:43]"

        print("\n--- Testing SQLFixService (with GenAI Diff Analysis) ---")
        fix_result_genai_diff = fix_service_with_genai_diff.generate_sql_fix(
            original_sql=test_original_sql,
            current_sql_that_failed=test_failed_sql,
            error_message=test_error_message
        )
        print(f"Fix Result (GenAI Diff): {json.dumps(fix_result_genai_diff, indent=2)}")
        if fix_result_genai_diff["success"] and fix_result_genai_diff["analysis"]:
            print(f"\nDiff Text (from GenAI Diff Analysis):\n{fix_result_genai_diff['analysis'].get('diff_text')}")


        print("\n--- Testing SQLFixService (with Basic Diff Analysis) ---")
        fix_result_basic_diff = fix_service_basic_diff.generate_sql_fix(
            original_sql=test_original_sql,
            current_sql_that_failed=test_failed_sql,
            error_message=test_error_message
        )
        print(f"Fix Result (Basic Diff): {json.dumps(fix_result_basic_diff, indent=2)}")
        if fix_result_basic_diff["success"] and fix_result_basic_diff["analysis"]:
            print(f"\nDiff Text (from Basic Diff Analysis):\n{fix_result_basic_diff['analysis'].get('diff_text')}")

        print("\n--- Testing SQL Validation via SQLFixService ---")
        valid_sql_script = "SELECT 1 AS test_column;"
        validation_output = fix_service_with_genai_diff.validate_sql(valid_sql_script)
        print(f"Validation of '{valid_sql_script}': {json.dumps(validation_output, indent=2)}")

        invalid_sql_script = "SELEC 1 AS test_column;" # Typo
        validation_output_invalid = fix_service_with_genai_diff.validate_sql(invalid_sql_script)
        print(f"Validation of '{invalid_sql_script}': {json.dumps(validation_output_invalid, indent=2)}")
