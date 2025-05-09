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
import difflib
import json
from typing import Dict, Any, Optional, List, Union

from google.cloud import bigquery
from google.api_core.exceptions import BadRequest
from google import genai
from google.genai.types import GenerateContentConfig, FunctionDeclaration, Tool, Content, Part

# Import SQLTransformationService directly
from .sql_transformation_service import SQLTransformationService

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class SQLFixService:
    """Service for fixing SQL errors using GenAI."""
    
    def __init__(self, project_id: str, location: str = "us-central1"):
        """Initialize the SQL Fix service.
        
        Args:
            project_id: The Google Cloud Project ID
            location: The GCP region (e.g., us-central1)
        """
        self.project_id = project_id
        self.location = location
        self.bigquery_client = bigquery.Client(project=project_id)
        
        # Initialize GenAI client
        self.client = genai.Client(
            vertexai=True,
            project=project_id,
            location=location,
        )
        
        # Set model name for Gemini from environment or use default
        self.model = os.environ.get("GEMINI_MODEL", "gemini-2.5-pro-preview-03-25")
        
        # Initialize the SQL Transformation Service
        try:
            self.sql_service = SQLTransformationService(project_id, location)
            logger.info(f"SQL Fix Service initialized: {project_id}/{location}")
        except Exception as e:
            logger.error(f"Error initializing SQL Transformation Service: {str(e)}")
            raise
    
    def validate_sql(self, sql_script: str, timeout_seconds: int = 30) -> Dict[str, Any]:
        """Perform a dry run of a SQL query to validate it.
        
        Args:
            sql_script: The SQL script to validate
            timeout_seconds: Timeout for the dry run in seconds
            
        Returns:
            A dictionary containing validation results
        """
        logger.info("Validating SQL with dry run")
        
        try:
            # Configure job for dry run
            job_config = bigquery.QueryJobConfig(
                dry_run=True,
                use_query_cache=False
            )
            
            # Start dry run
            query_job = self.bigquery_client.query(
                sql_script,
                job_config=job_config,
                timeout=timeout_seconds * 1000
            )
            
            # If we get here, the query is valid
            return {
                "valid": True,
                "message": f"SQL syntax validated successfully (Estimated bytes: {query_job.total_bytes_processed:,})",
                "details": {
                    "estimated_bytes_processed": query_job.total_bytes_processed,
                }
            }
            
        except BadRequest as e:
            # Handle BigQuery syntax and semantic errors
            logger.error(f"SQL validation error: {str(e)}")
            
            # Process error message for better user feedback
            error_message = str(e)
            error_details = {}
            
            # Extract specific field names for field reference errors
            missing_field = None
            if "Invalid field reference" in error_message:
                import re
                field_match = re.search(r"Invalid field reference '([^']+)'", error_message)
                if field_match:
                    missing_field = field_match.group(1)
                    error_details["missing_field"] = missing_field
            
            return {
                "valid": False,
                "error": error_message,
                "details": error_details if error_details else None
            }
        
        except Exception as e:
            # Handle all other errors
            logger.error(f"Error performing SQL dry run: {str(e)}", exc_info=True)
            
            # Format error message more user-friendly if possible
            error_message = str(e)
            error_type = type(e).__name__
            
            return {
                "valid": False,
                "error": f"Error performing SQL validation: {error_message}",
                "details": {"error_type": error_type}
            }
    
    def generate_sql_fix(self, 
                         original_sql: str, 
                         current_sql: str,
                         error_message: str) -> Dict[str, Any]:
        """Generate a fixed SQL script using Gemini.
        
        Args:
            original_sql: The original SQL that started the fix process
            current_sql: The current SQL version (might be a previous fix attempt)
            error_message: The error message from the failed validation
            
        Returns:
            A dictionary containing the fixed SQL and diff
        """
        logger.info(f"Generating SQL fix for error: {error_message[:100]}...")
        
        try:
            # Use the SQLTransformationService's refine_sql_script method
            suggested_sql = self.sql_service.refine_sql_script(current_sql, error_message)
            
            # Generate diff
            diff_lines = list(difflib.unified_diff(
                current_sql.splitlines(),
                suggested_sql.splitlines(),
                fromfile='current.sql',
                tofile='suggested.sql',
                lineterm='',
                n=3  # Context lines
            ))
            diff_text = '\n'.join(diff_lines)
            
            # Use our analyze_differences method to get a detailed analysis
            analysis = self.analyze_differences(current_sql, suggested_sql)
            
            return {
                "success": True,
                "suggested_sql": suggested_sql,
                "diff": diff_text,
                "changes": analysis.get("changes", [])
            }
            
        except Exception as e:
            logger.error(f"Error generating SQL fix: {str(e)}")
            return {
                "success": False,
                "error": f"Failed to generate SQL fix: {str(e)}"
            }
    
    def analyze_differences(self, original_sql: str, fixed_sql: str) -> Dict[str, Any]:
        """Analyze and explain the differences between the original and fixed SQL.
        
        Args:
            original_sql: The original SQL with errors
            fixed_sql: The fixed SQL
            
        Returns:
            A dictionary with analysis of the differences
        """
        # Define the analysis schema for function calling
        analysis_schema = FunctionDeclaration(
            name="sql_diff_analysis",
            description="Analyzes differences between original and fixed SQL scripts",
            parameters={
                "type": "OBJECT",
                "properties": {
                    "changes": {
                        "type": "ARRAY",
                        "items": {"type": "STRING"},
                        "description": "List of significant changes made in the fixed SQL"
                    },
                    "primary_issue_type": {
                        "type": "STRING",
                        "description": "The main type of issue that was fixed (e.g., 'missing field', 'syntax error', 'backtick formatting')"
                    },
                    "removed_lines_count": {
                        "type": "INTEGER",
                        "description": "Number of lines removed in the fix"
                    },
                    "added_lines_count": {
                        "type": "INTEGER",
                        "description": "Number of lines added in the fix"
                    }
                },
                "required": ["changes", "primary_issue_type"]
            }
        )
        
        analysis_tool = Tool(function_declarations=[analysis_schema])
        
        # Generate line-by-line diff for the prompt
        diff_lines = list(difflib.unified_diff(
            original_sql.splitlines(),
            fixed_sql.splitlines(),
            lineterm='',
        ))
        diff_text = '\n'.join(diff_lines)
        
        # Build the prompt for analysis
        prompt = f"""You are an expert SQL analyst. Analyze the differences between the original and fixed SQL scripts.

ORIGINAL SQL:
{original_sql}

FIXED SQL:
{fixed_sql}

DIFF:
{diff_text}

Provide a detailed analysis of the significant changes made between the scripts.
Specifically focus on:
1. Field replacements (e.g., source.field -> NULL)
2. Syntax corrections (e.g., backtick usage, spacing)
3. Value handling (e.g., adding IFNULL, default values)
4. Structural changes (e.g., whole sections added or removed)

Your response must follow the exact structure defined in the function schema.
"""
        
        try:
            # Create content structure for prompt
            contents = [Content(role="user", parts=[Part.from_text(text=prompt)])]
            
            # Set up generation configuration with our tool
            generate_content_config = GenerateContentConfig(
                temperature=0.2,
                max_output_tokens=2048,
                top_p=0.95,
                top_k=40,
                tools=[analysis_tool]
            )
            
            # Generate content
            response = self.client.models.generate_content(
                model=self.model,
                contents=contents,
                config=generate_content_config,
            )
            
            # Extract the structured function response
            function_response = response.candidates[0].content.parts[0].function_call
            result = json.loads(function_response.args["sql_diff_analysis"])
            
            # Log the analysis results
            logger.info(f"SQL diff analysis: Primary issue type: {result.get('primary_issue_type', 'unknown')}")
            for change in result.get("changes", []):
                logger.info(f"SQL analysis change: {change}")
            
            # Also add the diff text to the result
            result["diff"] = diff_text
            
            return result
            
        except Exception as e:
            logger.error(f"Error analyzing SQL differences: {str(e)}")
            # Fallback to basic diff analysis
            basic_diff = {
                "diff": diff_text,
                "changes": ["SQL structure was modified"],
                "primary_issue_type": "unknown",
                "removed_lines_count": len([line for line in diff_lines if line.startswith('-') and not line.startswith('---')]),
                "added_lines_count": len([line for line in diff_lines if line.startswith('+') and not line.startswith('+++')])
            }
            return basic_diff
