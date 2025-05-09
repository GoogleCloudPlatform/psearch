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
from google.genai.types import GenerateContentConfig, FunctionDeclaration, Tool, Content, Part
from typing import Dict, Any, Optional, List, Union

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class SQLTransformationService:
    """Service for generating SQL transformation scripts using GenAI with structured output."""
    
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
        
        # Set model name for Gemini from environment or use default
        self.model = os.environ.get("GEMINI_MODEL", "gemini-2.5-pro-preview-03-25")
        
        # Define the SQL output schema for function calling
        self.sql_schema = FunctionDeclaration(
            name="sql_transformation_output",
            description="Structured output for SQL transformation tasks",
            parameters={
                "type": "OBJECT",
                "properties": {
                    "sql_query": {
                        "type": "STRING",
                        "description": "The complete, properly formatted SQL query with correct syntax for BigQuery."
                    },
                    "formatted_table_references": {
                        "type": "BOOLEAN",
                        "description": "Confirmation that table references have proper backtick formatting with spaces."
                    },
                    "field_defaults": {
                        "type": "ARRAY",
                        "items": {
                            "type": "OBJECT",
                            "properties": {
                                "field_name": {"type": "STRING"},
                                "default_value": {"type": "STRING"},
                                "reason": {"type": "STRING"}
                            }
                        },
                        "description": "List of fields where default values were provided because they don't exist in source."
                    }
                },
                "required": ["sql_query", "formatted_table_references"]
            }
        )
        
        # Configure the tool with our schema
        self.sql_tool = Tool(function_declarations=[self.sql_schema])
        
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
        
        # Build the prompt with instructions for the structured output
        prompt = f"""You are an expert BigQuery SQL engineer. Generate a BigQuery SQL script that transforms data from a source table to a destination table, precisely matching the destination schema structure.

SOURCE TABLE: `{source_table}`
DESTINATION TABLE: `{destination_table}`
DESTINATION SCHEMA:
```json
{formatted_schema}
```

IMPORTANT FORMATTING REQUIREMENTS:
1. Start the script exactly with `CREATE OR REPLACE TABLE `{destination_table}` AS`
2. Ensure proper spacing between SQL keywords and backticked identifiers
3. Do NOT use backticks around nested field references like source.priceInfo.cost
4. If a field required by the destination schema does not exist in the source table, provide a NULL or appropriate default value
5. Add comments (-- comment) to explain non-obvious transformations or default values
6. For array fields, use IFNULL with empty arrays as defaults: IFNULL(source.array_field, [])
7. For nested fields, use proper STRUCT construction with NULL handling

Your response MUST follow the exact structure defined in the function schema. Do not include any explanatory text outside the function response structure.
"""
        
        try:
            # Create content structure for prompt
            contents = [Content(role="user", parts=[Part.from_text(text=prompt)])]
            
            # Set up generation configuration with our tool
            generate_content_config = GenerateContentConfig(
                temperature=0.2,
                max_output_tokens=8192,
                top_p=0.95,
                top_k=40,
                tools=[self.sql_tool]
            )
            
            # Generate content
            response = self.client.models.generate_content(
                model=self.model,
                contents=contents,
                config=generate_content_config,
            )
            
            # Log the response structure for debugging
            logger.info(f"Response structure: {type(response)}")
            logger.info(f"Candidates count: {len(response.candidates)}")
            
            if not response.candidates:
                raise ValueError("No candidates in the response")
                
            candidate = response.candidates[0]
            
            # Extract the function response with better error handling
            result = None
            sql_query = None
            
            try:
                # Check if we have a function_call in the expected location
                if hasattr(candidate.content.parts[0], 'function_call'):
                    function_response = candidate.content.parts[0].function_call
                    logger.info(f"Function call name: {function_response.name}")
                    
                    # Log the args structure
                    if hasattr(function_response, 'args'):
                        logger.info(f"Args keys: {list(function_response.args.keys()) if hasattr(function_response.args, 'keys') else 'args is not dict-like'}")
                    
                    # Try direct access first
                    if hasattr(function_response, 'args') and "sql_transformation_output" in function_response.args:
                        # Normal path - extract as expected
                        result = json.loads(function_response.args["sql_transformation_output"])
                    elif hasattr(function_response, 'args') and function_response.name == "sql_transformation_output":
                        # The function name matches but args doesn't have the key
                        # The args itself might be the result we want
                        if isinstance(function_response.args, dict):
                            result = function_response.args
                        else:
                            # Try to convert to dict if it's not already
                            result = json.loads(json.dumps(function_response.args))
                    else:
                        # Try to find any suitable key in args
                        found = False
                        if hasattr(function_response, 'args') and hasattr(function_response.args, 'keys'):
                            for key in function_response.args.keys():
                                if key.lower().endswith('output') or key.lower().endswith('result'):
                                    logger.info(f"Using alternative key: {key}")
                                    result_data = function_response.args[key]
                                    result = json.loads(result_data) if isinstance(result_data, str) else result_data
                                    found = True
                                    break
                        
                        if not found:
                            # Last resort - try to parse the first argument regardless of name
                            if hasattr(function_response, 'args') and hasattr(function_response.args, 'keys') and len(function_response.args.keys()) > 0:
                                first_key = list(function_response.args.keys())[0]
                                logger.info(f"Using first available key: {first_key}")
                                result_data = function_response.args[first_key]
                                result = json.loads(result_data) if isinstance(result_data, str) else result_data
                            else:
                                raise KeyError("Could not find usable key in function_response.args")
                
                # If we couldn't extract a function call, check for text response 
                elif hasattr(candidate.content.parts[0], 'text'):
                    text_response = candidate.content.parts[0].text
                    logger.info(f"Using text response as fallback: {text_response[:100]}...")
                    
                    # Try to extract SQL from text response - simple heuristic
                    if "CREATE OR REPLACE TABLE" in text_response:
                        sql_query = text_response
                        result = {
                            "sql_query": sql_query,
                            "formatted_table_references": True,
                            "field_defaults": []
                        }
                    else:
                        # Try to parse as JSON
                        try:
                            # Look for JSON block in text
                            import re
                            json_pattern = r'```json\s*(.*?)\s*```'
                            json_match = re.search(json_pattern, text_response, re.DOTALL)
                            
                            if json_match:
                                json_str = json_match.group(1)
                                result = json.loads(json_str)
                            else:
                                # Try parsing the whole text as JSON
                                result = json.loads(text_response)
                        except (json.JSONDecodeError, re.error):
                            raise ValueError("Could not extract SQL or valid JSON from text response")
                else:
                    raise ValueError(f"Unexpected response format. Cannot extract function call or text.")
                    
                # Ensure the result contains the required fields
                if not result:
                    raise ValueError("Could not extract result from response")
                
                if sql_query and "sql_query" not in result:
                    result["sql_query"] = sql_query
                
                if "sql_query" not in result:
                    raise ValueError("Could not find sql_query in extracted result")
                
            except Exception as e:
                logger.error(f"Error extracting function response: {str(e)}")
                logger.error(f"Response structure: {response}")
                raise ValueError(f"Failed to extract function response: {str(e)}")
            
            # Log any fields where defaults were provided
            if "field_defaults" in result and result["field_defaults"]:
                for field_default in result["field_defaults"]:
                    logger.info(f"Default value provided for field '{field_default['field_name']}': {field_default['default_value']} - {field_default['reason']}")
            
            logger.info(f"SQL transformation generated successfully")
            return result["sql_query"]
            
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
        
        # Define the SQL fix output schema for function calling
        sql_fix_schema = FunctionDeclaration(
            name="sql_fix_output",
            description="Structured output for SQL fix tasks",
            parameters={
                "type": "OBJECT",
                "properties": {
                    "fixed_sql": {
                        "type": "STRING",
                        "description": "The complete, fixed SQL query that resolves the error."
                    },
                    "changes": {
                        "type": "ARRAY",
                        "items": {"type": "STRING"},
                        "description": "List of changes made to fix the SQL"
                    },
                    "reasoning": {
                        "type": "STRING",
                        "description": "Brief explanation of why the changes fix the error"
                    }
                },
                "required": ["fixed_sql", "changes"]
            }
        )
        
        sql_fix_tool = Tool(function_declarations=[sql_fix_schema])
        
        # Build the prompt for the fix
        prompt = f"""You are an expert SQL engineer. Fix the following BigQuery SQL script based on the error message.

ERROR MESSAGE:
{error_message}

ORIGINAL SQL SCRIPT:
{sql_script}

SPECIFIC GUIDANCE FOR COMMON ERRORS:
1. For "Invalid field reference" errors - provide appropriate default values (NULL for most types, empty array [] for array types)
2. For "Syntax error" - check for proper backtick formatting and spacing between SQL elements
3. For nested field errors - ensure all parts of the path exist and add appropriate IFNULL handling

Provide a fixed version of the SQL script that resolves the error. Your response must follow the exact structure defined in the function schema.
"""
        
        try:
            # Create content structure for prompt
            contents = [Content(role="user", parts=[Part.from_text(text=prompt)])]
            
            # Set up generation configuration with our tool
            generate_content_config = GenerateContentConfig(
                temperature=0.2,
                max_output_tokens=8192,
                top_p=0.95,
                top_k=40,
                tools=[sql_fix_tool]
            )
            
            # Generate content
            response = self.client.models.generate_content(
                model=self.model,
                contents=contents,
                config=generate_content_config,
            )
            
            # Extract the structured function response
            function_response = response.candidates[0].content.parts[0].function_call
            result = json.loads(function_response.args["sql_fix_output"])
            
            # Log the changes made
            for change in result["changes"]:
                logger.info(f"SQL fix change: {change}")
            
            logger.info(f"SQL script refined successfully")
            if "reasoning" in result:
                logger.info(f"Reasoning: {result['reasoning']}")
                
            return result["fixed_sql"]
            
        except Exception as e:
            logger.error(f"Error refining SQL script: {str(e)}")
            raise Exception(f"Failed to refine SQL script: {str(e)}")
