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
import json
import re # Added import for regular expressions
from google import genai
from google.genai.types import GenerateContentConfig, Tool, Content, Part, FinishReason, FunctionCall
from typing import Dict, Any, Optional, List, Union, Tuple

logger = logging.getLogger(__name__)

class GenAIClient:
    """Handles GenAI client initialization and common API call logic."""

    def __init__(self, project_id: str, location: str, model_name: Optional[str] = None):
        """
        Initializes the GenAI client.

        Args:
            project_id: The Google Cloud Project ID.
            location: The GCP region (e.g., us-central1).
            model_name: Optional. The Gemini model name. Defaults to GEMINI_MODEL env var or "gemini-2.5-pro-preview-05-06".
        """
        self.project_id = project_id
        self.location = location
        
        try:
            self.client = genai.Client(
                vertexai=True, # Assuming Vertex AI based on original code
                project=project_id,
                location=location,
            )
            logger.info(f"GenAI client initialized for Vertex AI: {project_id}/{location}")
        except Exception as e:
            logger.error(f"Failed to initialize GenAI client for Vertex AI: {str(e)}")
            raise
            
        self.model_name = model_name or os.environ.get("GEMINI_MODEL", "gemini-2.5-pro-preview-05-06") # Updated default
        logger.info(f"Using Gemini model: {self.model_name}")


    def generate_content(
        self,
        prompt_text: str,
        generation_config_override: Optional[GenerateContentConfig] = None,
        tools: Optional[List[Tool]] = None
    ) -> Tuple[Optional[str], Optional[FunctionCall], Optional[str], Optional[FinishReason]]:
        """
        Makes a generate_content call to the GenAI model.

        Args:
            prompt_text: The text of the prompt.
            generation_config_override: Optional. Specific generation config for this call.
            tools: Optional. List of tools (e.g., for function calling).

        Returns:
            A tuple containing:
            - text_response (Optional[str]): The textual content from the response.
            - function_call_response (Optional[FunctionCall]): The function call object, if any.
            - error_message (Optional[str]): An error message if generation failed.
            - finish_reason (Optional[FinishReason]): The reason why generation finished.
        """
        contents = [Content(role="user", parts=[Part.from_text(text=prompt_text)])]
        
        if generation_config_override:
            config = generation_config_override
        else:
            config = GenerateContentConfig(
                temperature=0.2,
                max_output_tokens=8192, # Default, can be overridden
                top_p=0.95,
                top_k=40
            )
        
        if tools:
            config.tools = tools
            logger.debug(f"Using tools for generation: {[tool.function_declarations[0].name for tool in tools if tool.function_declarations]}")


        try:
            logger.info(f"Sending request to GenAI model {self.model_name}...")
            # logger.debug(f"Prompt: {prompt_text[:500]}...") # Log beginning of prompt
            # logger.debug(f"Config: {config}")

            response = self.client.models.generate_content(
                model=self.model_name,
                contents=contents,
                config=config,
            )

            if not response.candidates:
                logger.error("No candidates in the GenAI response.")
                return None, None, "No candidates in the GenAI response.", None
                
            candidate = response.candidates[0]
            finish_reason = candidate.finish_reason
            
            logger.info(f"GenAI response received. Finish reason: {finish_reason.name if finish_reason else 'UNKNOWN'}")
            if hasattr(candidate, 'finish_message') and candidate.finish_message:
                 logger.info(f"Finish message: {candidate.finish_message}")


            if finish_reason == FinishReason.MAX_TOKENS:
                logger.error("GenAI generation failed: Output truncated due to MAX_TOKENS limit.")
                return None, None, "Output truncated due to MAX_TOKENS limit.", finish_reason
            
            # Extract text content
            text_content: Optional[str] = None
            raw_text_from_parts = None
            if hasattr(candidate.content, 'parts') and candidate.content.parts and hasattr(candidate.content.parts[0], 'text'):
                raw_text_from_parts = candidate.content.parts[0].text
            
            raw_text_from_candidate = None
            if hasattr(candidate, 'text'): # Fallback for some response structures
                raw_text_from_candidate = candidate.text

            if raw_text_from_parts is not None:
                text_content = raw_text_from_parts.strip()
            elif raw_text_from_candidate is not None:
                text_content = raw_text_from_candidate.strip()


            # Extract function call
            function_call_content: Optional[FunctionCall] = None
            if hasattr(candidate.content, 'parts') and candidate.content.parts and hasattr(candidate.content.parts[0], 'function_call'):
                if candidate.content.parts[0].function_call is not None: # Ensure it's not None
                    function_call_content = candidate.content.parts[0].function_call
            
            if text_content:
                logger.debug(f"Extracted text content: {text_content[:200]}...")
            if function_call_content:
                logger.debug(f"Extracted function call: {function_call_content.name}")

            return text_content, function_call_content, None, finish_reason

        except Exception as e:
            logger.error(f"Error during GenAI content generation: {str(e)}", exc_info=True)
            return None, None, f"Failed to generate content: {str(e)}", None


    @staticmethod
    def extract_sql_from_text(text_content: Optional[str]) -> Optional[str]:
        """
        Strips markdown (e.g., ```sql ... ```, ```googlesql ... ```, or ``` ... ```) 
        from a text string if present.
        """
        if not text_content:
            return None
        
        stripped_content = text_content.strip()
        
        # Regex to match content within triple backticks, optionally with a language identifier
        # Handles:
        # ```sql\nSQL_QUERY\n```
        # ```googlesql\nSQL_QUERY\n```
        # ```\nSQL_QUERY\n```
        # SQL_QUERY (no backticks)
        # Also handles cases where there might be no newline after ```lang
        # e.g. ```sql SELECT ... ```
        
        # Try to match standard markdown block with optional language identifier
        match = re.match(r"^```(?:[a-zA-Z]+)?\s*\n(.*?)\n```$", stripped_content, re.DOTALL | re.IGNORECASE)
        if match:
            sql_query = match.group(1).strip()
        elif stripped_content.startswith("```") and stripped_content.endswith("```"):
            # Fallback for cases like ```SQL_QUERY``` or ```lang SQL_QUERY``` (no newlines)
            temp_query = stripped_content[3:-3].strip()
            # Check if the first part of the temp_query is a language identifier
            # Common identifiers: sql, googlesql, bigquery
            # We can split by space or newline to find the first "word"
            first_word_match = re.match(r"^([a-zA-Z]+)\s+", temp_query)
            if first_word_match:
                lang_identifier = first_word_match.group(1).lower()
                if lang_identifier in ["sql", "googlesql", "bigquery"]:
                    sql_query = temp_query[len(lang_identifier):].lstrip()
                else:
                    sql_query = temp_query # Assume no language identifier
            else: # No space after potential lang identifier, or no lang identifier
                sql_query = temp_query
        else:
            # Assume the content might be raw SQL without any backticks
            sql_query = stripped_content

        # Basic check if it looks like SQL
        if sql_query.upper().startswith(("CREATE OR REPLACE TABLE", "SELECT", "INSERT", "UPDATE", "DELETE", "WITH")):
            logger.info(f"Successfully extracted SQL content (preview: {sql_query[:100]}...).")
            return sql_query
        
        logger.warning(f"Extracted text does not appear to be a valid SQL query start. Original preview: '{text_content[:150]}...', Processed preview: '{sql_query[:100]}...'")
        return None

    @staticmethod
    def parse_function_call_args(function_call: Optional[FunctionCall], expected_tool_name: str) -> Optional[Dict[str, Any]]:
        """
        Parses arguments from a FunctionCall object.
        Handles cases where args might be a string needing json.loads or already a dict.
        """
        if not function_call:
            logger.debug("No function call provided to parse.")
            return None

        if function_call.name != expected_tool_name:
            logger.warning(f"Function call name '{function_call.name}' does not match expected tool name '{expected_tool_name}'.")
            # Depending on strictness, could return None here or attempt to parse anyway.
            # For now, let's be a bit lenient and try to parse if args exist.

        raw_args = function_call.args
        if raw_args is None:
            logger.warning(f"Function call '{function_call.name}' has no arguments (args is None).")
            return None
            
        parsed_args: Optional[Dict[str, Any]] = None
        try:
            if isinstance(raw_args, dict):
                # If args is already a dict, it might be structured like:
                # {"tool_name": {"arg1": "val1", ...}} or directly {"arg1": "val1", ...}
                if expected_tool_name in raw_args and isinstance(raw_args[expected_tool_name], dict):
                    parsed_args = raw_args[expected_tool_name]
                else: # Assume it's directly the args dict
                    parsed_args = raw_args
            elif isinstance(raw_args, str):
                # If it's a string, try to parse as JSON
                # The string might contain the tool name as a key, or be the direct JSON of args
                temp_parsed = json.loads(raw_args)
                if isinstance(temp_parsed, dict):
                    if expected_tool_name in temp_parsed and isinstance(temp_parsed[expected_tool_name], dict):
                        parsed_args = temp_parsed[expected_tool_name]
                    else: # Assume it's directly the args dict
                        parsed_args = temp_parsed
                else:
                    logger.error(f"Parsed JSON string from function call args is not a dictionary. Type: {type(temp_parsed)}")
            else:
                # Try to convert to dict if it's some other type (e.g. google.protobuf.struct_pb2.Struct)
                # This is a common case for Gemini function call args.
                parsed_args = dict(raw_args)
                # Check if it's nested under the tool name
                if expected_tool_name in parsed_args and isinstance(parsed_args[expected_tool_name], dict):
                     parsed_args = parsed_args[expected_tool_name]


            if parsed_args:
                logger.debug(f"Successfully parsed arguments for function call '{function_call.name}'.")
                return parsed_args
            else:
                logger.warning(f"Could not extract arguments dictionary for tool '{expected_tool_name}' from function call args: {raw_args}")
                return None

        except json.JSONDecodeError as e:
            logger.error(f"JSONDecodeError parsing function call arguments for '{function_call.name}': {str(e)}. Args: {raw_args}")
            return None
        except Exception as e:
            logger.error(f"Unexpected error parsing function call arguments for '{function_call.name}': {str(e)}. Args: {raw_args}", exc_info=True)
            return None

# Example usage (optional, for testing or direct script run):
if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    
    # Requires GOOGLE_APPLICATION_CREDENTIALS to be set, and a valid project/location
    # For example, export GOOGLE_CLOUD_PROJECT="your-gcp-project"
    project = os.environ.get("GOOGLE_CLOUD_PROJECT")
    if not project:
        logger.error("GOOGLE_CLOUD_PROJECT environment variable not set. Skipping example.")
    else:
        client_wrapper = GenAIClient(project_id=project, location="us-central1")
        
        # Test simple text generation
        logger.info("\n--- Testing Simple Text Generation ---")
        text_resp, _, err_msg, finish_reason = client_wrapper.generate_content("Explain quantum computing in simple terms.")
        if err_msg:
            logger.error(f"Error: {err_msg}")
        elif text_resp:
            logger.info(f"Text Response (Finish Reason: {finish_reason.name if finish_reason else 'N/A'}):\n{text_resp}")

        # Test with a dummy function tool
        logger.info("\n--- Testing Function Calling ---")
        dummy_tool_schema = FunctionDeclaration(
            name="get_weather_forecast",
            description="Gets the weather forecast for a location.",
            parameters={
                "type": "OBJECT",
                "properties": {
                    "location": {"type": "STRING", "description": "The city and state, e.g., San Francisco, CA"},
                    "days": {"type": "INTEGER", "description": "Number of days for the forecast"}
                },
                "required": ["location", "days"]
            }
        )
        dummy_tool = Tool(function_declarations=[dummy_tool_schema])
        
        text_resp_fc, func_call_resp, err_msg_fc, finish_reason_fc = client_wrapper.generate_content(
            prompt_text="What's the weather like in London for the next 3 days?",
            tools=[dummy_tool]
        )

        if err_msg_fc:
            logger.error(f"Error: {err_msg_fc}")
        else:
            logger.info(f"Function Call Test (Finish Reason: {finish_reason_fc.name if finish_reason_fc else 'N/A'}):")
            if text_resp_fc:
                logger.info(f"  Text part: {text_resp_fc}")
            if func_call_resp:
                logger.info(f"  Function call name: {func_call_resp.name}")
                args = GenAIClient.parse_function_call_args(func_call_resp, "get_weather_forecast")
                if args:
                    logger.info(f"  Parsed args: {args}")
                else:
                    logger.warning("  Failed to parse function call args.")
            if not text_resp_fc and not func_call_resp:
                logger.info("  No text or function call in response.")
