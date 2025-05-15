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
from google.genai.types import GenerateContentConfig, FunctionDeclaration, Tool, Content, Part, FinishReason
from typing import Dict, Any, Optional, List, Union

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class SQLTransformationService:
    """Service for generating SQL transformation scripts using GenAI with structured output."""
    
    # Fields that should be prioritized for semantic mapping when source fields don't match directly
    CRITICAL_FIELDS = [
        "id",                  # Primary identifier
        "name",                # Product name
        "title",               # Alternative title/name
        "description",         # Product description
        "images",              # Product images
        "categories",          # Product categories
        "brands",              # Brand information
        "priceInfo.price",     # Product price (nested field)
        "priceInfo.currencyCode"  # Currency (nested field)
    ]
    
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
        self.model = os.environ.get("GEMINI_MODEL", "gemini-2.5-pro-preview-05-06")
        
        # Load the fixed destination schema
        schema_path = os.path.join(os.path.dirname(__file__), 'schema.json')
        try:
            with open(schema_path, 'r') as f:
                self.destination_schema = json.load(f)
            logger.info(f"Successfully loaded fixed destination schema from {schema_path}")
        except Exception as e:
            logger.error(f"Error loading destination schema from {schema_path}: {str(e)}")
            self.destination_schema = None
        
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
        source_table_name: str,
        destination_table_name: str,
        source_schema_fields: List[str],
        destination_schema: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """Generate a SQL transformation script to map data from source to destination schema.
        Focuses on syntactic correctness, formatting, and basic defaults.
        
        Args:
            source_table_name: The BigQuery source table ID (project.dataset.table)
            destination_table_name: The BigQuery destination table ID (project.dataset.table)
            source_schema_fields: A list of field names available in the source table.
            destination_schema: The JSON schema of the destination table. If None, uses the default schema loaded from schema.json.
            
        Returns:
            A SQL script that transforms data from the source table to match the destination schema
        """
        # Use provided schema or fall back to default schema
        destination_schema = destination_schema or self.destination_schema
        if not destination_schema:
            raise ValueError("No destination schema provided and no default schema loaded from schema.json")
        logger.info(f"Generating initial SQL transformation from {source_table_name} to {destination_table_name}")
        import re # Moved import re to the top of the method
        
        # Format the schemas for better readability in the prompt
        formatted_destination_schema = json.dumps(destination_schema, indent=2)
        formatted_source_fields = ", ".join(source_schema_fields)

        prompt = f"""You are an expert GoogleSQL engineer specializing in BigQuery transformations.
Your primary goal is to generate a syntactically valid and executable BigQuery GoogleSQL script.
This script will transform data from a source table to a destination table, precisely matching the destination schema structure.

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
1. Direct Name Mapping: Match destination field names to source fields using CASE-INSENSITIVE comparison. 
   For example, if destination needs 'name' and source has 'Name' or 'NAME', map it directly.

2. Field Synonyms: If a direct match isn't found, look for these common field synonyms:
   - For 'name' fields: Look for 'title', 'product_name', 'item_name', 'label'
   - For 'price' fields: Look for 'cost', 'amount', 'unit_price', 'sale_price', 'price_amount'
   - For 'description' fields: Look for 'desc', 'details', 'summary', 'text'
   - For 'id' fields: Look for 'code', 'sku', 'item_id', 'product_id'
   - For 'images' or 'image' fields: Look for 'photo', 'picture', 'img', 'image_url'
   
   For example, if destination needs 'price' and there's no exact match, but source has 'unit_price', use that.

3. Nested Field Access: For nested destination fields like 'priceInfo.price', try to find appropriate source fields.
   Example: If destination needs 'priceInfo.price', search for source fields like 'price', 'cost', 'amount'.

4. Basic Type-Correct Defaults: For any field in the DESTINATION SCHEMA that still doesn't have a match after steps 1-3:
   - Apply a basic, BigQuery type-correct default value. Examples:
     - STRING, TIMESTAMP, DATE, GEOGRAPHY: `NULL`
     - INT64, NUMERIC, FLOAT64, BIGNUMERIC: `0`
     - ARRAY: `[]` (an empty array)
     - BOOL: `FALSE`
     - STRUCT: A `STRUCT()` constructor with all its sub-fields also set to their respective basic defaults (e.g., `STRUCT(field1 AS NULL, field2 AS 0)`).
   - Add a comment for each defaulted field: `-- Defaulted [destination_field_name] to [default_value] as no match found in source fields.`

5. Type Compatibility: Ensure type compatibility when mapping fields. Use SAFE_CAST where needed.
   Example: `SAFE_CAST(source.price_string AS FLOAT64) AS price`

6. Complete Coverage: Ensure EVERY field defined in the DESTINATION SCHEMA is present in the SELECT statement of your generated query.

7. Commenting: Add comments (`-- comment`) to explain your mapping decisions and default values.

Your response MUST be only the complete BigQuery GoogleSQL script. Do not include any explanatory text, markdown formatting, or anything else outside the SQL script itself.
"""
        
        sql_query = None # Initialize sql_query

        try:
            # Create content structure for prompt
            contents = [Content(role="user", parts=[Part.from_text(text=prompt)])]
            
            # Set up generation configuration (tool removed for direct SQL output)
            generate_content_config = GenerateContentConfig(
                temperature=0.2,
                max_output_tokens=65535,
                top_p=0.95,
                top_k=40
                # tools=[self.sql_tool] # Tool removed for direct SQL output
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

            # Check for MAX_TOKENS finish reason
            if candidate.finish_reason == FinishReason.MAX_TOKENS:
                logger.error("SQL generation failed: Output truncated due to MAX_TOKENS limit.")
                raise ValueError("SQL generation failed: Output truncated due to MAX_TOKENS limit. Consider simplifying the schema or prompt if increasing tokens further is not possible.")
            
            # Check for MALFORMED_FUNCTION_CALL finish reason - this is a special case
            if candidate.finish_reason == FinishReason.MALFORMED_FUNCTION_CALL:
                logger.warning(f"Received MALFORMED_FUNCTION_CALL finish reason from Gemini model. Attempting fallback strategies.")
                logger.warning(f"Finish message: {candidate.finish_message}")
                
                # Try to extract any useful plain text from candidate.text first
                if hasattr(candidate, 'text') and candidate.text:
                    text_content = candidate.text.strip()
                    if text_content.upper().startswith("CREATE OR REPLACE TABLE") or text_content.upper().startswith("SELECT"):
                        sql_query = text_content # Assign to sql_query to be processed by regex below
                        logger.info("Extracted SQL-like text directly from candidate.text (MALFORMED_FUNCTION_CALL fallback).")
                
                if not sql_query: # If candidate.text didn't yield SQL, try the simplified retry
                    logger.info("Falling back to simplified retry prompt without function calling.")
                    simple_prompt = f"""Generate a syntactically valid BigQuery GoogleSQL script that transforms `{source_table_name}` to `{destination_table_name}`.

SOURCE FIELDS: {", ".join(source_schema_fields[:10])}{'...' if len(source_schema_fields) > 10 else ''}
TARGET: CREATE OR REPLACE TABLE `{destination_table_name}` AS SELECT

MANDATORY RULES:
1. Start exactly with: CREATE OR REPLACE TABLE `{destination_table_name}` AS SELECT
   - Ensure there is exactly one space after TABLE and before the first backtick
   - Ensure there is exactly one space after the closing backtick and before AS

2. Field Mapping (in order of preference):
   a. Direct Case-Insensitive Match: Match destination fields to source fields regardless of case
   b. Synonym Mapping: Use these common synonyms if exact match not found:
      - 'name' → try: 'title', 'product_name', 'item_name'
      - 'price' → try: 'cost', 'amount', 'unit_price'
      - 'description' → try: 'desc', 'text', 'summary'
      - 'id' → try: 'sku', 'code', 'product_id'
   c. Nested Fields: For fields like 'priceInfo.price', look for matching source fields
   
3. Default Values (only if no match found after steps above):
   - STRING/TIMESTAMP/DATE/GEOGRAPHY: NULL
   - INT64/NUMERIC/FLOAT64/BIGNUMERIC: 0
   - ARRAY: [] (empty array)
   - BOOL: FALSE
   - STRUCT: STRUCT() with defaults for all sub-fields

4. Type Compatibility: Use SAFE_CAST where needed for type conversions
5. Add comments explaining your mapping choices
6. Ensure EVERY field in the destination schema is covered
7. Output only the complete SQL query with no explanation or markdown
"""

                    retry_contents = [Content(role="user", parts=[Part.from_text(text=simple_prompt)])]
                    retry_response = self.client.models.generate_content(
                        model=self.model,
                        contents=retry_contents,
                        config=GenerateContentConfig(temperature=0.2, max_output_tokens=65535, top_p=0.95, top_k=40)
                    )
                    
                    if not retry_response.candidates:
                        raise ValueError("No candidates in the retry response")
                    
                    retry_candidate = retry_response.candidates[0]
                    if hasattr(retry_candidate.content, 'parts') and retry_candidate.content.parts and hasattr(retry_candidate.content.parts[0], 'text'):
                        sql_query = retry_candidate.content.parts[0].text.strip()
                        logger.info(f"Successfully generated SQL via retry prompt: {sql_query[:100]}...")
                    elif hasattr(retry_candidate, 'text') and retry_candidate.text: # Fallback for retry
                        sql_query = retry_candidate.text.strip()
                        logger.info(f"Successfully generated SQL via retry prompt (candidate.text): {sql_query[:100]}...")
                    else:
                        raise ValueError("Failed to generate SQL via all fallback methods after MALFORMED_FUNCTION_CALL.")

            # If not MALFORMED_FUNCTION_CALL, or if it was but sql_query is now populated by its fallback
            if not sql_query: # sql_query would be None if not MALFORMED_FUNCTION_CALL path
                if hasattr(candidate.content, 'parts') and candidate.content.parts and hasattr(candidate.content.parts[0], 'text'):
                    sql_query = candidate.content.parts[0].text.strip()
                    logger.info(f"Successfully generated SQL (direct text from primary path): {sql_query[:100]}...")
                elif hasattr(candidate, 'text') and candidate.text: # Fallback for primary path
                    sql_query = candidate.text.strip()
                    logger.info(f"Successfully generated SQL (candidate.text from primary path): {sql_query[:100]}...")
                else:
                    logger.error(f"Could not extract SQL text from primary response path. Candidate: {candidate}")
                    raise ValueError("Could not extract SQL text from primary response path.")

            # Strip markdown from sql_query regardless of how it was obtained
            if sql_query:
                if sql_query.startswith("```sql"):
                    sql_query = sql_query.replace("```sql", "", 1).replace("```", "", 1).strip()
                elif sql_query.startswith("```"):
                     sql_query = sql_query.replace("```", "", 1).replace("```", "", 1).strip()

            if not sql_query or not (sql_query.upper().startswith("CREATE OR REPLACE TABLE") or sql_query.upper().startswith("SELECT")):
                logger.error(f"Final SQL content does not appear to be a valid SQL query: {sql_query[:200] if sql_query else 'None'}...")
                raise ValueError("Final SQL content does not appear to be a valid SQL query.")
                
            # Programmatic fix for CREATE OR REPLACE TABLE formatting, applied to sql_query from any path
            if sql_query:
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
                if sql_query.startswith("``"): # Specific fix for leading double backticks
                    sql_query = sql_query[2:]
                elif sql_query.startswith("`") and not sql_query.startswith("```"): # Fix for single leading backtick if not part of markdown
                    sql_query = sql_query[1:]


            result = {
                "sql_query": sql_query,
                "formatted_table_references": True, 
                "field_defaults": [] 
            }
            
            logger.info(f"SQL transformation generated successfully (direct text output)")
            return result
            
        except Exception as e:
            logger.error(f"Error generating SQL transformation: {str(e)}")
            raise Exception(f"Failed to generate SQL transformation: {str(e)}")

    def identify_defaulted_fields(self, sql_query: str, critical_fields: List[str] = None) -> List[str]:
        """
        Identify which critical fields were given default values in the SQL and need semantic refinement.
        
        Args:
            sql_query: The generated SQL query
            critical_fields: Optional list of fields to check, or use default CRITICAL_FIELDS
            
        Returns:
            List of field names that should be semantically refined
        """
        # import re # re is imported but not used. Not a syntax error, but can be removed if truly unused.
        
        if critical_fields is None:
            critical_fields = self.CRITICAL_FIELDS
            
        fields_to_refine = []
        sql_query_lowered = sql_query.lower() # Convert to lower once for efficiency and clarity
        
        # Simple pattern matching for default values
        for field in critical_fields:
            if "." in field:
                # Handle nested fields like priceInfo.price
                parent, child = field.split(".", 1)
                
                # Check for patterns like STRUCT(NULL AS child, or STRUCT(0 AS child,
                # Avoid complex regex pattern and use simple string search
                # Search for STRUCT( + default value + AS + field name
                null_pattern = "STRUCT(" + "NULL AS " + child
                zero_pattern = "STRUCT(" + "0 AS " + child
                false_pattern = "STRUCT(" + "FALSE AS " + child
                empty_array_pattern = "STRUCT(" + "[] AS " + child
                empty_obj_pattern = "STRUCT(" + "{} AS " + child # "{}" is a valid string part
                empty_str_pattern1 = "STRUCT(" + "'' AS " + child
                empty_str_pattern2 = "STRUCT(" + '""' + " AS " + child # '""' is a valid string for two double quotes

                # Use simple string contains check instead of regex
                # This avoids escape sequence issues
                # The parentheses around the condition allow implicit line continuation,
                # so no '\' is needed at the end of these lines.
                if (null_pattern.lower() in sql_query_lowered or
                    zero_pattern.lower() in sql_query_lowered or
                    false_pattern.lower() in sql_query_lowered or
                    empty_array_pattern.lower() in sql_query_lowered or
                    empty_obj_pattern.lower() in sql_query_lowered or
                    empty_str_pattern1.lower() in sql_query_lowered or
                    empty_str_pattern2.lower() in sql_query_lowered):
                    fields_to_refine.append(field)
            else:
                # For regular fields, look for NULL, 0, FALSE, etc. + AS + fieldname
                null_pattern = "NULL AS " + field
                zero_pattern = "0 AS " + field  
                false_pattern = "FALSE AS " + field
                empty_array_pattern = "[] AS " + field
                empty_obj_pattern = "{} AS " + field
                empty_str_pattern1 = "'' AS " + field
                empty_str_pattern2 = '""' + " AS " + field # '""' is fine here too
                
                # Simple string contains check
                # Again, parentheses allow implicit line continuation.
                if (null_pattern.lower() in sql_query_lowered or
                    zero_pattern.lower() in sql_query_lowered or
                    false_pattern.lower() in sql_query_lowered or
                    empty_array_pattern.lower() in sql_query_lowered or
                    empty_obj_pattern.lower() in sql_query_lowered or
                    empty_str_pattern1.lower() in sql_query_lowered or
                    empty_str_pattern2.lower() in sql_query_lowered):
                    fields_to_refine.append(field)
                
        return fields_to_refine
        
    def analyze_source_fields(self, source_schema_fields: List[str]) -> Dict[str, List[str]]:
        """
        Analyze source fields to identify potential semantic matches for critical destination fields.
        
        Args:
            source_schema_fields: List of available field names in the source
            
        Returns:
            Dictionary of potential semantic matches for critical fields
        """
        import re
        semantic_match_candidates = {}
        
        # Common field name patterns for important attributes
        patterns = {
            "id": [r"id$", r"sku", r"product.?id", r"item.?id", r"^code$", r"key"],
            "name": [r"name$", r"title", r"product.?name", r"item.?name", r"^name$", r"label"],
            "title": [r"title", r"name", r"headline", r"product.?title", r"item.?title"],
            "description": [r"description", r"desc", r"text", r"content", r"details", r"summary"],
            "price": [r"price$", r"cost", r"amount", r"^price$", r"sale.?price"],
            "image": [r"image", r"photo", r"picture", r"thumbnail", r"img", r"url"],
            "category": [r"category", r"group", r"type", r"department", r"section"],
            "brand": [r"brand", r"manufacturer", r"vendor", r"make", r"company"]
        }
        
        # For each source field, check if it might be a semantic match for any critical field
        for source_field in source_schema_fields:
            for critical_field, field_patterns in patterns.items():
                if any(re.search(pattern, source_field, re.IGNORECASE) for pattern in field_patterns):
                    if critical_field not in semantic_match_candidates:
                        semantic_match_candidates[critical_field] = []
                    semantic_match_candidates[critical_field].append(source_field)
    
        return semantic_match_candidates
    
    def complete_transformation_pipeline(
        self,
        source_table_name: str,
        destination_table_name: str,
        source_schema_fields: List[str],
        source_data_sample_json: Optional[str] = None
    ) -> str:
        """
        Complete multi-stage SQL transformation pipeline with intelligent field mapping.
        
        Args:
            source_table_name: The BigQuery source table ID 
            destination_table_name: The BigQuery destination table ID
            source_schema_fields: List of field names available in the source table
            source_data_sample_json: Optional sample of source data for semantic mapping
            
        Returns:
            Complete SQL transformation query
        """
        logger.info(f"Starting complete transformation pipeline for {source_table_name} to {destination_table_name}")
        
        # Parse source data sample if provided
        source_data_sample = None
        if source_data_sample_json:
            try:
                source_data_sample = json.loads(source_data_sample_json)
                logger.info(f"Successfully parsed source data sample, found {len(source_data_sample)} records")
            except Exception as e:
                logger.warning(f"Failed to parse source data sample as JSON: {str(e)}")
        
        # Extract destination field names from schema
        destination_fields = []
        if self.destination_schema:
            try:
                destination_fields = [field["name"] for field in self.destination_schema]
                # Also add nested fields that might need special handling
                for field in self.destination_schema:
                    if field.get("type") == "RECORD" and field.get("fields"):
                        parent = field["name"]
                        for child in field["fields"]:
                            destination_fields.append(f"{parent}.{child['name']}")
                logger.info(f"Extracted {len(destination_fields)} destination fields from schema")
            except Exception as e:
                logger.warning(f"Error extracting destination fields from schema: {str(e)}")
        
        # Stage 1: Generate initial SQL with enhanced mapping
        initial_result = self.generate_sql_transformation(
            source_table_name, 
            destination_table_name,
            source_schema_fields
        )
        sql_query = initial_result["sql_query"]
        
        # Stage 2: If we have sample data, improve field mappings
        if source_data_sample:
            # Identify fields that were defaulted and need semantic mapping
            fields_to_refine = self.identify_defaulted_fields(sql_query)
            
            if fields_to_refine:
                logger.info(f"Found {len(fields_to_refine)} fields requiring semantic mapping: {fields_to_refine}")
                # Use our best field matching logic to find better matches
                field_matches = self.select_best_field_matches(
                    source_schema_fields,
                    fields_to_refine,
                    source_data_sample
                )
                
                if field_matches:
                    logger.info(f"Found potential field matches: {field_matches}")
                
                # Perform semantic enhancement with identified matches
                sql_query = self.semantically_enhance_sql(
                    sql_query,
                    source_table_name,
                    source_schema_fields,
                    source_data_sample_json,
                    self.destination_schema,
                    fields_to_refine
                )
        
        return sql_query
    
    def select_best_field_matches(
        self, 
        source_fields: List[str], 
        destination_fields: List[str],
        source_data_sample: Optional[List[Dict[str, Any]]] = None
    ) -> Dict[str, str]:
        """
        Select the best source field for each destination field using a multi-tier approach.
        
        Args:
            source_fields: List of available source field names
            destination_fields: List of destination field names to find matches for
            source_data_sample: Optional sample of source data for content-based validation
            
        Returns:
            A dict mapping destination fields to source fields
        """
        matches = {}
        source_fields_lower = [f.lower() for f in source_fields]
        
        # Track used source fields to avoid duplicates (when appropriate)
        used_source_fields = set()
        
        # Step 1: First try exact matches (case-insensitive)
        for dest_field in destination_fields:
            dest_field_lower = dest_field.lower()
            
            # Check for exact match
            if dest_field_lower in source_fields_lower:
                idx = source_fields_lower.index(dest_field_lower)
                source_field = source_fields[idx]
                matches[dest_field] = source_field
                used_source_fields.add(source_field)
        
        # Step 2: For unmatched fields, try pattern-based matching
        unmatched_fields = [f for f in destination_fields if f not in matches]
        if unmatched_fields:
            # Get potential matches based on patterns
            candidates = self.analyze_source_fields(source_fields)
            
            for dest_field in unmatched_fields:
                # For nested fields, look for matches on the child field part
                if "." in dest_field:
                    parent, child = dest_field.split(".", 1)
                    if child in candidates and candidates[child]:
                        # Get the highest confidence match for the child field
                        best_match = candidates[child][0]
                        matches[dest_field] = best_match  # Map to the parent.child structure in destination
                        used_source_fields.add(best_match)
                # For regular fields
                elif dest_field in candidates and candidates[dest_field]:
                    # Get the highest confidence match
                    best_match = candidates[dest_field][0]
                    matches[dest_field] = best_match
                    used_source_fields.add(best_match)
        
        # Step 3: If we have sample data, validate/improve matches
        if source_data_sample and len(source_data_sample) > 0:
            # Analyze the content of fields to find potential matches
            # This could compare source field values to expected formats for IDs, names, prices, etc.
            # For now, we'll keep this as a placeholder for future enhancement
            pass
            
        return matches
    
    def semantically_enhance_sql(
        self,
        current_sql_query: str,
        source_table_name: str,
        source_schema_fields: List[str],
        source_data_sample_json: str, # Expecting a JSON string representation of List[Dict[str, Any]]
        destination_schema: Dict[str, Any],
        critical_fields_to_refine: List[str]
    ) -> str:
        """
        Refines an existing SQL query by attempting to semantically map critical fields
        using a sample of source data.

        Args:
            current_sql_query: The initial SQL query (likely from generate_sql_transformation).
            source_table_name: Name of the source BigQuery table.
            source_schema_fields: List of field names in the source schema.
            source_data_sample_json: A JSON string representing the first few rows of source data.
            destination_schema: The JSON schema of the destination table.
            critical_fields_to_refine: A list of destination field names that need semantic review and potential remapping.

        Returns:
            The refined BigQuery GoogleSQL query string.
        """
        logger.info(f"Starting semantic enhancement for SQL query targeting table {source_table_name} for fields: {critical_fields_to_refine}")

        formatted_destination_schema = json.dumps(destination_schema, indent=2)
        formatted_source_fields = ", ".join(source_schema_fields)
        # Ensure source_data_sample_json is indeed a string; if it's already parsed, dump it back.
        if not isinstance(source_data_sample_json, str):
            source_data_sample_json = json.dumps(source_data_sample_json, indent=2)


        prompt = f"""You are an data mapping expert specializing in BigQuery GoogleSQL transformations.
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

        # Define a simple function tool for the output
        semantic_enhancement_schema = FunctionDeclaration(
            name="semantic_sql_enhancement_output",
            description="Output for the semantically enhanced SQL query.",
            parameters={
                "type": "OBJECT",
                "properties": {
                    "refined_sql_query": {
                        "type": "STRING",
                        "description": "The complete, semantically enhanced BigQuery GoogleSQL query."
                    },
                    "semantic_changes_made": {
                        "type": "ARRAY",
                        "items": {"type": "STRING"},
                        "description": "Brief descriptions of semantic changes made or reasons for no change."
                    }
                },
                "required": ["refined_sql_query"]
            }
        )
        semantic_tool = Tool(function_declarations=[semantic_enhancement_schema])

        try:
            contents = [Content(role="user", parts=[Part.from_text(text=prompt)])]
            generate_content_config = GenerateContentConfig(
                temperature=0.2, # Lower temperature for more deterministic changes
                max_output_tokens=65535,
                top_p=0.95,
                top_k=40
                # tools=[semantic_tool] # Tool removed for direct SQL output
            )

            response = self.client.models.generate_content(
                model=self.model, # Or a model potentially better at reasoning/code editing
                contents=contents,
                config=generate_content_config,
            )

            if not response.candidates:
                raise ValueError("No candidates in the response for semantic SQL enhancement.")
            
            candidate = response.candidates[0]
            if candidate.finish_reason == FinishReason.MAX_TOKENS:
                logger.error("Semantic SQL enhancement failed: Output truncated due to MAX_TOKENS limit.")
                raise ValueError("Semantic SQL enhancement failed: Output truncated due to MAX_TOKENS limit.")

            # Simplified response extraction (expecting direct SQL text)
            refined_sql_query = None
            if hasattr(candidate.content, 'parts') and candidate.content.parts and hasattr(candidate.content.parts[0], 'text'):
                refined_sql_query = candidate.content.parts[0].text.strip()
                # Strip markdown
                if refined_sql_query.startswith("```sql"):
                    refined_sql_query = refined_sql_query.replace("```sql", "", 1).replace("```", "", 1).strip()
                elif refined_sql_query.startswith("```"):
                    refined_sql_query = refined_sql_query.replace("```", "", 1).replace("```", "", 1).strip()

                if not (refined_sql_query.upper().startswith("CREATE OR REPLACE TABLE") or refined_sql_query.upper().startswith("SELECT")):
                    logger.error(f"Semantic SQL enhancement: Extracted text does not appear to be a valid SQL query: {refined_sql_query[:200]}...")
                    # Fallback to original query if enhancement results in non-SQL
                    logger.warning("Semantic SQL enhancement resulted in non-SQL text, returning original query.")
                    return current_sql_query 
                
                logger.info(f"Successfully enhanced SQL (direct text): {refined_sql_query[:100]}...")

            elif hasattr(candidate, 'text') and candidate.text: # Fallback if parts[0].text is not available
                refined_sql_query = candidate.text.strip()
                # Strip markdown
                if refined_sql_query.startswith("```sql"):
                    refined_sql_query = refined_sql_query.replace("```sql", "", 1).replace("```", "", 1).strip()
                elif refined_sql_query.startswith("```"):
                    refined_sql_query = refined_sql_query.replace("```", "", 1).replace("```", "", 1).strip()
                
                if not (refined_sql_query.upper().startswith("CREATE OR REPLACE TABLE") or refined_sql_query.upper().startswith("SELECT")):
                    logger.error(f"Semantic SQL enhancement: Extracted candidate.text does not appear to be a valid SQL query: {refined_sql_query[:200]}...")
                    logger.warning("Semantic SQL enhancement (candidate.text) resulted in non-SQL text, returning original query.")
                    return current_sql_query
                logger.info(f"Successfully enhanced SQL (candidate.text): {refined_sql_query[:100]}...")
            else:
                logger.error(f"Semantic SQL enhancement: Could not extract SQL text from response. Candidate: {candidate}")
                logger.warning("Semantic SQL enhancement failed to extract text, returning original query.")
                return current_sql_query

            # Programmatic fix for CREATE OR REPLACE TABLE formatting, in case enhancement altered it
            import re
            if refined_sql_query:
                refined_sql_query = re.sub(
                    r"CREATE\s+OR\s+REPLACE\s+TABLE\s*`([^`]+)`\s*AS",
                    lambda m: f"CREATE OR REPLACE TABLE `{m.group(1)}` AS",
                    refined_sql_query, count=1, flags=re.IGNORECASE
                )
                refined_sql_query = re.sub(
                    r"(CREATE\s+OR\s+REPLACE\s+TABLE)\s+(?=`)" ,
                    r"\1 ",
                    refined_sql_query, count=1, flags=re.IGNORECASE
                )
                refined_sql_query = re.sub(
                    r"(?<=`)\s+(AS\s+SELECT)",
                    r" \1",
                    refined_sql_query, count=1, flags=re.IGNORECASE
                )
                if refined_sql_query.startswith("``"):
                    refined_sql_query = refined_sql_query[2:]
                elif refined_sql_query.startswith("`"):
                    refined_sql_query = refined_sql_query[1:]
            
            return refined_sql_query

        except Exception as e:
            logger.error(f"Error during semantic SQL enhancement: {str(e)}")
            logger.error(f"Semantic SQL enhancement - Full response candidate for debugging: {candidate if 'candidate' in locals() else 'candidate not available'}")
            logger.warning(f"Semantic SQL enhancement failed due to exception. Returning original query: {current_sql_query[:200]}...")
            return current_sql_query


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

Provide a fixed version of the SQL script that resolves the error. 
Your response MUST be ONLY a call to the `sql_fix_output` function. Do NOT include any other explanatory text, conversational pleasantries, or markdown formatting.
"""
        
        try:
            # Create content structure for prompt
            contents = [Content(role="user", parts=[Part.from_text(text=prompt)])]
            
            # Set up generation configuration with our tool
            generate_content_config = GenerateContentConfig(
                temperature=0.2,
                max_output_tokens=65535,
                top_p=0.95,
                top_k=40,
                tools=[sql_fix_tool]
            )
            
            # Generate content
            response = self.client.models.generate_content(
                model=self.model,
                contents=contents,
                config=generate_content_config
            )

            if not response.candidates:
                raise ValueError("No candidates in the response for SQL refinement")

            candidate = response.candidates[0]

            # Check for MAX_TOKENS finish reason
            if candidate.finish_reason == FinishReason.MAX_TOKENS:
                logger.error("SQL refinement failed: Output truncated due to MAX_TOKENS limit.")
                raise ValueError("SQL refinement failed: Output truncated due to MAX_TOKENS limit.")

            # Extract the structured function response
            result = None
            fixed_sql_query = None

            try:
                # Check if we have a function_call in the expected location
                # Also ensure that function_call itself is not None
                if hasattr(candidate.content.parts[0], 'function_call') and candidate.content.parts[0].function_call is not None:
                    function_response = candidate.content.parts[0].function_call
                    # The explicit 'if function_response is None:' check is now redundant
                    
                    # Safely access name with a defensive check
                    function_name = getattr(function_response, 'name', 'unknown_function')
                    if function_name == 'unknown_function':
                        logger.warning("Refine SQL - Function response has no 'name' attribute or it is None. Using 'unknown_function'.")
                    logger.info(f"Refine SQL - Function call name: {function_name}")
                    
                    function_args = getattr(function_response, 'args', None)
                    if function_args is not None and "sql_fix_output" in function_args:
                        result = json.loads(function_args["sql_fix_output"])
                    elif function_args is not None and function_name == "sql_fix_output":
                        if isinstance(function_response.args, dict):
                            result = function_response.args
                        else:
                            result = json.loads(json.dumps(function_response.args))
                    else:
                        logger.warning("Refine SQL - 'sql_fix_output' key not found in function_response.args or name mismatch.")
                        # Attempt to parse text as a fallback if function call is problematic
                        if hasattr(candidate.content.parts[0], 'text') and candidate.content.parts[0].text:
                            logger.info("Refine SQL - Attempting to parse text response as fallback.")
                            text_response = candidate.content.parts[0].text
                            try:
                                import re
                                json_pattern = r'```json\s*(.*?)\s*```'
                                json_match = re.search(json_pattern, text_response, re.DOTALL)
                                if json_match:
                                    json_str = json_match.group(1)
                                    result = json.loads(json_str)
                                else:
                                    result = json.loads(text_response) # Try parsing whole text
                            except (json.JSONDecodeError, re.error) as text_parse_error:
                                logger.error(f"Refine SQL - Failed to parse text response as JSON: {text_parse_error}")
                                raise ValueError("Refine SQL - Failed to extract function call and could not parse text response.")
                        else:
                            raise ValueError("Refine SQL - No valid function call and no text part to parse.")

                elif hasattr(candidate.content.parts[0], 'text') and candidate.content.parts[0].text:
                    text_response = candidate.content.parts[0].text
                    logger.info(f"Refine SQL - Using text response as primary: {text_response[:100]}...")
                    try:
                        import re
                        json_pattern = r'```json\s*(.*?)\s*```'
                        json_match = re.search(json_pattern, text_response, re.DOTALL)
                        if json_match:
                            json_str = json_match.group(1)
                            result = json.loads(json_str)
                        else:
                            result = json.loads(text_response) # Try parsing whole text
                        
                        # If parsing text gives a string, assume it's the SQL directly
                        if isinstance(result, str) and ("SELECT" in result.upper() or "CREATE OR REPLACE TABLE" in result.upper()): # Basic check for SQL
                             fixed_sql_query = result
                             result = {"fixed_sql": fixed_sql_query, "changes": ["Extracted SQL directly from text response."], "reasoning": "Fallback to text extraction."}
                        # If the text was parsed into a dict but doesn't have fixed_sql, it's an issue
                        elif isinstance(result, dict) and "fixed_sql" not in result:
                            logger.error(f"Refine SQL - Text parsed to dict but 'fixed_sql' key missing. Result: {result}")
                            raise ValueError("Refine SQL - Text parsed to dict but 'fixed_sql' key missing.")
                        elif not isinstance(result, dict) and not fixed_sql_query: # If it's not a dict and not SQL
                            raise ValueError("Refine SQL - Parsed text is neither a valid JSON structure for SQL fix nor direct SQL.")


                    except (json.JSONDecodeError, re.error) as text_parse_error:
                        logger.warning(f"Refine SQL - Failed to parse text response as JSON: {text_parse_error}. Checking if it's raw SQL.")
                        # If text parsing fails, and it looks like SQL, use it directly
                        if "SELECT" in text_response.upper() or "CREATE OR REPLACE TABLE" in text_response.upper(): # Basic check for SQL
                            logger.info("Refine SQL - Text response appears to be raw SQL. Using directly.")
                            fixed_sql_query = text_response
                            # Ensure result is a dict for consistent processing later
                            result = {"fixed_sql": fixed_sql_query, "changes": ["Used raw text response as SQL."], "reasoning": "Fallback to raw text SQL."}
                        else:
                            logger.error(f"Refine SQL - Text response is not valid JSON and not recognized as SQL. Content: {text_response[:500]}")
                            raise ValueError(f"Refine SQL - Could not extract valid JSON or direct SQL from text response.")
                else:
                    raise ValueError("Refine SQL - Unexpected response format. No function call or text part found.")

                if not result or "fixed_sql" not in result:
                    # This case should ideally be caught by earlier checks if fixed_sql_query was populated
                    if fixed_sql_query and (not result or "fixed_sql" not in result):
                         result = {"fixed_sql": fixed_sql_query, "changes": ["Extracted SQL directly from text response."], "reasoning": "Fallback to text extraction."}
                    elif not fixed_sql_query: # If fixed_sql_query is also None here, then we truly have no SQL
                        logger.error(f"Refine SQL - 'fixed_sql' not found in extracted result and no direct SQL extracted. Result: {result}")
                        raise ValueError("Refine SQL - Could not find 'fixed_sql' in extracted result and no direct SQL fallback.")
                    # If result exists but fixed_sql is missing, it's an error from structured parsing
                    elif result and "fixed_sql" not in result:
                         logger.error(f"Refine SQL - 'fixed_sql' not found in structured result. Result: {result}")
                         raise ValueError("Refine SQL - 'fixed_sql' not found in structured result.")


            except Exception as e:
                logger.error(f"Refine SQL - Error extracting structured response: {str(e)}")
                logger.error(f"Refine SQL - Full response candidate for debugging: {candidate}") 
                raise ValueError(f"Refine SQL - Failed to extract structured response: {str(e)}")

            # Ensure fixed_sql is present before proceeding
            if "fixed_sql" not in result or not result["fixed_sql"]:
                logger.error(f"Refine SQL - Final check: 'fixed_sql' is missing or empty in the result. Result: {result}")
                raise ValueError("Refine SQL - Final check: 'fixed_sql' is missing or empty.")

            # Log the changes made
            if "changes" in result and result["changes"]:
                for change in result["changes"]:
                    logger.info(f"SQL fix change: {change}")
            
            logger.info(f"SQL script refined successfully")
            if "reasoning" in result and result["reasoning"]:
                logger.info(f"Reasoning: {result['reasoning']}")
                
            return result["fixed_sql"]
            
        except Exception as e:
            logger.error(f"Error refining SQL script: {str(e)}")
            raise Exception(f"Failed to refine SQL script: {str(e)}")
