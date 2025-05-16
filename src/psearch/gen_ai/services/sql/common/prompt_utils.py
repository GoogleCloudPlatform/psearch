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

"""
Utilities for constructing prompts and managing related schemas for SQL generation.
"""

from google.genai.types import FunctionDeclaration, Tool

# --- FunctionDeclaration for Initial SQL Generation (from SQLTransformationService.__init__) ---
# This was originally self.sql_schema in SQLTransformationService
SQL_TRANSFORMATION_OUTPUT_SCHEMA = FunctionDeclaration(
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

SQL_TRANSFORMATION_TOOL = Tool(function_declarations=[SQL_TRANSFORMATION_OUTPUT_SCHEMA])


# --- FunctionDeclaration for Semantic SQL Enhancement (from SQLTransformationService.semantically_enhance_sql) ---
SEMANTIC_SQL_ENHANCEMENT_OUTPUT_SCHEMA = FunctionDeclaration(
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

SEMANTIC_SQL_ENHANCEMENT_TOOL = Tool(function_declarations=[SEMANTIC_SQL_ENHANCEMENT_OUTPUT_SCHEMA])


# --- FunctionDeclaration for SQL Fixing (from SQLTransformationService.refine_sql_script) ---
# This is also used by SQLFixService (indirectly via SQLTransformationService.refine_sql_script)
SQL_FIX_OUTPUT_SCHEMA = FunctionDeclaration(
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

SQL_FIX_TOOL = Tool(function_declarations=[SQL_FIX_OUTPUT_SCHEMA])

# --- FunctionDeclaration for SQL Diff Analysis (from SQLFixService.analyze_differences) ---
SQL_DIFF_ANALYSIS_SCHEMA = FunctionDeclaration(
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

SQL_DIFF_ANALYSIS_TOOL = Tool(function_declarations=[SQL_DIFF_ANALYSIS_SCHEMA])

# Add any prompt template strings or helper functions for prompt construction below if needed.
# For example:
# INITIAL_SQL_GENERATION_PROMPT_TEMPLATE = """
# You are an expert GoogleSQL engineer...
# SOURCE TABLE NAME: `{source_table_name}`
# ...
# """

# def format_initial_sql_prompt(source_table_name: str, ...):
#     return INITIAL_SQL_GENERATION_PROMPT_TEMPLATE.format(...)
