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
import difflib
import json
from typing import Dict, Any, Optional, List

from google.genai.types import GenerateContentConfig, FinishReason, FunctionCall

from ..common.client_utils import GenAIClient
from ..common.prompt_utils import SQL_DIFF_ANALYSIS_TOOL, SQL_DIFF_ANALYSIS_SCHEMA

logger = logging.getLogger(__name__)

class DiffAnalyzer:
    """
    Analyzes and explains the differences between original and fixed SQL scripts.
    Logic derived from SQLFixService.analyze_differences.
    """

    def __init__(self, project_id: Optional[str] = None, location: Optional[str] = None, model_name: Optional[str] = None, use_genai_for_analysis: bool = True):
        """
        Initializes the DiffAnalyzer.

        Args:
            project_id: The Google Cloud Project ID (required if use_genai_for_analysis is True).
            location: The GCP region (required if use_genai_for_analysis is True).
            model_name: Optional. The Gemini model name.
            use_genai_for_analysis: Whether to use GenAI for analyzing the diff. 
                                    If False, only a basic textual diff is provided.
        """
        self.use_genai_for_analysis = use_genai_for_analysis
        if self.use_genai_for_analysis:
            if not project_id or not location:
                raise ValueError("project_id and location must be provided if use_genai_for_analysis is True.")
            self.genai_client = GenAIClient(project_id, location, model_name)
        else:
            self.genai_client = None
            logger.info("DiffAnalyzer initialized without GenAI capabilities for analysis.")

    def _construct_analysis_prompt(self, original_sql: str, fixed_sql: str, diff_text: str) -> str:
        """Constructs the prompt for the GenAI diff analysis task."""
        prompt = rf"""You are an expert SQL analyst. Analyze the differences between the original and fixed SQL scripts.

ORIGINAL SQL:
```sql
{original_sql}
```

FIXED SQL:
```sql
{fixed_sql}
```

DIFF (unified format):
```diff
{diff_text}
```

Provide a detailed analysis of the significant changes made between the scripts.
Specifically focus on:
1. Field replacements or remapping (e.g., `source.old_field AS alias` changed to `source.new_field AS alias` or `NULL AS alias`).
2. Syntax corrections (e.g., backtick usage, spacing, keyword changes).
3. Value handling changes (e.g., adding `IFNULL`, `SAFE_CAST`, or changing default values).
4. Structural changes (e.g., modifications to JOIN conditions, WHERE clauses, or entire subqueries added/removed).
5. Identify the primary type of issue that was likely fixed.

Your response MUST be ONLY a call to the `{SQL_DIFF_ANALYSIS_SCHEMA.name}` function. Do NOT include any other explanatory text.
"""
        return prompt

    def analyze_sql_differences(self, original_sql: str, fixed_sql: str) -> Dict[str, Any]:
        """
        Analyzes the differences between two SQL scripts.

        Args:
            original_sql: The original SQL script.
            fixed_sql: The fixed or modified SQL script.

        Returns:
            A dictionary containing the analysis. If GenAI is used, this will be
            the structured output from the model. Otherwise, a basic diff is returned.
        """
        original_sql_lines = original_sql.splitlines()
        fixed_sql_lines = fixed_sql.splitlines()

        diff_lines = list(difflib.unified_diff(
            original_sql_lines,
            fixed_sql_lines,
            fromfile='original.sql',
            tofile='fixed.sql',
            lineterm='', # Keep original line endings in the diff lines
            n=3  # Number of context lines
        ))
        diff_text = '\n'.join(diff_lines)
        
        # Basic diff stats (can be part of the fallback or always included)
        removed_lines_count = len([line for line in diff_lines if line.startswith('-') and not line.startswith('---')])
        added_lines_count = len([line for line in diff_lines if line.startswith('+') and not line.startswith('+++')])

        if not self.use_genai_for_analysis or not self.genai_client:
            logger.info("Performing basic diff analysis (GenAI not used).")
            return {
                "diff_text": diff_text,
                "changes": ["SQL structure was modified. GenAI analysis not enabled."],
                "primary_issue_type": "unknown (GenAI analysis not enabled)",
                "removed_lines_count": removed_lines_count,
                "added_lines_count": added_lines_count
            }

        logger.info("Attempting GenAI-powered analysis of SQL differences.")
        prompt = self._construct_analysis_prompt(original_sql, fixed_sql, diff_text)

        generation_config = GenerateContentConfig(
            temperature=0.2,
            max_output_tokens=2048, # Analysis can be somewhat verbose
            top_p=0.95,
            top_k=40
        )

        text_resp, func_call_resp, gen_err_msg, finish_reason = self.genai_client.generate_content(
            prompt_text=prompt,
            generation_config_override=generation_config,
            tools=[SQL_DIFF_ANALYSIS_TOOL]
        )

        analysis_result: Dict[str, Any] = {}

        if gen_err_msg:
            logger.error(f"SQL diff analysis GenAI call failed: {gen_err_msg}")
            analysis_result["error"] = gen_err_msg
        elif func_call_resp and func_call_resp.name == SQL_DIFF_ANALYSIS_SCHEMA.name:
            logger.info(f"Received function call for diff analysis: {func_call_resp.name}")
            args = GenAIClient.parse_function_call_args(func_call_resp, SQL_DIFF_ANALYSIS_SCHEMA.name)
            if args:
                analysis_result = args
                logger.info(f"SQL diff analysis successful. Primary issue type: {args.get('primary_issue_type', 'N/A')}")
            else:
                logger.warning("Could not parse arguments from SQL diff analysis function call.")
                analysis_result["error"] = "Failed to parse function call arguments for diff analysis."
        elif text_resp: # Fallback if model provides text instead of function call
             logger.warning(f"SQL diff analysis: Model returned text instead of function call. Finish reason: {finish_reason.name if finish_reason else 'N/A'}. Text: {text_resp[:200]}")
             analysis_result["error"] = "Model returned text instead of expected function call for diff analysis."
             analysis_result["raw_text_response"] = text_resp
        else:
            err_msg = f"No function call or text response for SQL diff analysis. Finish reason: {finish_reason.name if finish_reason else 'UNKNOWN'}."
            logger.error(err_msg)
            analysis_result["error"] = err_msg
            
        # Always include the diff text and basic counts
        analysis_result["diff_text"] = diff_text
        if "removed_lines_count" not in analysis_result: # Add if not provided by GenAI
            analysis_result["removed_lines_count"] = removed_lines_count
        if "added_lines_count" not in analysis_result: # Add if not provided by GenAI
            analysis_result["added_lines_count"] = added_lines_count
            
        return analysis_result


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    
    original_query = """SELECT
    name,
    product_id AS id,
    description_old AS description,
    price AS priceInfo_price -- Incorrect nesting
FROM
    source_table
WHERE
    category = "electronics" AND price > 0;"""

    fixed_query = """SELECT
    name,
    product_id AS id,
    description_old AS description, -- Semantically mapped description from source.description_old based on data sample.
    STRUCT(
        price AS price,
        "USD" AS currencyCode
    ) AS priceInfo
FROM
    source_table AS source -- Added alias
WHERE
    source.category = "electronics" AND source.price > 0;"""

    # Test without GenAI
    print("\n--- Testing DiffAnalyzer (GenAI disabled) ---")
    analyzer_no_genai = DiffAnalyzer(use_genai_for_analysis=False)
    basic_analysis = analyzer_no_genai.analyze_sql_differences(original_query, fixed_query)
    print(f"Basic Analysis Result: {json.dumps(basic_analysis, indent=2)}")
    print(f"\nDiff Text:\n{basic_analysis['diff_text']}")

    # Test with GenAI (requires GOOGLE_APPLICATION_CREDENTIALS and GOOGLE_CLOUD_PROJECT)
    project = os.environ.get("GOOGLE_CLOUD_PROJECT")
    if not project:
        logger.warning("GOOGLE_CLOUD_PROJECT environment variable not set. Skipping GenAI DiffAnalyzer example.")
    else:
        print("\n--- Testing DiffAnalyzer (GenAI enabled) ---")
        analyzer_with_genai = DiffAnalyzer(project_id=project, location="us-central1", use_genai_for_analysis=True)
        genai_analysis = analyzer_with_genai.analyze_sql_differences(original_query, fixed_query)
        print(f"GenAI Analysis Result: {json.dumps(genai_analysis, indent=2)}")
        if "error" not in genai_analysis:
             print(f"\nGenAI Changes: {genai_analysis.get('changes')}")
             print(f"GenAI Primary Issue: {genai_analysis.get('primary_issue_type')}")
        # Diff text is always included
        # print(f"\nDiff Text (from GenAI analysis):\n{genai_analysis['diff_text']}")
