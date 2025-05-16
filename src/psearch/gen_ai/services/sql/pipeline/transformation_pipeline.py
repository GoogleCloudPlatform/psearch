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
import json # Added for json.dumps
from typing import Dict, Any, Optional, List, Tuple

from google.cloud import bigquery # Added for fetching sample data
from ..generation.initial_sql_generator import InitialSQLGenerator
from ..enhancement.field_analyzer import FieldAnalyzer
from ..enhancement.semantic_enhancer import SemanticEnhancer
from ..validation.sql_validator import SQLValidator
from ..fixing.sql_fixer import SQLFixer
from ..common.schema_utils import SchemaLoader
from ....tasks import task_manager # Import the new task manager

logger = logging.getLogger(__name__)

class TransformationPipeline:
    """
    Orchestrates the multi-stage SQL transformation pipeline, including
    initial generation, semantic enhancement, validation, and fixing.
    """

    DEFAULT_MAX_FIX_ATTEMPTS = 3

    def __init__(self, project_id: str, location: str, model_name: Optional[str] = None):
        """
        Initializes the TransformationPipeline with all necessary components.

        Args:
            project_id: The Google Cloud Project ID.
            location: The GCP region (e.g., us-central1).
            model_name: Optional. The Gemini model name to be used by components.
        """
        self.project_id = project_id
        self.location = location
        self.model_name = model_name

        self.initial_generator = InitialSQLGenerator(project_id, location, model_name)
        self.field_analyzer = FieldAnalyzer() # Does not require GenAI client for its current methods
        self.semantic_enhancer = SemanticEnhancer(project_id, location, model_name)
        self.sql_validator = SQLValidator(project_id)
        self.sql_fixer = SQLFixer(project_id, location, model_name)
        
        self.default_destination_schema = SchemaLoader.get_destination_schema()
        logger.info("TransformationPipeline initialized with all components.")

    def execute_pipeline( # Renamed from execute_pipeline for clarity if this becomes async later
        self,
        task_id: str, # Added task_id
        source_table_name: str,
        destination_table_name: str,
        source_schema_fields: List[str],
        destination_schema: Optional[Dict[str, Any]] = None,
        source_data_sample_json: Optional[str] = None,
        critical_fields_for_semantic_refinement: Optional[List[str]] = None,
        max_fix_attempts: int = DEFAULT_MAX_FIX_ATTEMPTS
    ) -> None: # Now returns None, status is updated via task_manager
        """
        Executes the complete SQL transformation pipeline for a given task_id.
        Updates task status and logs via the task_manager.

        Args:
            task_id: The unique ID for this transformation task.
            source_table_name: Source table ID.
            destination_table_name: Destination table ID.
            source_schema_fields: List of source field names.
            destination_schema: Optional. Destination schema dictionary. Uses default if None.
            source_data_sample_json: Optional. JSON string of source data sample.
            critical_fields_for_semantic_refinement: Optional. List of critical fields.
            max_fix_attempts: Maximum attempts to fix SQL errors.
        """
        current_sql: Optional[str] = None
        task_manager.add_task_log(task_id, f"SQL Transformation Pipeline started for {source_table_name} to {destination_table_name}.")
        task_manager.update_task_status(task_id, status="pipeline_started")

        current_destination_schema = destination_schema or self.default_destination_schema
        if not current_destination_schema:
            msg = "No destination schema available."
            logger.error(f"[Task {task_id}] {msg}")
            task_manager.add_task_log(task_id, f"ERROR: {msg}")
            task_manager.update_task_status(task_id, status="failed", error=msg)
            return

        try:
            # --- Step 1: Initial SQL Generation ---
            task_manager.update_task_status(task_id, status="generating_initial_sql")
            task_manager.add_task_log(task_id, "Step 1: Generating initial SQL.")
            current_sql, error_msg = self.initial_generator.generate(
                source_table_name,
                destination_table_name,
                source_schema_fields,
                current_destination_schema
            )
            if not current_sql:
                raise Exception(f"Initial SQL generation failed: {error_msg or 'No SQL returned'}")
            task_manager.add_task_log(task_id, f"Initial SQL generated (preview: {current_sql[:100]}...).")
            
            # --- Attempt to fetch source data sample if not provided ---
            fetched_sample_json_for_enhancement = source_data_sample_json
            if not fetched_sample_json_for_enhancement:
                task_manager.add_task_log(task_id, "Source data sample not provided by caller, attempting to fetch from BigQuery.")
                try:
                    # self.project_id is available from __init__
                    bq_client = bigquery.Client(project=self.project_id) 
                    # Ensure source_table_name is correctly formatted for BQ (e.g., `project.dataset.table`)
                    # The source_table_name argument should already be in this format.
                    sample_query = f"SELECT * FROM `{source_table_name}` LIMIT 3"
                    task_manager.add_task_log(task_id, f"Fetching source data sample with query: {sample_query}")
                    query_job = bq_client.query(sample_query)
                    rows = [dict(row) for row in query_job.result(timeout=30)] # Timeout for safety
                    if rows:
                        # Use default=str to handle non-serializable types like datetime
                        fetched_sample_json_for_enhancement = json.dumps(rows, default=str) 
                        task_manager.add_task_log(task_id, f"Successfully fetched {len(rows)} sample rows from source table.")
                        logger.info(f"[Task {task_id}] Fetched {len(rows)} sample rows for semantic enhancement.")
                    else:
                        task_manager.add_task_log(task_id, "No rows returned from source data sample query. Semantic enhancement might be skipped or limited.")
                        logger.info(f"[Task {task_id}] No sample rows fetched for semantic enhancement.")
                except Exception as bq_err:
                    task_manager.add_task_log(task_id, f"WARNING: Failed to fetch source data sample: {str(bq_err)}")
                    logger.warning(f"[Task {task_id}] Failed to fetch source data sample: {str(bq_err)}")
            
            # --- Step 2 & 3: Semantic Enhancement (if applicable) ---
            if fetched_sample_json_for_enhancement: # Check the potentially fetched sample
                task_manager.update_task_status(task_id, status="analyzing_for_semantic_enhancement")
                task_manager.add_task_log(task_id, "Step 2: Identifying fields for semantic refinement.")
                fields_to_check = critical_fields_for_semantic_refinement if critical_fields_for_semantic_refinement is not None else FieldAnalyzer.DEFAULT_CRITICAL_FIELDS
                defaulted_critical_fields = self.field_analyzer.identify_defaulted_fields(current_sql, fields_to_check)
                task_manager.add_task_log(task_id, f"Defaulted critical fields found: {defaulted_critical_fields if defaulted_critical_fields else 'None'}.")

                if defaulted_critical_fields:
                    task_manager.update_task_status(task_id, status="performing_semantic_enhancement")
                    task_manager.add_task_log(task_id, f"Step 3: Performing semantic enhancement for: {defaulted_critical_fields}.")
                    enhanced_sql, enhance_error_msg = self.semantic_enhancer.enhance_sql(
                        current_sql_query=current_sql,
                        source_table_name=source_table_name,
                        source_schema_fields=source_schema_fields,
                        source_data_sample_json=fetched_sample_json_for_enhancement, # Use the fetched/provided sample
                        critical_fields_to_refine=defaulted_critical_fields,
                        destination_schema=current_destination_schema
                    )
                    current_sql = enhanced_sql # enhance_sql returns original on error
                    if enhance_error_msg:
                        task_manager.add_task_log(task_id, f"WARNING: Semantic enhancement issue: {enhance_error_msg}. Continuing with previous SQL.")
                    else:
                        task_manager.add_task_log(task_id, f"Semantic enhancement applied (preview: {current_sql[:100]}...).")
                else: # This 'else' corresponds to 'if defaulted_critical_fields:'
                    task_manager.add_task_log(task_id, "Skipping semantic enhancement: No critical fields identified as needing refinement.")
            else: # This 'else' corresponds to 'if fetched_sample_json_for_enhancement:' (Corrected Indentation)
                task_manager.add_task_log(task_id, "Skipping semantic enhancement: No source data sample could be provided or fetched.")

            # --- Step 4 & 5: Validation and Fixing Loop ---
            for attempt in range(max_fix_attempts + 1):
                task_manager.update_task_status(task_id, status=f"validating_sql_attempt_{attempt+1}")
                log_attempt_msg = f"Initial Validation" if attempt == 0 else f"Validation Attempt {attempt + 1}"
                task_manager.add_task_log(task_id, f"Step 4: {log_attempt_msg}.")
                validation_result = self.sql_validator.validate_sql_dry_run(current_sql)
                
                if validation_result["valid"]:
                    task_manager.add_task_log(task_id, f"SQL validation successful. {validation_result.get('message', '')}")
                    task_manager.update_task_status(task_id, status="completed", result=current_sql)
                    logger.info(f"[Task {task_id}] Pipeline completed successfully.")
                    return

                error_detail = validation_result.get('error_message', 'Unknown validation error')
                task_manager.add_task_log(task_id, f"SQL validation failed: {error_detail}")
                logger.warning(f"[Task {task_id}] SQL validation failed: {error_detail}")

                if attempt >= max_fix_attempts:
                    raise Exception(f"Max fix attempts ({max_fix_attempts}) reached. SQL remains invalid. Last error: {error_detail}")

                task_manager.update_task_status(task_id, status=f"fixing_sql_attempt_{attempt+1}")
                task_manager.add_task_log(task_id, f"Step 5: Attempting SQL fix (Attempt {attempt + 1}/{max_fix_attempts}). Error: {error_detail[:100]}...")
                
                fixed_sql, fix_error_msg = self.sql_fixer.fix_sql(current_sql, error_detail)
                if fix_error_msg or not fixed_sql:
                    raise Exception(f"SQL fixing attempt {attempt + 1} failed: {fix_error_msg or 'No SQL returned by fixer'}")
                
                task_manager.add_task_log(task_id, f"SQL fix attempt {attempt + 1} applied (preview: {fixed_sql[:100]}...).")
                current_sql = fixed_sql
            
            # This part is reached if max_fix_attempts is exceeded and SQL is still invalid
            final_error_msg = f"Max fix attempts ({max_fix_attempts}) reached. SQL remains invalid. Last error: {error_detail}"
            logger.error(f"[Task {task_id}] {final_error_msg}")
            task_manager.add_task_log(task_id, f"ERROR: {final_error_msg}")
            # New status to indicate final SQL is available but invalid, along with the error
            task_manager.update_task_status(task_id, status="failed_validation_final_sql_available", error=error_detail, result=current_sql)
            return # End pipeline here, UI will handle next steps for simple_fix

        except Exception as e:
            error_str = str(e)
            logger.error(f"[Task {task_id}] Pipeline failed: {error_str}", exc_info=True)
            task_manager.add_task_log(task_id, f"FATAL ERROR in pipeline: {error_str}")
            task_manager.update_task_status(task_id, status="failed", error=error_str)


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    project = os.environ.get("GOOGLE_CLOUD_PROJECT")
    if not project:
        logger.error("GOOGLE_CLOUD_PROJECT environment variable not set. Skipping TransformationPipeline example.")
    else:
        pipeline = TransformationPipeline(project_id=project, location="us-central1")

        # Mock data similar to InitialSQLGenerator example
        mock_source_table = f"{project}.my_dataset.source_products_pipeline"
        mock_dest_table = f"{project}.my_dataset.dest_products_catalog_pipeline"
        mock_source_fields = ["product_ID", "productName", "PriceAmount", "description_text", "stockQty", "categories_list", "isAvailable", "brandName", "mainImageURL"]
        
        # Example source data sample (as a JSON string)
        mock_sample_data_json = json.dumps([
            {"product_ID": "P1001", "productName": "Super Laptop", "PriceAmount": 1200.50, "description_text": "High-end laptop", "brandName": "TechBrand"},
            {"product_ID": "P1002", "productName": "Basic Mouse", "PriceAmount": 25.00, "description_text": "Optical mouse", "brandName": "OfficeGear"},
        ])

        # Using default destination schema loaded by SchemaLoader
        # Ensure schema.json is present at src/psearch/gen_ai/services/schema.json
        if not pipeline.default_destination_schema:
             logger.error("Default destination schema not loaded for pipeline example. Ensure schema.json exists. Skipping.")
        else:
            logger.info(f"Pipeline example using destination schema with fields: {[f.get('name') for f in pipeline.default_destination_schema.get('fields', [])]}")
            
            final_sql, history = pipeline.execute_pipeline(
                source_table_name=mock_source_table,
                destination_table_name=mock_dest_table,
                source_schema_fields=mock_source_fields,
                source_data_sample_json=mock_sample_data_json,
                # critical_fields_for_semantic_refinement will use FieldAnalyzer.DEFAULT_CRITICAL_FIELDS
            )

            print("\n--- Transformation Pipeline Execution History ---")
            for i, entry in enumerate(history):
                print(f"\nStep {i+1}: {entry['step']}")
                print(f"  Status: {entry['status']}")
                if "message" in entry: print(f"  Message: {entry.get('message')}")
                if "error" in entry and entry["error"]: print(f"  Error: {entry['error']}")
                if "sql_generated" in entry: print(f"  SQL Generated: {entry['sql_generated']}")
                if "sql_enhanced" in entry: print(f"  SQL Enhanced: {entry['sql_enhanced']}")
                if "defaulted_critical_fields_found" in entry: print(f"  Defaulted Critical Fields: {entry['defaulted_critical_fields_found']}")
                if "validation_details" in entry:
                    val_details = entry['validation_details']
                    print(f"  Validation Valid: {val_details['valid']}")
                    if not val_details['valid']: print(f"  Validation Error: {val_details.get('error_message')}")


            print("\n--- Final Result ---")
            if final_sql:
                print("Pipeline completed successfully. Final SQL:")
                print(final_sql)
            else:
                print("Pipeline failed to produce a valid SQL query.")
