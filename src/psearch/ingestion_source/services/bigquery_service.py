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

import os
import logging
from typing import Dict, List, Any, Optional, Union
import json
from datetime import datetime

from google.cloud import bigquery
from google.api_core.exceptions import NotFound

logger = logging.getLogger(__name__)

class BigQueryService:
    """Service for interacting with BigQuery"""
    
    def __init__(self, project_id: str):
        """
        Initialize the BigQuery service.
        
        Args:
            project_id: The Google Cloud project ID
        """
        self.project_id = project_id
        self.client = bigquery.Client(project=project_id)
    
    async def create_dataset(
        self, 
        dataset_id: str, 
        location: str = "US", 
        description: str = None
    ) -> Dict[str, Any]:
        """
        Create a BigQuery dataset.
        
        Args:
            dataset_id: The ID of the dataset to create
            location: The geographic location of the dataset
            description: A description of the dataset
            
        Returns:
            A dictionary with the result of the operation
        """
        try:
            # Construct a full dataset reference
            dataset_ref = f"{self.project_id}.{dataset_id}"
            
            # Check if the dataset already exists
            try:
                self.client.get_dataset(dataset_ref)
                logger.info(f"Dataset {dataset_ref} already exists")
                return {
                    "created": False,
                    "message": f"Dataset {dataset_id} already exists",
                    "dataset_ref": dataset_ref
                }
            except NotFound:
                # Dataset doesn't exist, create it
                dataset = bigquery.Dataset(dataset_ref)
                dataset.location = location
                
                if description:
                    dataset.description = description
                
                # Create the dataset
                dataset = self.client.create_dataset(dataset)
                logger.info(f"Created dataset {dataset_ref} in {location}")
                
                return {
                    "created": True,
                    "message": f"Dataset {dataset_id} created successfully",
                    "dataset_ref": dataset_ref,
                    "location": location
                }
                
        except Exception as e:
            logger.error(f"Error creating dataset {dataset_id}: {str(e)}")
            raise
    
    async def create_table(
        self, 
        dataset_id: str, 
        table_id: str, 
        schema: List[Dict[str, Any]], 
        description: str = None
    ) -> Dict[str, Any]:
        """
        Create a BigQuery table with the specified schema.
        
        Args:
            dataset_id: The ID of the dataset containing the table
            table_id: The ID of the table to create
            schema: A list of schema field definitions
            description: A description of the table
            
        Returns:
            A dictionary with the result of the operation
        """
        try:
            # Construct a full table reference
            table_ref = f"{self.project_id}.{dataset_id}.{table_id}"
            
            # Check if the table already exists
            try:
                self.client.get_table(table_ref)
                logger.info(f"Table {table_ref} already exists")
                return {
                    "created": False,
                    "message": f"Table {table_id} already exists",
                    "table_ref": table_ref
                }
            except NotFound:
                # Table doesn't exist, create it
                table = bigquery.Table(table_ref)
                
                # Convert schema dict to SchemaField objects
                table.schema = self._create_schema_fields(schema)
                
                if description:
                    table.description = description
                
                # Create the table
                table = self.client.create_table(table)
                logger.info(f"Created table {table_ref}")
                
                return {
                    "created": True,
                    "message": f"Table {table_id} created successfully",
                    "table_ref": table_ref
                }
                
        except Exception as e:
            logger.error(f"Error creating table {dataset_id}.{table_id}: {str(e)}")
            raise
    
    async def load_table_from_uri(
        self,
        job_id: str,
        jobs_dict: Dict[str, Dict[str, Any]],
        dataset_id: str,
        table_id: str,
        uri: str,
        source_format: str = "CSV",
        write_disposition: str = "WRITE_TRUNCATE",
        skip_leading_rows: int = None,
        allow_jagged_rows: bool = None,
        allow_quoted_newlines: bool = None,
        field_delimiter: str = None,
        quote_character: str = None,
        autodetect: bool = True,  # Added autodetect parameter with default True
        max_bad_records: int = 0,  # Allow specifying number of bad records to accept
    ):
        """
        Load data into a BigQuery table from a Cloud Storage URI.
        This method is designed to be run as a background task.
        
        Args:
            job_id: The ID of the job in the jobs dictionary
            jobs_dict: A dictionary to store job status
            dataset_id: The ID of the dataset containing the table
            table_id: The ID of the table to load into
            uri: The Cloud Storage URI to load from
            source_format: The format of the source data
            write_disposition: How to handle existing data
            skip_leading_rows: Number of header rows to skip (CSV)
            allow_jagged_rows: Allow rows with missing trailing columns (CSV)
            allow_quoted_newlines: Allow quoted newlines (CSV)
            field_delimiter: Field delimiter character (CSV)
            quote_character: Quote character (CSV)
            autodetect: Whether to automatically detect schema from the source data
        """
        try:
            # Construct job config based on source format
            job_config = None
            
            if source_format == "CSV":
                job_config = bigquery.LoadJobConfig(
                    source_format=bigquery.SourceFormat.CSV,
                    write_disposition=getattr(bigquery.WriteDisposition, write_disposition),
                    allow_quoted_newlines=allow_quoted_newlines,
                    autodetect=autodetect,  # Use schema autodetection
                    max_bad_records=max_bad_records,  # Allow a specified number of bad records
                )
                
                # Add CSV-specific options if provided
                if skip_leading_rows is not None:
                    job_config.skip_leading_rows = skip_leading_rows
                
                if allow_jagged_rows is not None:
                    job_config.allow_jagged_rows = allow_jagged_rows
                
                if field_delimiter is not None:
                    job_config.field_delimiter = field_delimiter
                
                if quote_character is not None:
                    job_config.quote_character = quote_character
                
            elif source_format == "JSON":
                job_config = bigquery.LoadJobConfig(
                    source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
                    write_disposition=getattr(bigquery.WriteDisposition, write_disposition),
                    autodetect=autodetect,  # Use schema autodetection
                    max_bad_records=max_bad_records,  # Allow a specified number of bad records
                )
                
                # Log configuration details for debugging
                logger.info(f"Configuring JSON load job with: autodetect={autodetect}, max_bad_records={max_bad_records}")
            
            # Get full table reference
            table_ref = f"{self.project_id}.{dataset_id}.{table_id}"
            
            # Start the load job
            load_job = self.client.load_table_from_uri(
                uri,
                table_ref,
                job_config=job_config
            )
            
            # Update job status to running
            jobs_dict[job_id].update({
                "status": "RUNNING",
                "message": f"Loading data from {uri} to {table_ref}",
                "metadata": {
                    **jobs_dict[job_id].get("metadata", {}),
                    "bq_job_id": load_job.job_id
                }
            })
            
            # Wait for the job to complete
            load_job.result()  # This waits for the job to finish
            
            # Check for errors
            if load_job.errors:
                error_message = load_job.errors[0].get("message", "Unknown error")
                logger.error(f"Load job failed: {error_message}")
                
                # Provide more detailed error information
                error_details = ""
                if "Failed to parse JSON" in error_message:
                    error_details = (
                        "\n\nThis appears to be a JSON formatting error. BigQuery expects newline-delimited JSON (JSONL) format. "
                        "Each line should be a complete, valid JSON object. "
                        "You can try increasing max_bad_records to skip problematic records."
                    )
                
                # Update job status to failed with enhanced error information
                jobs_dict[job_id].update({
                    "status": "FAILED",
                    "message": f"Load job failed: {error_message}{error_details}",
                    "completed_at": datetime.now().isoformat(),
                    "metadata": {
                        **jobs_dict[job_id].get("metadata", {}),
                        "error_details": load_job.errors,
                        "bad_records_allowed": max_bad_records
                    }
                })
            else:
                # Get load job statistics
                destination_table = self.client.get_table(table_ref)
                
                # Update job status to completed
                # Get appropriate statistics based on what's available
                metadata = jobs_dict[job_id].get("metadata", {})
                metadata["row_count"] = destination_table.num_rows
                
                # Handle different attribute names for bytes_processed
                try:
                    if hasattr(load_job, 'total_bytes_processed'):
                        metadata["bytes_processed"] = load_job.total_bytes_processed
                    elif hasattr(load_job, 'output_bytes'):
                        metadata["bytes_processed"] = load_job.output_bytes
                    elif hasattr(load_job, 'bytes_processed'):
                        metadata["bytes_processed"] = load_job.bytes_processed
                    else:
                        # If no bytes processed attribute is available
                        logger.warning("Could not find bytes_processed attribute on LoadJob")
                except Exception as stats_err:
                    logger.warning(f"Error accessing job statistics: {stats_err}")
                
                jobs_dict[job_id].update({
                    "status": "COMPLETED",
                    "message": f"Loaded {destination_table.num_rows} rows into {table_ref}",
                    "completed_at": datetime.now().isoformat(),
                    "metadata": metadata
                })
                
                logger.info(f"Load job completed: {destination_table.num_rows} rows loaded into {table_ref}")
            
        except Exception as e:
            logger.error(f"Error loading data: {str(e)}")
            
            # Update job status to failed
            jobs_dict[job_id].update({
                "status": "FAILED",
                "message": f"Error loading data: {str(e)}",
                "completed_at": datetime.now().isoformat()
            })
    
    def _create_schema_fields(self, schema_fields: List[Dict[str, Any]]) -> List[bigquery.SchemaField]:
        """
        Convert schema field dictionaries to BigQuery SchemaField objects.
        
        Args:
            schema_fields: A list of schema field dictionaries
            
        Returns:
            A list of BigQuery SchemaField objects
        """
        if not schema_fields:
            return []
            
        result = []
        
        for field in schema_fields:
            # Skip any None fields
            if field is None:
                continue
                
            # Extract basic field properties
            name = field["name"]
            field_type = field["type"]
            mode = field.get("mode", "NULLABLE")
            description = field.get("description", None)
            
            # Handle nested fields (RECORD type)
            fields = None
            if field_type == "RECORD" and "fields" in field:
                nested_fields = field.get("fields")
                if nested_fields:
                    fields = self._create_schema_fields(nested_fields)
            
            # Create SchemaField object
            schema_field = bigquery.SchemaField(
                name=name,
                field_type=field_type,
                mode=mode,
                description=description,
                fields=fields
            )
            
            result.append(schema_field)
        
        return result
