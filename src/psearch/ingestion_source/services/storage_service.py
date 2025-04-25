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
import uuid
from typing import Dict, Any, Optional, List
import tempfile
import json # Add this import

from fastapi import UploadFile
from google.cloud import storage

logger = logging.getLogger(__name__)

class StorageService:
    """Service for handling file uploads to Google Cloud Storage"""
    
    # BigQuery allowed data types for schema validation
    BQ_ALLOWED_TYPES = [
        'STRING', 'INTEGER', 'FLOAT', 'BOOLEAN', 'TIMESTAMP', 
        'DATE', 'TIME', 'DATETIME', 'RECORD', 'NUMERIC', 'BYTES'
    ]
    
    def __init__(self, project_id: str):
        """
        Initialize the storage service.
        
        Args:
            project_id: The Google Cloud project ID
        """
        self.project_id = project_id
        self.client = storage.Client(project=project_id)
        
        # Default bucket name follows the specified pattern
        self.bucket_name = f"{project_id}_psearch_raw"
        
        # File type-specific folders
        self.csv_folder = "csv/"
        self.json_folder = "json/"
        
        # Ensure the bucket exists
        self._ensure_bucket_exists()
    
    def _ensure_bucket_exists(self):
        """Ensure the storage bucket exists, creating it if necessary"""
        try:
            self.bucket = self.client.get_bucket(self.bucket_name)
            logger.info(f"Using existing bucket: {self.bucket_name}")
        except Exception as e:
            logger.info(f"Bucket {self.bucket_name} does not exist, creating...")
            try:
                # Create bucket with standard settings
                # In production, consider adding lifecycle rules, etc.
                self.bucket = self.client.create_bucket(
                    self.bucket_name, 
                    location="us-central1"
                )
                logger.info(f"Created bucket: {self.bucket_name}")
            except Exception as create_error:
                logger.error(f"Failed to create bucket: {str(create_error)}")
                raise
    
    async def upload_file(self, file: UploadFile, file_id: str) -> str:
        """
        Upload a file to Google Cloud Storage.
        
        Args:
            file: The file to upload (from FastAPI)
            file_id: The unique identifier for this file
            
        Returns:
            The GCS URI for the uploaded file
        """
        # Determine file extension and select appropriate folder
        file_extension = file.filename.split(".")[-1].lower()
        
        if file_extension == "csv":
            folder_prefix = self.csv_folder
        elif file_extension == "json":
            folder_prefix = self.json_folder
        else:
            folder_prefix = "other/"
        
        # Generate a unique object name with folder structure
        object_name = f"{folder_prefix}{file_id}.{file_extension}"
        
        # Create a blob in the bucket
        blob = self.bucket.blob(object_name)
        
        # Set content type based on file extension
        content_type_map = {
            "csv": "text/csv",
            "json": "application/json",
        }
        content_type = content_type_map.get(file_extension, "application/octet-stream")
        blob.content_type = content_type
        
        try:
            # Create a temporary file path
            temp_fd, temp_file_path = tempfile.mkstemp(suffix=f'.{file_extension}')
            os.close(temp_fd) # Close the file descriptor, we'll open it properly

            logger.info(f"Created temporary file for processing: {temp_file_path}")

            if file_extension == "json":
                # Read the entire JSON file, parse, and write as NDJSON to the temp file
                ndjson_content_written = False
                try:
                    # Ensure reading from start
                    await file.seek(0)
                    content_bytes = await file.read()
                    logger.info(f"Read {len(content_bytes)} bytes from uploaded JSON file {file.filename}")

                    # Decode content, assuming UTF-8
                    content_str = content_bytes.decode('utf-8')
                    
                    # Advanced error repair for common JSON issues
                    try:
                        # Try standard JSON parse first
                        data = json.loads(content_str)
                        
                        # Check if this is a schema definition file
                        if self._is_schema_definition(data):
                            logger.info(f"Detected schema definition file: {file.filename}")
                            is_valid, errors = self._validate_schema_definition(data)
                            if not is_valid:
                                for error in errors:
                                    logger.warning(f"Schema validation warning: {error}")
                            
                            # Special handling for schema files - always process as JSONL regardless of structure
                            with open(temp_file_path, 'w', encoding='utf-8') as temp_file:
                                for item in data:
                                    json.dump(item, temp_file, ensure_ascii=False)
                                    temp_file.write('\n')
                            logger.info(f"Converted schema definition to JSONL format with {len(data)} fields")
                            ndjson_content_written = True
                            # Skip the rest of the JSON processing since we've handled it
                            data = None
                    except json.JSONDecodeError as e:
                        logger.warning(f"Initial JSON parse failed: {e}. Attempting repair...")
                        # If we get "No object found when new array is started" error, try to fix it
                        if "No object found when new array is started" in str(e) or "BeginArray returned false" in str(e):
                            # Try to handle the case where array brackets might be missing or malformed
                            content_str = content_str.strip()
                            
                            # Check if it starts with '[' - if not, add it
                            if not content_str.startswith('['):
                                content_str = '[' + content_str
                                logger.info("Added missing opening bracket '[' to JSON")
                            
                            # Check if it ends with ']' - if not, add it
                            if not content_str.endswith(']'):
                                content_str = content_str + ']'
                                logger.info("Added missing closing bracket ']' to JSON")
                                
                            # Handle malformed JSON arrays by checking for missing commas or extra commas
                            try:
                                # Try to parse the repaired content
                                data = json.loads(content_str)
                                logger.info("JSON repair successful!")
                            except json.JSONDecodeError as e2:
                                # If still failing, try line-by-line parsing method
                                logger.warning(f"First repair attempt failed: {e2}. Trying alternate method...")
                                
                                # Replace first [ and last ] to treat each line as separate object
                                lines = content_str.replace('[', '', 1)
                                if lines.endswith(']'):
                                    lines = lines[:-1]
                                    
                                lines = lines.split('\n')
                                
                                # Create an array of all valid JSON objects in the file
                                data = []
                                for line in lines:
                                    line = line.strip()
                                    if not line or line == ',' or line == ']' or line == '[':
                                        continue
                                        
                                    # Remove trailing commas which are invalid in JSON
                                    if line.endswith(','):
                                        line = line[:-1]
                                        
                                    try:
                                        item = json.loads(line)
                                        data.append(item)
                                    except json.JSONDecodeError:
                                        logger.warning(f"Skipping invalid JSON line: {line[:50]}...")
                                
                                if not data:
                                    # If all parsing attempts failed, raise the original error
                                    raise e
                                
                                logger.info(f"Extracted {len(data)} valid JSON objects using line-by-line parsing")
                        else:
                            # For other JSON errors, just re-raise
                            raise
                    
                    logger.info(f"Successfully parsed JSON content for {file.filename}")

                    # Open the temp file in write mode with UTF-8 encoding
                    with open(temp_file_path, 'w', encoding='utf-8') as temp_file:
                        if isinstance(data, list):
                            if not data:
                                logger.warning(f"JSON file {file.filename} contains an empty array.")
                            else:
                                for i, item in enumerate(data):
                                    # Use ensure_ascii=False for broader character support
                                    json.dump(item, temp_file, ensure_ascii=False)
                                    temp_file.write('\n')
                                logger.info(f"Converted JSON array ({len(data)} items) to NDJSON for file {file.filename} in {temp_file_path}")
                                ndjson_content_written = True
                        elif isinstance(data, dict):
                            # If it's not a list (e.g., single object), write it directly
                            json.dump(data, temp_file, ensure_ascii=False)
                            temp_file.write('\n')
                            logger.warning(f"Uploaded JSON file {file.filename} was a single object, wrote as one line NDJSON in {temp_file_path}")
                            ndjson_content_written = True
                        else:
                             logger.error(f"Parsed JSON content from {file.filename} is neither a list nor an object.")
                             # Write original content as fallback
                             temp_file.write(content_str)
                             logger.warning(f"Wrote original content to {temp_file_path} due to unexpected JSON structure.")

                except json.JSONDecodeError as json_err:
                    logger.error(f"Failed to parse uploaded JSON file {file.filename}: {json_err}. Writing original content.")
                    # Write the original, potentially invalid content directly to the temp file (binary mode)
                    with open(temp_file_path, 'wb') as temp_file:
                        await file.seek(0)
                        temp_file.write(await file.read())
                except Exception as proc_err:
                    logger.error(f"Error processing JSON file {file.filename} for NDJSON conversion: {proc_err}. Writing original content.")
                     # Write the original content directly to the temp file (binary mode)
                    with open(temp_file_path, 'wb') as temp_file:
                        await file.seek(0)
                        temp_file.write(await file.read())

            else:
                # For non-JSON files (like CSV), write directly to the temp file in binary mode
                with open(temp_file_path, 'wb') as temp_file:
                    await file.seek(0) # Ensure reading from start
                    chunk_size = 1024 * 1024  # 1 MB chunks
                    while chunk := await file.read(chunk_size):
                        temp_file.write(chunk)
                logger.info(f"Wrote non-JSON file {file.filename} directly to {temp_file_path}")

            # --- Uploading ---
            logger.info(f"Uploading processed file from {temp_file_path} to GCS object {object_name}")
            # Upload the processed temp file to GCS
            blob.upload_from_filename(temp_file_path)
            logger.info(f"Successfully uploaded to {object_name}")
            
            # Clean up the temp file
            os.unlink(temp_file_path)
            
            # Build the GCS URI
            gcs_uri = f"gs://{self.bucket_name}/{object_name}"
            
            # Store in our file map
            logger.info(f"Uploaded file {file.filename} to {gcs_uri}")
            return gcs_uri
        except Exception as e:
            logger.error(f"Error uploading file: {str(e)}")
            raise

    def get_file_uri(self, file_id: str, file_type: str) -> Optional[str]:
        """
        Get the GCS URI for a previously uploaded file using its ID and type.
        
        Args:
            file_id: The unique identifier of the file.
            file_type: The file extension (e.g., 'csv', 'json').
            
        Returns:
            The GCS URI or None if not found.
        """
        logger.info(f"Looking for file ID: {file_id} with type: {file_type}")
        
        file_type_lower = file_type.lower()
        
        if file_type_lower == "csv":
            folder_prefix = self.csv_folder
        elif file_type_lower == "json":
            folder_prefix = self.json_folder
        else:
            logger.error(f"Unsupported file_type provided: {file_type}")
            return None # Or raise an error, depending on desired behavior
            
        # Construct the deterministic object name
        object_name = f"{folder_prefix}{file_id}.{file_type_lower}"
        gcs_uri = f"gs://{self.bucket_name}/{object_name}"
        logger.info(f"Constructed expected GCS URI: {gcs_uri}")
        
        try:
            # Check if the specific blob exists
            blob = self.bucket.blob(object_name)
            if blob.exists():
                logger.info(f"Confirmed file exists at {gcs_uri}")
                return gcs_uri
            else:
                # Log specific blob names for debugging if not found
                logger.warning(f"File not found at the expected path: {gcs_uri}. Checking blobs...")
                try:
                    blobs = list(self.bucket.list_blobs(prefix=folder_prefix))
                    if blobs:
                         logger.info(f"Blobs found in {folder_prefix}: {[b.name for b in blobs]}")
                    else:
                         logger.info(f"No blobs found in {folder_prefix}")
                except Exception as list_e:
                    logger.error(f"Error listing blobs during debug check: {list_e}")
                return None
        except Exception as e:
            logger.error(f"Error finding file {file_id} in GCS: {str(e)}")
            return None

    # Removed delete_file method as it relied on the removed file_map
    # If deletion is needed, it should be reimplemented to accept file_type
    # or discover the file path similarly to get_file_uri.

    def _is_schema_definition(self, content_json) -> bool:
        """
        Detect if JSON content is likely a schema definition.
        
        Args:
            content_json: Parsed JSON content to analyze
            
        Returns:
            True if the content appears to be a schema definition, False otherwise
        """
        # Must be a list with at least one item
        if not isinstance(content_json, list) or len(content_json) == 0:
            return False
        
        # Check for schema-like structure
        schema_indicators = ['name', 'type', 'mode']
        sample = content_json[0]
        
        # If it's a dictionary with common schema fields, it's likely a schema definition
        if isinstance(sample, dict):
            # Check if most of the schema indicators are present
            indicators_present = sum(1 for key in schema_indicators if key in sample)
            return indicators_present >= 2  # At least name and type should be present
        
        return False
    
    def _validate_schema_definition(self, schema_items) -> tuple:
        """
        Validate that schema items conform to BigQuery expectations.
        
        Args:
            schema_items: List of schema field definitions to validate
            
        Returns:
            A tuple of (is_valid, errors)
        """
        valid = True
        errors = []
        
        for i, item in enumerate(schema_items):
            # Check required fields
            if not all(key in item for key in ['name', 'type']):
                valid = False
                errors.append(f"Item {i}: Missing required fields (name, type)")
            
            # Validate type values against BigQuery allowed types
            if 'type' in item and item['type'] not in self.BQ_ALLOWED_TYPES:
                valid = False
                errors.append(f"Item {i}: Invalid type '{item['type']}'")
                
            # Validate mode values if present
            if 'mode' in item and item['mode'] not in ['NULLABLE', 'REQUIRED', 'REPEATED']:
                valid = False
                errors.append(f"Item {i}: Invalid mode '{item['mode']}'")
        
        return valid, errors
        
    def list_buckets(self) -> List[Dict[str, Any]]:
        """
        List all available buckets in the project.
        
        Returns:
            A list of bucket information dictionaries
        """
        try:
            buckets = list(self.client.list_buckets())
            
            # Format bucket info for API response
            bucket_list = []
            for bucket in buckets:
                bucket_list.append({
                    "name": bucket.name,
                    "location": bucket.location,
                    "created": bucket.time_created.isoformat() if bucket.time_created else None,
                    "storage_class": bucket.storage_class,
                    # Add a flag to indicate if this is the default bucket
                    "is_default": bucket.name == self.bucket_name
                })
                
            return bucket_list
            
        except Exception as e:
            logger.error(f"Error listing buckets: {str(e)}")
            raise
