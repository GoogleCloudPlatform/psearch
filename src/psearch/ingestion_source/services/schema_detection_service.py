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
import csv
import json
import logging
from typing import Dict, List, Any, Optional, Union
import tempfile
import io
import re

import pandas as pd
from fastapi import UploadFile

logger = logging.getLogger(__name__)

class SchemaDetectionService:
    """Service for detecting schema from uploaded CSV and JSON files"""
    
    # Maximum number of rows to sample for schema detection
    MAX_ROWS_TO_SAMPLE = 1000
    
    # Mapping from inferred Python types to BigQuery types
    TYPE_MAPPING = {
        "int": "INTEGER",
        "float": "FLOAT",
        "bool": "BOOLEAN",
        "str": "STRING",
        "datetime64[ns]": "TIMESTAMP",
        "date": "DATE",
        "time": "TIME",
        "dict": "RECORD",
        "list": "RECORD",
        "object": "STRING",  # Default for objects without more specific type
    }
    
    def __init__(self):
        """Initialize the schema detection service"""
        pass
    
    async def detect_schema(self, file: UploadFile, file_type: str) -> Dict[str, Any]:
        """
        Detect schema from an uploaded file.
        
        Args:
            file: The uploaded file
            file_type: The file type (csv or json)
            
        Returns:
            A dictionary containing the detected schema
        """
        # Reset the file pointer to the beginning
        await file.seek(0)
        
        try:
            if file_type == "csv":
                return await self._detect_csv_schema(file)
            elif file_type == "json":
                return await self._detect_json_schema(file)
            else:
                raise ValueError(f"Unsupported file type: {file_type}")
        except Exception as e:
            logger.error(f"Error detecting schema: {str(e)}")
            raise
    
    async def _detect_csv_schema(self, file: UploadFile) -> Dict[str, Any]:
        """
        Detect schema from a CSV file.
        
        Args:
            file: The uploaded CSV file
            
        Returns:
            A dictionary containing the detected schema
        """
        # Read the CSV file into a pandas DataFrame
        # We limit to a sample to avoid loading very large files
        try:
            # First read a small sample to determine the dialect
            sample_data = await file.read(4096)
            await file.seek(0)  # Reset the file pointer
            
            # Use csv.Sniffer to determine the dialect
            dialect = csv.Sniffer().sniff(sample_data.decode('utf-8'))
            has_header = csv.Sniffer().has_header(sample_data.decode('utf-8'))
            
            # Read the CSV into a DataFrame
            content = await file.read()
            csv_file = io.StringIO(content.decode('utf-8'))
            
            df = pd.read_csv(
                csv_file, 
                dialect=dialect,
                header=0 if has_header else None,
                nrows=self.MAX_ROWS_TO_SAMPLE
            )
            
            # If no header, generate column names
            if not has_header:
                df.columns = [f"column_{i}" for i in range(len(df.columns))]
            
            # Clean column names to be BigQuery-friendly
            df.columns = [self._clean_column_name(col) for col in df.columns]
            
            # Infer types for each column
            schema_fields = []
            
            for column in df.columns:
                # Get the pandas type
                pandas_type = str(df[column].dtype)
                
                # Handle special cases
                if pandas_type == 'object':
                    # For object type, check if it's actually a date, time, or boolean
                    # by sampling the first few non-null values
                    sample_values = df[column].dropna().head(10).tolist()
                    inferred_type = self._infer_string_type(sample_values)
                else:
                    inferred_type = self._map_pandas_type_to_bq(pandas_type)
                
                # Create the schema field
                schema_fields.append({
                    "name": column,
                    "type": inferred_type,
                    "mode": "NULLABLE"
                })
            
            return {
                "schema_fields": schema_fields,
                "row_count_estimate": len(df),
                "has_header": has_header,
                "dialect": {
                    "delimiter": dialect.delimiter,
                    "quotechar": dialect.quotechar,
                    "escapechar": dialect.escapechar or "",
                    "doublequote": dialect.doublequote,
                    "skipinitialspace": dialect.skipinitialspace,
                }
            }
            
        except Exception as e:
            logger.error(f"Error detecting CSV schema: {str(e)}")
            raise
    
    async def _detect_json_schema(self, file: UploadFile) -> Dict[str, Any]:
        """
        Detect schema from a JSON file.
        
        Args:
            file: The uploaded JSON file
            
        Returns:
            A dictionary containing the detected schema
        """
        try:
            # Read the content of the file
            content = await file.read()
            
            # Parse JSON content
            json_data = json.loads(content)
            
            # Determine if the root is a single object or an array of objects
            if isinstance(json_data, list):
                # For an array, sample a subset of records
                sample_size = min(len(json_data), self.MAX_ROWS_TO_SAMPLE)
                sample = json_data[:sample_size]
                row_count = len(json_data)
                
                # Detect schema from the first object as a starting point
                if sample:
                    schema_fields = self._detect_json_object_schema(sample[0])
                    
                    # Iterate through the rest to refine/expand the schema
                    field_map = {field["name"]: field for field in schema_fields}
                    
                    for record in sample[1:]:
                        self._update_schema_from_object(field_map, record)
                    
                    # Convert back to list
                    schema_fields = list(field_map.values())
                else:
                    # Empty array
                    schema_fields = []
            else:
                # Single object
                schema_fields = self._detect_json_object_schema(json_data)
                row_count = 1
            
            return {
                "schema_fields": schema_fields,
                "row_count_estimate": row_count,
                "is_array": isinstance(json_data, list)
            }
            
        except Exception as e:
            logger.error(f"Error detecting JSON schema: {str(e)}")
            raise
    
    def _detect_json_object_schema(self, obj: Dict[str, Any]) -> List[Dict[str, str]]:
        """
        Detect schema from a JSON object.
        
        Args:
            obj: A JSON object
            
        Returns:
            A list of schema fields
        """
        schema_fields = []
        
        for key, value in obj.items():
            # Clean the key to make it a valid BigQuery column name
            clean_key = self._clean_column_name(key)
            
            # Determine the type and mode
            field_type, field_mode = self._get_json_field_type_and_mode(value)
            
            # For RECORD types, recurse to get the nested fields
            if field_type == "RECORD" and not isinstance(value, list):
                nested_fields = self._detect_json_object_schema(value)
                schema_fields.append({
                    "name": clean_key,
                    "type": field_type,
                    "mode": field_mode,
                    "fields": nested_fields
                })
            elif field_type == "RECORD" and isinstance(value, list) and value and isinstance(value[0], dict):
                # For arrays of objects, detect schema from the first object
                nested_fields = self._detect_json_object_schema(value[0])
                schema_fields.append({
                    "name": clean_key,
                    "type": field_type,
                    "mode": field_mode,
                    "fields": nested_fields
                })
            else:
                schema_fields.append({
                    "name": clean_key,
                    "type": field_type,
                    "mode": field_mode
                })
        
        return schema_fields
    
    def _update_schema_from_object(self, field_map: Dict[str, Dict[str, Any]], obj: Dict[str, Any]):
        """
        Update an existing schema based on a new object.
        
        Args:
            field_map: A map of field names to their schema
            obj: A JSON object to incorporate into the schema
        """
        for key, value in obj.items():
            clean_key = self._clean_column_name(key)
            
            if clean_key not in field_map:
                # New field
                field_type, field_mode = self._get_json_field_type_and_mode(value)
                
                if field_type == "RECORD" and not isinstance(value, list):
                    nested_fields = self._detect_json_object_schema(value)
                    field_map[clean_key] = {
                        "name": clean_key,
                        "type": field_type,
                        "mode": field_mode,
                        "fields": nested_fields
                    }
                elif field_type == "RECORD" and isinstance(value, list) and value and isinstance(value[0], dict):
                    nested_fields = self._detect_json_object_schema(value[0])
                    field_map[clean_key] = {
                        "name": clean_key,
                        "type": field_type,
                        "mode": field_mode,
                        "fields": nested_fields
                    }
                else:
                    field_map[clean_key] = {
                        "name": clean_key,
                        "type": field_type,
                        "mode": field_mode
                    }
            else:
                # Existing field - check for type conflicts or nested field updates
                existing = field_map[clean_key]
                new_type, new_mode = self._get_json_field_type_and_mode(value)
                
                # Update mode if necessary (NULLABLE -> REPEATED)
                if existing["mode"] == "NULLABLE" and new_mode == "REPEATED":
                    existing["mode"] = "REPEATED"
                
                # Handle type conflicts by defaulting to string
                if existing["type"] != new_type:
                    if existing["type"] != "STRING":
                        existing["type"] = "STRING"
                
                # Update nested fields if both are records
                if existing["type"] == "RECORD" and new_type == "RECORD":
                    if "fields" in existing and not isinstance(value, list):
                        # Create a nested field map
                        nested_field_map = {field["name"]: field for field in existing["fields"]}
                        self._update_schema_from_object(nested_field_map, value)
                        existing["fields"] = list(nested_field_map.values())
    
    def _get_json_field_type_and_mode(self, value: Any) -> tuple:
        """
        Determine the BigQuery type and mode for a JSON value.
        
        Args:
            value: A JSON value
            
        Returns:
            A tuple of (type, mode)
        """
        if value is None:
            return "STRING", "NULLABLE"
        
        if isinstance(value, bool):
            return "BOOLEAN", "NULLABLE"
        
        if isinstance(value, int):
            return "INTEGER", "NULLABLE"
        
        if isinstance(value, float):
            return "FLOAT", "NULLABLE"
        
        if isinstance(value, str):
            # Check if the string might be a timestamp
            if self._looks_like_timestamp(value):
                return "TIMESTAMP", "NULLABLE"
            
            # Check if the string might be a date
            if self._looks_like_date(value):
                return "DATE", "NULLABLE"
            
            # Default to string
            return "STRING", "NULLABLE"
        
        if isinstance(value, dict):
            return "RECORD", "NULLABLE"
        
        if isinstance(value, list):
            if not value:
                # Empty list, default to STRING REPEATED
                return "STRING", "REPEATED"
            
            # For lists, determine the type of the first element
            # and set mode to REPEATED
            sample = value[0]
            
            if isinstance(sample, dict):
                return "RECORD", "REPEATED"
            
            if isinstance(sample, bool):
                return "BOOLEAN", "REPEATED"
            
            if isinstance(sample, int):
                return "INTEGER", "REPEATED"
            
            if isinstance(sample, float):
                return "FLOAT", "REPEATED"
            
            if isinstance(sample, str):
                return "STRING", "REPEATED"
            
            # Default
            return "STRING", "REPEATED"
        
        # Default case
        return "STRING", "NULLABLE"
    
    def _map_pandas_type_to_bq(self, pandas_type: str) -> str:
        """
        Map pandas data type to BigQuery data type.
        
        Args:
            pandas_type: The pandas data type string
            
        Returns:
            The corresponding BigQuery data type
        """
        # Handle common pandas types
        if pandas_type.startswith("int"):
            return "INTEGER"
        
        if pandas_type.startswith("float"):
            return "FLOAT"
        
        if pandas_type.startswith("bool"):
            return "BOOLEAN"
        
        if pandas_type.startswith("datetime"):
            return "TIMESTAMP"
        
        # Default to string for object and other types
        return "STRING"
    
    def _infer_string_type(self, sample_values: List[Any]) -> str:
        """
        Try to infer a more specific type from string values.
        
        Args:
            sample_values: A list of sample values
            
        Returns:
            The inferred BigQuery type
        """
        if not sample_values:
            return "STRING"
        
        # Check if all non-null values match timestamp format
        non_null_values = [v for v in sample_values if v is not None and v != ""]
        
        if all(self._looks_like_timestamp(str(v)) for v in non_null_values):
            return "TIMESTAMP"
        
        # Check if all non-null values match date format
        if all(self._looks_like_date(str(v)) for v in non_null_values):
            return "DATE"
        
        # Check if all values are boolean-like
        bool_values = {"true", "false", "t", "f", "yes", "no", "y", "n", "1", "0"}
        if all(str(v).lower() in bool_values for v in non_null_values):
            return "BOOLEAN"
        
        # Check if all values are integers
        try:
            if all(float(v).is_integer() for v in non_null_values):
                return "INTEGER"
        except (ValueError, TypeError):
            pass
        
        # Check if all values are floats
        try:
            if all(isinstance(float(v), float) for v in non_null_values):
                return "FLOAT"
        except (ValueError, TypeError):
            pass
        
        # Default to string
        return "STRING"
    
    def _looks_like_timestamp(self, value: str) -> bool:
        """
        Check if a string value looks like a timestamp.
        
        Args:
            value: A string value
            
        Returns:
            True if the value looks like a timestamp, False otherwise
        """
        # Common timestamp patterns
        timestamp_patterns = [
            r'^\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2}',  # ISO format or similar
            r'^\d{4}/\d{2}/\d{2} \d{2}:\d{2}:\d{2}',     # YYYY/MM/DD HH:MM:SS
            r'^\d{2}/\d{2}/\d{4} \d{2}:\d{2}:\d{2}',     # MM/DD/YYYY HH:MM:SS
        ]
        
        return any(re.match(pattern, value) for pattern in timestamp_patterns)
    
    def _looks_like_date(self, value: str) -> bool:
        """
        Check if a string value looks like a date.
        
        Args:
            value: A string value
            
        Returns:
            True if the value looks like a date, False otherwise
        """
        # Common date patterns
        date_patterns = [
            r'^\d{4}-\d{2}-\d{2}$',  # YYYY-MM-DD
            r'^\d{4}/\d{2}/\d{2}$',  # YYYY/MM/DD
            r'^\d{2}/\d{2}/\d{4}$',  # MM/DD/YYYY
            r'^\d{2}-\d{2}-\d{4}$',  # MM-DD-YYYY
        ]
        
        return any(re.match(pattern, value) for pattern in date_patterns)
    
    def _clean_column_name(self, name: str) -> str:
        """
        Clean a column name to be BigQuery-friendly.
        
        Args:
            name: The original column name
            
        Returns:
            A BigQuery-friendly column name
        """
        # Replace any character that's not alphanumeric or underscore with underscore
        cleaned = re.sub(r'[^\w]', '_', name)
        
        # Ensure the name starts with a letter or underscore
        if cleaned and not (cleaned[0].isalpha() or cleaned[0] == '_'):
            cleaned = f"col_{cleaned}"
        
        # Handle empty names
        if not cleaned:
            cleaned = "unnamed_column"
        
        return cleaned
