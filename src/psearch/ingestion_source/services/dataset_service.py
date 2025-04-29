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
from typing import Dict, Any, Optional
from google.cloud import bigquery
from google.api_core.exceptions import NotFound

logger = logging.getLogger(__name__)

class DatasetService:
    """Service for managing BigQuery datasets and related utilities"""
    
    def __init__(self, project_id: str):
        """
        Initialize the Dataset service.
        
        Args:
            project_id: The Google Cloud project ID
        """
        self.project_id = project_id
        self.client = bigquery.Client(project=project_id)
    
    async def ensure_dataset_exists(self, dataset_id: str, location: str = "US") -> Dict[str, Any]:
        """
        Ensures a BigQuery dataset exists, creating it if necessary.
        
        Args:
            dataset_id: The ID of the dataset (can be simple ID or fully qualified 'project.dataset')
            location: The geographic location of the dataset
            
        Returns:
            A dictionary with the result of the operation
        """
        try:
            # Handle dataset_id that might already include project
            if "." in dataset_id:
                # Extract just the dataset part if project.dataset format is provided
                parts = dataset_id.split(".")
                if len(parts) == 2:
                    project, simple_dataset_id = parts
                    # If project part doesn't match our project_id, log a warning
                    if project != self.project_id:
                        logger.warning(f"Dataset {dataset_id} references different project than {self.project_id}")
                    # Use the simple dataset ID for dataset_ref
                    dataset_ref = f"{self.project_id}.{simple_dataset_id}"
                else:
                    # Invalid format, use as-is for meaningful error
                    dataset_ref = dataset_id
            else:
                # Simple dataset ID, prepend project
                dataset_ref = f"{self.project_id}.{dataset_id}"
            
            logger.info(f"Checking if dataset {dataset_ref} exists")
            
            # Try to get the dataset
            try:
                dataset = self.client.get_dataset(dataset_ref)
                logger.info(f"Dataset {dataset_ref} already exists")
                return {
                    "created": False,
                    "message": f"Dataset {dataset_id} already exists",
                    "dataset_ref": dataset_ref,
                    "location": dataset.location
                }
            except NotFound:
                # Dataset doesn't exist, create it
                logger.info(f"Dataset {dataset_ref} not found, creating it")
                
                dataset = bigquery.Dataset(dataset_ref)
                dataset.location = location
                dataset.description = f"Dataset created automatically by PSearch"
                
                # Create the dataset
                created_dataset = self.client.create_dataset(dataset)
                logger.info(f"Created dataset {dataset_ref} in {location}")
                
                return {
                    "created": True,
                    "message": f"Dataset {dataset_id} created successfully",
                    "dataset_ref": dataset_ref,
                    "location": location
                }
                
        except Exception as e:
            logger.error(f"Error ensuring dataset {dataset_id} exists: {str(e)}")
            raise
