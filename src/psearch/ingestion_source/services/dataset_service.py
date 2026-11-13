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
from typing import Any, Dict, Optional

from google.api_core.exceptions import NotFound
from google.cloud import bigquery

from ..validation import validate_dataset_id

logger = logging.getLogger(__name__)


class DatasetService:
    """Service for managing BigQuery datasets and related utilities"""

    def __init__(
        self,
        project_id: str,
        client: Optional[bigquery.Client] = None,
    ):
        """
        Initialize the Dataset service.

        Args:
            project_id: The Google Cloud project ID
            client: Optional pre-configured BigQuery client (used for testing/DI)
        """
        self.project_id = project_id
        self.client = (
            client if client is not None else bigquery.Client(project=project_id)
        )

    async def ensure_dataset_exists(
        self, dataset_id: str, location: str = "US"
    ) -> Dict[str, Any]:
        """
        Ensures a BigQuery dataset exists within self.project_id, creating it if necessary.

        Args:
            dataset_id: The ID of the dataset (simple ID or '<self.project_id>.<dataset>')
            location: The geographic location of the dataset

        Returns:
            A dictionary with the result of the operation
        """
        simple_dataset_id = validate_dataset_id(
            dataset_id,
            project_id=self.project_id,
            allow_project_prefix=True,
        )
        dataset_ref = f"{self.project_id}.{simple_dataset_id}"

        try:
            logger.info(f"Checking if dataset {dataset_ref} exists")

            try:
                dataset = self.client.get_dataset(dataset_ref)
                logger.info(f"Dataset {dataset_ref} already exists")
                return {
                    "created": False,
                    "message": f"Dataset {simple_dataset_id} already exists",
                    "dataset_ref": dataset_ref,
                    "location": dataset.location,
                }
            except NotFound:
                logger.info(f"Dataset {dataset_ref} not found, creating it")

                dataset = bigquery.Dataset(dataset_ref)
                dataset.location = location
                dataset.description = "Dataset created automatically by PSearch"

                self.client.create_dataset(dataset)
                logger.info(f"Created dataset {dataset_ref} in {location}")

                return {
                    "created": True,
                    "message": f"Dataset {simple_dataset_id} created successfully",
                    "dataset_ref": dataset_ref,
                    "location": location,
                }

        except Exception as e:
            logger.error(f"Error ensuring dataset {dataset_id} exists: {str(e)}")
            raise
