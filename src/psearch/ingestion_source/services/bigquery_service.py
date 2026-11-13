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

from datetime import datetime
import logging
from typing import Any, Dict, List, Optional

from google.api_core.exceptions import NotFound
from google.cloud import bigquery

from ..validation import (
    validate_dataset_id,
    validate_table_id,
    validate_write_disposition,
)

logger = logging.getLogger(__name__)


class BigQueryService:
    """Service for interacting with BigQuery"""

    WRITE_DISPOSITION_MAP = {
        "WRITE_EMPTY": bigquery.WriteDisposition.WRITE_EMPTY,
        "WRITE_APPEND": bigquery.WriteDisposition.WRITE_APPEND,
        "WRITE_TRUNCATE": bigquery.WriteDisposition.WRITE_TRUNCATE,
    }

    def __init__(
        self,
        project_id: str,
        client: Optional[bigquery.Client] = None,
    ):
        """
        Initialize the BigQuery service.

        Args:
            project_id: The Google Cloud project ID
            client: Optional pre-configured BigQuery client (used for testing/DI)
        """
        self.project_id = project_id
        self.client = (
            client if client is not None else bigquery.Client(project=project_id)
        )

    async def create_dataset(
        self,
        dataset_id: str,
        location: str = "US",
        description: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Create a BigQuery dataset.
        """
        validated_dataset_id = validate_dataset_id(dataset_id)
        try:
            dataset_ref = f"{self.project_id}.{validated_dataset_id}"

            try:
                self.client.get_dataset(dataset_ref)
                logger.info(f"Dataset {dataset_ref} already exists")
                return {
                    "created": False,
                    "message": f"Dataset {validated_dataset_id} already exists",
                    "dataset_ref": dataset_ref,
                }
            except NotFound:
                dataset = bigquery.Dataset(dataset_ref)
                dataset.location = location

                if description:
                    dataset.description = description

                self.client.create_dataset(dataset)
                logger.info(f"Created dataset {dataset_ref} in {location}")

                return {
                    "created": True,
                    "message": f"Dataset {validated_dataset_id} created successfully",
                    "dataset_ref": dataset_ref,
                    "location": location,
                }

        except Exception as e:
            logger.error(
                f"Error creating dataset {validated_dataset_id}: {str(e)}"
            )
            raise

    async def create_table(
        self,
        dataset_id: str,
        table_id: str,
        schema: List[Dict[str, Any]],
        description: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Create a BigQuery table with the specified schema.
        """
        validated_dataset_id = validate_dataset_id(dataset_id)
        validated_table_id = validate_table_id(table_id)
        try:
            table_ref = (
                f"{self.project_id}.{validated_dataset_id}.{validated_table_id}"
            )

            try:
                self.client.get_table(table_ref)
                logger.info(f"Table {table_ref} already exists")
                return {
                    "created": False,
                    "message": f"Table {validated_table_id} already exists",
                    "table_ref": table_ref,
                }
            except NotFound:
                table = bigquery.Table(table_ref)
                table.schema = self._create_schema_fields(schema)

                if description:
                    table.description = description

                self.client.create_table(table)
                logger.info(f"Created table {table_ref}")

                return {
                    "created": True,
                    "message": f"Table {validated_table_id} created successfully",
                    "table_ref": table_ref,
                }

        except Exception as e:
            logger.error(
                f"Error creating table {validated_dataset_id}.{validated_table_id}: {str(e)}"
            )
            raise

    async def load_table_from_uri(
        self,
        job_id: str,
        jobs_dict: Dict[str, Dict[str, Any]],
        dataset_id: str,
        table_id: str,
        uri: str,
        source_format: str = "CSV",
        write_disposition: str = "WRITE_EMPTY",
        skip_leading_rows: Optional[int] = None,
        allow_jagged_rows: Optional[bool] = None,
        allow_quoted_newlines: Optional[bool] = None,
        field_delimiter: Optional[str] = None,
        quote_character: Optional[str] = None,
        autodetect: bool = True,
        max_bad_records: int = 0,
    ):
        """
        Load data into a BigQuery table from a Cloud Storage URI.
        Validates identifiers and write_disposition before starting a load job.
        """
        validated_dataset_id = validate_dataset_id(dataset_id)
        validated_table_id = validate_table_id(table_id)
        validated_disposition = validate_write_disposition(write_disposition)
        bq_write_disposition = self.WRITE_DISPOSITION_MAP[validated_disposition]

        normalized_format = (
            source_format.upper() if isinstance(source_format, str) else ""
        )
        if normalized_format not in {"CSV", "JSON"}:
            raise ValueError(
                f"Unsupported source_format '{source_format}'; must be CSV or JSON"
            )

        try:
            if normalized_format == "CSV":
                job_config = bigquery.LoadJobConfig(
                    source_format=bigquery.SourceFormat.CSV,
                    write_disposition=bq_write_disposition,
                    allow_quoted_newlines=allow_quoted_newlines,
                    autodetect=autodetect,
                    max_bad_records=max_bad_records,
                )

                if skip_leading_rows is not None:
                    job_config.skip_leading_rows = skip_leading_rows

                if allow_jagged_rows is not None:
                    job_config.allow_jagged_rows = allow_jagged_rows

                if field_delimiter is not None:
                    job_config.field_delimiter = field_delimiter

                if quote_character is not None:
                    job_config.quote_character = quote_character

            else:
                job_config = bigquery.LoadJobConfig(
                    source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
                    write_disposition=bq_write_disposition,
                    autodetect=autodetect,
                    max_bad_records=max_bad_records,
                )
                logger.info(
                    f"Configuring JSON load job with: autodetect={autodetect}, max_bad_records={max_bad_records}"
                )

            table_ref = (
                f"{self.project_id}.{validated_dataset_id}.{validated_table_id}"
            )

            load_job = self.client.load_table_from_uri(
                uri,
                table_ref,
                job_config=job_config,
            )

            jobs_dict[job_id].update(
                {
                    "status": "RUNNING",
                    "message": f"Loading data from {uri} to {table_ref}",
                    "metadata": {
                        **jobs_dict[job_id].get("metadata", {}),
                        "bq_job_id": load_job.job_id,
                    },
                }
            )

            load_job.result()

            if load_job.errors:
                error_message = load_job.errors[0].get("message", "Unknown error")
                logger.error(f"Load job failed: {error_message}")

                error_details = ""
                if "Failed to parse JSON" in error_message:
                    error_details = (
                        "\n\nThis appears to be a JSON formatting error. BigQuery expects newline-delimited JSON (JSONL) format. "
                        "Each line should be a complete, valid JSON object. "
                        "You can try increasing max_bad_records to skip problematic records."
                    )

                jobs_dict[job_id].update(
                    {
                        "status": "FAILED",
                        "message": f"Load job failed: {error_message}{error_details}",
                        "completed_at": datetime.now().isoformat(),
                        "metadata": {
                            **jobs_dict[job_id].get("metadata", {}),
                            "error_details": load_job.errors,
                            "bad_records_allowed": max_bad_records,
                        },
                    }
                )
            else:
                destination_table = self.client.get_table(table_ref)

                metadata = jobs_dict[job_id].get("metadata", {})
                metadata["row_count"] = destination_table.num_rows

                try:
                    if hasattr(load_job, "total_bytes_processed"):
                        metadata["bytes_processed"] = load_job.total_bytes_processed
                    elif hasattr(load_job, "output_bytes"):
                        metadata["bytes_processed"] = load_job.output_bytes
                    elif hasattr(load_job, "bytes_processed"):
                        metadata["bytes_processed"] = load_job.bytes_processed
                    else:
                        logger.warning(
                            "Could not find bytes_processed attribute on LoadJob"
                        )
                except Exception as stats_err:
                    logger.warning(f"Error accessing job statistics: {stats_err}")

                jobs_dict[job_id].update(
                    {
                        "status": "COMPLETED",
                        "message": f"Loaded {destination_table.num_rows} rows into {table_ref}",
                        "completed_at": datetime.now().isoformat(),
                        "metadata": metadata,
                    }
                )

                logger.info(
                    f"Load job completed: {destination_table.num_rows} rows loaded into {table_ref}"
                )

        except Exception as e:
            logger.error(f"Error loading data: {str(e)}")

            jobs_dict[job_id].update(
                {
                    "status": "FAILED",
                    "message": f"Error loading data: {str(e)}",
                    "completed_at": datetime.now().isoformat(),
                }
            )

    def _create_schema_fields(
        self, schema_fields: List[Dict[str, Any]]
    ) -> List[bigquery.SchemaField]:
        """Convert schema field dictionaries to BigQuery SchemaField objects."""
        if not schema_fields:
            return []

        result = []

        for field in schema_fields:
            if field is None:
                continue

            name = field["name"]
            field_type = field["type"]
            mode = field.get("mode", "NULLABLE")
            description = field.get("description", None)

            fields = None
            if field_type == "RECORD" and "fields" in field:
                nested_fields = field.get("fields")
                if nested_fields:
                    fields = self._create_schema_fields(nested_fields)

            schema_field = bigquery.SchemaField(
                name=name,
                field_type=field_type,
                mode=mode,
                description=description,
                fields=fields,
            )

            result.append(schema_field)

        return result
