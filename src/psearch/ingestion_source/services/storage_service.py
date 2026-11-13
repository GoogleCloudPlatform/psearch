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

import json
import logging
import os
import tempfile
from typing import Any, Dict, List, Optional

from fastapi import UploadFile
from google.cloud import storage

from ..validation import (
    UploadTooLargeError,
    get_max_upload_size_bytes,
    validate_file_id,
    validate_upload_filename,
)

logger = logging.getLogger(__name__)


class StorageService:
    """Service for handling file uploads to Google Cloud Storage"""

    # BigQuery allowed data types for schema validation
    BQ_ALLOWED_TYPES = [
        "STRING",
        "INTEGER",
        "FLOAT",
        "BOOLEAN",
        "TIMESTAMP",
        "DATE",
        "TIME",
        "DATETIME",
        "RECORD",
        "NUMERIC",
        "BYTES",
    ]

    STREAM_CHUNK_BYTES = 1024 * 1024  # 1 MiB

    def __init__(
        self,
        project_id: str,
        client: Optional[storage.Client] = None,
        max_upload_size_bytes: Optional[int] = None,
    ):
        """
        Initialize the storage service.

        Args:
            project_id: The Google Cloud project ID
            client: Optional pre-configured GCS client (used for testing/DI)
            max_upload_size_bytes: Optional maximum upload size cap in bytes
        """
        self.project_id = project_id
        self.max_upload_size_bytes = (
            get_max_upload_size_bytes()
            if max_upload_size_bytes is None
            else max_upload_size_bytes
        )
        if self.max_upload_size_bytes <= 0:
            raise ValueError("max_upload_size_bytes must be a positive integer")

        self.client = (
            client if client is not None else storage.Client(project=project_id)
        )

        # Default bucket name follows the specified pattern
        self.bucket_name = f"{project_id}_psearch_raw"

        # File type-specific folders
        self.csv_folder = "csv/"
        self.json_folder = "json/"

        # Ensure the bucket exists
        self._ensure_bucket_exists()

    def _ensure_bucket_exists(self) -> None:
        """Ensure the storage bucket exists, creating it with UBLA if absent."""
        existing_bucket = self.client.lookup_bucket(self.bucket_name)
        if existing_bucket is not None:
            self.bucket = existing_bucket
            logger.info(f"Using existing bucket: {self.bucket_name}")
            return

        logger.info(f"Bucket {self.bucket_name} does not exist, creating...")
        try:
            bucket = self.client.bucket(self.bucket_name)
            bucket.iam_configuration.uniform_bucket_level_access_enabled = True
            self.bucket = self.client.create_bucket(
                bucket,
                location="us-central1",
            )
            logger.info(f"Created bucket: {self.bucket_name}")
        except Exception as create_error:
            logger.error(f"Failed to create bucket: {str(create_error)}")
            raise

    async def _read_bounded_bytes(self, file: UploadFile) -> bytes:
        """Read stream in bounded chunks, raising UploadTooLargeError mid-stream if exceeded."""
        await file.seek(0)
        chunk_size = min(self.STREAM_CHUNK_BYTES, self.max_upload_size_bytes + 1)
        chunks: List[bytes] = []
        total_bytes = 0
        while chunk := await file.read(chunk_size):
            total_bytes += len(chunk)
            if total_bytes > self.max_upload_size_bytes:
                raise UploadTooLargeError(
                    f"Uploaded file exceeds maximum allowed size of {self.max_upload_size_bytes} bytes"
                )
            chunks.append(chunk)
        return b"".join(chunks)

    async def upload_file(self, file: UploadFile, file_id: str) -> str:
        """
        Upload a file to Google Cloud Storage.

        Args:
            file: The file to upload (from FastAPI)
            file_id: The canonical UUIDv4 identifier for this file

        Returns:
            The GCS URI for the uploaded file
        """
        validated_file_id = validate_file_id(file_id)
        file_extension = validate_upload_filename(file.filename)

        folder_prefix = (
            self.csv_folder if file_extension == "csv" else self.json_folder
        )
        object_name = f"{folder_prefix}{validated_file_id}.{file_extension}"

        blob = self.bucket.blob(object_name)
        content_type_map = {
            "csv": "text/csv",
            "json": "application/json",
        }
        blob.content_type = content_type_map.get(
            file_extension, "application/octet-stream"
        )

        temp_file_path: Optional[str] = None
        try:
            temp_fd, temp_file_path = tempfile.mkstemp(suffix=f".{file_extension}")
            os.close(temp_fd)
            logger.info(f"Created temporary file for processing: {temp_file_path}")

            if file_extension == "json":
                content_bytes = await self._read_bounded_bytes(file)
                logger.info(
                    f"Read {len(content_bytes)} bytes from uploaded JSON file {file.filename}"
                )

                ndjson_content_written = False
                try:
                    content_str = content_bytes.decode("utf-8")
                    data: Any = None
                    try:
                        data = json.loads(content_str)

                        if self._is_schema_definition(data):
                            logger.info(
                                f"Detected schema definition file: {file.filename}"
                            )
                            is_valid, errors = self._validate_schema_definition(data)
                            if not is_valid:
                                for error in errors:
                                    logger.warning(
                                        f"Schema validation warning: {error}"
                                    )

                            with open(
                                temp_file_path, "w", encoding="utf-8"
                            ) as temp_file:
                                for item in data:
                                    json.dump(item, temp_file, ensure_ascii=False)
                                    temp_file.write("\n")
                            logger.info(
                                f"Converted schema definition to JSONL format with {len(data)} fields"
                            )
                            ndjson_content_written = True
                            data = None
                    except json.JSONDecodeError as e:
                        logger.warning(
                            f"Initial JSON parse failed: {e}. Attempting repair..."
                        )
                        if (
                            "No object found when new array is started" in str(e)
                            or "BeginArray returned false" in str(e)
                        ):
                            content_str = content_str.strip()
                            if not content_str.startswith("["):
                                content_str = "[" + content_str
                                logger.info("Added missing opening bracket '[' to JSON")
                            if not content_str.endswith("]"):
                                content_str = content_str + "]"
                                logger.info("Added missing closing bracket ']' to JSON")

                            try:
                                data = json.loads(content_str)
                                logger.info("JSON repair successful!")
                            except json.JSONDecodeError as e2:
                                logger.warning(
                                    f"First repair attempt failed: {e2}. Trying alternate method..."
                                )
                                lines = content_str.replace("[", "", 1)
                                if lines.endswith("]"):
                                    lines = lines[:-1]
                                lines = lines.split("\n")

                                data = []
                                for line in lines:
                                    line = line.strip()
                                    if (
                                        not line
                                        or line == ","
                                        or line == "]"
                                        or line == "["
                                    ):
                                        continue
                                    if line.endswith(","):
                                        line = line[:-1]
                                    try:
                                        item = json.loads(line)
                                        data.append(item)
                                    except json.JSONDecodeError:
                                        logger.warning(
                                            f"Skipping invalid JSON line: {line[:50]}..."
                                        )

                                if not data:
                                    raise e

                                logger.info(
                                    f"Extracted {len(data)} valid JSON objects using line-by-line parsing"
                                )
                        else:
                            raise

                    logger.info(f"Successfully parsed JSON content for {file.filename}")

                    if not ndjson_content_written:
                        with open(temp_file_path, "w", encoding="utf-8") as temp_file:
                            if isinstance(data, list):
                                if not data:
                                    logger.warning(
                                        f"JSON file {file.filename} contains an empty array."
                                    )
                                else:
                                    for item in data:
                                        json.dump(item, temp_file, ensure_ascii=False)
                                        temp_file.write("\n")
                                    logger.info(
                                        f"Converted JSON array ({len(data)} items) to NDJSON for file {file.filename} in {temp_file_path}"
                                    )
                            elif isinstance(data, dict):
                                json.dump(data, temp_file, ensure_ascii=False)
                                temp_file.write("\n")
                                logger.warning(
                                    f"Uploaded JSON file {file.filename} was a single object, wrote as one line NDJSON in {temp_file_path}"
                                )
                            else:
                                logger.error(
                                    f"Parsed JSON content from {file.filename} is neither a list nor an object."
                                )
                                temp_file.write(content_str)
                                logger.warning(
                                    f"Wrote original content to {temp_file_path} due to unexpected JSON structure."
                                )

                except UploadTooLargeError:
                    raise
                except json.JSONDecodeError as json_err:
                    logger.error(
                        f"Failed to parse uploaded JSON file {file.filename}: {json_err}. Writing original content."
                    )
                    with open(temp_file_path, "wb") as temp_file:
                        temp_file.write(content_bytes)
                except Exception as proc_err:
                    logger.error(
                        f"Error processing JSON file {file.filename} for NDJSON conversion: {proc_err}. Writing original content."
                    )
                    with open(temp_file_path, "wb") as temp_file:
                        temp_file.write(content_bytes)

            else:
                chunk_size = min(
                    self.STREAM_CHUNK_BYTES, self.max_upload_size_bytes + 1
                )
                total_bytes = 0
                with open(temp_file_path, "wb") as temp_file:
                    await file.seek(0)
                    while chunk := await file.read(chunk_size):
                        total_bytes += len(chunk)
                        if total_bytes > self.max_upload_size_bytes:
                            raise UploadTooLargeError(
                                f"Uploaded file exceeds maximum allowed size of {self.max_upload_size_bytes} bytes"
                            )
                        temp_file.write(chunk)
                logger.info(
                    f"Wrote non-JSON file {file.filename} ({total_bytes} bytes) directly to {temp_file_path}"
                )

            logger.info(
                f"Uploading processed file from {temp_file_path} to GCS object {object_name}"
            )
            blob.upload_from_filename(temp_file_path)
            logger.info(f"Successfully uploaded to {object_name}")

            gcs_uri = f"gs://{self.bucket_name}/{object_name}"
            logger.info(f"Uploaded file {file.filename} to {gcs_uri}")
            return gcs_uri
        except Exception as e:
            logger.error(f"Error uploading file: {str(e)}")
            raise
        finally:
            if temp_file_path and os.path.exists(temp_file_path):
                try:
                    os.unlink(temp_file_path)
                except OSError as unlink_err:
                    logger.warning(
                        f"Failed to remove temporary file {temp_file_path}: {unlink_err}"
                    )

    def get_file_uri(self, file_id: str, file_type: str) -> Optional[str]:
        """
        Get the GCS URI for a previously uploaded file using its validated UUIDv4 ID and type.

        Args:
            file_id: The canonical UUIDv4 identifier of the file.
            file_type: The file extension ('csv' or 'json').

        Returns:
            The GCS URI or None if not found.
        """
        validated_file_id = validate_file_id(file_id)
        logger.info(
            f"Looking for file ID: {validated_file_id} with type: {file_type}"
        )

        if not isinstance(file_type, str):
            logger.error(f"Unsupported file_type provided: {file_type}")
            return None

        file_type_lower = file_type.strip().lower()
        if file_type_lower == "csv":
            folder_prefix = self.csv_folder
        elif file_type_lower == "json":
            folder_prefix = self.json_folder
        else:
            logger.error(f"Unsupported file_type provided: {file_type}")
            return None

        object_name = f"{folder_prefix}{validated_file_id}.{file_type_lower}"
        gcs_uri = f"gs://{self.bucket_name}/{object_name}"
        logger.info(f"Constructed expected GCS URI: {gcs_uri}")

        try:
            blob = self.bucket.blob(object_name)
            if blob.exists():
                logger.info(f"Confirmed file exists at {gcs_uri}")
                return gcs_uri
            logger.warning(f"File not found at the expected path: {gcs_uri}")
            return None
        except Exception as e:
            logger.error(f"Error finding file {validated_file_id} in GCS: {str(e)}")
            return None

    def _is_schema_definition(self, content_json: Any) -> bool:
        """
        Detect if JSON content is likely a schema definition.
        """
        if not isinstance(content_json, list) or len(content_json) == 0:
            return False

        schema_indicators = ["name", "type", "mode"]
        sample = content_json[0]

        if isinstance(sample, dict):
            indicators_present = sum(
                1 for key in schema_indicators if key in sample
            )
            return indicators_present >= 2

        return False

    def _validate_schema_definition(self, schema_items: List[Dict[str, Any]]) -> tuple:
        """
        Validate that schema items conform to BigQuery expectations.
        """
        valid = True
        errors = []

        for i, item in enumerate(schema_items):
            if not all(key in item for key in ["name", "type"]):
                valid = False
                errors.append(f"Item {i}: Missing required fields (name, type)")

            if "type" in item and item["type"] not in self.BQ_ALLOWED_TYPES:
                valid = False
                errors.append(f"Item {i}: Invalid type '{item['type']}'")

            if "mode" in item and item["mode"] not in [
                "NULLABLE",
                "REQUIRED",
                "REPEATED",
            ]:
                valid = False
                errors.append(f"Item {i}: Invalid mode '{item['mode']}'")

        return valid, errors

    def list_buckets(self) -> List[Dict[str, Any]]:
        """
        Return metadata for the scoped ingestion bucket only (never enumerating project buckets).

        Returns:
            A single-element list for `<project_id>_psearch_raw`, or `[]` if absent.
        """
        try:
            bucket = self.client.lookup_bucket(self.bucket_name)
            if bucket is None:
                return []

            return [
                {
                    "name": bucket.name,
                    "location": bucket.location,
                    "created": (
                        bucket.time_created.isoformat()
                        if bucket.time_created
                        else None
                    ),
                    "storage_class": bucket.storage_class,
                    "is_default": True,
                }
            ]
        except Exception as e:
            logger.error(f"Error looking up bucket {self.bucket_name}: {str(e)}")
            raise
