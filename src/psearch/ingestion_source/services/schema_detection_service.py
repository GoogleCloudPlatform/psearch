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

import csv
import io
import json
import logging
import re
from typing import Any, Dict, List, Optional

from fastapi import UploadFile
import pandas as pd

from ..validation import UploadTooLargeError, get_max_upload_size_bytes

logger = logging.getLogger(__name__)


class SchemaDetectionService:
    """Service for detecting schema from uploaded CSV and JSON files"""

    # Maximum number of rows to sample for schema detection
    MAX_ROWS_TO_SAMPLE = 1000

    # Maximum bytes read from a CSV stream when sampling schema (5 MiB)
    MAX_SCHEMA_SAMPLE_BYTES = 5 * 1024 * 1024

    STREAM_CHUNK_BYTES = 1024 * 1024  # 1 MiB

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

    def __init__(self, max_upload_size_bytes: Optional[int] = None):
        """Initialize the schema detection service."""
        self.max_upload_size_bytes = (
            get_max_upload_size_bytes()
            if max_upload_size_bytes is None
            else max_upload_size_bytes
        )
        if self.max_upload_size_bytes <= 0:
            raise ValueError("max_upload_size_bytes must be a positive integer")

    async def detect_schema(self, file: UploadFile, file_type: str) -> Dict[str, Any]:
        """
        Detect schema from an uploaded file.

        Args:
            file: The uploaded file
            file_type: The file type (csv or json)

        Returns:
            A dictionary containing the detected schema
        """
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
        Detect schema from a bounded prefix sample of a CSV file.

        Reads at most MAX_SCHEMA_SAMPLE_BYTES (+ 1 sentinel byte to detect truncation)
        and trims any partial trailing line when truncated so pandas never parses a torn row.
        """
        try:
            target_bytes = self.MAX_SCHEMA_SAMPLE_BYTES + 1
            chunks: List[bytes] = []
            bytes_collected = 0
            while bytes_collected < target_bytes:
                to_read = min(self.STREAM_CHUNK_BYTES, target_bytes - bytes_collected)
                chunk = await file.read(to_read)
                if not chunk:
                    break
                chunks.append(chunk)
                bytes_collected += len(chunk)

            raw_sample = b"".join(chunks)
            truncated = len(raw_sample) > self.MAX_SCHEMA_SAMPLE_BYTES
            if truncated:
                raw_sample = raw_sample[: self.MAX_SCHEMA_SAMPLE_BYTES]
                last_newline = raw_sample.rfind(b"\n")
                if last_newline != -1:
                    raw_sample = raw_sample[: last_newline + 1]
                    sample_text = raw_sample.decode("utf-8")
                else:
                    sample_text = raw_sample.decode("utf-8", errors="ignore")
            else:
                sample_text = raw_sample.decode("utf-8")

            sniff_sample = sample_text[:4096]
            try:
                dialect = csv.Sniffer().sniff(sniff_sample)
            except csv.Error:
                dialect = csv.excel()

            try:
                has_header = csv.Sniffer().has_header(sniff_sample)
            except csv.Error:
                has_header = True

            csv_file = io.StringIO(sample_text)
            df = pd.read_csv(
                csv_file,
                dialect=dialect,
                header=0 if has_header else None,
                nrows=self.MAX_ROWS_TO_SAMPLE,
            )

            if not has_header:
                df.columns = [f"column_{i}" for i in range(len(df.columns))]

            df.columns = [self._clean_column_name(str(col)) for col in df.columns]

            schema_fields = []
            for column in df.columns:
                pandas_type = str(df[column].dtype)
                if pandas_type == "object":
                    sample_values = df[column].dropna().head(10).tolist()
                    inferred_type = self._infer_string_type(sample_values)
                else:
                    inferred_type = self._map_pandas_type_to_bq(pandas_type)

                schema_fields.append(
                    {
                        "name": column,
                        "type": inferred_type,
                        "mode": "NULLABLE",
                    }
                )

            return {
                "schema_fields": schema_fields,
                "row_count_estimate": len(df),
                "has_header": has_header,
                "dialect": {
                    "delimiter": dialect.delimiter,
                    "quotechar": dialect.quotechar or '"',
                    "escapechar": dialect.escapechar or "",
                    "doublequote": bool(dialect.doublequote),
                    "skipinitialspace": bool(dialect.skipinitialspace),
                },
            }

        except Exception as e:
            logger.error(f"Error detecting CSV schema: {str(e)}")
            raise

    async def _detect_json_schema(self, file: UploadFile) -> Dict[str, Any]:
        """
        Detect schema from a JSON file while enforcing max_upload_size_bytes.
        """
        try:
            chunk_size = min(
                self.STREAM_CHUNK_BYTES, self.max_upload_size_bytes + 1
            )
            chunks: List[bytes] = []
            total_bytes = 0
            while chunk := await file.read(chunk_size):
                total_bytes += len(chunk)
                if total_bytes > self.max_upload_size_bytes:
                    raise UploadTooLargeError(
                        f"Uploaded file exceeds maximum allowed size of {self.max_upload_size_bytes} bytes"
                    )
                chunks.append(chunk)

            content = b"".join(chunks)
            json_data = json.loads(content)

            if isinstance(json_data, list):
                sample_size = min(len(json_data), self.MAX_ROWS_TO_SAMPLE)
                sample = json_data[:sample_size]
                row_count = len(json_data)

                if sample:
                    schema_fields = self._detect_json_object_schema(sample[0])
                    field_map = {field["name"]: field for field in schema_fields}

                    for record in sample[1:]:
                        self._update_schema_from_object(field_map, record)

                    schema_fields = list(field_map.values())
                else:
                    schema_fields = []
            else:
                schema_fields = self._detect_json_object_schema(json_data)
                row_count = 1

            return {
                "schema_fields": schema_fields,
                "row_count_estimate": row_count,
                "is_array": isinstance(json_data, list),
            }

        except Exception as e:
            logger.error(f"Error detecting JSON schema: {str(e)}")
            raise

    def _detect_json_object_schema(
        self, obj: Dict[str, Any]
    ) -> List[Dict[str, Any]]:
        """Detect schema from a JSON object."""
        schema_fields = []

        for key, value in obj.items():
            clean_key = self._clean_column_name(key)
            field_type, field_mode = self._get_json_field_type_and_mode(value)

            if field_type == "RECORD" and not isinstance(value, list):
                nested_fields = self._detect_json_object_schema(value)
                schema_fields.append(
                    {
                        "name": clean_key,
                        "type": field_type,
                        "mode": field_mode,
                        "fields": nested_fields,
                    }
                )
            elif (
                field_type == "RECORD"
                and isinstance(value, list)
                and value
                and isinstance(value[0], dict)
            ):
                nested_fields = self._detect_json_object_schema(value[0])
                schema_fields.append(
                    {
                        "name": clean_key,
                        "type": field_type,
                        "mode": field_mode,
                        "fields": nested_fields,
                    }
                )
            else:
                schema_fields.append(
                    {
                        "name": clean_key,
                        "type": field_type,
                        "mode": field_mode,
                    }
                )

        return schema_fields

    def _update_schema_from_object(
        self, field_map: Dict[str, Dict[str, Any]], obj: Dict[str, Any]
    ):
        """Update an existing schema based on a new object."""
        for key, value in obj.items():
            clean_key = self._clean_column_name(key)

            if clean_key not in field_map:
                field_type, field_mode = self._get_json_field_type_and_mode(value)

                if field_type == "RECORD" and not isinstance(value, list):
                    nested_fields = self._detect_json_object_schema(value)
                    field_map[clean_key] = {
                        "name": clean_key,
                        "type": field_type,
                        "mode": field_mode,
                        "fields": nested_fields,
                    }
                elif (
                    field_type == "RECORD"
                    and isinstance(value, list)
                    and value
                    and isinstance(value[0], dict)
                ):
                    nested_fields = self._detect_json_object_schema(value[0])
                    field_map[clean_key] = {
                        "name": clean_key,
                        "type": field_type,
                        "mode": field_mode,
                        "fields": nested_fields,
                    }
                else:
                    field_map[clean_key] = {
                        "name": clean_key,
                        "type": field_type,
                        "mode": field_mode,
                    }
            else:
                existing = field_map[clean_key]
                new_type, new_mode = self._get_json_field_type_and_mode(value)

                if existing["mode"] == "NULLABLE" and new_mode == "REPEATED":
                    existing["mode"] = "REPEATED"

                if existing["type"] != new_type:
                    if existing["type"] != "STRING":
                        existing["type"] = "STRING"

                if existing["type"] == "RECORD" and new_type == "RECORD":
                    if "fields" in existing and not isinstance(value, list):
                        nested_field_map = {
                            field["name"]: field for field in existing["fields"]
                        }
                        self._update_schema_from_object(nested_field_map, value)
                        existing["fields"] = list(nested_field_map.values())

    def _get_json_field_type_and_mode(self, value: Any) -> tuple:
        """Determine the BigQuery type and mode for a JSON value."""
        if value is None:
            return "STRING", "NULLABLE"

        if isinstance(value, bool):
            return "BOOLEAN", "NULLABLE"

        if isinstance(value, int):
            return "INTEGER", "NULLABLE"

        if isinstance(value, float):
            return "FLOAT", "NULLABLE"

        if isinstance(value, str):
            if self._looks_like_timestamp(value):
                return "TIMESTAMP", "NULLABLE"

            if self._looks_like_date(value):
                return "DATE", "NULLABLE"

            return "STRING", "NULLABLE"

        if isinstance(value, dict):
            return "RECORD", "NULLABLE"

        if isinstance(value, list):
            if not value:
                return "STRING", "REPEATED"

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

            return "STRING", "REPEATED"

        return "STRING", "NULLABLE"

    def _map_pandas_type_to_bq(self, pandas_type: str) -> str:
        """Map pandas data type to BigQuery data type."""
        if pandas_type.startswith("int"):
            return "INTEGER"

        if pandas_type.startswith("float"):
            return "FLOAT"

        if pandas_type.startswith("bool"):
            return "BOOLEAN"

        if pandas_type.startswith("datetime"):
            return "TIMESTAMP"

        return "STRING"

    def _infer_string_type(self, sample_values: List[Any]) -> str:
        """Try to infer a more specific type from string values."""
        if not sample_values:
            return "STRING"

        non_null_values = [
            v for v in sample_values if v is not None and v != ""
        ]
        if not non_null_values:
            return "STRING"

        if all(self._looks_like_timestamp(str(v)) for v in non_null_values):
            return "TIMESTAMP"

        if all(self._looks_like_date(str(v)) for v in non_null_values):
            return "DATE"

        bool_values = {"true", "false", "t", "f", "yes", "no", "y", "n", "1", "0"}
        if all(str(v).lower() in bool_values for v in non_null_values):
            return "BOOLEAN"

        try:
            if all(float(v).is_integer() for v in non_null_values):
                return "INTEGER"
        except (ValueError, TypeError):
            pass

        try:
            if all(isinstance(float(v), float) for v in non_null_values):
                return "FLOAT"
        except (ValueError, TypeError):
            pass

        return "STRING"

    def _looks_like_timestamp(self, value: str) -> bool:
        """Check if a string value looks like a timestamp."""
        timestamp_patterns = [
            r"^\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2}",
            r"^\d{4}/\d{2}/\d{2} \d{2}:\d{2}:\d{2}",
            r"^\d{2}/\d{2}/\d{4} \d{2}:\d{2}:\d{2}",
        ]
        return any(re.match(pattern, value) for pattern in timestamp_patterns)

    def _looks_like_date(self, value: str) -> bool:
        """Check if a string value looks like a date."""
        date_patterns = [
            r"^\d{4}-\d{2}-\d{2}$",
            r"^\d{4}/\d{2}/\d{2}$",
            r"^\d{2}/\d{2}/\d{4}$",
            r"^\d{2}-\d{2}-\d{4}$",
        ]
        return any(re.match(pattern, value) for pattern in date_patterns)

    def _clean_column_name(self, name: str) -> str:
        """Clean a column name to be BigQuery-friendly."""
        cleaned = re.sub(r"[^\w]", "_", name)

        if cleaned and not (cleaned[0].isalpha() or cleaned[0] == "_"):
            cleaned = f"col_{cleaned}"

        if not cleaned:
            cleaned = "unnamed_column"

        return cleaned
