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

"""Input validation choke points for the PSearch ingestion_source service."""

import os
import re
from typing import Optional
import uuid

DEFAULT_MAX_UPLOAD_SIZE_BYTES = 52_428_800

_ALLOWED_UPLOAD_EXTENSIONS = frozenset({"csv", "json"})
_ALLOWED_WRITE_DISPOSITIONS = frozenset(
    {"WRITE_EMPTY", "WRITE_APPEND", "WRITE_TRUNCATE"}
)
_BQ_IDENTIFIER_RE = re.compile(r"\A[A-Za-z0-9_]{1,1024}\Z")


class UploadTooLargeError(ValueError):
    """Raised when an uploaded file exceeds MAX_UPLOAD_SIZE_BYTES."""


def get_max_upload_size_bytes() -> int:
    """Return the configured maximum upload size in bytes (defaults to 50 MiB)."""
    raw_value = os.environ.get("MAX_UPLOAD_SIZE_BYTES", "").strip()
    if not raw_value:
        return DEFAULT_MAX_UPLOAD_SIZE_BYTES
    try:
        parsed = int(raw_value)
    except ValueError as exc:
        raise ValueError("MAX_UPLOAD_SIZE_BYTES must be a positive integer") from exc
    if parsed <= 0:
        raise ValueError("MAX_UPLOAD_SIZE_BYTES must be a positive integer")
    return parsed


def validate_upload_filename(filename: Optional[str]) -> str:
    """Validate upload filename and return normalized extension ('csv' or 'json')."""
    if not isinstance(filename, str) or "\x00" in filename:
        raise ValueError("Filename is required and must be a valid string")

    stripped = filename.strip()
    if not stripped or stripped.endswith(("/", "\\")):
        raise ValueError("Filename must not be empty")

    basename = os.path.basename(stripped.replace("\\", "/")).strip()
    if not basename or "." not in basename:
        raise ValueError(
            "Unsupported file type. Filename must have a .csv or .json extension."
        )

    stem, ext = basename.rsplit(".", 1)
    if not stem.strip("."):
        raise ValueError("Filename must include a non-empty name before the extension")

    ext_lower = ext.lower()
    if ext_lower not in _ALLOWED_UPLOAD_EXTENSIONS:
        raise ValueError(
            "Unsupported file type. Only CSV and JSON files are supported."
        )

    return ext_lower


def validate_file_id(file_id: str) -> str:
    """Validate that file_id is a canonical UUIDv4 string and return it lowercased."""
    if not isinstance(file_id, str):
        raise ValueError("file_id must be a valid UUIDv4 string")

    cleaned = file_id.strip().lower()
    if not cleaned:
        raise ValueError("file_id must not be empty")

    try:
        parsed = uuid.UUID(cleaned)
    except (ValueError, AttributeError, TypeError) as exc:
        raise ValueError("file_id must be a valid UUIDv4") from exc

    if parsed.version != 4 or str(parsed) != cleaned:
        raise ValueError("file_id must be a canonical UUIDv4")

    return str(parsed)


def validate_dataset_id(
    dataset_id: str,
    project_id: Optional[str] = None,
    allow_project_prefix: bool = False,
) -> str:
    """Validate a BigQuery dataset identifier and enforce INGESTION_ALLOWED_DATASETS."""
    if not isinstance(dataset_id, str) or not dataset_id:
        raise ValueError("dataset_id must be a non-empty string")

    if "." in dataset_id:
        expected_project = project_id.strip() if isinstance(project_id, str) else ""
        if not allow_project_prefix or not expected_project:
            raise ValueError(
                "Project-qualified dataset_id is not allowed for this operation"
            )
        parts = dataset_id.split(".")
        if len(parts) != 2:
            raise ValueError(f"Invalid dataset_id format: '{dataset_id}'")
        prefix_project, simple_ds = parts
        if prefix_project != expected_project:
            raise ValueError(
                f"Dataset '{dataset_id}' references disallowed project '{prefix_project}'"
            )
    else:
        simple_ds = dataset_id

    if not _BQ_IDENTIFIER_RE.fullmatch(simple_ds):
        raise ValueError(f"Invalid BigQuery dataset_id: '{dataset_id}'")

    allowed_raw = os.environ.get("INGESTION_ALLOWED_DATASETS", "")
    allowed_datasets = {
        entry.strip() for entry in allowed_raw.split(",") if entry.strip()
    }
    if allowed_datasets and simple_ds not in allowed_datasets:
        raise PermissionError(
            f"Dataset '{simple_ds}' is not in INGESTION_ALLOWED_DATASETS"
        )

    return simple_ds


def validate_table_id(table_id: str) -> str:
    """Validate a BigQuery table identifier."""
    if not isinstance(table_id, str) or not _BQ_IDENTIFIER_RE.fullmatch(table_id):
        raise ValueError(f"Invalid BigQuery table_id: '{table_id}'")
    return table_id


def validate_write_disposition(disposition: str) -> str:
    """Validate BigQuery write_disposition and gate WRITE_TRUNCATE behind opt-in env."""
    if (
        not isinstance(disposition, str)
        or disposition not in _ALLOWED_WRITE_DISPOSITIONS
    ):
        raise ValueError(
            "Invalid write_disposition; must be WRITE_EMPTY, WRITE_APPEND, or WRITE_TRUNCATE"
        )

    if (
        disposition == "WRITE_TRUNCATE"
        and os.environ.get("ALLOW_WRITE_TRUNCATE", "").strip().lower() != "true"
    ):
        raise PermissionError(
            "WRITE_TRUNCATE is disabled unless ALLOW_WRITE_TRUNCATE=true"
        )

    return disposition
