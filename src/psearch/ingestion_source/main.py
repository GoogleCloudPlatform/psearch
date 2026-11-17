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
from datetime import datetime
import inspect
import json
import logging
import os
import re
from typing import Any, Dict, List, Optional
import urllib.parse
import uuid

from fastapi import (
    APIRouter,
    BackgroundTasks,
    Depends,
    FastAPI,
    File,
    HTTPException,
    Query,
    Request,
    UploadFile,
)
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from google.api_core.exceptions import BadRequest, NotFound
from google.cloud import bigquery
import pandas as pd
from pydantic import BaseModel, ConfigDict, Field, field_validator

from .auth import require_auth
from .services.bigquery_service import BigQueryService
from .services.dataset_service import DatasetService
from .services.schema_detection_service import SchemaDetectionService
from .services.storage_service import StorageService
from .validation import (
    UploadTooLargeError,
    get_max_upload_size_bytes,
    validate_dataset_id,
    validate_file_id,
    validate_table_id,
    validate_upload_filename,
    validate_write_disposition,
)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

# In-memory storage for jobs (would be replaced with a database in production)
jobs: Dict[str, Dict[str, Any]] = {}

# Allowance for multipart/form-data boundaries and part headers on raw HTTP streams
_MULTIPART_FRAMING_ALLOWANCE_BYTES = 64 * 1024


def _is_os_permission_error(exc: PermissionError) -> bool:
    """Distinguish OS/filesystem PermissionError (OSError) from policy PermissionError."""
    return (
        getattr(exc, "errno", None) is not None
        or getattr(exc, "filename", None) is not None
    )


def _sanitize_bq_bad_request_message(exc: BadRequest) -> str:
    """Return user-actionable SQL validation message stripped of internal REST URLs."""
    raw = (getattr(exc, "message", None) or "").strip()
    if not raw:
        raw = str(exc).strip()
    cleaned = re.sub(
        r"^(?:\d{3}\s+)?(?:GET|POST|PUT|DELETE|PATCH)\s+https?://\S+?:\s*",
        "",
        raw,
        flags=re.IGNORECASE,
    )
    cleaned = re.sub(r"^\d{3}\s+", "", cleaned)
    cleaned = re.sub(r"https?://\S+", "[redacted-url]", cleaned)
    return cleaned.strip() or "SQL query validation failed."


# Define models for API requests and responses
class SchemaField(BaseModel):
    name: str
    type: str
    mode: str = "NULLABLE"
    description: Optional[str] = None


class DatasetRequest(BaseModel):
    dataset_id: str
    location: str = "US"
    description: Optional[str] = None

    @field_validator("dataset_id")
    @classmethod
    def _validate_dataset_id_syntax(cls, value: str) -> str:
        try:
            validate_dataset_id(
                value,
                project_id=os.environ.get("PROJECT_ID"),
                allow_project_prefix=True,
            )
        except PermissionError as exc:
            if _is_os_permission_error(exc):
                raise
            # Defer allowlist policy errors to route handlers so they map to HTTP 403
            return value
        return value


class TableRequest(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    dataset_id: str
    table_id: str
    schema_fields: List[SchemaField] = Field(..., alias="schema")
    description: Optional[str] = None

    @field_validator("dataset_id")
    @classmethod
    def _validate_dataset_id_syntax(cls, value: str) -> str:
        try:
            return validate_dataset_id(value)
        except PermissionError as exc:
            if _is_os_permission_error(exc):
                raise
            return value

    @field_validator("table_id")
    @classmethod
    def _validate_table_id_syntax(cls, value: str) -> str:
        return validate_table_id(value)


class LoadJobRequest(BaseModel):
    dataset_id: str
    table_id: str
    source_format: str = "CSV"
    write_disposition: str = "WRITE_EMPTY"
    skip_leading_rows: int = 1  # Default to 1 for CSV, but 0 is used for JSON
    allow_jagged_rows: bool = False
    allow_quoted_newlines: bool = True
    field_delimiter: str = ","
    quote_character: str = '"'
    max_bad_records: int = 0  # Default to 0, can be increased to allow bad records

    @field_validator("dataset_id")
    @classmethod
    def _validate_dataset_id_syntax(cls, value: str) -> str:
        try:
            return validate_dataset_id(value)
        except PermissionError as exc:
            if _is_os_permission_error(exc):
                raise
            return value

    @field_validator("table_id")
    @classmethod
    def _validate_table_id_syntax(cls, value: str) -> str:
        return validate_table_id(value)


class JobStatusResponse(BaseModel):
    job_id: str
    status: str
    message: Optional[str] = None
    created_at: str
    completed_at: Optional[str] = None
    metadata: Optional[Dict[str, Any]] = None


class DryRunRequest(BaseModel):
    sql_script: str
    max_timeout_seconds: Optional[int] = 30


class DryRunResponse(BaseModel):
    valid: bool
    message: Optional[str] = None
    error: Optional[str] = None
    details: Optional[Dict[str, Any]] = None


class SQLFixAttemptRequest(BaseModel):
    original_sql: str
    current_sql: str
    error_message: str
    attempt_number: int = 1
    max_attempts: int = 3


class SQLFixResponse(BaseModel):
    original_sql: str
    current_sql: str
    suggested_sql: str
    diff: str
    attempt_number: int
    valid: bool = False
    message: Optional[str] = None
    error: Optional[str] = None


class ApplySQLFixRequest(BaseModel):
    sql_to_apply: str
    attempt_number: int


# Initialize services
def get_storage_service() -> StorageService:
    project_id = os.environ.get("PROJECT_ID")
    if not project_id:
        logger.error("PROJECT_ID environment variable is not set")
        raise HTTPException(
            status_code=500, detail="Internal server configuration error"
        )
    try:
        return StorageService(project_id)
    except HTTPException:
        raise
    except Exception:
        logger.error("Failed to initialize StorageService", exc_info=True)
        raise HTTPException(
            status_code=500, detail="Internal server error initializing storage service"
        )


def get_schema_detection_service() -> SchemaDetectionService:
    try:
        return SchemaDetectionService()
    except HTTPException:
        raise
    except Exception:
        logger.error("Failed to initialize SchemaDetectionService", exc_info=True)
        raise HTTPException(
            status_code=500, detail="Internal server error initializing schema service"
        )


def get_bigquery_service() -> BigQueryService:
    project_id = os.environ.get("PROJECT_ID")
    if not project_id:
        logger.error("PROJECT_ID environment variable is not set")
        raise HTTPException(
            status_code=500, detail="Internal server configuration error"
        )
    try:
        return BigQueryService(project_id)
    except HTTPException:
        raise
    except Exception:
        logger.error("Failed to initialize BigQueryService", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail="Internal server error initializing BigQuery service",
        )


def get_dataset_service() -> DatasetService:
    project_id = os.environ.get("PROJECT_ID")
    if not project_id:
        logger.error("PROJECT_ID environment variable is not set")
        raise HTTPException(
            status_code=500, detail="Internal server configuration error"
        )
    try:
        return DatasetService(project_id)
    except HTTPException:
        raise
    except Exception:
        logger.error("Failed to initialize DatasetService", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail="Internal server error initializing dataset service",
        )


protected_router = APIRouter(dependencies=[Depends(require_auth)])


async def root() -> Dict[str, str]:
    """Public health check endpoint."""
    return {"status": "healthy", "service": "PSearch Source Ingestion API"}


@protected_router.post("/upload", response_model=Dict[str, Any])
async def upload_file(
    request: Request,
    file: UploadFile = File(...),
    storage_service: StorageService = Depends(get_storage_service),
    schema_service: SchemaDetectionService = Depends(get_schema_detection_service),
):
    """
    Upload a file (CSV or JSON) and detect its schema.
    """
    logger.info(f"Received file upload: {file.filename}")

    try:
        max_upload_bytes = get_max_upload_size_bytes()
    except ValueError:
        logger.error("Invalid MAX_UPLOAD_SIZE_BYTES server configuration", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail="Internal server configuration error",
        )

    content_length_raw = request.headers.get("content-length")
    if content_length_raw is not None:
        try:
            content_length = int(content_length_raw.strip())
            if content_length < 0:
                raise ValueError("Content-Length must be non-negative")
        except ValueError as exc:
            raise HTTPException(
                status_code=400,
                detail="Invalid Content-Length header",
            ) from exc
        if content_length > max_upload_bytes + _MULTIPART_FRAMING_ALLOWANCE_BYTES:
            raise HTTPException(
                status_code=413,
                detail="Uploaded file exceeds maximum allowed size",
            )

    try:
        file_extension = validate_upload_filename(file.filename)
    except ValueError as e:
        logger.warning(f"Rejected invalid upload filename {file.filename}: {str(e)}")
        raise HTTPException(status_code=400, detail=str(e))

    try:
        try:
            schema = await schema_service.detect_schema(file, file_extension)
        except UploadTooLargeError:
            raise
        except (
            UnicodeDecodeError,
            json.JSONDecodeError,
            csv.Error,
            pd.errors.ParserError,
            pd.errors.EmptyDataError,
            ValueError,
        ) as parse_err:
            logger.warning(
                f"Malformed {file_extension} content in upload {file.filename}: {parse_err}"
            )
            raise HTTPException(
                status_code=400,
                detail="Malformed CSV or JSON file content.",
            )

        file_id = str(uuid.uuid4())
        gcs_uri = await storage_service.upload_file(file, file_id)

        return {
            "file_id": file_id,
            "original_filename": file.filename,
            "gcs_uri": gcs_uri,
            "file_type": file_extension,
            "detected_schema": schema,
            "row_count_estimate": schema.get("row_count_estimate", 0),
            "upload_timestamp": datetime.now().isoformat(),
        }

    except HTTPException:
        raise
    except UploadTooLargeError as e:
        logger.warning(f"Rejected oversized upload {file.filename}: {str(e)}")
        raise HTTPException(
            status_code=413,
            detail="Uploaded file exceeds maximum allowed size",
        )
    except Exception:
        logger.error("Error processing upload", exc_info=True)
        raise HTTPException(
            status_code=500, detail="Internal server error while processing upload"
        )


@protected_router.post("/datasets", response_model=Dict[str, Any])
async def create_dataset(
    request: DatasetRequest,
    bq_service: BigQueryService = Depends(get_bigquery_service),
):
    """
    Create a BigQuery dataset.
    """
    logger.info(f"Creating dataset: {request.dataset_id}")

    try:
        dataset_id = validate_dataset_id(request.dataset_id)
    except PermissionError as e:
        if _is_os_permission_error(e):
            logger.error("OS PermissionError during dataset validation", exc_info=True)
            raise HTTPException(
                status_code=500, detail="Internal server error while creating dataset"
            )
        raise HTTPException(status_code=403, detail=str(e))
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

    try:
        result = await bq_service.create_dataset(
            dataset_id=dataset_id,
            location=request.location,
            description=request.description
            or f"Dataset created by PSearch Source Ingestion on {datetime.now().isoformat()}",
        )

        return {
            "dataset_id": dataset_id,
            "location": request.location,
            "created": result.get("created", True),
            "message": result.get("message", "Dataset created successfully"),
        }

    except HTTPException:
        raise
    except Exception:
        logger.error("Error creating dataset", exc_info=True)
        raise HTTPException(
            status_code=500, detail="Internal server error while creating dataset"
        )


@protected_router.post("/tables", response_model=Dict[str, Any])
async def create_table(
    request: TableRequest,
    bq_service: BigQueryService = Depends(get_bigquery_service),
):
    """
    Create a BigQuery table with the specified schema.
    """
    logger.info(f"Creating table: {request.dataset_id}.{request.table_id}")

    try:
        dataset_id = validate_dataset_id(request.dataset_id)
        table_id = validate_table_id(request.table_id)
    except PermissionError as e:
        if _is_os_permission_error(e):
            logger.error("OS PermissionError during table validation", exc_info=True)
            raise HTTPException(
                status_code=500, detail="Internal server error while creating table"
            )
        raise HTTPException(status_code=403, detail=str(e))
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

    try:
        schema_fields = [field.model_dump() for field in request.schema_fields]

        result = await bq_service.create_table(
            dataset_id=dataset_id,
            table_id=table_id,
            schema=schema_fields,
            description=request.description
            or f"Table created by PSearch Source Ingestion on {datetime.now().isoformat()}",
        )

        return {
            "dataset_id": dataset_id,
            "table_id": table_id,
            "created": result.get("created", True),
            "message": result.get("message", "Table created successfully"),
            "schema_field_count": len(request.schema_fields),
        }

    except HTTPException:
        raise
    except Exception:
        logger.error("Error creating table", exc_info=True)
        raise HTTPException(
            status_code=500, detail="Internal server error while creating table"
        )


def _validate_load_inputs(
    request: LoadJobRequest,
    file_id: str,
    file_type: str,
) -> tuple:
    """Validate all load job parameters before creating job state or scheduling tasks."""
    validated_file_id = validate_file_id(file_id)

    file_type_lower = file_type.strip().lower() if isinstance(file_type, str) else ""
    if file_type_lower not in {"csv", "json"}:
        raise HTTPException(
            status_code=400,
            detail="Invalid file_type specified. Must be 'csv' or 'json'.",
        )

    dataset_id = validate_dataset_id(request.dataset_id)
    table_id = validate_table_id(request.table_id)
    write_disposition = validate_write_disposition(request.write_disposition)

    source_format = (
        request.source_format.strip().upper()
        if isinstance(request.source_format, str)
        else ""
    )
    if source_format not in {"CSV", "JSON"}:
        raise HTTPException(
            status_code=400,
            detail="Invalid source_format specified. Must be 'CSV' or 'JSON'.",
        )

    return (
        validated_file_id,
        file_type_lower,
        dataset_id,
        table_id,
        write_disposition,
        source_format,
    )


@protected_router.post("/create_and_load", response_model=JobStatusResponse)
async def create_and_load_table(
    request: LoadJobRequest,
    file_id: str = Query(..., description="The ID of the uploaded file"),
    file_type: str = Query(
        ..., description="The type of the uploaded file (csv or json)"
    ),
    background_tasks: BackgroundTasks = None,
    storage_service: StorageService = Depends(get_storage_service),
    bq_service: BigQueryService = Depends(get_bigquery_service),
):
    """
    Create a BigQuery table and load data in one step with schema autodetection.
    """
    logger.info(
        f"Creating table and loading data from file {file_id} into {request.dataset_id}.{request.table_id}"
    )

    try:
        (
            validated_file_id,
            file_type_lower,
            dataset_id,
            table_id,
            write_disposition,
            source_format,
        ) = _validate_load_inputs(request, file_id, file_type)
    except HTTPException:
        raise
    except PermissionError as e:
        if _is_os_permission_error(e):
            logger.error("OS PermissionError during load validation", exc_info=True)
            raise HTTPException(
                status_code=500,
                detail="Internal server error while initiating create and load job",
            )
        raise HTTPException(status_code=403, detail=str(e))
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

    try:
        gcs_uri = storage_service.get_file_uri(validated_file_id, file_type_lower)
        if not gcs_uri:
            raise HTTPException(
                status_code=404,
                detail=f"File with ID {validated_file_id} and type {file_type_lower} not found in GCS bucket.",
            )

        job_id = f"createload_{uuid.uuid4()}"
        jobs[job_id] = {
            "job_id": job_id,
            "status": "RUNNING",
            "message": "Job started - creating table and loading data with autodetection",
            "created_at": datetime.now().isoformat(),
            "completed_at": None,
            "metadata": {
                "file_id": validated_file_id,
                "gcs_uri": gcs_uri,
                "dataset_id": dataset_id,
                "table_id": table_id,
                "source_format": source_format,
                "auto_schema_detection": True,
            },
        }

        if background_tasks:
            background_tasks.add_task(
                bq_service.load_table_from_uri,
                job_id=job_id,
                jobs_dict=jobs,
                dataset_id=dataset_id,
                table_id=table_id,
                uri=gcs_uri,
                source_format=source_format,
                write_disposition=write_disposition,
                skip_leading_rows=(
                    request.skip_leading_rows if source_format == "CSV" else None
                ),
                allow_jagged_rows=(
                    request.allow_jagged_rows if source_format == "CSV" else None
                ),
                allow_quoted_newlines=(
                    request.allow_quoted_newlines if source_format == "CSV" else None
                ),
                field_delimiter=(
                    request.field_delimiter if source_format == "CSV" else None
                ),
                quote_character=(
                    request.quote_character if source_format == "CSV" else None
                ),
                autodetect=True,
                max_bad_records=request.max_bad_records,
            )

        return JobStatusResponse(**jobs[job_id])

    except HTTPException:
        raise
    except Exception:
        logger.error("Error initiating create and load job", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail="Internal server error while initiating create and load job",
        )


@protected_router.post("/load", response_model=JobStatusResponse)
async def load_data(
    request: LoadJobRequest,
    file_id: str = Query(..., description="The ID of the uploaded file"),
    file_type: str = Query(
        ..., description="The type of the uploaded file (csv or json)"
    ),
    background_tasks: BackgroundTasks = None,
    storage_service: StorageService = Depends(get_storage_service),
    bq_service: BigQueryService = Depends(get_bigquery_service),
):
    """
    Load data from a previously uploaded file into an existing BigQuery table.
    """
    logger.info(
        f"Loading data from file {file_id} into {request.dataset_id}.{request.table_id}"
    )

    try:
        (
            validated_file_id,
            file_type_lower,
            dataset_id,
            table_id,
            write_disposition,
            source_format,
        ) = _validate_load_inputs(request, file_id, file_type)
    except HTTPException:
        raise
    except PermissionError as e:
        if _is_os_permission_error(e):
            logger.error("OS PermissionError during load validation", exc_info=True)
            raise HTTPException(
                status_code=500, detail="Internal server error while initiating load job"
            )
        raise HTTPException(status_code=403, detail=str(e))
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

    try:
        gcs_uri = storage_service.get_file_uri(validated_file_id, file_type_lower)
        if not gcs_uri:
            raise HTTPException(
                status_code=404,
                detail=f"File with ID {validated_file_id} and type {file_type_lower} not found in GCS bucket.",
            )

        job_id = f"load_{uuid.uuid4()}"
        jobs[job_id] = {
            "job_id": job_id,
            "status": "RUNNING",
            "message": "Job started",
            "created_at": datetime.now().isoformat(),
            "completed_at": None,
            "metadata": {
                "file_id": validated_file_id,
                "gcs_uri": gcs_uri,
                "dataset_id": dataset_id,
                "table_id": table_id,
                "source_format": source_format,
            },
        }

        if background_tasks:
            background_tasks.add_task(
                bq_service.load_table_from_uri,
                job_id=job_id,
                jobs_dict=jobs,
                dataset_id=dataset_id,
                table_id=table_id,
                uri=gcs_uri,
                source_format=source_format,
                write_disposition=write_disposition,
                skip_leading_rows=(
                    request.skip_leading_rows if source_format == "CSV" else None
                ),
                allow_jagged_rows=(
                    request.allow_jagged_rows if source_format == "CSV" else None
                ),
                allow_quoted_newlines=(
                    request.allow_quoted_newlines if source_format == "CSV" else None
                ),
                field_delimiter=(
                    request.field_delimiter if source_format == "CSV" else None
                ),
                quote_character=(
                    request.quote_character if source_format == "CSV" else None
                ),
                autodetect=False,
                max_bad_records=request.max_bad_records,
            )

        return JobStatusResponse(**jobs[job_id])

    except HTTPException:
        raise
    except Exception:
        logger.error("Error initiating load job", exc_info=True)
        raise HTTPException(
            status_code=500, detail="Internal server error while initiating load job"
        )


@protected_router.post("/dry-run-query", response_model=DryRunResponse)
async def dry_run_query(
    request: DryRunRequest,
):
    """
    Perform a dry run of a SQL query to validate it without executing it.
    """
    logger.info("Received SQL dry run request")

    if not request.sql_script or not request.sql_script.strip():
        raise HTTPException(status_code=400, detail="SQL script is required")

    try:
        project_id = os.environ.get("PROJECT_ID")
        if not project_id:
            logger.error("PROJECT_ID environment variable is not set")
            raise HTTPException(
                status_code=500, detail="Internal server configuration error"
            )

        client = bigquery.Client(project=project_id)
        job_config = bigquery.QueryJobConfig(dry_run=True, use_query_cache=False)
        timeout_seconds = (
            float(request.max_timeout_seconds)
            if request.max_timeout_seconds and request.max_timeout_seconds > 0
            else 30.0
        )

        start_time = datetime.now()
        query_job = client.query(
            request.sql_script,
            job_config=job_config,
            timeout=timeout_seconds,
        )
        execution_time = (datetime.now() - start_time).total_seconds()

        return DryRunResponse(
            valid=True,
            message=(
                f"SQL syntax validated successfully "
                f"(Estimated bytes: {query_job.total_bytes_processed:,})"
            ),
            details={
                "estimated_bytes_processed": query_job.total_bytes_processed,
                "execution_time_seconds": execution_time,
            },
        )

    except HTTPException:
        raise
    except BadRequest as e:
        logger.warning(f"SQL validation error: {str(e)}")
        error_message = _sanitize_bq_bad_request_message(e)
        error_details: Dict[str, Any] = {}
        if "Invalid field reference" in error_message:
            field_match = re.search(
                r"Invalid field reference '([^']+)'", error_message
            )
            if field_match:
                error_details["missing_field"] = field_match.group(1)

        return DryRunResponse(
            valid=False,
            error=error_message,
            details=error_details if error_details else None,
        )

    except NotFound as e:
        logger.warning(f"BigQuery Not Found error: {str(e)}")
        raw_not_found = (getattr(e, "message", None) or str(e)).strip()
        error_details = {"error_type": "not_found"}
        error_message = "Referenced BigQuery dataset or table was not found."

        dataset_match = re.search(
            r"Dataset\s+`?([A-Za-z0-9_-]+[:.][A-Za-z0-9_]+)`?\s+(?:was\s+)?not\s+found",
            raw_not_found,
            flags=re.IGNORECASE,
        )
        if dataset_match:
            missing_ds = dataset_match.group(1)
            error_details["missing_dataset"] = missing_ds
            error_message = (
                f"Dataset '{missing_ds}' not found. "
                "Please create this dataset before running the query."
            )

        table_match = re.search(
            r"Table\s+`?([A-Za-z0-9_-]+[:.][A-Za-z0-9_]+[:.][A-Za-z0-9_]+)`?\s+(?:was\s+)?not\s+found",
            raw_not_found,
            flags=re.IGNORECASE,
        )
        if table_match:
            missing_tbl = table_match.group(1)
            error_details["missing_table"] = missing_tbl
            error_message = (
                f"Table '{missing_tbl}' not found. "
                "Please ensure this table exists before running the query."
            )

        return DryRunResponse(
            valid=False,
            error=error_message,
            details=error_details,
        )

    except Exception as e:
        logger.error("Error performing SQL dry run", exc_info=True)
        raw_message = str(e).lower()

        if "gateway timeout" in raw_message or "deadline exceeded" in raw_message:
            error_category = "timeout"
            safe_message = (
                "The query validation timed out. "
                "Please try with a simpler query or increase the timeout."
            )
        elif (
            "unable to create a client" in raw_message
            or "could not automatically determine" in raw_message
        ):
            error_category = "auth_error"
            safe_message = (
                "Authentication error: Could not connect to Google Cloud. "
                "Please check service credentials and permissions."
            )
        else:
            error_category = "internal_error"
            safe_message = "Internal server error while performing SQL validation."

        return DryRunResponse(
            valid=False,
            error=safe_message,
            details={"error_type": error_category},
        )


@protected_router.get("/jobs/{job_id}", response_model=JobStatusResponse)
async def get_job_status(job_id: str):
    """
    Get the status of a background job.
    """
    logger.info(f"Getting status for job: {job_id}")

    try:
        if job_id not in jobs:
            raise HTTPException(
                status_code=404, detail=f"Job with ID {job_id} not found"
            )
        return JobStatusResponse(**jobs[job_id])
    except HTTPException:
        raise
    except Exception:
        logger.error("Error retrieving job status", exc_info=True)
        raise HTTPException(
            status_code=500, detail="Internal server error while retrieving job status"
        )


@protected_router.get("/jobs", response_model=List[JobStatusResponse])
async def list_jobs(
    limit: int = Query(100, ge=1, le=1000),
    status: Optional[str] = Query(
        None, description="Filter jobs by status (RUNNING, COMPLETED, FAILED)"
    ),
):
    """
    List all jobs with optional filtering.
    """
    logger.info(f"Listing jobs with status filter: {status}")

    try:
        filtered_jobs = jobs.values()
        if status:
            filtered_jobs = [
                job for job in filtered_jobs if job["status"] == status
            ]

        sorted_jobs = sorted(
            filtered_jobs, key=lambda job: job["created_at"], reverse=True
        )[:limit]
        return [JobStatusResponse(**job) for job in sorted_jobs]
    except HTTPException:
        raise
    except Exception:
        logger.error("Error listing jobs", exc_info=True)
        raise HTTPException(
            status_code=500, detail="Internal server error while listing jobs"
        )


@protected_router.post("/ensure-dataset", response_model=Dict[str, Any])
async def ensure_dataset_exists(
    request: DatasetRequest,
    dataset_service: DatasetService = Depends(get_dataset_service),
):
    """
    Ensure a BigQuery dataset exists within the configured project, creating it if necessary.
    """
    logger.info(f"Ensuring dataset exists: {request.dataset_id}")

    try:
        dataset_id = validate_dataset_id(
            request.dataset_id,
            project_id=os.environ.get("PROJECT_ID"),
            allow_project_prefix=True,
        )
    except PermissionError as e:
        if _is_os_permission_error(e):
            logger.error(
                "OS PermissionError during ensure-dataset validation", exc_info=True
            )
            raise HTTPException(
                status_code=500,
                detail="Internal server error while ensuring dataset exists",
            )
        raise HTTPException(status_code=403, detail=str(e))
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

    try:
        result = await dataset_service.ensure_dataset_exists(
            dataset_id=dataset_id,
            location=request.location,
        )

        return {
            "dataset_id": dataset_id,
            "location": result.get("location", request.location),
            "created": result.get("created", False),
            "message": result.get("message", "Dataset operation completed"),
        }

    except HTTPException:
        raise
    except Exception:
        logger.error("Error ensuring dataset exists", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail="Internal server error while ensuring dataset exists",
        )


@protected_router.get("/buckets", response_model=List[Dict[str, Any]])
async def list_buckets(
    storage_service: StorageService = Depends(get_storage_service),
):
    """
    List the scoped ingestion bucket for the configured project.
    """
    logger.info("Listing available buckets")

    try:
        return storage_service.list_buckets()
    except HTTPException:
        raise
    except Exception:
        logger.error("Error listing buckets", exc_info=True)
        raise HTTPException(
            status_code=500, detail="Internal server error while listing buckets"
        )


class _UploadGuardMiddleware:
    """Pre-multipart ASGI guard on POST /upload enforcing auth and raw body caps."""

    def __init__(self, app: Any, fastapi_app: Optional[FastAPI] = None):
        self.app = app
        self.fastapi_app = fastapi_app

    async def __call__(self, scope: Dict[str, Any], receive: Any, send: Any) -> None:
        if scope.get("type") != "http" or scope.get("method", "").upper() != "POST":
            await self.app(scope, receive, send)
            return

        raw_path = scope.get("path", "")
        root_path = scope.get("root_path", "")
        route_path = (
            raw_path[len(root_path) :]
            if root_path and raw_path.startswith(root_path)
            else raw_path
        )
        if route_path.rstrip("/") != "/upload":
            await self.app(scope, receive, send)
            return

        request = Request(scope)
        try:
            auth_override = (
                self.fastapi_app.dependency_overrides.get(require_auth)
                if self.fastapi_app is not None
                else None
            )
            if auth_override is not None:
                maybe_coro = auth_override()
                if inspect.isawaitable(maybe_coro):
                    await maybe_coro
            else:
                require_auth(
                    x_admin_token=request.headers.get("x-admin-token"),
                    authorization=request.headers.get("authorization"),
                )
        except HTTPException as exc:
            response = JSONResponse(
                status_code=exc.status_code,
                content={"detail": exc.detail},
                headers=exc.headers,
            )
            await response(scope, receive, send)
            return

        try:
            max_upload_bytes = get_max_upload_size_bytes()
        except ValueError:
            logger.error(
                "Invalid MAX_UPLOAD_SIZE_BYTES server configuration",
                exc_info=True,
            )
            response = JSONResponse(
                status_code=500,
                content={"detail": "Internal server configuration error"},
            )
            await response(scope, receive, send)
            return

        max_wire_bytes = max_upload_bytes + _MULTIPART_FRAMING_ALLOWANCE_BYTES

        content_length_raw = request.headers.get("content-length")
        if content_length_raw is not None:
            try:
                content_length = int(content_length_raw.strip())
                if content_length < 0:
                    raise ValueError("Content-Length must be non-negative")
            except ValueError:
                response = JSONResponse(
                    status_code=400,
                    content={"detail": "Invalid Content-Length header"},
                )
                await response(scope, receive, send)
                return
            if content_length > max_wire_bytes:
                response = JSONResponse(
                    status_code=413,
                    content={"detail": "Uploaded file exceeds maximum allowed size"},
                )
                await response(scope, receive, send)
                return

        bytes_received = 0
        response_started = False

        async def guarded_send(message: Dict[str, Any]) -> None:
            nonlocal response_started
            if message.get("type") == "http.response.start":
                response_started = True
            await send(message)

        async def counting_receive() -> Dict[str, Any]:
            nonlocal bytes_received
            message = await receive()
            if message.get("type") == "http.request":
                body = message.get("body", b"") or b""
                bytes_received += len(body)
                if bytes_received > max_wire_bytes:
                    raise HTTPException(
                        status_code=413,
                        detail="Uploaded file exceeds maximum allowed size",
                    )
            return message

        try:
            await self.app(scope, counting_receive, guarded_send)
        except HTTPException as exc:
            if response_started:
                raise
            response = JSONResponse(
                status_code=exc.status_code,
                content={"detail": exc.detail},
                headers=exc.headers,
            )
            await response(scope, receive, send)


def _sanitize_allowed_origins(raw_value: str) -> List[str]:
    """Normalize explicit HTTP(S) CORS origins and skip wildcard or malformed entries."""
    origins: List[str] = []
    seen: set[str] = set()
    for part in raw_value.split(","):
        candidate = part.strip()
        if not candidate:
            continue
        if "*" in candidate:
            logger.warning(
                "Ignoring wildcard %r in ALLOWED_ORIGINS; explicit origins are required",
                candidate,
            )
            continue
        if "?" in candidate or "#" in candidate or any(ch.isspace() for ch in candidate):
            logger.warning(
                "Ignoring malformed origin %r in ALLOWED_ORIGINS",
                candidate,
            )
            continue
        parsed = urllib.parse.urlsplit(candidate)
        scheme = parsed.scheme.lower()
        try:
            _ = parsed.port
        except ValueError:
            logger.warning(
                "Ignoring invalid port in ALLOWED_ORIGINS entry %r",
                candidate,
            )
            continue
        if (
            scheme not in {"http", "https"}
            or not parsed.netloc
            or not parsed.hostname
            or parsed.username is not None
            or parsed.password is not None
            or "@" in parsed.netloc
            or parsed.query
            or parsed.fragment
            or parsed.path not in ("", "/")
        ):
            logger.warning(
                "Ignoring malformed origin %r in ALLOWED_ORIGINS",
                candidate,
            )
            continue
        normalized = f"{scheme}://{parsed.netloc.lower()}"
        if normalized not in seen:
            seen.add(normalized)
            origins.append(normalized)
    return origins


def create_app() -> FastAPI:
    """Create and configure the FastAPI application instance."""
    application = FastAPI(
        title="PSearch Source Ingestion API",
        description="API for ingesting data sources into BigQuery for PSearch",
        version="1.0.0",
        docs_url=None,
        redoc_url=None,
        openapi_url=None,
    )

    origins = _sanitize_allowed_origins(os.environ.get("ALLOWED_ORIGINS", ""))

    application.add_middleware(_UploadGuardMiddleware, fastapi_app=application)
    application.add_middleware(
        CORSMiddleware,
        allow_origins=origins,
        allow_credentials=bool(origins),
        allow_methods=["GET", "POST", "OPTIONS"],
        allow_headers=["Authorization", "Content-Type", "X-Admin-Token"],
    )

    @application.exception_handler(PermissionError)
    async def _permission_error_handler(_request: Request, exc: PermissionError):
        if _is_os_permission_error(exc):
            logger.error("Unhandled OS PermissionError", exc_info=True)
            return JSONResponse(
                status_code=500, content={"detail": "Internal server error"}
            )
        return JSONResponse(status_code=403, content={"detail": str(exc)})

    application.get("/")(root)
    application.include_router(protected_router)
    return application


app = create_app()


def main():
    """Run the FastAPI application."""
    import uvicorn

    host = os.environ.get("HOST", "0.0.0.0")
    port = int(os.environ.get("PORT", "8080"))
    uvicorn.run(app, host=host, port=port, reload=False)


if __name__ == "__main__":
    main()
