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
import uuid
import logging
from typing import Dict, List, Any, Optional, Union
from datetime import datetime
import json

from fastapi import (
    FastAPI,
    File,
    UploadFile,
    Form,
    HTTPException,
    BackgroundTasks,
    Query,
    Depends,
)
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field
import uvicorn

from .services.storage_service import StorageService
from .services.schema_detection_service import SchemaDetectionService
from .services.bigquery_service import BigQueryService

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

# Initialize FastAPI app
app = FastAPI(
    title="PSearch Source Ingestion API",
    description="API for ingesting data sources into BigQuery for PSearch",
    version="1.0.0",
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # In production, restrict this to specific domains
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
    expose_headers=["*"],
)

# In-memory storage for jobs (would be replaced with a database in production)
jobs = {}


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


class TableRequest(BaseModel):
    dataset_id: str
    table_id: str
    schema: List[SchemaField]
    description: Optional[str] = None


class LoadJobRequest(BaseModel):
    dataset_id: str
    table_id: str
    source_format: str = "CSV"
    write_disposition: str = "WRITE_TRUNCATE"
    skip_leading_rows: int = 1  # Default to 1 for CSV, but 0 is used for JSON
    allow_jagged_rows: bool = False
    allow_quoted_newlines: bool = True
    field_delimiter: str = ","
    quote_character: str = '"'
    max_bad_records: int = 0  # Default to 0, can be increased to allow bad records


class JobStatusResponse(BaseModel):
    job_id: str
    status: str
    message: Optional[str] = None
    created_at: str
    completed_at: Optional[str] = None
    metadata: Optional[Dict[str, Any]] = None


# Initialize services
def get_storage_service():
    project_id = os.environ.get("PROJECT_ID")
    if not project_id:
        raise ValueError("PROJECT_ID environment variable is not set")
    return StorageService(project_id)


def get_schema_detection_service():
    return SchemaDetectionService()


def get_bigquery_service():
    project_id = os.environ.get("PROJECT_ID")
    if not project_id:
        raise ValueError("PROJECT_ID environment variable is not set")
    return BigQueryService(project_id)


@app.get("/")
async def root():
    """Health check endpoint"""
    return {"status": "healthy", "service": "PSearch Source Ingestion API"}


@app.post("/upload", response_model=Dict[str, Any])
async def upload_file(
    file: UploadFile = File(...),
    storage_service: StorageService = Depends(get_storage_service),
    schema_service: SchemaDetectionService = Depends(get_schema_detection_service),
):
    """
    Upload a file (CSV or JSON) and detect its schema
    """
    logger.info(f"Received file upload: {file.filename}")

    # Validate file type
    file_extension = file.filename.split(".")[-1].lower()
    if file_extension not in ["csv", "json"]:
        raise HTTPException(
            status_code=400,
            detail="Unsupported file type. Only CSV and JSON files are supported.",
        )

    try:
        # Generate a unique file ID
        file_id = str(uuid.uuid4())

        # Store file in Cloud Storage
        gcs_uri = await storage_service.upload_file(file, file_id)

        # Detect schema from file
        schema = await schema_service.detect_schema(file, file_extension)

        return {
            "file_id": file_id,
            "original_filename": file.filename,
            "gcs_uri": gcs_uri,
            "file_type": file_extension,
            "detected_schema": schema,
            "row_count_estimate": schema.get("row_count_estimate", 0),
            "upload_timestamp": datetime.now().isoformat(),
        }

    except Exception as e:
        logger.error(f"Error processing upload: {str(e)}")
        raise HTTPException(
            status_code=500, detail=f"Error processing upload: {str(e)}"
        )


@app.post("/datasets", response_model=Dict[str, Any])
async def create_dataset(
    request: DatasetRequest,
    bq_service: BigQueryService = Depends(get_bigquery_service),
):
    """
    Create a BigQuery dataset
    """
    logger.info(f"Creating dataset: {request.dataset_id}")

    try:
        result = await bq_service.create_dataset(
            dataset_id=request.dataset_id,
            location=request.location,
            description=request.description
            or f"Dataset created by PSearch Source Ingestion on {datetime.now().isoformat()}",
        )

        return {
            "dataset_id": request.dataset_id,
            "location": request.location,
            "created": result.get("created", True),
            "message": result.get("message", "Dataset created successfully"),
        }

    except Exception as e:
        logger.error(f"Error creating dataset: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error creating dataset: {str(e)}")


@app.post("/tables", response_model=Dict[str, Any])
async def create_table(
    request: TableRequest,
    bq_service: BigQueryService = Depends(get_bigquery_service),
):
    """
    Create a BigQuery table with the specified schema
    """
    logger.info(f"Creating table: {request.dataset_id}.{request.table_id}")

    try:
        schema_fields = [field.model_dump() for field in request.schema]

        result = await bq_service.create_table(
            dataset_id=request.dataset_id,
            table_id=request.table_id,
            schema=schema_fields,
            description=request.description
            or f"Table created by PSearch Source Ingestion on {datetime.now().isoformat()}",
        )

        return {
            "dataset_id": request.dataset_id,
            "table_id": request.table_id,
            "created": result.get("created", True),
            "message": result.get("message", "Table created successfully"),
            "schema_field_count": len(request.schema),
        }

    except Exception as e:
        logger.error(f"Error creating table: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error creating table: {str(e)}")


@app.post("/create_and_load", response_model=JobStatusResponse)
async def create_and_load_table(
    request: LoadJobRequest,
    file_id: str = Query(..., description="The ID of the uploaded file"),
    file_type: str = Query(
        ..., description="The type of the uploaded file (csv or json)"
    ),  # Added file_type
    background_tasks: BackgroundTasks = None,
    storage_service: StorageService = Depends(get_storage_service),
    bq_service: BigQueryService = Depends(get_bigquery_service),
):
    """
    Create a BigQuery table and load data in one step with schema autodetection
    """
    logger.info(
        f"Creating table and loading data from file {file_id} into {request.dataset_id}.{request.table_id}"
    )

    try:
        # Generate a unique job ID
        job_id = f"createload_{str(uuid.uuid4())}"

        # Get the GCS URI for the file using file_id and file_type
        gcs_uri = storage_service.get_file_uri(file_id, file_type)
        if not gcs_uri:
            # Make error more specific if file_type is invalid
            if file_type.lower() not in ["csv", "json"]:
                raise HTTPException(
                    status_code=400,
                    detail=f"Invalid file_type specified: {file_type}. Must be 'csv' or 'json'.",
                )
            raise HTTPException(
                status_code=404,
                detail=f"File with ID {file_id} and type {file_type} not found in GCS bucket.",
            )

        # Create job entry
        jobs[job_id] = {
            "job_id": job_id,
            "status": "RUNNING",
            "message": "Job started - creating table and loading data with autodetection",
            "created_at": datetime.now().isoformat(),
            "completed_at": None,
            "metadata": {
                "file_id": file_id,
                "gcs_uri": gcs_uri,
                "dataset_id": request.dataset_id,
                "table_id": request.table_id,
                "source_format": request.source_format,
                "auto_schema_detection": True,
            },
        }

        # Start create and load job in background
        if background_tasks:
            background_tasks.add_task(
                bq_service.load_table_from_uri,
                job_id=job_id,
                jobs_dict=jobs,
                dataset_id=request.dataset_id,
                table_id=request.table_id,
                uri=gcs_uri,
                source_format=request.source_format,
                write_disposition=request.write_disposition,
                skip_leading_rows=(
                    request.skip_leading_rows
                    if request.source_format == "CSV"
                    else None
                ),
                allow_jagged_rows=(
                    request.allow_jagged_rows
                    if request.source_format == "CSV"
                    else None
                ),
                allow_quoted_newlines=(
                    request.allow_quoted_newlines
                    if request.source_format == "CSV"
                    else None
                ),
                field_delimiter=(
                    request.field_delimiter if request.source_format == "CSV" else None
                ),
                quote_character=(
                    request.quote_character if request.source_format == "CSV" else None
                ),
                autodetect=True,  # Enable schema autodetection
                max_bad_records=request.max_bad_records,  # Pass max_bad_records parameter
            )

        return JobStatusResponse(**jobs[job_id])

    except Exception as e:
        logger.error(f"Error initiating create and load job: {str(e)}")
        raise HTTPException(
            status_code=500, detail=f"Error initiating create and load job: {str(e)}"
        )


@app.post("/load", response_model=JobStatusResponse)
async def load_data(
    request: LoadJobRequest,
    file_id: str = Query(..., description="The ID of the uploaded file"),
    file_type: str = Query(
        ..., description="The type of the uploaded file (csv or json)"
    ),  # Added file_type
    background_tasks: BackgroundTasks = None,
    storage_service: StorageService = Depends(get_storage_service),
    bq_service: BigQueryService = Depends(get_bigquery_service),
):
    """
    Load data from a previously uploaded file into a BigQuery table.
    Note: This route assumes the table already exists.
    For automatic table creation with schema detection, use /create_and_load.
    """
    logger.info(
        f"Loading data from file {file_id} into {request.dataset_id}.{request.table_id}"
    )

    try:
        # Generate a unique job ID
        job_id = f"load_{str(uuid.uuid4())}"

        # Get the GCS URI for the file using file_id and file_type
        gcs_uri = storage_service.get_file_uri(file_id, file_type)
        if not gcs_uri:
            # Make error more specific if file_type is invalid
            if file_type.lower() not in ["csv", "json"]:
                raise HTTPException(
                    status_code=400,
                    detail=f"Invalid file_type specified: {file_type}. Must be 'csv' or 'json'.",
                )
            raise HTTPException(
                status_code=404,
                detail=f"File with ID {file_id} and type {file_type} not found in GCS bucket.",
            )

        # Create job entry
        jobs[job_id] = {
            "job_id": job_id,
            "status": "RUNNING",
            "message": "Job started",
            "created_at": datetime.now().isoformat(),
            "completed_at": None,
            "metadata": {
                "file_id": file_id,
                "gcs_uri": gcs_uri,
                "dataset_id": request.dataset_id,
                "table_id": request.table_id,
                "source_format": request.source_format,
            },
        }

        # Start load job in background
        if background_tasks:
            background_tasks.add_task(
                bq_service.load_table_from_uri,
                job_id=job_id,
                jobs_dict=jobs,
                dataset_id=request.dataset_id,
                table_id=request.table_id,
                uri=gcs_uri,
                source_format=request.source_format,
                write_disposition=request.write_disposition,
                skip_leading_rows=(
                    request.skip_leading_rows
                    if request.source_format == "CSV"
                    else None
                ),
                allow_jagged_rows=(
                    request.allow_jagged_rows
                    if request.source_format == "CSV"
                    else None
                ),
                allow_quoted_newlines=(
                    request.allow_quoted_newlines
                    if request.source_format == "CSV"
                    else None
                ),
                field_delimiter=(
                    request.field_delimiter if request.source_format == "CSV" else None
                ),
                quote_character=(
                    request.quote_character if request.source_format == "CSV" else None
                ),
                autodetect=False,  # Disable schema autodetection for existing tables
                max_bad_records=request.max_bad_records,  # Pass max_bad_records parameter
            )

        return JobStatusResponse(**jobs[job_id])

    except Exception as e:
        logger.error(f"Error initiating load job: {str(e)}")
        raise HTTPException(
            status_code=500, detail=f"Error initiating load job: {str(e)}"
        )


@app.get("/jobs/{job_id}", response_model=JobStatusResponse)
async def get_job_status(job_id: str):
    """
    Get the status of a background job
    """
    logger.info(f"Getting status for job: {job_id}")

    if job_id not in jobs:
        raise HTTPException(status_code=404, detail=f"Job with ID {job_id} not found")

    return JobStatusResponse(**jobs[job_id])


@app.get("/jobs", response_model=List[JobStatusResponse])
async def list_jobs(
    limit: int = Query(100, ge=1, le=1000),
    status: Optional[str] = Query(
        None, description="Filter jobs by status (RUNNING, COMPLETED, FAILED)"
    ),
):
    """
    List all jobs with optional filtering
    """
    logger.info(f"Listing jobs with status filter: {status}")

    filtered_jobs = jobs.values()

    if status:
        filtered_jobs = [job for job in filtered_jobs if job["status"] == status]

    # Sort by creation time (newest first) and apply limit
    sorted_jobs = sorted(
        filtered_jobs, key=lambda job: job["created_at"], reverse=True
    )[:limit]

    return [JobStatusResponse(**job) for job in sorted_jobs]


@app.get("/buckets", response_model=List[Dict[str, Any]])
async def list_buckets(
    storage_service: StorageService = Depends(get_storage_service),
):
    """
    List all available buckets in the project
    """
    logger.info("Listing available buckets")

    try:
        buckets = storage_service.list_buckets()
        return buckets

    except Exception as e:
        logger.error(f"Error listing buckets: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error listing buckets: {str(e)}")


def main():
    """Run the FastAPI application"""
    # Get configuration from environment variables
    host = os.environ.get("HOST", "0.0.0.0")
    port = int(os.environ.get("PORT", "8080"))

    # Start the server - use direct app reference for direct script execution
    uvicorn.run(app, host=host, port=port, reload=False)


if __name__ == "__main__":
    main()
