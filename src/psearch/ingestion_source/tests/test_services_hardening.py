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

"""Unit tests for hardened Storage, SchemaDetection, Dataset, and BigQuery services."""

import asyncio
from datetime import datetime, timezone
import io
import json
import os
import tempfile
from unittest.mock import MagicMock
import uuid

from fastapi import UploadFile
from google.cloud import bigquery
import pytest

try:
    from psearch.ingestion_source import validation
    from psearch.ingestion_source.services import (
        bigquery_service,
        dataset_service,
        schema_detection_service,
        storage_service,
    )
except ModuleNotFoundError as exc:
    if exc.name not in {
        "psearch",
        "psearch.ingestion_source",
        "psearch.ingestion_source.services",
    }:
        raise
    from ingestion_source import validation
    from ingestion_source.services import (
        bigquery_service,
        dataset_service,
        schema_detection_service,
        storage_service,
    )


class _CountingAsyncUploadFile(UploadFile):
    """UploadFile test double that tracks total bytes read without materializing upfront."""

    def __init__(self, filename: str, payload: bytes):
        super().__init__(filename=filename, file=io.BytesIO(payload))
        self._payload = payload
        self._pos = 0
        self.bytes_read = 0

    async def read(self, size: int = -1) -> bytes:
        if size is None or size < 0:
            chunk = self._payload[self._pos :]
        else:
            chunk = self._payload[self._pos : self._pos + size]
        self._pos += len(chunk)
        self.bytes_read += len(chunk)
        return chunk

    async def seek(self, offset: int) -> None:
        self._pos = offset


def test_storage_upload_aborts_mid_stream_and_cleans_temp_file_when_exceeding_limit(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
):
    mock_client = MagicMock()
    mock_bucket = MagicMock()
    mock_blob = MagicMock()
    mock_bucket.blob.return_value = mock_blob
    mock_client.lookup_bucket.return_value = mock_bucket
    mock_client.get_bucket.return_value = mock_bucket

    real_mkstemp = tempfile.mkstemp
    created_temp_files = []

    def tracking_mkstemp(*args, **kwargs):
        kwargs["dir"] = str(tmp_path)
        fd, path = real_mkstemp(*args, **kwargs)
        created_temp_files.append(path)
        return fd, path

    monkeypatch.setattr(storage_service.tempfile, "mkstemp", tracking_mkstemp)

    svc = storage_service.StorageService(
        "proj-test", client=mock_client, max_upload_size_bytes=1024
    )
    valid_file_id = str(uuid.uuid4())

    # 1. 2 KiB CSV exceeding 1 KiB limit -> aborts, never uploads, cleans temp file
    csv_header = b"id,name\n"
    csv_body = (b"1,widget_item\n" * 200)[: 2048 - len(csv_header)]
    csv_payload = csv_header + csv_body
    assert len(csv_payload) == 2048

    csv_file = _CountingAsyncUploadFile("oversized.csv", csv_payload)
    with pytest.raises(validation.UploadTooLargeError):
        asyncio.run(svc.upload_file(csv_file, valid_file_id))

    mock_blob.upload_from_filename.assert_not_called()
    assert created_temp_files, "Expected a temp file to have been opened for streaming"
    assert all(not os.path.exists(p) for p in created_temp_files)
    assert list(tmp_path.iterdir()) == []

    # 2. 2 KiB JSON exceeding 1 KiB limit -> UploadTooLargeError is NOT swallowed by JSON fallback
    created_temp_files.clear()
    items = [{"id": idx, "title": "item-" + ("x" * 40)} for idx in range(40)]
    json_payload = json.dumps(items).encode("utf-8")
    assert len(json_payload) >= 2048

    json_file = _CountingAsyncUploadFile("oversized.json", json_payload)
    with pytest.raises(validation.UploadTooLargeError):
        asyncio.run(svc.upload_file(json_file, valid_file_id))

    mock_blob.upload_from_filename.assert_not_called()
    assert all(not os.path.exists(p) for p in created_temp_files)
    assert list(tmp_path.iterdir()) == []

    # 3. 5 MiB stream aborts mid-stream well before reading the whole payload
    created_temp_files.clear()
    huge_csv = _CountingAsyncUploadFile("huge.csv", b"a,b\n" + b"1,2\n" * (1310720))
    with pytest.raises(validation.UploadTooLargeError):
        asyncio.run(svc.upload_file(huge_csv, valid_file_id))

    assert huge_csv.bytes_read <= 1024 * 1024 + 1024
    mock_blob.upload_from_filename.assert_not_called()
    assert list(tmp_path.iterdir()) == []

    # 4. Invalid file_id or filename fails BEFORE creating blobs or temp files
    mock_bucket.blob.reset_mock()
    created_temp_files.clear()
    small_csv = _CountingAsyncUploadFile("small.csv", b"a,b\n1,2\n")
    with pytest.raises(ValueError):
        asyncio.run(svc.upload_file(small_csv, "../../evil"))
    mock_bucket.blob.assert_not_called()
    assert created_temp_files == []

    # 5. Valid within-limit JSON upload converts to NDJSON and cleans temp file; GCS error also cleans temp file
    small_json = _CountingAsyncUploadFile(
        "items.json", json.dumps([{"id": 1}, {"id": 2}]).encode("utf-8")
    )
    uri = asyncio.run(svc.upload_file(small_json, valid_file_id))
    assert uri == f"gs://proj-test_psearch_raw/json/{valid_file_id}.json"
    mock_blob.upload_from_filename.assert_called_once()
    assert list(tmp_path.iterdir()) == []

    mock_blob.upload_from_filename.side_effect = RuntimeError("transient GCS error")
    valid_csv = _CountingAsyncUploadFile("small.csv", b"a,b\n1,2\n")
    with pytest.raises(RuntimeError, match="transient GCS error"):
        asyncio.run(svc.upload_file(valid_csv, valid_file_id))
    assert list(tmp_path.iterdir()) == []


def test_storage_list_buckets_scopes_to_single_ingestion_bucket():
    mock_client = MagicMock()
    mock_client.list_buckets.side_effect = AssertionError(
        "must not list all project buckets"
    )

    mock_bucket = MagicMock()
    mock_bucket.name = "proj_psearch_raw"
    mock_bucket.location = "US-CENTRAL1"
    mock_bucket.time_created = datetime(2026, 1, 15, 12, 0, 0, tzinfo=timezone.utc)
    mock_bucket.storage_class = "STANDARD"
    mock_client.lookup_bucket.return_value = mock_bucket

    svc = storage_service.StorageService("proj", client=mock_client)
    buckets = svc.list_buckets()

    mock_client.lookup_bucket.assert_called_with("proj_psearch_raw")
    assert buckets == [
        {
            "name": "proj_psearch_raw",
            "location": "US-CENTRAL1",
            "created": "2026-01-15T12:00:00+00:00",
            "storage_class": "STANDARD",
            "is_default": True,
        }
    ]

    # When lookup_bucket returns None, list_buckets returns [] without calling client.list_buckets()
    mock_client.lookup_bucket.return_value = None
    assert svc.list_buckets() == []

    # When bucket is missing on initialization, _ensure_bucket_exists enables UBLA
    create_client = MagicMock()
    create_client.lookup_bucket.return_value = None
    candidate_bucket = MagicMock()
    create_client.bucket.return_value = candidate_bucket
    storage_service.StorageService("proj-new", client=create_client)
    assert (
        candidate_bucket.iam_configuration.uniform_bucket_level_access_enabled is True
    )
    create_client.create_bucket.assert_called_once_with(
        candidate_bucket, location="us-central1"
    )


def test_storage_get_file_uri_rejects_non_uuid_and_never_lists_blobs():
    mock_client = MagicMock()
    mock_bucket = MagicMock()
    mock_bucket.list_blobs.side_effect = AssertionError("must not list blobs")
    mock_client.lookup_bucket.return_value = mock_bucket

    svc = storage_service.StorageService("proj", client=mock_client)

    with pytest.raises(ValueError):
        svc.get_file_uri("../../secret", "csv")

    mock_bucket.blob.assert_not_called()

    # Valid UUID that does not exist returns None without calling list_blobs
    missing_id = str(uuid.uuid4())
    missing_blob = MagicMock()
    missing_blob.exists.return_value = False
    mock_bucket.blob.return_value = missing_blob

    assert svc.get_file_uri(missing_id, "csv") is None
    mock_bucket.blob.assert_called_once_with(f"csv/{missing_id}.csv")
    mock_bucket.list_blobs.assert_not_called()


def test_schema_detection_csv_samples_without_reading_entire_stream():
    header = b"id,product_name,price,in_stock\n"
    row = b"42, ergonomic_mechanical_keyboard_v2 ,129.99,true\n"
    target_size = 10 * 1024 * 1024  # 10 MiB
    repeat_count = (target_size // len(row)) + 2
    payload = (header + (row * repeat_count))[:target_size]
    assert len(payload) == target_size

    upload = _CountingAsyncUploadFile("large_catalog.csv", payload)
    svc = schema_detection_service.SchemaDetectionService()

    assert svc.MAX_SCHEMA_SAMPLE_BYTES == 5 * 1024 * 1024

    detected = asyncio.run(svc.detect_schema(upload, "csv"))

    assert upload.bytes_read <= svc.MAX_SCHEMA_SAMPLE_BYTES + 4096
    assert detected["has_header"] is True
    field_names = [f["name"] for f in detected["schema_fields"]]
    assert field_names == ["id", "product_name", "price", "in_stock"]
    assert detected["row_count_estimate"] == svc.MAX_ROWS_TO_SAMPLE

    # Single-column CSV where csv.Sniffer().sniff raises csv.Error falls back cleanly to csv.excel
    single_col = _CountingAsyncUploadFile("skus.csv", b"sku\nABC123\nDEF456\n")
    single_detected = asyncio.run(svc.detect_schema(single_col, "csv"))
    assert [f["name"] for f in single_detected["schema_fields"]] == ["sku"]

    # JSON schema detection also enforces max_upload_size_bytes mid-stream
    bounded_svc = schema_detection_service.SchemaDetectionService(
        max_upload_size_bytes=1024
    )
    oversized_json = _CountingAsyncUploadFile(
        "big.json", json.dumps([{"a": "x" * 100} for _ in range(30)]).encode("utf-8")
    )
    with pytest.raises(validation.UploadTooLargeError):
        asyncio.run(bounded_svc.detect_schema(oversized_json, "json"))


def test_dataset_service_rejects_cross_project_dataset(
    monkeypatch: pytest.MonkeyPatch,
):
    monkeypatch.delenv("INGESTION_ALLOWED_DATASETS", raising=False)
    mock_bq = MagicMock()

    svc = dataset_service.DatasetService("proj-a", client=mock_bq)

    with pytest.raises(ValueError):
        asyncio.run(svc.ensure_dataset_exists("proj-b.secret"))

    mock_bq.get_dataset.assert_not_called()
    mock_bq.create_dataset.assert_not_called()

    # Same-project prefix is allowed unless blocked by INGESTION_ALLOWED_DATASETS
    monkeypatch.setenv("INGESTION_ALLOWED_DATASETS", "raw_ds")
    with pytest.raises(PermissionError):
        asyncio.run(svc.ensure_dataset_exists("proj-a.unlisted_ds"))
    mock_bq.get_dataset.assert_not_called()

    existing_ds = MagicMock(location="US")
    mock_bq.get_dataset.return_value = existing_ds
    res = asyncio.run(svc.ensure_dataset_exists("proj-a.raw_ds"))
    assert res["created"] is False
    assert res["dataset_ref"] == "proj-a.raw_ds"
    mock_bq.get_dataset.assert_called_once_with("proj-a.raw_ds")


def test_bigquery_service_defaults_to_write_empty_and_validates_disposition(
    monkeypatch: pytest.MonkeyPatch,
):
    monkeypatch.delenv("ALLOW_WRITE_TRUNCATE", raising=False)
    monkeypatch.delenv("INGESTION_ALLOWED_DATASETS", raising=False)

    mock_bq = MagicMock()
    mock_load_job = MagicMock(
        job_id="bq-load-123", errors=None, total_bytes_processed=512
    )
    mock_bq.load_table_from_uri.return_value = mock_load_job
    mock_bq.get_table.return_value = MagicMock(num_rows=10)

    svc = bigquery_service.BigQueryService("proj-a", client=mock_bq)

    assert svc.WRITE_DISPOSITION_MAP == {
        "WRITE_EMPTY": bigquery.WriteDisposition.WRITE_EMPTY,
        "WRITE_APPEND": bigquery.WriteDisposition.WRITE_APPEND,
        "WRITE_TRUNCATE": bigquery.WriteDisposition.WRITE_TRUNCATE,
    }

    jobs_dict = {"job-1": {"job_id": "job-1", "status": "RUNNING", "metadata": {}}}
    asyncio.run(
        svc.load_table_from_uri(
            job_id="job-1",
            jobs_dict=jobs_dict,
            dataset_id="raw_ds",
            table_id="items_tbl",
            uri="gs://proj-a_psearch_raw/csv/file.csv",
            source_format="CSV",
        )
    )

    mock_bq.load_table_from_uri.assert_called_once()
    _, call_kwargs = mock_bq.load_table_from_uri.call_args
    assert (
        call_kwargs["job_config"].write_disposition
        == bigquery.WriteDisposition.WRITE_EMPTY
    )
    assert jobs_dict["job-1"]["status"] == "COMPLETED"

    # Invalid disposition is rejected before starting any load job
    mock_bq.load_table_from_uri.reset_mock()
    for invalid_disp in ("__class__", "WRITE_DROP", ""):
        with pytest.raises(ValueError):
            asyncio.run(
                svc.load_table_from_uri(
                    job_id="job-1",
                    jobs_dict=jobs_dict,
                    dataset_id="raw_ds",
                    table_id="items_tbl",
                    uri="gs://proj-a_psearch_raw/csv/file.csv",
                    write_disposition=invalid_disp,
                )
            )
    mock_bq.load_table_from_uri.assert_not_called()

    # WRITE_TRUNCATE without ALLOW_WRITE_TRUNCATE=true raises PermissionError before starting load job
    with pytest.raises(PermissionError):
        asyncio.run(
            svc.load_table_from_uri(
                job_id="job-1",
                jobs_dict=jobs_dict,
                dataset_id="raw_ds",
                table_id="items_tbl",
                uri="gs://proj-a_psearch_raw/csv/file.csv",
                write_disposition="WRITE_TRUNCATE",
            )
        )
    mock_bq.load_table_from_uri.assert_not_called()

    # Identifier validation on create_dataset, create_table, and load_table_from_uri
    with pytest.raises(ValueError):
        asyncio.run(svc.create_dataset("proj-b.raw_ds"))
    with pytest.raises(ValueError):
        asyncio.run(svc.create_table("raw_ds", "bad-table", []))
    with pytest.raises(ValueError):
        asyncio.run(
            svc.load_table_from_uri(
                job_id="job-1",
                jobs_dict=jobs_dict,
                dataset_id="proj-b.raw_ds",
                table_id="items_tbl",
                uri="gs://proj-a_psearch_raw/csv/file.csv",
            )
        )
    mock_bq.load_table_from_uri.assert_not_called()
