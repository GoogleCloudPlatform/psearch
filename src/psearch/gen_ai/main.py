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

import inspect
import json
import logging
import os
import re
import threading
from typing import Any, Callable, Dict, List, Optional, Tuple
import urllib.parse
import uuid

from fastapi import (
    APIRouter,
    BackgroundTasks,
    Body,
    Depends,
    FastAPI,
    HTTPException,
    Request,
)
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field, ValidationInfo, field_validator

from .auth import require_auth
from .rate_limit import (
    enforce_paid_inference_rate_limit,
    enforce_sql_task_rate_limit,
    enforce_status_poll_rate_limit,
    reset_limiters,
    sql_task_concurrency_limiter,
)
from .services.conversational_search_service import ConversationalSearchService
from .services.enrichment_service import EnrichmentService
from .services.image_fetcher import (
    ImageTooLargeError,
    ImageValidationError,
    decode_bounded_base64_image,
    get_max_image_bytes,
    max_base64_chars_for_bytes,
)
from .services.imagen_service import ImageGenerationService
from .services.marketing_service import MarketingService
from .services.sql.common.input_validation import (
    InputValidationError,
    MAX_GENERATED_SQL_CHARS,
    sanitize_data_sample_json,
    validate_critical_fields,
    validate_destination_schema,
    validate_source_schema_fields,
    validate_table_id,
)
from .services.sql.common.schema_utils import SchemaLoader
from .services.sql.pipeline.transformation_pipeline import TransformationPipeline
from .services.sql_fix_service import SQLFixService
from .tasks import task_manager

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Lazy per-(service, project_id, location) cache so importing gen_ai.main never
# instantiates Vertex AI or BigQuery SDK clients at module import time.
_service_cache: Dict[Tuple[str, str, str], Any] = {}
_service_cache_lock = threading.Lock()

_SAFE_ENRICHMENT_ERRORS = {"Failed to parse response"}
_SAFE_IMAGE_ERRORS = {
    "Invalid base64 image format",
    "Image exceeds maximum allowed size",
    "Gemini response did not contain valid image data",
}
_BOUNDED_DATA_URI_HEADER_RE = re.compile(
    r"^data:[^,]{1,100};base64,",
    flags=re.IGNORECASE,
)
_SAFE_SQL_GENAI_ERROR_PREFIXES = (
    "No candidates in the GenAI response.",
    "Output truncated due to MAX_TOKENS limit.",
    "No SQL returned",
    "SQL Fix: No function call and text response does not appear to be SQL.",
    "Failed to obtain fixed SQL. Finish reason:",
    "Final fixed SQL content after programmatic fixes does not appear to be a valid SQL query:",
    "Simple SQL fix: No text response received. Finish reason:",
    "Simple SQL fix: Could not extract valid SQL from GenAI response.",
    "Final 'simple fixed' SQL content after programmatic fixes does not appear to be a valid SQL query:",
    "Failed to parse function call arguments for diff analysis.",
    "Model returned text instead of expected function call for diff analysis.",
    "No function call or text response for SQL diff analysis. Finish reason:",
)


def clear_service_cache() -> None:
    """Clear cached lazy service instances in a thread-safe manner."""
    with _service_cache_lock:
        _service_cache.clear()


def _get_project_and_location() -> Tuple[str, str]:
    project_id = (os.environ.get("PROJECT_ID") or "").strip()
    if not project_id:
        logger.error("PROJECT_ID environment variable is not set")
        raise HTTPException(
            status_code=500,
            detail="Internal server configuration error",
        )
    # Gemini 3.x models are served from the Vertex AI "global" endpoint by default.
    location = (os.environ.get("GENAI_LOCATION") or "global").strip() or "global"
    return project_id, location


def _get_or_create_service(
    service_key: str,
    builder: Callable[[str, str], Any],
    service_label: str,
) -> Any:
    project_id, location = _get_project_and_location()
    cache_key = (service_key, project_id, location)
    cached = _service_cache.get(cache_key)
    if cached is not None:
        return cached
    with _service_cache_lock:
        cached = _service_cache.get(cache_key)
        if cached is not None:
            return cached
        try:
            instance = builder(project_id, location)
        except HTTPException:
            raise
        except Exception:
            logger.error("Failed to initialize %s", service_label, exc_info=True)
            raise HTTPException(
                status_code=500,
                detail=f"Internal server error initializing {service_label}",
            )
        _service_cache[cache_key] = instance
        return instance


def get_conversational_search_service() -> ConversationalSearchService:
    return _get_or_create_service(
        "conversational_search",
        lambda project_id, location: ConversationalSearchService(project_id, location),
        "conversational search service",
    )


def get_enrichment_service() -> EnrichmentService:
    return _get_or_create_service(
        "enrichment",
        lambda project_id, location: EnrichmentService(project_id, location),
        "enrichment service",
    )


def get_marketing_service() -> MarketingService:
    return _get_or_create_service(
        "marketing",
        lambda project_id, location: MarketingService(project_id, location),
        "marketing service",
    )


def get_image_generation_service() -> ImageGenerationService:
    return _get_or_create_service(
        "image_generation",
        lambda project_id, location: ImageGenerationService(project_id, location),
        "image generation service",
    )


def get_sql_fix_service() -> SQLFixService:
    return _get_or_create_service(
        "sql_fix",
        lambda project_id, location: SQLFixService(project_id, location),
        "SQL fix service",
    )


def get_transformation_pipeline() -> TransformationPipeline:
    return _get_or_create_service(
        "transformation_pipeline",
        lambda project_id, location: TransformationPipeline(
            project_id=project_id, location=location
        ),
        "SQL transformation pipeline",
    )


def _validate_task_id(task_id: str) -> str:
    """Validate that task_id is a canonical hyphenated UUIDv4 string."""
    if not isinstance(task_id, str):
        raise HTTPException(status_code=400, detail="Invalid task_id format")
    try:
        parsed = uuid.UUID(task_id)
    except (ValueError, AttributeError, TypeError) as exc:
        raise HTTPException(status_code=400, detail="Invalid task_id format") from exc
    if parsed.version != 4 or str(parsed) != task_id.lower():
        raise HTTPException(status_code=400, detail="Invalid task_id format")
    return str(parsed)


def _enforce_enhanced_image_size_limit(image_base64: str) -> None:
    """Reject oversized base64 image payloads with HTTP 413 before allocating decoded bytes."""
    try:
        max_bytes = get_max_image_bytes()
    except ValueError as exc:
        logger.error("Invalid GEN_AI_MAX_IMAGE_BYTES configuration: %s", exc)
        raise HTTPException(
            status_code=500,
            detail="Internal server configuration error",
        ) from exc

    max_encoded_chars = max_base64_chars_for_bytes(max_bytes)
    stripped = image_base64.strip()
    if stripped[:5].lower() == "data:":
        header_match = _BOUNDED_DATA_URI_HEADER_RE.match(stripped[:128])
        if header_match is not None:
            stripped_payload = stripped[header_match.end() :]
        elif len(stripped) > max_encoded_chars + 128:
            raise HTTPException(
                status_code=413,
                detail="Image exceeds maximum allowed size",
            )
        else:
            stripped_payload = stripped
    else:
        stripped_payload = stripped

    if len(stripped_payload) > max_encoded_chars:
        raise HTTPException(
            status_code=413,
            detail="Image exceeds maximum allowed size",
        )

    if len(stripped_payload) == max_encoded_chars and max_bytes % 3 != 0:
        try:
            decode_bounded_base64_image(image_base64, max_bytes=max_bytes)
        except ImageTooLargeError as exc:
            raise HTTPException(
                status_code=413,
                detail="Image exceeds maximum allowed size",
            ) from exc
        except ImageValidationError:
            pass


def _sanitize_enrichment_result(result: Dict[str, Any]) -> Dict[str, Any]:
    """Scrub raw downstream SDK str(e) strings from enrichment response dicts."""
    sanitized = dict(result)
    if "error" in sanitized and sanitized["error"] is not None:
        if sanitized["error"] not in _SAFE_ENRICHMENT_ERRORS:
            logger.warning("Sanitizing downstream enrichment error: %s", sanitized["error"])
            sanitized["error"] = "Error generating enriched content"

    enriched_fields = sanitized.get("enriched_fields")
    if isinstance(enriched_fields, dict) and enriched_fields.get("error") is not None:
        clean_fields = dict(enriched_fields)
        if clean_fields["error"] not in _SAFE_ENRICHMENT_ERRORS:
            logger.warning(
                "Sanitizing downstream enrichment field error: %s",
                clean_fields["error"],
            )
            clean_fields["error"] = "Error generating enriched content"
        sanitized["enriched_fields"] = clean_fields
    return sanitized


def _sanitize_sql_genai_error(
    error_msg: Optional[str],
    fallback: str,
) -> Optional[str]:
    """Allowlist known model-level SQL feedback and scrub all SDK exception strings."""
    if error_msg is None:
        return None
    if not isinstance(error_msg, str):
        return fallback
    unwrapped = error_msg.removeprefix("SQLFixer component failed: ").strip()
    if not unwrapped.startswith(_SAFE_SQL_GENAI_ERROR_PREFIXES):
        return fallback
    return re.sub(
        r"\b[a-zA-Z][a-zA-Z0-9+.-]*://\S+",
        "[redacted-url]",
        error_msg,
    )


def _sanitize_bq_validation_message(raw: str) -> str:
    """Strip REST transport prefixes and URLs from BigQuery validation messages."""
    cleaned = raw.strip()
    cleaned = re.sub(
        r"^(?:\d{3}\s+)?(?:GET|POST|PUT|DELETE|PATCH)\s+https?://\S+?:\s*",
        "",
        cleaned,
        flags=re.IGNORECASE,
    )
    cleaned = re.sub(r"^\d{3}\s+", "", cleaned)
    cleaned = re.sub(r"https?://\S+", "[redacted-url]", cleaned)
    cleaned = re.sub(
        r"\b[a-zA-Z][a-zA-Z0-9+.-]*://\S+",
        "[redacted-url]",
        cleaned,
    )
    return cleaned.strip() or "SQL validation failed."


def _sanitize_sql_validation_result(
    validation_result: Dict[str, Any],
) -> Dict[str, Any]:
    """Sanitize SQLValidator dry-run results, raising 500 on unexpected non-BadRequest errors."""
    if not isinstance(validation_result, dict):
        raise HTTPException(
            status_code=500,
            detail="Internal server error while validating SQL",
        )

    msg = validation_result.get("message")
    details = validation_result.get("details")
    if (
        isinstance(msg, str)
        and msg.startswith("An unexpected error occurred during SQL validation:")
    ) or (isinstance(details, dict) and "error_type" in details):
        logger.error(
            "Unexpected downstream error during SQL validation: %s",
            validation_result.get("error_message"),
        )
        raise HTTPException(
            status_code=500,
            detail="Internal server error while validating SQL",
        )

    sanitized = dict(validation_result)
    if isinstance(sanitized.get("error_message"), str) and sanitized["error_message"]:
        sanitized["error_message"] = _sanitize_bq_validation_message(
            sanitized["error_message"]
        )
    if isinstance(sanitized.get("error"), str) and sanitized["error"]:
        sanitized["error"] = _sanitize_bq_validation_message(sanitized["error"])
    if isinstance(details, dict):
        clean_details = dict(details)
        if isinstance(clean_details.get("raw_error"), str) and clean_details["raw_error"]:
            clean_details["raw_error"] = _sanitize_bq_validation_message(
                clean_details["raw_error"]
            )
        sanitized["details"] = clean_details
    return sanitized


# Define request models
class ConversationalSearchRequest(BaseModel):
    query: str
    conversation_history: Optional[List[Dict[str, str]]] = None
    product_context: Optional[Dict[str, Any]] = None
    max_results: Optional[int] = 5


class EnrichmentRequest(BaseModel):
    product_id: str
    product_data: Dict[str, Any]
    fields_to_enrich: Optional[List[str]] = None


class MarketingRequest(BaseModel):
    product_id: str
    product_data: Dict[str, Any]
    content_type: str
    tone: Optional[str] = "professional"
    target_audience: Optional[str] = None
    max_length: Optional[int] = 500


class ImagenRequest(BaseModel):
    product_id: str
    product_data: Dict[str, Any]
    prompt: Optional[str] = None
    image_type: Optional[str] = "lifestyle"
    style: Optional[str] = "photorealistic"


class EnhancedImageRequest(BaseModel):
    product_id: str
    product_data: Dict[str, Any]
    image_base64: str
    background_prompt: str
    person_description: Optional[str] = None
    style: Optional[str] = "photorealistic"


class SQLGenerationRequest(BaseModel):
    """Request body for POST /generate-sql.

    Every field here is attacker-controlled and ends up in an LLM prompt and in
    BigQuery API calls, so each one is validated against a strict allow-list
    before the request is accepted. See
    ``services/sql/common/input_validation.py`` for the grammars and rationale.
    """

    source_table: str = Field(
        ...,
        description="The source BigQuery table ID (e.g., project.dataset.table)",
        json_schema_extra={"example": "psearch-dev-ze.raw_data.product_catalog"},
    )
    destination_table: str = Field(
        ...,
        description="The destination BigQuery table ID (e.g., project.dataset.table)",
        json_schema_extra={"example": "psearch-dev-ze.processed_data.products"},
    )
    destination_schema: Optional[Dict[str, Any]] = Field(
        None,
        description=(
            "Optional JSON schema definition for the destination table structure. "
            "If not provided, the system will use the default schema.json file."
        ),
    )
    source_schema_fields: List[str] = Field(
        ...,
        description="A list of field names available in the source table.",
        json_schema_extra={"example": ["id", "title", "description", "price"]},
    )
    source_data_sample_json: Optional[str] = Field(
        None,
        description="Optional JSON string of source data sample for semantic enhancement.",
        json_schema_extra={
            "example": '[{"product_id": "123", "name": "Sample Product"}]'
        },
    )
    critical_fields_to_refine: Optional[List[str]] = Field(
        None,
        description="Optional list of critical fields for semantic refinement.",
        json_schema_extra={"example": ["name", "priceInfo.price"]},
    )

    @field_validator("source_table", "destination_table")
    @classmethod
    def _check_table_id(cls, value: str, info: ValidationInfo) -> str:
        return validate_table_id(value, info.field_name)

    @field_validator("source_schema_fields")
    @classmethod
    def _check_source_schema_fields(cls, value: List[str]) -> List[str]:
        return validate_source_schema_fields(value)

    @field_validator("critical_fields_to_refine")
    @classmethod
    def _check_critical_fields(
        cls, value: Optional[List[str]]
    ) -> Optional[List[str]]:
        return validate_critical_fields(value) or None

    @field_validator("destination_schema")
    @classmethod
    def _check_destination_schema(
        cls, value: Optional[Dict[str, Any]]
    ) -> Optional[Dict[str, Any]]:
        if value is None:
            return None
        return validate_destination_schema(value)

    @field_validator("source_data_sample_json")
    @classmethod
    def _check_data_sample(cls, value: Optional[str]) -> Optional[str]:
        return sanitize_data_sample_json(value)

    model_config = {
        "json_schema_extra": {
            "example": {
                "source_table": "psearch-dev-ze.raw_data.product_catalog",
                "destination_table": "psearch-dev-ze.processed_data.products",
                "source_schema_fields": [
                    "product_id",
                    "product_title",
                    "category",
                    "vendor_price",
                ],
                "destination_schema": {
                    "fields": [
                        {"name": "id", "type": "STRING", "mode": "REQUIRED"},
                        {"name": "name", "type": "STRING", "mode": "REQUIRED"},
                        {"name": "description", "type": "STRING", "mode": "NULLABLE"},
                    ]
                },
            }
        }
    }


class SQLValidationRequest(BaseModel):
    sql_script: str = Field(
        ...,
        max_length=MAX_GENERATED_SQL_CHARS,
        description="SQL script to validate",
        json_schema_extra={
            "example": (
                "CREATE OR REPLACE TABLE `project.dataset.table` "
                "AS SELECT * FROM `source.table`"
            )
        },
    )
    timeout_seconds: Optional[int] = Field(
        30,
        description="Timeout in seconds for validation",
        json_schema_extra={"example": 30},
        gt=0,
        le=300,
    )

    model_config = {
        "json_schema_extra": {
            "example": {
                "sql_script": (
                    "CREATE OR REPLACE TABLE `psearch-dev-ze.processed_data.products` AS\n"
                    "SELECT id, name, description FROM `psearch-dev-ze.raw_data.catalog`;"
                ),
                "timeout_seconds": 30,
            }
        }
    }


class SQLFixRequest(BaseModel):
    original_sql: str = Field(
        ...,
        max_length=MAX_GENERATED_SQL_CHARS,
        description="Original SQL that started the fix process",
        json_schema_extra={
            "example": (
                "CREATE OR REPLACE TABLE `project.dataset.table` "
                "AS SELECT * FROM `source.table`"
            )
        },
    )
    current_sql: str = Field(
        ...,
        max_length=MAX_GENERATED_SQL_CHARS,
        description="Current SQL version (might be a previous fix attempt)",
        json_schema_extra={
            "example": (
                "CREATE OR REPLACE TABLE `project.dataset.table` "
                "AS SELECT id, colorFamily FROM `source.table`"
            )
        },
    )
    error_message: str = Field(
        ...,
        max_length=MAX_GENERATED_SQL_CHARS,
        description="Error message from failed validation",
        json_schema_extra={"example": "Invalid field reference 'colorFamily'"},
    )
    attempt_number: Optional[int] = Field(
        1,
        description="Current attempt number for tracking multiple fix attempts",
        json_schema_extra={"example": 1},
        ge=1,
    )

    model_config = {
        "json_schema_extra": {
            "example": {
                "original_sql": (
                    "CREATE OR REPLACE TABLE `products` AS\n"
                    "SELECT id, colorFamily FROM `raw_catalog`;"
                ),
                "current_sql": (
                    "CREATE OR REPLACE TABLE `products` AS\n"
                    "SELECT id, colorFamily FROM `raw_catalog`;"
                ),
                "error_message": "Invalid field reference 'colorFamily'",
                "attempt_number": 1,
            }
        }
    }


class SQLFixResponse(BaseModel):
    success: bool = Field(
        ...,
        description="Whether the fix generation was successful",
        json_schema_extra={"example": True},
    )
    suggested_sql: Optional[str] = Field(
        None,
        description="The suggested fixed SQL script",
        json_schema_extra={
            "example": (
                "CREATE OR REPLACE TABLE `project.dataset.table` "
                "AS SELECT id, NULL AS colorFamily FROM `source.table`"
            )
        },
    )
    diff: Optional[str] = Field(
        None,
        description="Unified diff showing the changes between current and suggested SQL",
        json_schema_extra={
            "example": (
                "--- current.sql\n+++ suggested.sql\n@@ -1,5 +1,5 @@\n"
                "CREATE OR REPLACE TABLE...\n-colorFamily\n+NULL AS colorFamily"
            )
        },
    )
    error: Optional[str] = Field(
        None,
        description="Error message if fix generation failed",
        json_schema_extra={"example": "Failed to generate SQL fix: Model error"},
    )

    model_config = {
        "json_schema_extra": {
            "example": {
                "success": True,
                "suggested_sql": (
                    "CREATE OR REPLACE TABLE `products` AS\n"
                    "SELECT id, NULL AS colorFamily FROM `raw_catalog`;"
                ),
                "diff": (
                    "--- current.sql\n+++ suggested.sql\n@@ -1,5 +1,5 @@\n"
                    "CREATE OR REPLACE TABLE `products` AS\nSELECT id, \n"
                    "-colorFamily \n+NULL AS colorFamily \nFROM `raw_catalog`;"
                ),
            }
        }
    }


class SQLSimpleFixRequest(BaseModel):
    sql_script: str = Field(
        ...,
        max_length=MAX_GENERATED_SQL_CHARS,
        description="The SQL script to fix.",
    )
    error_message: str = Field(
        ...,
        max_length=MAX_GENERATED_SQL_CHARS,
        description="The BigQuery error message for the script.",
    )

    model_config = {
        "json_schema_extra": {
            "example": {
                "sql_script": "SELECT name FROM `my_table` WHER id = 1;",
                "error_message": (
                    "Syntax error: Expected end of input but got identifier WHER at [1:27]"
                ),
            }
        }
    }


protected_router = APIRouter(dependencies=[Depends(require_auth)])


async def root() -> Dict[str, str]:
    return {"message": "Gen AI Service is running"}


async def health_check() -> Dict[str, str]:
    return {"status": "healthy"}


@protected_router.post(
    "/conversational-search",
    tags=["Search"],
    dependencies=[Depends(enforce_paid_inference_rate_limit)],
)
async def conversational_search(
    request: ConversationalSearchRequest,
    conversational_search_service: ConversationalSearchService = Depends(
        get_conversational_search_service
    ),
):
    """Process a conversational search query and return relevant results."""
    logger.info(f"Received conversational search request: {request.query[:50]}...")
    logger.info(f"Context: {request.product_context}")
    logger.info(f"Conversation history: {request.conversation_history}")

    try:
        result = conversational_search_service.process_query(
            query=request.query,
            conversation_history=request.conversation_history,
            product_context=request.product_context,
            max_results=request.max_results,
        )

        response = {
            "answer": result.answer,
            "suggested_products": result.suggested_products,
            "follow_up_questions": result.follow_up_questions,
        }

        logger.info(
            f"Conversational search response: Answer: {(response['answer'] or '')[:100]}..."
        )
        logger.info(f"Follow-up questions: {response['follow_up_questions']}")
        logger.info(f"Suggested products count: {len(response['suggested_products'])}")

        if response["suggested_products"]:
            for i, product in enumerate(response["suggested_products"]):
                logger.info(
                    f"Suggestion {i+1}: {product.get('id', 'unknown')} - "
                    f"Reason: {(product.get('reason') or 'not provided')[:100]}..."
                )

        return response

    except HTTPException:
        raise
    except Exception:
        logger.error("Error processing conversational search", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail="Internal server error while processing conversational search",
        )


@protected_router.post(
    "/enrichment",
    tags=["Enrichment"],
    dependencies=[Depends(enforce_paid_inference_rate_limit)],
)
async def enrichment(
    request: EnrichmentRequest,
    enrichment_service: EnrichmentService = Depends(get_enrichment_service),
):
    """Enrich product data with AI-generated content."""
    logger.info(f"Received enrichment request for product: {request.product_id}")
    logger.info(f"Fields to enrich: {request.fields_to_enrich}")
    images = request.product_data.get("images")
    if isinstance(images, list):
        logger.info(f"Product has {len(images)} images")

    try:
        result = enrichment_service.process_enrichment(
            product_id=request.product_id,
            product_data=request.product_data,
            fields_to_enrich=request.fields_to_enrich,
        )

        logger.info(f"Enrichment completed for product: {request.product_id}")
        if "enriched_fields" in result:
            for field, content in result["enriched_fields"].items():
                if isinstance(content, str):
                    logger.info(
                        f"Enriched field '{field}': "
                        f"{content[:100] if len(content) > 100 else content}"
                    )
                elif isinstance(content, dict):
                    try:
                        content_str = json.dumps(content)
                        logger.info(
                            f"Enriched field '{field}': "
                            f"{content_str[:200] if len(content_str) > 200 else content_str}"
                        )
                    except (TypeError, ValueError):
                        logger.info(f"Enriched field '{field}': {str(content)}")
                else:
                    logger.info(f"Enriched field '{field}': (type: {type(content)})")
        if "error" in result:
            logger.warning(f"Enrichment error: {result['error']}")

        if isinstance(result, dict):
            result = _sanitize_enrichment_result(result)
        return result

    except HTTPException:
        raise
    except Exception:
        logger.error("Error processing enrichment", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail="Internal server error while processing product enrichment",
        )


@protected_router.post(
    "/marketing",
    tags=["Marketing"],
    dependencies=[Depends(enforce_paid_inference_rate_limit)],
)
async def marketing(
    request: MarketingRequest,
    marketing_service: MarketingService = Depends(get_marketing_service),
):
    """Generate marketing content for a product."""
    logger.info(f"Received marketing request for product: {request.product_id}")
    logger.info(f"Content type: {request.content_type}, Tone: {request.tone}")
    logger.info(
        f"Target audience: {request.target_audience}, Max length: {request.max_length}"
    )
    images = request.product_data.get("images")
    if isinstance(images, list):
        logger.info(f"Product has {len(images)} images")

    try:
        result = marketing_service.generate_content(
            product_id=request.product_id,
            product_data=request.product_data,
            content_type=request.content_type,
            tone=request.tone,
            target_audience=request.target_audience,
            max_length=request.max_length,
        )

        if isinstance(result, dict):
            content_val = result.get("content")
            if isinstance(content_val, str) and content_val.startswith(
                "Error generating marketing content:"
            ):
                logger.error(
                    "Downstream marketing content generation failed: %s", content_val
                )
                raise HTTPException(
                    status_code=500,
                    detail="Internal server error while generating marketing content",
                )
            if result.get("error"):
                logger.error("Downstream marketing error: %s", result["error"])
                raise HTTPException(
                    status_code=500,
                    detail="Internal server error while generating marketing content",
                )

        logger.info(f"Marketing content generated for product: {request.product_id}")
        if result.get("content"):
            logger.info(
                f"Generated content (first 100 chars): {result['content'][:100]}..."
            )

        return result

    except HTTPException:
        raise
    except Exception:
        logger.error("Error generating marketing content", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail="Internal server error while generating marketing content",
        )


@protected_router.post(
    "/generate-enhanced-image",
    tags=["Images"],
    dependencies=[Depends(enforce_paid_inference_rate_limit)],
)
async def generate_enhanced_image(
    request: EnhancedImageRequest,
    image_generation_service: ImageGenerationService = Depends(
        get_image_generation_service
    ),
):
    """Generate an enhanced image using Gemini, changing background or adding a person."""
    logger.info(f"Received enhanced image request for product: {request.product_id}")
    logger.info(f"Style: {request.style}")
    if request.person_description:
        logger.info(
            f"Person description provided: {request.person_description[:50]}..."
        )
    else:
        logger.info(f"Background prompt: {request.background_prompt[:50]}...")

    try:
        _enforce_enhanced_image_size_limit(request.image_base64)
        result = image_generation_service.generate_image(
            product_id=request.product_id,
            product_data=request.product_data,
            image_base64=request.image_base64,
            background_prompt=request.background_prompt,
            person_description=request.person_description,
            style=request.style,
        )

        if (
            isinstance(result, dict)
            and result.get("error") == "Image exceeds maximum allowed size"
        ):
            raise HTTPException(
                status_code=413,
                detail="Image exceeds maximum allowed size",
            )

        logger.info(
            f"Enhanced image generation completed for product: {request.product_id}"
        )
        if isinstance(result, dict) and "generated_image_base64" in result:
            logger.info("Generated image successfully (base64 data).")
        elif isinstance(result, dict) and result.get("error") is not None:
            logger.warning(f"Image generation error: {result['error']}")
            if result["error"] not in _SAFE_IMAGE_ERRORS:
                result = {
                    **result,
                    "error": "Error generating enhanced image",
                }
        else:
            logger.warning("Image generation result format unexpected.")

        return result

    except HTTPException:
        raise
    except Exception:
        logger.error("Error generating enhanced image", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail="Internal server error while generating enhanced image",
        )


@protected_router.post(
    "/generate-sql",
    tags=["SQL"],
    summary="Generate SQL transformation script",
    description="Initiates a background task to generate a SQL transformation script.",
    response_model=Dict[str, str],
    dependencies=[
        Depends(enforce_paid_inference_rate_limit),
        Depends(enforce_sql_task_rate_limit),
    ],
)
async def generate_sql_task(
    http_request: Request,
    request: SQLGenerationRequest,
    background_tasks: BackgroundTasks,
):
    """Initiates a SQL transformation script generation task."""
    logger.info(
        f"Received request to generate SQL task: "
        f"{request.source_table} -> {request.destination_table}"
    )

    sql_task_concurrency_limiter.acquire_or_reject()
    slot_transferred = False
    task_id: Optional[str] = None
    try:
        pipeline_provider = http_request.app.dependency_overrides.get(
            get_transformation_pipeline, get_transformation_pipeline
        )
        pipeline = (
            await pipeline_provider()
            if inspect.iscoroutinefunction(pipeline_provider)
            else pipeline_provider()
        )

        schema_to_use = (
            request.destination_schema or SchemaLoader.get_destination_schema()
        )
        if not schema_to_use:
            raise HTTPException(
                status_code=400,
                detail="No destination schema provided or loaded. Cannot initiate task.",
            )

        task_id = str(uuid.uuid4())
        input_details = {
            "source_table_name": request.source_table,
            "destination_table_name": request.destination_table,
            "source_schema_fields_count": len(request.source_schema_fields),
            "destination_schema_provided": bool(request.destination_schema),
            "source_data_sample_provided": bool(request.source_data_sample_json),
        }
        task_manager.init_task(
            task_id, task_type="sql_generation", input_details=input_details
        )

        runner = (
            sql_task_concurrency_limiter.run_guarded_async
            if inspect.iscoroutinefunction(pipeline.execute_pipeline)
            else sql_task_concurrency_limiter.run_guarded
        )
        background_tasks.add_task(
            runner,
            pipeline.execute_pipeline,
            task_id=task_id,
            source_table_name=request.source_table,
            destination_table_name=request.destination_table,
            source_schema_fields=request.source_schema_fields,
            destination_schema=schema_to_use,
            source_data_sample_json=request.source_data_sample_json,
            critical_fields_for_semantic_refinement=request.critical_fields_to_refine,
        )
        slot_transferred = True

        return {
            "task_id": task_id,
            "message": (
                "SQL generation task started successfully. "
                "Poll /sql/task-status/{task_id} for updates."
            ),
        }

    except HTTPException:
        raise
    except InputValidationError as exc:
        logger.warning(f"InputValidationError in generate_sql_task: {exc}")
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception:
        logger.error("Error initiating SQL generation task", exc_info=True)
        if task_id is not None:
            try:
                if task_manager.get_task_status(task_id):
                    task_manager.update_task_status(
                        task_id,
                        status="failed",
                        error="Internal server error while initiating SQL generation task",
                    )
            except Exception:
                logger.error("Failed to mark SQL task as failed", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail="Internal server error while initiating SQL generation task",
        )
    finally:
        if not slot_transferred:
            sql_task_concurrency_limiter.release()


@protected_router.get(
    "/sql/task-status/{task_id}",
    tags=["SQL"],
    summary="Get SQL generation task status",
    response_model=Optional[Dict[str, Any]],
    dependencies=[Depends(enforce_status_poll_rate_limit)],
)
async def get_sql_task_status(task_id: str):
    """Retrieves the status, logs, and result of a SQL generation task."""
    normalized_task_id = _validate_task_id(task_id)
    logger.info(f"Request received for task status: {normalized_task_id}")

    try:
        status_info = task_manager.get_task_status(normalized_task_id)
        if not status_info:
            logger.warning(f"Task ID {normalized_task_id} not found.")
            raise HTTPException(
                status_code=404, detail=f"Task ID {normalized_task_id} not found."
            )
        return status_info
    except HTTPException:
        raise
    except Exception:
        logger.error("Error retrieving SQL task status", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail="Internal server error while retrieving SQL task status",
        )


@protected_router.get(
    "/sql/tasks-summary",
    tags=["SQL"],
    summary="Get summary of all SQL generation tasks",
    dependencies=[Depends(enforce_status_poll_rate_limit)],
)
async def get_all_sql_tasks_summary():
    """Retrieves a summary list of all tasks managed by the task manager."""
    logger.info("Request received for all tasks summary.")
    try:
        return task_manager.get_all_tasks_summary()
    except HTTPException:
        raise
    except Exception:
        logger.error("Error retrieving SQL tasks summary", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail="Internal server error while retrieving SQL tasks summary",
        )


@protected_router.post(
    "/sql/simple-fix",
    tags=["SQL"],
    summary="Attempt a simple AI fix for an SQL script",
    description=(
        "Uses a simplified prompt to attempt a direct AI fix for a given SQL "
        "script and its error message."
    ),
    response_model=Dict[str, Optional[str]],
    dependencies=[Depends(enforce_paid_inference_rate_limit)],
)
async def simple_sql_fix(
    request: SQLSimpleFixRequest,
    sql_fix_service: SQLFixService = Depends(get_sql_fix_service),
):
    logger.info(
        f"Received simple SQL fix request for error: {request.error_message[:100]}..."
    )
    try:
        fixed_sql, error_msg = sql_fix_service.fixer.simple_fix_sql(
            sql_script_to_fix=request.sql_script,
            error_message=request.error_message,
        )

        if error_msg:
            logger.error(f"Simple SQL fix attempt failed: {error_msg}")
            sanitized_err = _sanitize_sql_genai_error(
                error_msg,
                "Failed to generate SQL fix due to an internal AI service error.",
            )
            return {"suggested_fixed_sql": None, "error": sanitized_err}

        return {"suggested_fixed_sql": fixed_sql, "error": None}

    except HTTPException:
        raise
    except Exception:
        logger.error("Error in simple_sql_fix endpoint", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail="Internal server error while performing simple SQL fix",
        )


@protected_router.post(
    "/sql/validate",
    tags=["SQL"],
    summary="Validate SQL script",
    description="Performs a dry run validation of a SQL script without executing it",
    response_model=Dict[str, Any],
    dependencies=[Depends(enforce_paid_inference_rate_limit)],
)
async def validate_sql(
    request: SQLValidationRequest,
    sql_fix_service: SQLFixService = Depends(get_sql_fix_service),
):
    """Validate a SQL script with a BigQuery dry run."""
    logger.info(f"Validating SQL script ({len(request.sql_script)} chars)")

    try:
        validation_result = sql_fix_service.validate_sql(
            request.sql_script,
            request.timeout_seconds,
        )
        return _sanitize_sql_validation_result(validation_result)

    except HTTPException:
        raise
    except Exception:
        logger.error("Error validating SQL", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail="Internal server error while validating SQL",
        )


@protected_router.post(
    "/sql/fix",
    tags=["SQL"],
    summary="Generate SQL fix",
    description="Generates a fixed SQL script using AI based on error message",
    response_model=SQLFixResponse,
    dependencies=[Depends(enforce_paid_inference_rate_limit)],
)
async def fix_sql(
    request: SQLFixRequest,
    sql_fix_service: SQLFixService = Depends(get_sql_fix_service),
):
    """Generate a fixed SQL script based on an error message."""
    logger.info(f"Generating SQL fix for error: {request.error_message[:100]}...")

    try:
        fix_result_dict = sql_fix_service.generate_sql_fix(
            request.original_sql,
            request.current_sql,
            request.error_message,
        )

        diff_text = None
        if fix_result_dict.get("analysis") and isinstance(
            fix_result_dict["analysis"], dict
        ):
            diff_text = fix_result_dict["analysis"].get("diff_text")

        sanitized_err = _sanitize_sql_genai_error(
            fix_result_dict.get("error"),
            "Failed to generate SQL fix due to an internal AI service error.",
        )

        return SQLFixResponse(
            success=fix_result_dict.get("success", False),
            suggested_sql=fix_result_dict.get("suggested_sql"),
            diff=diff_text,
            error=sanitized_err,
        )

    except HTTPException:
        raise
    except Exception:
        logger.error("Error generating SQL fix", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail="Internal server error while generating SQL fix",
        )


@protected_router.post(
    "/sql/analyze",
    tags=["SQL"],
    summary="Analyze SQL differences",
    description="Analyzes and explains differences between original and fixed SQL",
    response_model=Dict[str, Any],
    dependencies=[Depends(enforce_paid_inference_rate_limit)],
)
async def analyze_sql_diff(
    original_sql: str = Body(
        ...,
        max_length=MAX_GENERATED_SQL_CHARS,
        description="Original SQL with errors",
        examples=[
            "CREATE OR REPLACE TABLE `products` AS\nSELECT id, colorFamily FROM `raw_catalog`;"
        ],
    ),
    fixed_sql: str = Body(
        ...,
        max_length=MAX_GENERATED_SQL_CHARS,
        description="Fixed SQL script",
        examples=[
            "CREATE OR REPLACE TABLE `products` AS\nSELECT id, NULL AS colorFamily FROM `raw_catalog`;"
        ],
    ),
    sql_fix_service: SQLFixService = Depends(get_sql_fix_service),
):
    """Analyze and explain the differences between original and fixed SQL."""
    logger.info("Analyzing SQL differences")

    try:
        analysis = sql_fix_service.diff_analyzer.analyze_sql_differences(
            original_sql, fixed_sql
        )
        if isinstance(analysis, dict) and analysis.get("error") is not None:
            analysis = {
                **analysis,
                "error": _sanitize_sql_genai_error(
                    analysis["error"],
                    "Failed to analyze SQL differences due to an internal AI service error.",
                ),
            }
        return analysis

    except HTTPException:
        raise
    except Exception:
        logger.error("Error analyzing SQL differences", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail="Internal server error while analyzing SQL differences",
        )


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
    """Create and configure the Gen AI FastAPI application instance."""
    reset_limiters()
    application = FastAPI(
        title="Gen AI Service",
        description=(
            "API for AI-powered product enhancements with conversational search, "
            "content generation, and SQL transformation capabilities"
        ),
        version="1.0.0",
        docs_url=None,
        redoc_url=None,
        openapi_url=None,
    )

    origins = _sanitize_allowed_origins(os.environ.get("ALLOWED_ORIGINS", ""))

    @application.middleware("http")
    async def log_requests(request: Request, call_next):
        """Log requests and responses."""
        logger.info(f"Request: {request.method} {request.url.path}")
        response = await call_next(request)
        logger.info(f"Response: {response.status_code}")
        return response

    application.add_middleware(
        CORSMiddleware,
        allow_origins=origins,
        allow_credentials=bool(origins),
        allow_methods=["GET", "POST", "OPTIONS"],
        allow_headers=["Authorization", "Content-Type", "X-Admin-Token"],
    )

    application.get("/", tags=["General"])(root)
    application.get("/health", tags=["General"])(health_check)
    application.include_router(protected_router)
    return application


app = create_app()


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8080)
