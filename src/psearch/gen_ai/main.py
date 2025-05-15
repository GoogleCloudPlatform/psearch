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
import json
import logging
import re
from fastapi import FastAPI, HTTPException, Request, Body
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field
from typing import Dict, List, Any, Optional, Union

from .services.conversational_search_service import ConversationalSearchService
from .services.enrichment_service import EnrichmentService
from .services.marketing_service import MarketingService
from .services.imagen_service import ImageGenerationService
from .services.sql_transformation_service import SQLTransformationService
from .services.sql_fix_service import SQLFixService

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Initialize FastAPI app
app = FastAPI(
    title="Gen AI Service",
    description="API for AI-powered product enhancements with conversational search, content generation, and SQL transformation capabilities",
    version="1.0.0",
    openapi_tags=[
        {"name": "General", "description": "Basic health and status endpoints"},
        {"name": "Search", "description": "Conversational product search endpoints"},
        {"name": "Enrichment", "description": "Product data enrichment endpoints"},
        {"name": "Marketing", "description": "Marketing content generation endpoints"},
        {"name": "SQL", "description": "SQL transformation and optimization endpoints"},
        {"name": "Images", "description": "Image generation and enhancement endpoints"},
    ],
    contact={
        "name": "PSearch Development Team",
        "email": "psearch-dev@example.com",
    },
    license_info={
        "name": "Apache 2.0",
        "url": "https://www.apache.org/licenses/LICENSE-2.0.html",
    },
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # For production, restrict to specific domains
    allow_credentials=True,
    allow_methods=["GET", "POST", "OPTIONS"],
    allow_headers=["*"],
)

# Get environment variables
project_id = os.environ.get("PROJECT_ID")
location = "us-central1"

# Initialize services
conversational_search_service = ConversationalSearchService(project_id, location)
enrichment_service = EnrichmentService(project_id, location)
marketing_service = MarketingService(project_id, location)
image_generation_service = ImageGenerationService(project_id, location)
sql_transformation_service = SQLTransformationService(project_id, location)
sql_fix_service = SQLFixService(project_id, location)


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
    source_table: str = Field(
        ..., 
        description="The source BigQuery table ID (e.g., project.dataset.table)",
        example="psearch-dev-ze.raw_data.product_catalog"
    )
    destination_table: str = Field(
        ..., 
        description="The destination BigQuery table ID (e.g., project.dataset.table)",
        example="psearch-dev-ze.processed_data.products"
    )
    destination_schema: Optional[Dict[str, Any]] = Field(
        None, 
        description="Optional JSON schema definition for the destination table structure. If not provided, the system will use the default schema.json file."
    )
    source_schema_fields: List[str] = Field(
        ...,
        description="A list of field names available in the source table.",
        example=["id", "title", "description", "price"]
    )
    
    model_config = {
        "json_schema_extra": {
            "example": {
                "source_table": "psearch-dev-ze.raw_data.product_catalog",
                "destination_table": "psearch-dev-ze.processed_data.products",
                "source_schema_fields": ["product_id", "product_title", "category", "vendor_price"],
                "destination_schema": {
                    "fields": [
                        {"name": "id", "type": "STRING", "mode": "REQUIRED"},
                        {"name": "name", "type": "STRING", "mode": "REQUIRED"},
                        {"name": "description", "type": "STRING", "mode": "NULLABLE"}
                    ]
                }
            }
        }
    }

# SQL Validation models
class SQLValidationRequest(BaseModel):
    sql_script: str = Field(
        ..., 
        description="SQL script to validate",
        example="CREATE OR REPLACE TABLE `project.dataset.table` AS SELECT * FROM `source.table`"
    )
    timeout_seconds: Optional[int] = Field(
        30, 
        description="Timeout in seconds for validation",
        example=30,
        gt=0,
        le=300
    )
    
    model_config = {
        "json_schema_extra": {
            "example": {
                "sql_script": "CREATE OR REPLACE TABLE `psearch-dev-ze.processed_data.products` AS\nSELECT id, name, description FROM `psearch-dev-ze.raw_data.catalog`;",
                "timeout_seconds": 30
            }
        }
    }

# SQL Fix models
class SQLFixRequest(BaseModel):
    original_sql: str = Field(
        ..., 
        description="Original SQL that started the fix process",
        example="CREATE OR REPLACE TABLE `project.dataset.table` AS SELECT * FROM `source.table`"
    )
    current_sql: str = Field(
        ..., 
        description="Current SQL version (might be a previous fix attempt)",
        example="CREATE OR REPLACE TABLE `project.dataset.table` AS SELECT id, colorFamily FROM `source.table`"
    )
    error_message: str = Field(
        ..., 
        description="Error message from failed validation",
        example="Invalid field reference 'colorFamily'"
    )
    attempt_number: Optional[int] = Field(
        1, 
        description="Current attempt number for tracking multiple fix attempts",
        example=1,
        ge=1
    )
    
    model_config = {
        "json_schema_extra": {
            "example": {
                "original_sql": "CREATE OR REPLACE TABLE `products` AS\nSELECT id, colorFamily FROM `raw_catalog`;",
                "current_sql": "CREATE OR REPLACE TABLE `products` AS\nSELECT id, colorFamily FROM `raw_catalog`;",
                "error_message": "Invalid field reference 'colorFamily'",
                "attempt_number": 1
            }
        }
    }

class SQLFixResponse(BaseModel):
    success: bool = Field(
        ..., 
        description="Whether the fix generation was successful",
        example=True
    )
    suggested_sql: Optional[str] = Field(
        None, 
        description="The suggested fixed SQL script",
        example="CREATE OR REPLACE TABLE `project.dataset.table` AS SELECT id, NULL AS colorFamily FROM `source.table`"
    )
    diff: Optional[str] = Field(
        None, 
        description="Unified diff showing the changes between current and suggested SQL",
        example="--- current.sql\n+++ suggested.sql\n@@ -1,5 +1,5 @@\nCREATE OR REPLACE TABLE...\n-colorFamily\n+NULL AS colorFamily"
    )
    error: Optional[str] = Field(
        None, 
        description="Error message if fix generation failed",
        example="Failed to generate SQL fix: Model error"
    )
    
    model_config = {
        "json_schema_extra": {
            "example": {
                "success": True,
                "suggested_sql": "CREATE OR REPLACE TABLE `products` AS\nSELECT id, NULL AS colorFamily FROM `raw_catalog`;",
                "diff": "--- current.sql\n+++ suggested.sql\n@@ -1,5 +1,5 @@\nCREATE OR REPLACE TABLE `products` AS\nSELECT id, \n-colorFamily \n+NULL AS colorFamily \nFROM `raw_catalog`;"
            }
        }
    }


@app.get("/", tags=["General"])
async def root():
    return {"message": "Gen AI Service is running"}


@app.get("/health", tags=["General"])
async def health_check():
    return {"status": "healthy"}


@app.post("/conversational-search", tags=["Search"])
async def conversational_search(request: ConversationalSearchRequest):
    """
    Process a conversational search query and return relevant results
    """
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

        # Convert to dictionary for JSON response
        response = {
            "answer": result.answer,
            "suggested_products": result.suggested_products,
            "follow_up_questions": result.follow_up_questions,
        }

        # Log the response for debugging
        logger.info(
            f"Conversational search response: Answer: {response['answer'][:100]}..."
        )
        logger.info(f"Follow-up questions: {response['follow_up_questions']}")
        logger.info(f"Suggested products count: {len(response['suggested_products'])}")

        if response["suggested_products"]:
            for i, product in enumerate(response["suggested_products"]):
                logger.info(
                    f"Suggestion {i+1}: {product.get('id', 'unknown')} - Reason: {product.get('reason', 'not provided')[:100]}..."
                )

        return response

    except Exception as e:
        logger.error(f"Error processing conversational search: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/enrichment", tags=["Enrichment"])
async def enrichment(request: EnrichmentRequest):
    """
    Enrich product data with AI-generated content
    """
    logger.info(f"Received enrichment request for product: {request.product_id}")
    logger.info(f"Fields to enrich: {request.fields_to_enrich}")
    if request.product_data.get("images"):
        logger.info(f"Product has {len(request.product_data['images'])} images")

    try:
        result = enrichment_service.process_enrichment(
            product_id=request.product_id,
            product_data=request.product_data,
            fields_to_enrich=request.fields_to_enrich,
        )

        # Log the enriched fields for debugging
        logger.info(f"Enrichment completed for product: {request.product_id}")
        if "enriched_fields" in result:
            for field, content in result["enriched_fields"].items():
                # Handle different content types safely
                if isinstance(content, str):
                    # Safe string slicing
                    logger.info(
                        f"Enriched field '{field}': {content[:100] if len(content) > 100 else content}"
                    )
                elif isinstance(content, dict):
                    # For dictionaries (like technical_specs), convert to string first
                    try:
                        content_str = json.dumps(content)
                        logger.info(
                            f"Enriched field '{field}': {content_str[:200] if len(content_str) > 200 else content_str}"
                        )
                    except:
                        logger.info(f"Enriched field '{field}': {str(content)}")
                else:
                    # For any other type, just log the type
                    logger.info(f"Enriched field '{field}': (type: {type(content)})")
        if "error" in result:
            logger.warning(f"Enrichment error: {result['error']}")

        return result

    except Exception as e:
        logger.error(f"Error processing enrichment: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/marketing", tags=["Marketing"])
async def marketing(request: MarketingRequest):
    """
    Generate marketing content for a product
    """
    logger.info(f"Received marketing request for product: {request.product_id}")
    logger.info(f"Content type: {request.content_type}, Tone: {request.tone}")
    logger.info(
        f"Target audience: {request.target_audience}, Max length: {request.max_length}"
    )
    if request.product_data.get("images"):
        logger.info(f"Product has {len(request.product_data['images'])} images")

    try:
        result = marketing_service.generate_content(
            product_id=request.product_id,
            product_data=request.product_data,
            content_type=request.content_type,
            tone=request.tone,
            target_audience=request.target_audience,
            max_length=request.max_length,
        )

        # Log the generated content for debugging
        logger.info(f"Marketing content generated for product: {request.product_id}")
        if "content" in result:
            logger.info(
                f"Generated content (first 100 chars): {result['content'][:100]}..."
            )
        if "error" in result:
            logger.warning(f"Marketing content error: {result['error']}")

        return result

    except Exception as e:
        logger.error(f"Error generating marketing content: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/generate-enhanced-image", tags=["Images"])
async def generate_enhanced_image(request: EnhancedImageRequest):
    """
    Generate an enhanced image using Gemini, changing background or adding a person.
    """
    logger.info(f"Received enhanced image request for product: {request.product_id}")
    logger.info(f"Style: {request.style}")
    if request.person_description:
        logger.info(
            f"Person description provided: {request.person_description[:50]}..."
        )
    else:
        logger.info(f"Background prompt: {request.background_prompt[:50]}...")

    try:
        result = image_generation_service.generate_image(
            product_id=request.product_id,
            product_data=request.product_data,
            image_base64=request.image_base64,
            background_prompt=request.background_prompt,
            person_description=request.person_description,
            style=request.style,
        )

        logger.info(
            f"Enhanced image generation completed for product: {request.product_id}"
        )
        if "generated_image_base64" in result:
            logger.info("Generated image successfully (base64 data).")
        elif "error" in result:
            logger.warning(f"Image generation error: {result['error']}")
        else:
            logger.warning("Image generation result format unexpected.")

        return result

    except Exception as e:
        logger.error(f"Error generating enhanced image: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


# Add middleware to log request/response
@app.middleware("http")
async def log_requests(request: Request, call_next):
    """Log requests and responses"""
    logger.info(f"Request: {request.method} {request.url.path}")
    response = await call_next(request)
    logger.info(f"Response: {response.status_code}")
    return response


@app.post(
    "/generate-sql",
    tags=["SQL"],
    summary="Generate SQL transformation script",
    description="Generates a SQL transformation script to map data from a source table to a destination schema",
    response_model=Dict[str, str]
)
async def generate_sql(request: SQLGenerationRequest):
    """
    Generate a SQL transformation script to map data from source to destination schema.
    """
    logger.info(f"Received SQL generation request: {request.source_table} -> {request.destination_table}")
    
    try:
        # --- Step 1: Initial Syntactic SQL Generation ---
        initial_generation_result = sql_transformation_service.generate_sql_transformation(
            source_table_name=request.source_table,
            destination_table_name=request.destination_table,
            source_schema_fields=request.source_schema_fields,
            destination_schema=request.destination_schema
        )
        current_sql = initial_generation_result.get("sql_query")
        if not current_sql:
            raise ValueError("Initial SQL generation did not return a SQL query.")

        logger.info(f"Initial SQL generated: {current_sql[:200]}...")
        
        # --- Step 2: Internal Validation & Prep for Semantic Enhancement ---
        source_data_sample_json = None
        
        # Use the new method to identify defaulted fields that need semantic mapping
        fields_to_semantically_refine = sql_transformation_service.identify_defaulted_fields(current_sql)
        
        if fields_to_semantically_refine:
            logger.info(f"Critical fields defaulted, attempting to fetch source sample for semantic enhancement: {fields_to_semantically_refine}")
            try:
                from google.cloud import bigquery
                # Ensure project_id is available, falling back to environment if service's isn't directly accessible
                bq_project_id = sql_transformation_service.project_id or os.environ.get("PROJECT_ID")
                if not bq_project_id:
                    raise ValueError("Project ID not configured for BigQuery client.")
                
                bq_client = bigquery.Client(project=bq_project_id)
                sample_query = f"SELECT * FROM `{request.source_table}` LIMIT 3" # Use request.source_table which is the full ID
                logger.info(f"Fetching source data sample with query: {sample_query}")
                query_job = bq_client.query(sample_query)
                rows = [dict(row) for row in query_job.result(timeout=30)] # Timeout for safety
                if rows:
                    source_data_sample_json = json.dumps(rows, indent=2)
                    # We already have fields_to_semantically_refine from the identify_defaulted_fields method
                    logger.info(f"Source data sample fetched successfully. Sample: {source_data_sample_json[:200]}...")
                else:
                    logger.warning("No rows returned for source data sample.")
            except Exception as bq_err:
                logger.error(f"Failed to fetch source data sample: {bq_err}")
                # Continue without semantic enhancement if sample fetching fails

        # --- Step 3: Call Semantic Enhancement if needed ---
        if fields_to_semantically_refine and source_data_sample_json:
            logger.info(f"Calling semantic enhancement for fields: {fields_to_semantically_refine}")
            current_sql = sql_transformation_service.semantically_enhance_sql(
                current_sql_query=current_sql,
                source_table_name=request.source_table,
                source_schema_fields=request.source_schema_fields,
                source_data_sample_json=source_data_sample_json,
                destination_schema=request.destination_schema,
                critical_fields_to_refine=fields_to_semantically_refine
            )
            logger.info(f"SQL after semantic enhancement: {current_sql[:200]}...")
        else:
            logger.info("Skipping semantic enhancement step.")

        # --- Step 4 & 5: BigQuery Dry Run & Iterative Fixing ---
        max_fix_attempts = 3
        last_error_message = "SQL script initially passed to validation." # Placeholder for first iteration

        for attempt in range(max_fix_attempts + 1): # +1 to allow initial validation
            logger.info(f"Attempting BigQuery Dry Run (Attempt {attempt + 1 if attempt < max_fix_attempts else 'Final Validation'}) for SQL: {current_sql[:200]}...")
            try:
                validation_result = sql_fix_service.validate_sql(sql_script=current_sql, timeout_seconds=60)
            except Exception as val_err: # Catch errors from validate_sql itself
                 logger.error(f"Dry run call failed: {val_err}")
                 validation_result = {"valid": False, "error": f"Dry run execution failed: {str(val_err)}"}


            if validation_result.get("valid"):
                logger.info("SQL script validated successfully via BigQuery Dry Run.")
                break # Exit loop if valid
            
            last_error_message = validation_result.get("error", "Unknown validation error from BigQuery Dry Run.")
            logger.warning(f"SQL Dry Run failed: {last_error_message}")

            if attempt < max_fix_attempts:
                logger.info(f"Attempting AI-powered fix (Attempt {attempt + 1}/{max_fix_attempts}) for error: {last_error_message[:100]}...")
                try:
                    current_sql = sql_transformation_service.refine_sql_script(
                        sql_script=current_sql,
                        error_message=last_error_message
                    )
                    logger.info(f"SQL after fix attempt {attempt + 1}: {current_sql[:200]}...")
                except Exception as refine_err:
                    logger.error(f"Call to refine_sql_script failed: {refine_err}")
                    # If refine_sql_script itself errors, we can't continue this loop path
                    raise HTTPException(status_code=500, detail=f"Error during SQL refinement process: {str(refine_err)}")
            else:
                logger.error(f"Failed to produce valid SQL after {max_fix_attempts} fix attempts. Last error: {last_error_message}")
                raise HTTPException(status_code=500, detail=f"Failed to produce valid SQL after {max_fix_attempts} fix attempts. Last error: {last_error_message}")
        
        # --- Optional: Final Whitespace Cleanup (Contingency) ---
        # Example: current_sql = current_sql.replace("TABLE\`", "TABLE \`").replace("\`AS", "\` AS")
        # This should be used cautiously and with robust regex if implemented.
        # For now, relying on prompt engineering for formatting.

        return {
            "sql_script": current_sql,
            "source_table": request.source_table,
            "destination_table": request.destination_table,
            "message": "SQL script generated and validated successfully." if validation_result.get("valid") else "SQL script generated but failed final validation."
        }
        
    except HTTPException: # Re-raise HTTPExceptions directly
        raise
    except Exception as e:
        logger.error(f"Error in full SQL generation orchestrator: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"An unexpected error occurred during SQL generation: {str(e)}")


@app.post(
    "/refine-sql",
    tags=["SQL"],
    summary="Refine SQL script with errors",
    description="Refines an SQL script that has errors based on the error message",
    response_model=Dict[str, str]
)
async def refine_sql(request: Dict[str, str]):
    """
    Refine an SQL script that has errors based on the error message.
    """
    logger.info("Received SQL refinement request")
    
    # Validate request
    if "sql_script" not in request or "error_message" not in request:
        raise HTTPException(status_code=400, detail="Request must include 'sql_script' and 'error_message'")
        
    try:
        # Refine the SQL script
        refined_sql = sql_transformation_service.refine_sql_script(
            sql_script=request["sql_script"],
            error_message=request["error_message"]
        )
        
        # Return the refined SQL script
        return {
            "refined_sql_script": refined_sql
        }
        
    except Exception as e:
        logger.error(f"Error refining SQL script: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post(
    "/sql/validate",
    tags=["SQL"],
    summary="Validate SQL script",
    description="Performs a dry run validation of a SQL script without executing it",
    response_model=Dict[str, Any],
    responses={
        200: {
            "description": "Validation result",
            "content": {
                "application/json": {
                    "examples": {
                        "success": {
                            "summary": "Valid SQL",
                            "value": {
                                "valid": True,
                                "message": "SQL syntax validated successfully (Estimated bytes: 1,024)",
                                "details": {
                                    "estimated_bytes_processed": 1024
                                }
                            }
                        },
                        "error": {
                            "summary": "Invalid SQL",
                            "value": {
                                "valid": False,
                                "error": "Invalid field reference 'missing_field'",
                                "details": {
                                    "missing_field": "missing_field"
                                }
                            }
                        }
                    }
                }
            }
        },
        500: {
            "description": "Server error",
            "content": {
                "application/json": {
                    "example": {
                        "detail": "Error performing SQL validation"
                    }
                }
            }
        }
    }
)
async def validate_sql(request: SQLValidationRequest):
    """
    Validate a SQL script with a BigQuery dry run.
    
    - Checks syntax and semantics without executing the query
    - Estimates the bytes that would be processed
    - Identifies missing fields and other common errors
    
    Returns a validation result with status and details.
    """
    logger.info(f"Validating SQL script ({len(request.sql_script)} chars)")
    
    try:
        # Validate the SQL
        validation_result = sql_fix_service.validate_sql(
            request.sql_script,
            request.timeout_seconds
        )
        
        return validation_result
    
    except Exception as e:
        logger.error(f"Error validating SQL: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post(
    "/sql/fix",
    tags=["SQL"],
    summary="Generate SQL fix",
    description="Generates a fixed SQL script using AI based on error message",
    response_model=SQLFixResponse,
    responses={
        200: {
            "description": "Generated SQL fix",
            "content": {
                "application/json": {
                    "example": {
                        "success": True,
                        "suggested_sql": "CREATE OR REPLACE TABLE `products` AS\nSELECT id, NULL AS colorFamily FROM `raw_catalog`;",
                        "diff": "--- current.sql\n+++ suggested.sql\n@@ -1,5 +1,5 @@\nCREATE OR REPLACE TABLE `products` AS\nSELECT id, \n-colorFamily \n+NULL AS colorFamily \nFROM `raw_catalog`;"
                    }
                }
            }
        },
        400: {
            "description": "Invalid request",
            "content": {
                "application/json": {
                    "example": {
                        "detail": "Missing SQL or error message to generate a fix"
                    }
                }
            }
        },
        500: {
            "description": "Server error",
            "content": {
                "application/json": {
                    "example": {
                        "detail": "Error generating SQL fix"
                    }
                }
            }
        }
    }
)
async def fix_sql(request: SQLFixRequest):
    """
    Generate a fixed SQL script based on an error message.
    
    - Uses AI to analyze the error and suggest fixes
    - Returns the suggested SQL with a diff showing changes
    - Can handle common errors like missing fields, syntax issues, etc.
    
    Returns the suggested SQL script and a diff showing the changes.
    """
    logger.info(f"Generating SQL fix for error: {request.error_message[:100]}...")
    
    try:
        # Generate the fix
        fix_result = sql_fix_service.generate_sql_fix(
            request.original_sql,
            request.current_sql,
            request.error_message
        )
        
        return fix_result
    
    except Exception as e:
        logger.error(f"Error generating SQL fix: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post(
    "/sql/analyze",
    tags=["SQL"],
    summary="Analyze SQL differences",
    description="Analyzes and explains differences between original and fixed SQL",
    response_model=Dict[str, Any],
    responses={
        200: {
            "description": "Analysis of SQL differences",
            "content": {
                "application/json": {
                    "example": {
                        "diff": "--- current.sql\n+++ fixed.sql\n@@ -1,5 +1,5 @@\n...",
                        "changes": ["Modified field reference: colorFamily -> NULL AS colorFamily"],
                        "removed_lines_count": 1,
                        "added_lines_count": 1
                    }
                }
            }
        },
        500: {
            "description": "Server error",
            "content": {
                "application/json": {
                    "example": {
                        "detail": "Error analyzing SQL differences"
                    }
                }
            }
        }
    }
)
async def analyze_sql_diff(
    original_sql: str = Body(..., description="Original SQL with errors", example="CREATE OR REPLACE TABLE `products` AS\nSELECT id, colorFamily FROM `raw_catalog`;"),
    fixed_sql: str = Body(..., description="Fixed SQL script", example="CREATE OR REPLACE TABLE `products` AS\nSELECT id, NULL AS colorFamily FROM `raw_catalog`;")
):
    """
    Analyze and explain the differences between original and fixed SQL.
    
    - Provides a detailed analysis of what was changed and why
    - Identifies key changes like field replacements or removals
    - Includes a diff showing exact changes
    
    Returns analysis details including a list of significant changes.
    """
    logger.info("Analyzing SQL differences")
    
    try:
        analysis = sql_fix_service.analyze_differences(original_sql, fixed_sql)
        return analysis
    
    except Exception as e:
        logger.error(f"Error analyzing SQL differences: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8080)
