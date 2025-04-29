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
import logging
from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import Dict, List, Any, Optional

from .services.conversational_search_service import ConversationalSearchService
from .services.enrichiment_service import EnrichmentService
from .services.marketing_service import MarketingService
from .services.imagen_service import ImageGenerationService
from .services.sql_transformation_service import SQLTransformationService

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Initialize FastAPI app
app = FastAPI(
    title="Gen AI Service",
    description="API for AI-powered product enhancements",
    version="1.0.0",
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
project_id = os.environ.get("PROJECT_ID", "psearch-dev-ze")
location = "us-central1"

# Initialize services
conversational_search_service = ConversationalSearchService(project_id, location)
enrichment_service = EnrichmentService(project_id, location)
marketing_service = MarketingService(project_id, location)
image_generation_service = ImageGenerationService(project_id, location)
sql_transformation_service = SQLTransformationService(project_id, location)


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
    source_table: str
    destination_table: str
    destination_schema: Dict[str, Any]


@app.get("/")
async def root():
    return {"message": "Gen AI Service is running"}


@app.get("/health")
async def health_check():
    return {"status": "healthy"}


@app.post("/conversational-search")
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


@app.post("/enrichment")
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


@app.post("/marketing")
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


@app.post("/generate-enhanced-image")
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


@app.post("/generate-sql")
async def generate_sql(request: SQLGenerationRequest):
    """
    Generate a SQL transformation script to map data from source to destination schema.
    """
    logger.info(f"Received SQL generation request: {request.source_table} -> {request.destination_table}")
    
    try:
        # Generate the SQL transformation script
        sql_script = sql_transformation_service.generate_sql_transformation(
            source_table=request.source_table,
            destination_table=request.destination_table,
            destination_schema=request.destination_schema
        )
        
        # Return the generated SQL script
        return {
            "sql_script": sql_script,
            "source_table": request.source_table,
            "destination_table": request.destination_table
        }
        
    except Exception as e:
        logger.error(f"Error generating SQL transformation: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/refine-sql")
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


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8080)
