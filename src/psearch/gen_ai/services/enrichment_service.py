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

"""
Enrichment Service - Enhances product data with AI-generated content
"""

from google import genai
from google.genai import types
import logging
import requests
from typing import Dict, List, Any, Optional
import json
import base64
from urllib.parse import urlparse

logger = logging.getLogger(__name__)


class EnrichmentService:
    def __init__(self, project_id: str, location: str):
        self.project_id = project_id
        self.location = location
        self.client = genai.Client(
            vertexai=True,
            project=project_id,
            location=location,
        )
        self.model = "gemini-2.0-flash-001"

    def process_enrichment(
        self,
        product_id: str,
        product_data: Dict[str, Any],
        fields_to_enrich: Optional[List[str]] = None,
    ) -> Dict:
        """
        Enrich product data with AI-generated content

        Args:
            product_id: ID of the product to enrich
            product_data: Dictionary containing product data
            fields_to_enrich: List of fields to enrich (defaults to standard set)

        Returns:
            Dictionary with product_id and enriched_fields
        """
        logger.info(f"Processing enrichment for product {product_id}")

        # If no fields specified, use default set
        if not fields_to_enrich:
            fields_to_enrich = [
                "description",
                "features",
                "benefits",
                "use_cases",
                "technical_specs",
            ]

        # Build prompt with product data and fields to enrich
        prompt = self._build_prompt(product_data, fields_to_enrich)

        # Create response schema for enriched fields
        response_schema = {"type": "OBJECT", "properties": {}}

        # Add each requested field to the schema
        for field in fields_to_enrich:
            if field == "technical_specs":
                # Create a more detailed schema for technical specifications
                # with specific properties that we expect to receive
                response_schema["properties"][field] = {
                    "type": "OBJECT",
                    "description": "Technical specifications as key-value pairs",
                    "properties": {
                        "Material": {"type": "STRING"},
                        "Color": {"type": "STRING"},
                        "Dimensions": {"type": "STRING"},
                        "Weight": {"type": "STRING"},
                        "Style": {"type": "STRING"},
                        "Brand": {"type": "STRING"},
                        "Category": {"type": "STRING"},
                    },
                    # Requiring at least these properties to be present
                    "required": ["Material", "Color", "Dimensions"],
                }
            else:
                # For other fields like description, features, etc.
                response_schema["properties"][field] = {"type": "STRING"}

        # Set up generation configuration with structured JSON output
        generate_content_config = types.GenerateContentConfig(
            temperature=0.7,  # Higher temperature for more creative responses
            top_p=0.95,
            max_output_tokens=4096,
            response_modalities=["TEXT"],
            response_mime_type="application/json",
            response_schema=response_schema,
            safety_settings=[
                types.SafetySetting(
                    category="HARM_CATEGORY_HATE_SPEECH", threshold="OFF"
                ),
                types.SafetySetting(
                    category="HARM_CATEGORY_DANGEROUS_CONTENT", threshold="OFF"
                ),
                types.SafetySetting(
                    category="HARM_CATEGORY_SEXUALLY_EXPLICIT", threshold="OFF"
                ),
                types.SafetySetting(
                    category="HARM_CATEGORY_HARASSMENT", threshold="OFF"
                ),
            ],
        )

        try:
            # Call Gemini model
            response = self.client.models.generate_content(
                model=self.model,
                contents=prompt,
                config=generate_content_config,
            )

            # Parse the response
            enriched_fields = self._parse_response(response.text, fields_to_enrich)

            return {"product_id": product_id, "enriched_fields": enriched_fields}

        except Exception as e:
            logger.error(f"Error generating enriched content: {str(e)}")
            return {
                "product_id": product_id,
                "enriched_fields": {
                    "error": f"Error generating enriched content: {str(e)}"
                },
            }

    def _get_image_bytes_from_url(self, image_url: str) -> Optional[bytes]:
        """Fetch image bytes from URL"""
        try:
            # Handle different URL formats (especially for Google Cloud Storage)
            parsed_url = urlparse(image_url)

            # If it's a GCS URL (gs://), convert it to HTTPS
            if parsed_url.scheme == "gs":
                bucket = parsed_url.netloc
                object_path = parsed_url.path.lstrip("/")
                url = f"https://storage.googleapis.com/{bucket}/{object_path}"
            else:
                url = image_url

            response = requests.get(url, timeout=10)
            if response.status_code == 200:
                return response.content
            else:
                logger.warning(
                    f"Failed to fetch image from {url}: {response.status_code}"
                )
                return None
        except Exception as e:
            logger.warning(f"Error fetching image from {image_url}: {str(e)}")
            return None

    def _build_prompt(
        self, product_data: Dict[str, Any], fields_to_enrich: List[str]
    ) -> List[types.Content]:
        """Build the prompt for the Gemini model"""

        instruction = """
        You are a product content specialist with expertise in enhancing product information with engaging, 
        accurate, and SEO-friendly content. Your task is to enrich product data with high-quality content.

        For each field requested, create content that is:
        1. Engaging and persuasive to potential customers
        2. Factually accurate based on the product data provided
        3. Well-structured and professionally written
        4. SEO-optimized with relevant keywords
        
        Your response should be in JSON format with the following structure:
        {
            "field_name_1": "Enhanced content for field 1",
            "field_name_2": "Enhanced content for field 2",
            ...
        }
        
        For different field types:
        - "description": Create a compelling product description (250-300 words)
        - "features": List 5-8 key product features with brief explanations
        - "benefits": Describe 3-5 benefits of using this product
        - "use_cases": Provide 3-4 practical use cases or scenarios
        - "technical_specs": Format technical specifications as key-value pairs, making informed inferences based on the product data.
          You MUST include all the following properties in your response:
          "technical_specs": {
            "Material": "Describe the main materials used (e.g., 'Premium cotton blend', 'Stainless steel')",
            "Color": "Describe the color in detail (e.g., 'Deep navy blue', 'Vibrant crimson')",
            "Dimensions": "Provide measurements (e.g., '30cm x 20cm x 5cm', 'Standard fit')",
            "Weight": "Describe the weight (e.g., '250g', 'Lightweight')",
            "Style": "Describe the style (e.g., 'Contemporary', 'Vintage', 'Sporty')",
            "Brand": "The brand name from the product data",
            "Category": "The product category from the product data"
          }
          
          Even if you don't have exact information for a field, use the product context to make a reasonable inference
          rather than saying "Not specified" or leaving fields empty. Use visual cues from any product images provided.
        
        Pay close attention to the product image provided and use visual information to enrich your content. 
        Include specific details visible in the image such as color, design features, materials, and overall 
        appearance in your descriptions.
        
        Only include the fields that were requested in the output.
        """

        user_prompt = f"{instruction}\n\nProduct Data:\n"
        user_prompt += json.dumps(product_data, indent=2)
        user_prompt += "\n\nFields to enrich:\n"
        user_prompt += ", ".join(fields_to_enrich)
        user_prompt += "\n\nPlease provide your response in the specified JSON format."

        parts = [types.Part.from_text(text=user_prompt)]

        # Try to get the product image and include it in the prompt
        try:
            if product_data.get("images") and len(product_data["images"]) > 0:
                image_url = product_data["images"][0].get("uri")
                if image_url:
                    image_bytes = self._get_image_bytes_from_url(image_url)
                    if image_bytes:
                        # Determine MIME type based on file extension
                        if image_url.lower().endswith(".png"):
                            mime_type = "image/png"
                        elif image_url.lower().endswith((".jpg", ".jpeg")):
                            mime_type = "image/jpeg"
                        elif image_url.lower().endswith(".gif"):
                            mime_type = "image/gif"
                        else:
                            mime_type = "image/jpeg"  # Default to JPEG

                        # Add image part to the prompt
                        image_part = types.Part.from_bytes(
                            data=image_bytes, mime_type=mime_type
                        )
                        parts.append(image_part)
                        logger.info(f"Added image from {image_url} to the prompt")
                    else:
                        logger.warning(f"Could not fetch image bytes from {image_url}")
                else:
                    logger.warning("Product has images but no URI found")
            else:
                logger.info("No product images found")
        except Exception as e:
            logger.warning(f"Error adding image to prompt: {str(e)}")

        # Create the contents for the Gemini model - using only user role
        contents = [types.Content(role="user", parts=parts)]

        return contents

    def _parse_response(
        self, response_text: str, fields_to_enrich: List[str]
    ) -> Dict[str, Any]:
        """Parse the response from the Gemini model"""
        try:
            # Log the raw response for debugging
            logger.info(
                f"Raw response from Gemini: {response_text[:500]}..."
            )  # Log first 500 chars

            # Try to parse the response as JSON
            response_json = json.loads(response_text)

            # Log the parsed response
            logger.info(f"Parsed JSON response: {json.dumps(response_json)[:500]}...")

            # Extract only the requested fields
            enriched_fields = {}
            for field in fields_to_enrich:
                if field in response_json:
                    # Handle technical_specs specially to ensure consistent format
                    if field == "technical_specs":
                        tech_specs = response_json[field]

                        # Log the tech specs format for debugging
                        logger.info(
                            f"Technical specs format: {type(tech_specs)}, Content: {tech_specs}"
                        )

                        # If it's an empty dict, populate with product-specific values
                        if isinstance(tech_specs, dict) and len(tech_specs) == 0:
                            logger.info(
                                "Empty technical_specs dict detected, generating from product data"
                            )

                            # Initialize with better defaults
                            default_specs = {}

                            # Extract product attributes
                            try:
                                # Add brand info
                                if (
                                    product_data
                                    and "brands" in product_data
                                    and product_data["brands"]
                                ):
                                    default_specs["Brand"] = product_data["brands"][0]
                                else:
                                    default_specs["Brand"] = "Premium Brand"

                                # Add category info
                                if (
                                    product_data
                                    and "categories" in product_data
                                    and product_data["categories"]
                                ):
                                    default_specs["Category"] = product_data[
                                        "categories"
                                    ][0]
                                else:
                                    default_specs["Category"] = "Fashionable Apparel"

                                # Get attributes from product data
                                if product_data and "attributes" in product_data:
                                    for attr in product_data["attributes"]:
                                        if (
                                            attr.get("key")
                                            and attr.get("value")
                                            and attr["value"].get("text")
                                        ):
                                            key = attr["key"].capitalize()
                                            value = ", ".join(attr["value"]["text"])
                                            default_specs[key] = value

                                # Add any missing required specs
                                if "Material" not in default_specs:
                                    default_specs["Material"] = "High Quality Fabric"
                                if "Color" not in default_specs:
                                    default_specs["Color"] = "As Shown"
                                if "Dimensions" not in default_specs:
                                    default_specs["Dimensions"] = "Standard Size"
                                if "Weight" not in default_specs:
                                    default_specs["Weight"] = "Lightweight"
                                if "Style" not in default_specs:
                                    default_specs["Style"] = "Contemporary"

                                logger.info(
                                    f"Generated specs from product data: {default_specs}"
                                )
                            except Exception as e:
                                logger.warning(
                                    f"Error generating specs from product data: {str(e)}"
                                )
                                # Fallback to generic specs if error occurs
                                default_specs = {
                                    "Material": "Quality Materials",
                                    "Color": "As Shown in Image",
                                    "Dimensions": "Standard Size",
                                    "Weight": "Lightweight",
                                    "Style": "Contemporary Design",
                                    "Brand": "Premium Brand",
                                    "Category": "Fashionable Apparel",
                                }

                            enriched_fields[field] = default_specs
                        # Non-empty dict, use it directly
                        elif isinstance(tech_specs, dict):
                            enriched_fields[field] = tech_specs
                        # If it's a string (sometimes happens with Gemini), try to parse it
                        elif isinstance(tech_specs, str):
                            try:
                                parsed_specs = json.loads(tech_specs)
                                if isinstance(parsed_specs, dict):
                                    enriched_fields[field] = parsed_specs
                                else:
                                    # Convert list to dict if needed
                                    if isinstance(parsed_specs, list):
                                        specs_dict = {
                                            f"Spec {i+1}": item
                                            for i, item in enumerate(parsed_specs)
                                        }
                                        enriched_fields[field] = specs_dict
                                    else:
                                        # Fallback to string as-is
                                        enriched_fields[field] = {
                                            "Specifications": tech_specs
                                        }
                            except json.JSONDecodeError:
                                # If it's not valid JSON, use as plain text
                                enriched_fields[field] = {"Specifications": tech_specs}
                        # If it's a list, convert to dict with numbered keys
                        elif isinstance(tech_specs, list):
                            specs_dict = {}
                            for i, item in enumerate(tech_specs):
                                if (
                                    isinstance(item, dict)
                                    and "name" in item
                                    and "value" in item
                                ):
                                    specs_dict[item["name"]] = item["value"]
                                else:
                                    specs_dict[f"Spec {i+1}"] = str(item)
                            enriched_fields[field] = specs_dict
                        else:
                            # Fallback for unexpected types
                            enriched_fields[field] = {"Specifications": str(tech_specs)}
                    else:
                        enriched_fields[field] = response_json[field]
                else:
                    logger.warning(f"Field {field} not found in response")

            # Log the final enriched fields
            logger.info(
                f"Final enriched fields: {json.dumps(enriched_fields)[:500]}..."
            )

            return enriched_fields
        except json.JSONDecodeError:
            # If JSON parsing fails, extract what we can
            logger.warning(f"Failed to parse response as JSON: {response_text}")

            # Return raw response as error
            return {
                "error": "Failed to parse response",
                "raw_response": response_text[:1000],  # Truncate to first 1000 chars
            }
