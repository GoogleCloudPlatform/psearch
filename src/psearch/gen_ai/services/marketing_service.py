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
Marketing Service - Generates marketing content for products
"""

import os

from google import genai
from google.genai import types
import logging
import requests
from typing import Dict, List, Any, Optional
import json
import base64
from urllib.parse import urlparse

logger = logging.getLogger(__name__)


class MarketingService:
    def __init__(self, project_id: str, location: str):
        self.project_id = project_id
        self.location = location
        self.client = genai.Client(
            vertexai=True,
            project=project_id,
            location=location,
        )
        self.model = os.environ.get("GEMINI_TEXT_MODEL", "gemini-3.5-flash")

    def generate_content(
        self,
        product_id: str,
        product_data: Dict[str, Any],
        content_type: str,
        tone: str = "professional",
        target_audience: Optional[str] = None,
        max_length: int = 500,
    ) -> Dict:
        """
        Generate marketing content for a product

        Args:
            product_id: ID of the product
            product_data: Dictionary containing product data
            content_type: Type of content to generate (e.g., "product_description", "email_campaign")
            tone: Tone of the content (e.g., "professional", "casual", "luxury")
            target_audience: Target audience for the content
            max_length: Maximum length of the content in words

        Returns:
            Dictionary with product_id and generated content
        """
        logger.info(
            f"Generating {content_type} content for product {product_id} with {tone} tone"
        )

        # Build prompt with product data and content requirements
        prompt = self._build_prompt(
            product_data=product_data,
            content_type=content_type,
            tone=tone,
            target_audience=target_audience,
            max_length=max_length,
        )

        # Create response schema for marketing content
        response_schema = {
            "type": "OBJECT",
            "properties": {
                "content": {"type": "STRING"},
                "headline": {"type": "STRING"},
                "tags": {"type": "ARRAY", "items": {"type": "STRING"}},
            },
            "required": ["content"],
        }

        # Set up generation configuration with structured JSON output
        generate_content_config = types.GenerateContentConfig(
            temperature=0.8,  # Higher temperature for more creative marketing content
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

            # Parse and clean the response
            content = self._parse_response(response.text)

            return {"product_id": product_id, "content": content}

        except Exception as e:
            logger.error(f"Error generating marketing content: {str(e)}")
            return {
                "product_id": product_id,
                "content": f"Error generating marketing content: {str(e)}",
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
        self,
        product_data: Dict[str, Any],
        content_type: str,
        tone: str,
        target_audience: Optional[str],
        max_length: int,
    ) -> List[types.Content]:
        """Build the prompt for the Gemini model"""

        content_type_description = {
            "product_description": "a compelling product description that highlights key features, benefits, and unique selling points",
            "email_campaign": "an email marketing campaign that promotes the product, encourages click-through, and drives conversions",
            "social_post": "engaging social media content to promote the product, tailored to the platform's constraints and audience",
            "product_page": "optimized content for a product detail page with all necessary information for customer conversion",
            "ad_copy": "compelling advertising copy that drives interest and conversions in limited space",
            "blog_post": "an informative and engaging blog post about the product, its benefits, and use cases",
        }

        tone_description = {
            "professional": "formal, authoritative, and business-like",
            "casual": "conversational, friendly, and approachable",
            "luxury": "sophisticated, exclusive, and premium",
            "technical": "detailed, precise, and feature-focused",
            "emotional": "empathetic, personal, and focused on feelings",
            "humorous": "light-hearted, fun, and entertaining",
        }

        instruction = f"""
        You are an expert marketing copywriter specializing in creating high-converting, engaging content.
        
        Your task is to create {content_type_description.get(content_type, "marketing content")} 
        in a {tone_description.get(tone, "professional")} tone.
        
        The content should be:
        1. Compelling and persuasive
        2. Focused on benefits and value proposition
        3. SEO-optimized with relevant keywords
        4. Clear and concise (maximum {max_length} words)
        5. Written in a consistent voice and style
        
        Do not use placeholder text or generic content. Create unique, specific content based on the 
        product details provided. Focus on what makes this product special and why customers should care.
        
        Pay close attention to the product image provided and incorporate visual details into your marketing copy.
        Mention specific visual elements, colors, design features, and other visual aspects that would resonate with customers.
        """

        if target_audience:
            instruction += f"\n\nThe target audience is: {target_audience}\n"
            instruction += "Ensure the language, examples, and messaging resonate with this specific audience."

        user_prompt = f"{instruction}\n\nContent Type: {content_type}\nTone: {tone}\nMax Length: {max_length} words\n\n"
        user_prompt += "Product Data:\n"
        user_prompt += json.dumps(product_data, indent=2)
        user_prompt += "\n\nPlease generate the requested marketing content based on these requirements."

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
                        logger.info(
                            f"Added image from {image_url} to the marketing prompt"
                        )
                    else:
                        logger.warning(f"Could not fetch image bytes from {image_url}")
                else:
                    logger.warning("Product has images but no URI found")
            else:
                logger.info("No product images found for marketing content")
        except Exception as e:
            logger.warning(f"Error adding image to marketing prompt: {str(e)}")

        # Create the contents for the Gemini model - using only user role
        contents = [types.Content(role="user", parts=parts)]

        return contents

    def _parse_response(self, response_text: str) -> str:
        """Parse and clean the response from the Gemini model"""
        # For marketing content, we typically want the raw text response
        # Just do some basic cleaning
        cleaned_text = response_text.strip()

        # Remove any JSON formatting if present (the model might mistakenly return JSON)
        try:
            parsed_json = json.loads(cleaned_text)
            if isinstance(parsed_json, dict) and "content" in parsed_json:
                return parsed_json["content"]
            if isinstance(parsed_json, str):
                return parsed_json
        except json.JSONDecodeError:
            # Not JSON, which is fine
            pass

        return cleaned_text
