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
Imagen Service - Generates product images using Imagen API
"""

import os

from google import genai
from google.genai import types
import logging
import base64
from typing import Dict, List, Any, Optional

logger = logging.getLogger(__name__)


class ImageGenerationService:
    def __init__(self, project_id: str, location: str):
        self.project_id = project_id
        self.location = location
        self.client = genai.Client(
            vertexai=True,
            project=project_id,
            location=location,
        )
        self.model = os.environ.get("GEMINI_IMAGE_MODEL", "gemini-3.1-flash-image")

    def generate_image(
        self,
        product_id: str,
        product_data: Dict[str, Any],
        image_base64: str,
        background_prompt: str,
        person_description: Optional[str] = None,
        style: str = "photorealistic",
    ) -> Dict:
        """
        Generate an enhanced image for a product using Gemini, based on a base image.
        Can either change the background or add a person wearing the product.

        Args:
            product_id: ID of the product.
            product_data: Dictionary containing product data.
            image_base64: Base64 encoded string of the base product image.
            background_prompt: Text description for the desired background (used if person_description is None).
            person_description: Optional text description of a person to add, wearing the product.
                                If provided, background_prompt might be ignored or used contextually.
            style: Visual style for the image (e.g., "photorealistic").

        Returns:
            Dictionary with product_id and generated_image_base64, or an error.
        """
        logger.info(f"Generating enhanced image for product {product_id}")

        # Decode base image
        try:
            # Ensure padding is correct for base64 decoding
            missing_padding = len(image_base64) % 4
            if missing_padding:
                image_base64 += "=" * (4 - missing_padding)
            image_bytes = base64.b64decode(image_base64)
        except Exception as e:
            logger.error(f"Error decoding base64 image for product {product_id}: {e}")
            return {
                "product_id": product_id,
                "error": "Invalid base64 image format",
            }

        # Build the appropriate prompt based on whether a person is requested
        prompt_text = self._build_gemini_image_prompt(
            product_data=product_data,
            background_prompt=background_prompt,
            person_description=person_description,
            style=style,
        )

        # Prepare image part
        # TODO: Determine correct mime_type dynamically or ensure input is consistent
        image_part = types.Part.from_bytes(data=image_bytes, mime_type="image/png")

        # Prepare text part
        text_part = types.Part.from_text(text=prompt_text)

        # Assemble contents for Gemini
        contents = [image_part, text_part]

        # Configure Gemini for IMAGE generation
        # TODO: Fine-tune temperature, top_p etc. for image generation
        generate_content_config = types.GenerateContentConfig(
            temperature=0.8,
            top_p=0.95,
            max_output_tokens=8192,
            response_modalities=["TEXT", "IMAGE"],
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
            # Call Gemini API to generate the image
            response = self.client.models.generate_content(
                model=self.model,
                contents=contents,
                config=generate_content_config,
            )

            try:
                for part in response.candidates[0].content.parts:
                    if hasattr(
                        part, "inline_data"
                    ) and part.inline_data.mime_type.startswith("image/"):
                        # Ensure the data is properly base64 encoded string, not bytes
                        if isinstance(part.inline_data.data, bytes):
                            image_b64 = base64.b64encode(part.inline_data.data).decode(
                                "ascii"
                            )
                        else:
                            image_b64 = part.inline_data.data

                        return {
                            "product_id": product_id,
                            "generated_image_base64": image_b64,
                        }
            except (AttributeError, IndexError):
                pass

            try:
                for part in response.parts:
                    if hasattr(
                        part, "inline_data"
                    ) and part.inline_data.mime_type.startswith("image/"):
                        # Ensure the data is properly base64 encoded string, not bytes
                        if isinstance(part.inline_data.data, bytes):
                            image_b64 = base64.b64encode(part.inline_data.data).decode(
                                "ascii"
                            )
                        else:
                            image_b64 = part.inline_data.data

                        return {
                            "product_id": product_id,
                            "generated_image_base64": image_b64,
                        }
            except (AttributeError, IndexError):
                pass

            error_message = "Gemini response did not contain valid image data"
            logger.error(f"{error_message} for product {product_id}")

            placeholder_base64 = "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mNkYAAAAAYAAjCB0C8AAAAASUVORK5CYII="

            return {
                "product_id": product_id,
                "generated_image_base64": placeholder_base64,
            }

        except Exception as e:
            logger.error(f"Error calling Gemini API for image generation: {str(e)}")
            return {
                "product_id": product_id,
                "error": f"Gemini API error: {str(e)}",
            }

    # --- Helper methods ---

    def _build_gemini_image_prompt(
        self,
        product_data: Dict[str, Any],
        background_prompt: str,
        person_description: Optional[str] = None,
        style: str = "photorealistic",
    ) -> str:
        """Build a prompt for Gemini image generation based on the request type."""

        product_name = product_data.get("name", "product")
        # Extract key details for the prompt
        brands = product_data.get("brands", [])
        brand = brands[0] if brands else ""
        categories = product_data.get("categories", [])
        category = categories[0] if categories else ""
        attributes = {
            attr["key"]: attr["value"]["text"][0]
            for attr in product_data.get("attributes", [])
            if "text" in attr.get("value", {})
        }
        color = attributes.get("color", "")
        material = attributes.get("material", "")

        product_description = f"{color} {material} {category} named {product_name}"
        if brand:
            product_description += f" by {brand}"

        if person_description:
            # Case 1: Add a person wearing the product
            prompt = f"Generate a {style} image of {person_description} wearing the {product_description}. "
            prompt += f"The background should be suitable for a lifestyle product shot. Professional photography."
        else:
            # Case 2: Change the background of the product image
            prompt = f"Place the product against the following background: {background_prompt}. Professional product photography. The product needs to be in the exact same angle"

        return prompt
