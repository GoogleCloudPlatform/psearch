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
from dataclasses import dataclass

from google.genai import types

from gemini_client import get_genai_client

# Issue #11: imagen-3.0-generate-002 (deprecated `vertexai.preview.vision_models`)
# replaced by Gemini native image generation via the Google Gen AI SDK.
GEMINI_IMAGE_MODEL = os.environ.get("GEMINI_IMAGE_MODEL", "gemini-3.1-flash-image")


@dataclass
class GeneratedImage:
    image_bytes: bytes
    mime_type: str = "image/png"


def generate_image(row_data, project_id):
    """Generate a product image with Gemini image generation (Vertex AI, GENAI_LOCATION from gemini_client)."""
    client = get_genai_client(project_id)

    prompt = f"""Create a professional product image for an e-commerce listing:
Product: {row_data['name']}
Brand: {row_data['brand']}
Category: {row_data['category']} in {row_data['department']} department
Style: Clean, well-lit product photography style with white background
Focus: Show the product clearly with attention to detail and key features"""

    try:
        response = client.models.generate_content(
            model=GEMINI_IMAGE_MODEL,
            contents=prompt,
            config=types.GenerateContentConfig(
                response_modalities=["TEXT", "IMAGE"],
                image_config=types.ImageConfig(aspect_ratio="1:1"),
            ),
        )

        for candidate in response.candidates or []:
            if not candidate.content:
                continue
            for part in candidate.content.parts or []:
                if part.inline_data and part.inline_data.data:
                    return GeneratedImage(
                        image_bytes=part.inline_data.data,
                        mime_type=part.inline_data.mime_type or "image/png",
                    )

        print(f"No images generated for product: {row_data['name']}")
        return None
    except Exception as e:
        print(f"Error generating image for product {row_data['name']}: {str(e)}")
        print(f"Prompt used: {prompt}")
        return None
