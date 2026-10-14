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
from functools import lru_cache

from google import genai
from google.genai import types

# Issue #11: migrated from the deprecated `vertexai.generative_models` SDK to the
# Google Gen AI SDK (`google-genai`). Gemini 3.x is served from the Vertex AI
# "global" location, not us-central1.
GEMINI_TEXT_MODEL = os.environ.get("GEMINI_TEXT_MODEL", "gemini-3.5-flash")
GENAI_LOCATION = os.environ.get("GENAI_LOCATION", "global")


@lru_cache(maxsize=None)
def get_genai_client(project_id):
    """Return a cached Vertex AI-backed Gen AI client."""
    return genai.Client(vertexai=True, project=project_id, location=GENAI_LOCATION)


def get_image_description(image_bytes, project_id, product_data):
    """Generate an e-commerce description for a product image with Gemini."""
    client = get_genai_client(project_id)

    safety_settings = [
        types.SafetySetting(category=category, threshold=types.HarmBlockThreshold.OFF)
        for category in (
            types.HarmCategory.HARM_CATEGORY_HATE_SPEECH,
            types.HarmCategory.HARM_CATEGORY_DANGEROUS_CONTENT,
            types.HarmCategory.HARM_CATEGORY_SEXUALLY_EXPLICIT,
            types.HarmCategory.HARM_CATEGORY_HARASSMENT,
        )
    ]
    config = types.GenerateContentConfig(
        max_output_tokens=8192,
        temperature=0.2,
        top_p=0.95,
        safety_settings=safety_settings,
    )

    try:
        image_part = types.Part.from_bytes(data=image_bytes, mime_type="image/png")
        brand_name = product_data.get('brand', 'Unknown Brand')
        product_name = product_data.get('name', '')
        category = product_data.get('category', '')
        retail_price = product_data.get('retail_price', '')

        prompt = f"""Analyze this {brand_name} product image and provide a compelling e-commerce pharmacy description that includes:
1. Product name: {product_name}
2. Brand highlights: Emphasize {brand_name}'s reputation and quality in the {category} category
3. Key product features and specifications
4. Materials and construction quality
5. Colors and design elements
6. Size and dimensions (if visible)
7. Unique selling points and value proposition (considering the retail price of ${retail_price})
8. Target audience or use cases
9. Any visible brand elements or distinctive features

Focus on creating persuasive content that highlights the {brand_name} brand value and helps shoppers make a confident purchase decision."""
        
        response = client.models.generate_content(
            model=GEMINI_TEXT_MODEL,
            contents=[prompt, image_part],
            config=config,
        )

        return response.text
    except Exception as e:
        print(f"Error generating image description: {str(e)}")
        return None
