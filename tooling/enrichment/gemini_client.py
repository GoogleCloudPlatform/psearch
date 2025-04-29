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
import vertexai
from vertexai.generative_models import GenerativeModel, Part, SafetySetting

def init_gemini(project_id):
    """Initialize Gemini client."""
    vertexai.init(project=project_id, location="us-central1")

def get_image_description(image_bytes, project_id, product_data):
    """Generate image description using Vertex AI Gemini Flash."""
    init_gemini(project_id)
    
    model = GenerativeModel("gemini-1.5-flash-001")
    
    generation_config = {
        "max_output_tokens": 8192,
        "temperature": 0.2,
        "top_p": 0.95,
    }
    
    safety_settings = [
        SafetySetting(
            category=SafetySetting.HarmCategory.HARM_CATEGORY_HATE_SPEECH,
            threshold=SafetySetting.HarmBlockThreshold.OFF
        ),
        SafetySetting(
            category=SafetySetting.HarmCategory.HARM_CATEGORY_DANGEROUS_CONTENT,
            threshold=SafetySetting.HarmBlockThreshold.OFF
        ),
        SafetySetting(
            category=SafetySetting.HarmCategory.HARM_CATEGORY_SEXUALLY_EXPLICIT,
            threshold=SafetySetting.HarmBlockThreshold.OFF
        ),
        SafetySetting(
            category=SafetySetting.HarmCategory.HARM_CATEGORY_HARASSMENT,
            threshold=SafetySetting.HarmBlockThreshold.OFF
        ),
    ]
    
    try:
        image_part = Part.from_data(data=image_bytes, mime_type="image/png")
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
        
        response = model.generate_content(
            [prompt, image_part],
            generation_config=generation_config,
            safety_settings=safety_settings,
        )
        
        return response.text
    except Exception as e:
        print(f"Error generating image description: {str(e)}")
        return None 