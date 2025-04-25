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

from google import genai
from google.genai import types
from google.api_core import exceptions as core_exceptions
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
)
import requests
from typing import Dict, Any
import logging
import mimetypes


class GeminiService:
    def __init__(self, project_id: str, location: str):
        """
        Initialize Gemini service using the google-genai SDK with Vertex AI backend.

        Args:
            project_id: Google Cloud project ID
            location: Region for Vertex AI services
        """
        # Initialize the client specifying Vertex AI usage
        self.client = genai.Client(
            vertexai=True,
            project=project_id,
            location="us-central1",
        )
        self.model_name = "gemini-2.0-flash-001"

    def _get_image_content(self, image_url: str):
        """
        Download image and prepare it for Gemini vision analysis.

        Args:
            image_url: URL of the product image

        Returns:
            Part: Image part for Gemini model
        """
        try:
            return types.Part.from_uri(file_uri=image_url, mime_type="image/png")
        except Exception as e:
            logging.error(f"Error processing image from {image_url}: {str(e)}")
            return None

    @retry(
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=2, min=2, max=10),
        retry=retry_if_exception_type(core_exceptions.ResourceExhausted),
        before_sleep=lambda retry_state: logging.warning(
            f"Rate limit hit for product. Retrying in {retry_state.next_action.sleep:.2f} seconds... (Attempt {retry_state.attempt_number})"
        ),
        retry_error_callback=lambda retry_state: logging.error(
            f"Max retries reached after rate limit errors. Last exception: {retry_state.outcome.exception()}"
        ),  # Logs error before tenacity re-raises the exception
    )
    def generate_product_content(self, product: Dict[str, Any]) -> str:
        """
        Generate enhanced product content using Gemini, incorporating both text and image analysis, with tenacity retry on rate limits.

        Args:
            product: Dictionary containing product data with fields:
                    id, title, categories, brands, image_url, product_url

        Returns:
            str: Enhanced product description combining title, description and attributes
        """
        image_part = None
        if product.get("image_url"):
            try:
                image_part = self._get_image_content(product["image_url"])
                if not image_part:
                    logging.warning(
                        f"Could not retrieve image part for {product.get('id', 'N/A')}"
                    )
            except Exception as e:
                logging.warning(
                    f"Failed to get image content for {product.get('id', 'N/A')}: {str(e)}"
                )

        prompt = f"""
            Analyze the provided product information and image (if supplied) to generate enriched e-commerce content and combine it into a single text chunk optimized for semantic search embedding. If an image is provided, incorporate visual details into your analysis.

            **Input Product Data:**
            * Title: {product.get('title', 'N/A')}
            * Brand: {product.get('brands', 'N/A')}
            * Provided Description: {product.get('description', 'N/A')}
            * Categories: {product.get('categories', 'N/A')}
            * Product URL: {product.get('product_url', 'N/A')}
            * Target Audience: [Optional: Specify your target audience, e.g., 'Outdoor Enthusiasts']
            * Keywords to consider: [Optional: Specify keywords from input, e.g., 'lightweight, durable']

            **Instructions:**
            Based *only* on the provided data and the included image (if applicable), first conceptually generate the following components focusing on information that helps users find this product via search:
            1.  An SEO-optimized product title (under 70 characters, include key identifying terms).
            2.  A compelling product description (150-300 words) integrating details from the input data and visual analysis (if image provided). Focus on searchable attributes and benefits.
            3.  A list of 3-5 key product features/highlights (as descriptive strings, focusing on searchable characteristics, including visual ones if image provided).
            4.  A list of 2-3 relevant use cases or applications (as strings, indicating context).
            5.  A list of 5-7 relevant keywords or search terms (as strings) that accurately represent the product.

            **Final Output Construction:**
            Combine the generated components above into a **single block of plain text** specifically formatted for embedding. Use the following structure, separating elements clearly. Use commas to separate items within lists (Features, Keywords, Use Cases):

            Title: [Generated SEO Title].
            Description: [Generated Product Description] 
            Features: [Feature 1], [Feature 2], [Feature 3]. 
            Keywords: [Keyword 1], [Keyword 2], [Keyword 3]. 
            Use Cases: [Use Case 1], [Use Case 2]. 

            **Output Format:**
            Return **only** the final combined text chunk as plain text. Do not include any headers, explanations, markdown formatting, or JSON structure. Ensure the output is a single continuous block of text following the specified construction format. Prioritize accuracy and relevance for search embedding. Do not invent information not supported by the provided inputs or image.
        """

        parts_list = [types.Part.from_text(text=prompt)]
        if image_part:
            parts_list.append(image_part)

        main_contents = [types.Content(role="user", parts=parts_list)]

        system_instruction = f""""
            You are an AI expert specializing in e-commerce content generation for semantic search embedding. Your task is to analyze provided product data, including text details and an image (if supplied), to create a concise, information-rich text chunk optimized for search relevance.

            **Core Task:**
            Analyze the input product information (title, brand, description, categories, URL, optional target audience, optional keywords) and the provided image (if available). Based *strictly* on this information, generate enriched content components. Then, combine these components into a single, plain text block formatted specifically for embedding.

            **Analysis and Generation Steps (Internal Process):**
            1.  **Input Review:** Carefully examine all provided text fields (Title, Brand, Description, Categories, URL, Target Audience, Keywords). If an image is provided, analyze its visual details (e.g., color, shape, texture, context, depicted use).
            2.  **Information Extraction:** Identify key features, benefits, attributes, potential uses, and target concepts present in the text and image. Integrate visual details from the image analysis where relevant (e.g., "sleek black finish," "compact design visible in image").
            3.  **Component Generation:** Create the following components, focusing solely on details derived from the provided inputs and prioritizing search relevance:
                *   **SEO-Optimized Title:** Under 70 characters, including key identifying terms from the input and image analysis.
                *   **Compelling Description:** 150-300 words. Integrate details from the input text (brand, categories, provided description) and visual analysis (if image provided). Focus on searchable attributes, benefits, and visual cues. Use provided keywords and target audience information as guidance if available.
                *   **Key Features:** A list of 3-5 descriptive strings highlighting searchable characteristics, including visual ones identified from the image.
                *   **Use Cases/Applications:** A list of 2-3 relevant strings suggesting contexts or ways the product can be used, derived from inputs/image.
                *   **Keywords/Search Terms:** A list of 5-7 relevant strings accurately representing the product, based on all analyzed inputs.

            **Output Construction and Formatting:**
            1.  **Combine Components:** Assemble the generated components into a **single block of plain text**.
            2.  **Strict Format:** Use the following structure precisely, separating components as shown. Use commas without trailing spaces to separate items within the Features, Keywords, and Use Cases lists.

                ```text
                Title: [Generated SEO Title].
                Description: [Generated Product Description]
                Features: [Feature 1],[Feature 2],[Feature 3].
                Keywords: [Keyword 1],[Keyword 2],[Keyword 3],[Keyword 4],[Keyword 5].
                Use Cases: [Use Case 1],[Use Case 2].
                ```
            3.  **Output Purity:** Return **ONLY** this combined text block. Do **NOT** include any introductory phrases, concluding remarks, explanations, markdown formatting (like ```), headers, titles (other than the specified "Title:" label within the block), or JSON structure. The output must be the single, continuous plain text chunk ready for embedding.

            **Constraint Checklist:**
            *   Use *only* provided data and image. No external knowledge.
            *   Incorporate image analysis if image provided.
            *   Generate all specified components according to constraints.
            *   Adhere strictly to the specified single-block plain text format with labels and comma separation for lists.
            *   Output *only* the formatted text block.
        """

        generate_content_config = types.GenerateContentConfig(
            temperature=0.5,
            top_p=0.95,
            max_output_tokens=2048,
            response_modalities=["TEXT"],
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
            system_instruction=[types.Part.from_text(text=system_instruction)],
        )

        # The actual API call remains the same, tenacity wraps the execution.
        response = self.client.models.generate_content(
            model=self.model_name,
            contents=main_contents,
            config=generate_content_config,
        )
        return response.text
