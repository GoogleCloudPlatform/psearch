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
Conversational Search Service - Generates natural filter questions for product search
"""

import os

from google import genai
from google.genai import types
import logging
from typing import Dict, List, Any, Optional
import json
import random

logger = logging.getLogger(__name__)


class ConversationalSearchResponse:
    def __init__(
        self,
        answer: str,
        suggested_products: List[Dict[str, Any]] = None,
        follow_up_questions: List[str] = None,
    ):
        self.answer = answer
        self.suggested_products = suggested_products or []
        self.follow_up_questions = follow_up_questions or []


class ConversationalSearchService:
    def __init__(self, project_id: str, location: str):
        self.project_id = project_id
        self.location = location
        self.client = genai.Client(
            vertexai=True,
            project=project_id,
            location=location,
        )
        self.model = os.environ.get("GEMINI_LITE_MODEL", "gemini-3.5-flash-lite")

    def process_query(
        self,
        query: str,
        conversation_history: Optional[List[Dict[str, str]]] = None,
        product_context: Optional[Dict[str, Any]] = None,
        max_results: int = 5,
    ) -> ConversationalSearchResponse:
        """
        Generate natural filter questions based on search query and available filters

        Args:
            query: User's search query
            conversation_history: List of previous messages in the conversation
            product_context: Context about products and available filters
            max_results: Maximum number of results to return

        Returns:
            ConversationalSearchResponse object with a greeting, filter mappings as suggested products,
            and natural-sounding filter questions
        """
        logger.info(f"Generating filter questions for query: {query}")
        logger.info(
            f"Available filters: {product_context.get('available_filters') if product_context else 'None'}"
        )

        # Build prompt for filter question generation
        prompt = self._build_filter_questions_prompt(
            query, conversation_history, product_context
        )

        # Create response schema for filter questions
        response_schema = {
            "type": "OBJECT",
            "properties": {
                "greeting": {"type": "STRING"},
                "filter_questions": {
                    "type": "ARRAY",
                    "items": {"type": "STRING"},
                    "description": "5-7 natural, conversational questions to help filter products",
                },
                "filter_mappings": {
                    "type": "ARRAY",
                    "items": {
                        "type": "OBJECT",
                        "properties": {
                            "id": {"type": "STRING"},
                            "question": {"type": "STRING"},
                            "reason": {"type": "STRING"},
                        },
                    },
                    "description": "Mappings between filter IDs and natural questions",
                },
            },
            "required": ["greeting", "filter_questions", "filter_mappings"],
        }

        # Set up generation configuration
        generate_content_config = types.GenerateContentConfig(
            temperature=0.7,  # Higher temperature for more creative and varied responses
            top_p=0.95,
            max_output_tokens=2048,
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
            parsed_response = self._parse_response(response.text)
            logger.info(
                f"Generated {len(parsed_response.get('filter_questions', []))} filter questions"
            )

            # Extract data from parsed response
            greeting = parsed_response.get(
                "greeting", f"Here are some suggestions for your search: {query}"
            )
            filter_questions = parsed_response.get("filter_questions", [])
            filter_mappings = parsed_response.get("filter_mappings", [])

            # Ensure we have at least 5 questions
            if len(filter_questions) < 5:
                # Add placeholder questions if we don't have enough AI-generated ones
                logger.warning(
                    f"Not enough filter questions generated ({len(filter_questions)}), adding placeholders"
                )
                filter_questions.extend(
                    [
                        f"Would you like to narrow down these {query} results more?",
                        f"Any specific features you're looking for in {query}?",
                        f"Do you have a preferred price range for {query}?",
                        f"Any particular brand you prefer for {query}?",
                        f"Are you looking for something for a special occasion?",
                    ][: 5 - len(filter_questions)]
                )

            # Make sure filter_mappings has entries for every available filter
            if product_context and "available_filters" in product_context:
                # Create a set of filter IDs already mapped
                mapped_ids = {
                    mapping.get("id")
                    for mapping in filter_mappings
                    if mapping.get("id")
                }

                # Check for any filters not yet mapped and add them
                for filter_info in product_context["available_filters"]:
                    filter_id = filter_info.get("id")
                    if filter_id and filter_id not in mapped_ids:
                        # Generate a natural question if none exists
                        question = self._create_natural_question_for_filter(
                            filter_id, filter_info.get("title", "")
                        )
                        filter_mappings.append(
                            {"id": filter_id, "question": question, "reason": question}
                        )

            # Limit to max_results
            filter_questions = filter_questions[
                : max_results + 2
            ]  # Add a couple extra for variety

            return ConversationalSearchResponse(
                answer=greeting,
                suggested_products=filter_mappings,
                follow_up_questions=filter_questions,
            )

        except Exception as e:
            logger.error(f"Error generating filter questions: {str(e)}")
            return ConversationalSearchResponse(
                answer=f"I can help you find the right {query}. What are you looking for?",
                suggested_products=[],
                follow_up_questions=[
                    f"What type of {query} are you interested in?",
                    "What's your budget?",
                    "Do you have a specific brand in mind?",
                    "What features are most important to you?",
                    "Are you looking for something with specific characteristics?",
                ],
            )

    def _build_filter_questions_prompt(
        self,
        query: str,
        conversation_history: Optional[List[Dict[str, str]]] = None,
        product_context: Optional[Dict[str, Any]] = None,
    ) -> List[types.Content]:
        """Build the prompt to generate natural filter questions"""

        instruction = """
        You are an expert shopping assistant who helps users find products through natural conversation.
        
        Your task is to create engaging, conversational filter questions that feel natural and helpful.
        These questions should help narrow down search results while sounding like something a human assistant would ask.
        
        For each available filter type (categories, brands, price ranges, colors, etc.):
        1. Create natural-sounding questions that help users refine their search 
        2. Map each question to the appropriate filter ID in the system
        3. Create 5-7 different follow-up questions that sound varied and conversational
        
        Your response should include:
        1. A brief, friendly greeting that acknowledges the search query
        2. A list of 5-7 natural-sounding filter questions that feel conversational
        3. A mapping between filter IDs and natural questions for the system to use
        
        Some IMPORTANT guidelines:
        - Questions should sound natural, NOT mechanical or formulaic
        - Each question should focus on a different aspect of filtering
        - Questions should incorporate knowledge of the product domain when possible
        - Avoid questions that sound robotic like "Looking for specific [filter]?"
        - Don't repeat the same question patterns with different words
        
        Format your response as JSON with the following structure:
        {
            "greeting": "Brief, friendly response acknowledging search",
            "filter_questions": [
                "Natural question 1?",
                "Natural question 2?",
                "Natural question 3?",
                "Natural question 4?",
                "Natural question 5?",
                "Natural question 6?",
                "Natural question 7?"
            ],
            "filter_mappings": [
                {
                    "id": "filter_id_from_context",
                    "question": "Natural question for this filter type?",
                    "reason": "Same as question - will be displayed to user"
                }
            ]
        }
        """

        user_prompt = f"{instruction}\n\nUser Search Query: {query}\n\n"

        # Add conversation history if available
        if conversation_history and len(conversation_history) > 0:
            user_prompt += "Previous conversation:\n"
            for message in conversation_history:
                role = message.get("role", "unknown")
                content = message.get("content", "")
                user_prompt += f"{role}: {content}\n"
            user_prompt += "\n"

        # Add product context if available
        if product_context:
            user_prompt += "Available filters and product context:\n"
            user_prompt += json.dumps(product_context, indent=2)
            user_prompt += "\n\n"

        user_prompt += "Generate natural filter questions and mappings in the specified JSON format."

        # Create the contents for the Gemini model
        contents = [
            types.Content(role="user", parts=[types.Part.from_text(text=user_prompt)])
        ]

        return contents

    def _parse_response(self, response_text: str) -> Dict[str, Any]:
        """Parse the response from the Gemini model"""
        try:
            # Try to parse the response as JSON
            response_json = json.loads(response_text)
            return response_json
        except json.JSONDecodeError:
            # If JSON parsing fails, extract what we can
            logger.warning(f"Failed to parse response as JSON: {response_text}")

            # Return a simple structure
            return {
                "greeting": "I can help you find what you're looking for.",
                "filter_questions": [],
                "filter_mappings": [],
            }

    def _create_natural_question_for_filter(
        self, filter_id: str, filter_title: str
    ) -> str:
        """Create a natural-sounding question for a filter type if AI doesn't provide one"""

        # Templates for different filter types
        templates = {
            "categories": [
                "What type or style are you interested in?",
                "Which category catches your eye?",
                "Any specific style you're gravitating towards?",
                "What kind of {title} would work best for you?",
            ],
            "brands": [
                "Do you have a favorite brand in mind?",
                "Any particular brands you love or would like to explore?",
                "Are there specific brands you trust for {title}?",
                "Would you prefer a well-known brand or something more unique?",
            ],
            "prices": [
                "What's your budget looking like?",
                "How much are you thinking of spending?",
                "Any particular price range you're comfortable with?",
                "Are you looking for something more premium or budget-friendly?",
            ],
            "colors": [
                "Any specific color you have in mind?",
                "What colors would complement your style?",
                "Do you prefer something bold or more neutral?",
                "Any favorite colors or shades you're drawn to?",
            ],
            "sizes": [
                "What size works best for you?",
                "Do you need a specific size or fit?",
                "What size are you typically comfortable with?",
                "Any specific sizing requirements I should know about?",
            ],
            "availability": [
                "Do you need it right away or can you wait for something special?",
                "Are you looking for something that's in stock and ready to ship?",
                "How soon do you need this?",
                "Is immediate availability important to you?",
            ],
        }

        # Check if we have templates for this filter type
        templates_to_use = None

        # Check for exact matches
        if filter_id in templates:
            templates_to_use = templates[filter_id]
        # Check for filter_id that starts with one of our categories
        else:
            for key in templates:
                if filter_id.startswith(key) or filter_id.endswith(key):
                    templates_to_use = templates[key]
                    break

        # If no specific templates found, use generic ones
        if not templates_to_use:
            templates_to_use = [
                f"Do you have any preferences for {filter_title}?",
                f"Any specific {filter_title} you're looking for?",
                f"What's important to you when it comes to {filter_title}?",
                f"How would you like to filter by {filter_title}?",
            ]

        # Select a random template and format it with the filter title
        template = random.choice(templates_to_use)
        return template.format(title=filter_title.lower())
