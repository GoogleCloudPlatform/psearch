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
from typing import List, Dict, Any, Tuple
import logging
from datetime import datetime
import json
from time import sleep
import numpy as np
from google import genai
from google.genai.types import EmbedContentConfig

from .services.bigquery_service import BigQueryService
from .services.spanner_service import SpannerService
from .services.gemini_service import GeminiService


class IngestionController:
    def __init__(self):
        # Get environment variables
        self.project_id = os.environ["PROJECT_ID"]
        self.location = os.environ["REGION"]
        self.enrichiment = os.environ.get("ENRICHIMENT", False)
        self.model_name = os.environ.get(
            "DENSE_MODEL_NAME", "text-multilingual-embedding-002"
        )
        self.dimensions = int(os.environ.get("DENSE_DIMENSIONS", "768"))

        # Initialize services
        self.bq_service = BigQueryService(self.project_id)
        self.spanner_service = SpannerService(self.project_id)
        self.gemini_service = GeminiService(self.project_id, self.location)

        # Initialize the embedding client
        self._init_embedding_client()

    def _init_embedding_client(self):
        """Initialize the Google Generative AI client for embeddings"""
        logging.info(f"Initializing Google embedding model: {self.model_name}")

        # Initialize the genai client with Vertex AI
        self.embedding_client = genai.Client(
            vertexai=True,
            project=self.project_id,
            location=self.location,
        )

        logging.info(f"Google embedding client initialized successfully")

    def generate_embeddings_batch(self, texts: List[str]) -> List[np.ndarray]:
        """
        Generate embeddings for a batch of texts (up to 100)

        Args:
            texts: List of input texts to generate embeddings for

        Returns:
            List of embedding vectors
        """
        try:
            # Use Google's embedding model for batch processing
            response = self.embedding_client.models.embed_content(
                model=self.model_name,
                contents=texts,
                config=EmbedContentConfig(
                    task_type="RETRIEVAL_DOCUMENT",
                    output_dimensionality=self.dimensions,
                ),
            )

            # Extract embeddings from the response
            embedding_arrays = []
            for embedding in response.embeddings:
                embedding_arrays.append(np.array(embedding.values))

            return embedding_arrays

        except Exception as e:
            logging.error(f"Error generating batch embeddings: {str(e)}")
            raise

    def get_product_text(self, product: Dict[str, Any]) -> str:
        """Prepare text for embedding based on enrichment flag"""
        if self.enrichiment:
            return self._prepare_product_text(product)
        else:
            return product.get("description", "")

    def _prepare_product_text(self, product: Dict[str, Any]) -> str:
        """
        Prepare product text for embedding by generating enhanced content
        using product details and image analysis
        """
        product_data = {
            "title": product.get("title"),
            "brands": product.get("brands"),
            "description": product.get("description"),
            "categories": product.get("categories"),
            "image_url": product.get("images")[0][
                "uri"
            ],  # TODO Adjust this later to process all the images in the array
            "product_url": product.get("uri"),
        }

        return self.gemini_service.generate_product_content(product_data)

    def run(self):
        """Run the ingestion process"""
        try:
            # Extract products from BigQuery
            query = f"""
            SELECT *
            FROM `{self.project_id}.products_retail_search_rich.products_enriched_3`
            WHERE availability = 'IN_STOCK'
            """
            products = self.bq_service.extract_product_data(query)
            logging.info(f"Extracted {len(products)} products from BigQuery")

            # Process in batches of 100 (maximum allowed for embedding API)
            batch_size = 50
            for i in range(0, len(products), batch_size):
                batch = products[i : i + batch_size]

                # Prepare product ids and texts for batch embedding
                product_ids = []
                product_texts = []

                for product in batch:
                    product_id = product["id"]
                    product_ids.append(product_id)

                    # Get text for embedding
                    product_text = self.get_product_text(product)
                    product_texts.append(product_text)

                # Generate embeddings in batch
                embeddings = self.generate_embeddings_batch(product_texts)

                # Prepare data for Spanner storage (including embeddings) - Step 2 of plan
                products_to_store = []
                for idx, product in enumerate(batch):
                    product_id = product.get("id")
                    if not product_id:
                        logging.warning(
                            f"Product missing ID in batch, skipping: {product.get('title')}"
                        )
                        continue

                    products_to_store.append(
                        {
                            "id": product_id,
                            "data": product,  # Pass the full original product data
                            "title": product.get(
                                "title", ""
                            ),  # Extract title explicitly
                            "embedding": embeddings[
                                idx
                            ].tolist(),  # Add the embedding list
                        }
                    )

                # Store product data and embeddings in Spanner batch - Step 3 of plan
                if products_to_store:  # Check if there are valid products to store
                    self.spanner_service.store_products_batch(products_to_store)
                else:
                    logging.warning(
                        f"No valid products with IDs found in batch {i//batch_size + 1}, skipping Spanner storage for this batch."
                    )

                logging.info(
                    f"Processed batch of {len(batch)} products (batch {i//batch_size + 1}/{(len(products)-1)//batch_size + 1})"
                )

                # Add small delay between batches
                if i + batch_size < len(products):
                    sleep(1)

            logging.info("Ingestion completed successfully using batch mode")

        except Exception as e:
            logging.error(f"Ingestion failed: {str(e)}")
            raise


def main():
    """Entry point for ingestion job"""
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
    )

    controller = IngestionController()
    controller.run()


if __name__ == "__main__":
    main()
