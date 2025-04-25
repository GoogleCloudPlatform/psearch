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

from typing import Dict, Any, List
from google.cloud import spanner
import json
import logging
import os
import math


class SpannerService:
    def __init__(self, project_id: str):
        """
        Initialize Spanner client and database connections

        Args:
            project_id: Google Cloud project ID
        """
        self.project_id = project_id
        self.instance_id = os.environ.get("SPANNER_INSTANCE_ID")
        self.database_id = os.environ.get("SPANNER_DATABASE_ID")

        if not self.instance_id or not self.database_id:
            raise ValueError("SPANNER_INSTANCE_ID and SPANNER_DATABASE_ID must be set")

        # Initialize Spanner client
        self.client = spanner.Client(project=project_id)
        self.instance = self.client.instance(self.instance_id)
        self.database = self.instance.database(self.database_id)

        logging.info(f"Initialized Spanner service for database: {self.database_id}")

    def store_product(self, product_id: str, product_data: Dict[str, Any]) -> bool:
        """
        Store product data in Spanner

        Args:
            product_id: Unique product identifier
            product_data: Product metadata

        Returns:
            bool: True if successful, False otherwise
        """
        try:
            # Convert non-serializable objects to serializable format
            product_json = self._prepare_product_for_spanner(product_data)

            # Prepare mutation to insert or update product
            with self.database.batch() as batch:
                batch.insert_or_update(
                    table="products",
                    columns=[
                        "product_id",
                        "product_data",
                        "title",
                        "description",
                        "timestamp",
                    ],
                    values=[
                        (
                            product_id,
                            product_json,
                            product_data.get("title", ""),
                            product_data.get("description", ""),
                            spanner.COMMIT_TIMESTAMP,
                        )
                    ],
                )

            logging.info(f"Successfully stored product {product_id} in Spanner")
            return True

        except Exception as e:
            logging.error(f"Error storing product {product_id} in Spanner: {str(e)}")
            return False

    def store_products_batch(self, products: List[Dict[str, Any]]) -> bool:
        """
        Store multiple products in Spanner in a batch

        Args:
            products: List of product dictionaries with "id" field

        Returns:
            bool: True if successful, False otherwise
        """
        if not products:
            return True

        try:
            # Prepare all valid products for batch insertion
            batch_values = []
            for product_info in products:
                product_id = product_info.get("id")
                if not product_id:
                    logging.warning("Product info missing ID, skipping")
                    continue

                product_data_dict = product_info.get(
                    "data", {}
                )  # Get the original product data dict
                product_json = self._prepare_product_for_spanner(product_data_dict)
                title = product_info.get("title", "")
                embedding = product_info.get("embedding")

                if embedding is None:
                    logging.warning(f"Product {product_id} missing embedding, skipping")
                    continue

                batch_values.append(
                    (
                        product_id,
                        product_json,
                        title,
                        embedding,
                    )
                )

            # If no valid products, return early
            if not batch_values:
                logging.warning("No valid products to insert")
                return True

            # Perform a single batch operation
            with self.database.batch() as batch:
                batch.insert_or_update(
                    table="products",
                    columns=[
                        "product_id",
                        "product_data",
                        "title",
                        "embedding",  # Updated columns list
                    ],
                    values=batch_values,
                )

            logging.info(
                f"Successfully stored {len(batch_values)} products in Spanner batch"
            )
            return True

        except Exception as e:
            logging.error(f"Error batch storing products in Spanner: {str(e)}")
            raise e

    def _prepare_product_for_spanner(self, product_data: Dict[str, Any]) -> str:
        """
        Prepares product data for storage in Spanner by converting to JSON
        and handling non-serializable values

        Args:
            product_data: Raw product data

        Returns:
            str: JSON string representation of product data
        """
        # Create a copy to avoid modifying the original
        product_copy = product_data.copy()

        # Recursively sanitize the product data
        def sanitize_value(value):
            if isinstance(value, dict):
                return {k: sanitize_value(v) for k, v in value.items()}
            elif isinstance(value, list):
                return [sanitize_value(item) for item in value]
            elif isinstance(value, (int, bool, str)) or value is None:
                return value
            elif isinstance(value, float):
                # Handle NaN and Infinity which Spanner doesn't accept
                if math.isnan(value) or math.isinf(value):
                    return None

                # Handle very large or very small numbers that might cause issues
                if abs(value) > 1e15:  # Very large numbers
                    # Round to avoid precision issues
                    return round(value, 2)
                elif 0 < abs(value) < 1e-10:  # Very small non-zero numbers
                    # Round to avoid precision issues with very small numbers
                    return 0.0

                # For price-like values (common in product data), round to 2 decimal places
                if 0.01 <= abs(value) <= 10000.0:
                    return round(value, 2)

                # For other normal range numbers, ensure we don't have too many decimal places
                return round(value, 6)
            else:
                # Convert any other type to string
                return str(value)

        # Sanitize the entire product
        sanitized_product = sanitize_value(product_copy)

        # Special handling for priceInfo to ensure consistent formatting
        if "priceInfo" in sanitized_product:
            price_info = sanitized_product["priceInfo"]
            if "price" in price_info and isinstance(price_info["price"], float):
                price_info["price"] = round(price_info["price"], 2)
            if "cost" in price_info and isinstance(price_info["cost"], float):
                price_info["cost"] = round(price_info["cost"], 2)
            if "originalPrice" in price_info and isinstance(
                price_info["originalPrice"], float
            ):
                price_info["originalPrice"] = round(price_info["originalPrice"], 2)

        # Convert to JSON string with special handling for Spanner
        json_str = json.dumps(sanitized_product, ensure_ascii=True)

        return json_str
