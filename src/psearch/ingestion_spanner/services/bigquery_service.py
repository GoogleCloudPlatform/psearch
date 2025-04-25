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

from typing import List, Dict, Any
from google.cloud import bigquery
import logging


class BigQueryService:
    def __init__(self, project_id: str):
        self.client = bigquery.Client(project=project_id)

    def extract_product_data(self, query: str) -> List[Dict[str, Any]]:
        """
        Extract product data from BigQuery

        Args:
            query: SQL query to extract product data

        Returns:
            List of product dictionaries
        """
        try:
            query_job = self.client.query(query)
            results = query_job.result()

            products = []
            for row in results:
                product = dict(row.items())
                products.append(product)

            logging.info(f"Extracted {len(products)} products from BigQuery")
            return products

        except Exception as e:
            logging.error(f"Error extracting products from BigQuery: {str(e)}")
            raise
