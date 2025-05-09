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

# Spanner instance configuration
resource "google_spanner_instance" "psearch_spanner" {
  name             = "psearch-spanner"
  config           = "regional-${var.region}"
  display_name     = "PSearch Spanner DB"
  processing_units = var.processing_units
  edition          = "ENTERPRISE"

  # Ensure we have enough processing units for vector search
  lifecycle {
    precondition {
      condition     = var.processing_units >= 1000
      error_message = "Processing units must be at least 1000 for vector search capabilities."
    }
  }

  labels = {
    "environment" = var.environment
  }
}

# Spanner database
resource "google_spanner_database" "products_db" {
  instance                 = google_spanner_instance.psearch_spanner.name
  name                     = var.database_name
  version_retention_period = "1d"
  deletion_protection      = false

  ddl = [
    "CREATE TABLE products (product_id STRING(MAX), product_data JSON, title STRING(MAX), title_tokens TOKENLIST AS (TOKENIZE_FULLTEXT(title)) HIDDEN, embedding ARRAY<FLOAT32>(vector_length=>768)) PRIMARY KEY(product_id)",
    "CREATE SEARCH INDEX products_by_title ON products(title_tokens)",
    "CREATE VECTOR INDEX products_by_embedding ON products(embedding) WHERE embedding IS NOT NULL OPTIONS(distance_type=\"COSINE\", num_leaves=1000)"
  ]
}

# Grant service account access to Spanner
resource "google_project_iam_member" "spanner_user" {
  project = var.project_id
  role    = "roles/spanner.databaseUser"
  member  = "serviceAccount:${var.service_account_email}"
}
