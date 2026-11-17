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

# Create a service account for the ingestion task
resource "google_service_account" "ingestion_service_account" {
  account_id   = "ingestion-service-account"
  display_name = "Ingestion Service Account"
  description  = "Service account for ingestion tasks"
  project      = var.project_id
}

# Create a service account for the embeddings function
resource "google_service_account" "embeddings_service_account" {
  account_id   = "embeddings-service-account"
  display_name = "Embeddings Service Account"
  description  = "Service account for embeddings generation function"
  project      = var.project_id
}

# Grant least-privilege runtime roles to Ingestion Service Account (Spanner / BQ Data Transfer)
resource "google_project_iam_member" "grant_sa_function_roles" {
  for_each = toset([
    "roles/bigquery.user",
    "roles/spanner.databaseUser",
    "roles/aiplatform.user",
    "roles/logging.logWriter",
    "roles/serviceusage.serviceUsageConsumer"
  ])

  role    = each.key
  project = var.project_id
  member  = "serviceAccount:${google_service_account.ingestion_service_account.email}"
}

# Grant Roles to Embeddings Service Account
resource "google_project_iam_member" "grant_embeddings_sa_roles" {
  for_each = toset([
    "roles/aiplatform.user",
    "roles/aiplatform.serviceAgent",
    "roles/storage.objectViewer",
    "roles/cloudfunctions.invoker",
    "roles/run.invoker",
    "roles/logging.logWriter",
    "roles/serviceusage.serviceUsageConsumer"
  ])

  role    = each.key
  project = var.project_id
  member  = "serviceAccount:${google_service_account.embeddings_service_account.email}"
}

resource "google_project_iam_member" "compute_default_sa_permissions" {
  for_each = toset([
    "roles/logging.logWriter",
    "roles/cloudfunctions.developer",
    "roles/iam.serviceAccountUser",
    "roles/artifactregistry.writer",
    "roles/storage.objectViewer"
  ])

  project = var.project_id
  role    = each.key
  member  = "serviceAccount:${var.project_number}-compute@developer.gserviceaccount.com"
}

# Create a dedicated least-privilege service account for ingestion-source-service
resource "google_service_account" "ingestion_source_service_account" {
  account_id   = "ingestion-source-sa"
  display_name = "Ingestion Source Service Account"
  description  = "Dedicated least-privilege service account for ingestion-source-service"
  project      = var.project_id
}

# Raw ingestion staging bucket scoped for ingestion-source-service uploads
resource "google_storage_bucket" "psearch_raw" {
  name                        = "${var.project_id}_psearch_raw"
  project                     = var.project_id
  location                    = "US"
  uniform_bucket_level_access = true
  public_access_prevention    = "enforced"
}

resource "google_storage_bucket_iam_member" "ingestion_source_raw_bucket_admin" {
  bucket = google_storage_bucket.psearch_raw.name
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:${google_service_account.ingestion_source_service_account.email}"
}

resource "google_storage_bucket_iam_member" "ingestion_source_raw_bucket_reader" {
  bucket = google_storage_bucket.psearch_raw.name
  role   = "roles/storage.legacyBucketReader"
  member = "serviceAccount:${google_service_account.ingestion_source_service_account.email}"
}

resource "google_project_iam_member" "grant_ingestion_source_sa_roles" {
  for_each = toset([
    "roles/bigquery.user",
    "roles/logging.logWriter",
    "roles/serviceusage.serviceUsageConsumer"
  ])

  role    = each.key
  project = var.project_id
  member  = "serviceAccount:${google_service_account.ingestion_source_service_account.email}"
}

# Create a dedicated least-privilege service account for gen-ai-service
resource "google_service_account" "gen_ai_service_account" {
  account_id   = "gen-ai-sa"
  display_name = "Gen AI Service Account"
  description  = "Dedicated least-privilege service account for gen-ai-service"
  project      = var.project_id
}

resource "google_project_iam_member" "grant_gen_ai_sa_roles" {
  for_each = toset([
    "roles/aiplatform.user",
    "roles/bigquery.jobUser",
    "roles/logging.logWriter",
    "roles/serviceusage.serviceUsageConsumer"
  ])

  role    = each.key
  project = var.project_id
  member  = "serviceAccount:${google_service_account.gen_ai_service_account.email}"
}

# Create a dedicated least-privilege service account for product-search-api
resource "google_service_account" "search_api_service_account" {
  account_id   = "search-api-sa"
  display_name = "Search API Service Account"
  description  = "Dedicated least-privilege service account for product-search-api"
  project      = var.project_id
}

resource "google_project_iam_member" "grant_search_api_sa_roles" {
  for_each = toset([
    "roles/aiplatform.user",
    "roles/logging.logWriter",
    "roles/serviceusage.serviceUsageConsumer"
  ])

  role    = each.key
  project = var.project_id
  member  = "serviceAccount:${google_service_account.search_api_service_account.email}"
}

# Create a dedicated least-privilege service account for psearch-ui
resource "google_service_account" "ui_service_account" {
  account_id   = "psearch-ui-sa"
  display_name = "PSearch UI Service Account"
  description  = "Dedicated least-privilege service account for psearch-ui"
  project      = var.project_id
}

resource "google_project_iam_member" "grant_ui_sa_roles" {
  for_each = toset([
    "roles/logging.logWriter"
  ])

  role    = each.key
  project = var.project_id
  member  = "serviceAccount:${google_service_account.ui_service_account.email}"
}
