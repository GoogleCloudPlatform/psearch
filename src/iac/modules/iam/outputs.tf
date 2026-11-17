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

output "ingestion_service_account_email" {
  description = "The email address of the ingestion service account"
  value       = google_service_account.ingestion_service_account.email
}

output "embeddings_service_account_email" {
  description = "The email address of the embeddings service account"
  value       = google_service_account.embeddings_service_account.email
}

output "ingestion_source_service_account_email" {
  description = "The email address of the dedicated ingestion source service account"
  value       = google_service_account.ingestion_source_service_account.email
}

output "gen_ai_service_account_email" {
  description = "The email address of the dedicated gen-ai-service account"
  value       = google_service_account.gen_ai_service_account.email
}

output "search_api_service_account_email" {
  description = "The email address of the dedicated product-search-api service account"
  value       = google_service_account.search_api_service_account.email
}

output "ui_service_account_email" {
  description = "The email address of the dedicated psearch-ui service account"
  value       = google_service_account.ui_service_account.email
}
