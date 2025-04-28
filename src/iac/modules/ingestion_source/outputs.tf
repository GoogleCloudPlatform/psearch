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

output "service_name" {
  description = "The name of the Cloud Run service"
  value       = google_cloud_run_v2_service.ingestion_source_service.name
}

output "service_url" {
  description = "The URL of the Cloud Run service"
  value       = google_cloud_run_v2_service.ingestion_source_service.uri
}

output "source_bucket" {
  description = "The name of the bucket storing source code"
  value       = google_storage_bucket.source_bucket.name
}

output "container_image" {
  description = "The full path to the container image"
  value       = "${local.image_path}:latest"
}

output "artifact_repository" {
  description = "The name of the Artifact Registry repository"
  value       = google_artifact_registry_repository.ingestion_source_repo.repository_id
}
