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

variable "project_id" {
  description = "The GCP project ID"
  type        = string
}

variable "region" {
  description = "The GCP region where resources will be created"
  type        = string
}

variable "project_number" {
  description = "The GCP project number"
  type        = number
}

variable "dimensions" {
  description = "The number of dimensions for the Vector Search index"
  type        = number
  default     = 768
}

variable "ingestion_source_invoker_members" {
  description = "Explicit IAM principals granted roles/run.invoker on ingestion-source-service (defaults to none)"
  type        = list(string)
  default     = []

  validation {
    condition     = !contains(var.ingestion_source_invoker_members, "allUsers") && !contains(var.ingestion_source_invoker_members, "allAuthenticatedUsers")
    error_message = "Public principals (allUsers, allAuthenticatedUsers) are not permitted on ingestion-source-service."
  }
}

variable "ingestion_auth_audience" {
  description = "Expected OIDC token audience for ingestion-source-service Bearer authentication"
  type        = string
  default     = ""
}

variable "ingestion_allowed_principals" {
  description = "Comma-separated allowlist of authenticated email principals allowed to call ingestion-source-service"
  type        = string
  default     = ""
}

variable "ingestion_allowed_domains" {
  description = "Comma-separated allowlist of Google Workspace domains allowed to call ingestion-source-service"
  type        = string
  default     = ""
}

variable "ingestion_allowed_origins" {
  description = "Comma-separated CORS allowed origins for ingestion-source-service"
  type        = string
  default     = ""
}

variable "ingestion_max_upload_size_bytes" {
  description = "Maximum allowed upload payload size in bytes for ingestion-source-service (defaults to 50 MiB)"
  type        = number
  default     = 52428800
}
