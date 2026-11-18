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

variable "ingestion_source_ingress" {
  description = "Cloud Run ingress traffic setting for ingestion-source-service"
  type        = string
  default     = "INGRESS_TRAFFIC_INTERNAL_LOAD_BALANCER"

  validation {
    condition     = contains(["INGRESS_TRAFFIC_INTERNAL_ONLY", "INGRESS_TRAFFIC_INTERNAL_LOAD_BALANCER", "INGRESS_TRAFFIC_ALL"], var.ingestion_source_ingress)
    error_message = "ingestion_source_ingress must be one of INGRESS_TRAFFIC_INTERNAL_ONLY, INGRESS_TRAFFIC_INTERNAL_LOAD_BALANCER, or INGRESS_TRAFFIC_ALL."
  }
}

variable "gen_ai_invoker_members" {
  description = "Explicit IAM principals granted roles/run.invoker on gen-ai-service (defaults to none)"
  type        = list(string)
  default     = []

  validation {
    condition     = !contains(var.gen_ai_invoker_members, "allUsers") && !contains(var.gen_ai_invoker_members, "allAuthenticatedUsers")
    error_message = "Public principals (allUsers, allAuthenticatedUsers) are not permitted on gen-ai-service."
  }
}

variable "gen_ai_ingress" {
  description = "Cloud Run ingress traffic setting for gen-ai-service"
  type        = string
  default     = "INGRESS_TRAFFIC_INTERNAL_LOAD_BALANCER"

  validation {
    condition     = contains(["INGRESS_TRAFFIC_INTERNAL_ONLY", "INGRESS_TRAFFIC_INTERNAL_LOAD_BALANCER", "INGRESS_TRAFFIC_ALL"], var.gen_ai_ingress)
    error_message = "gen_ai_ingress must be one of INGRESS_TRAFFIC_INTERNAL_ONLY, INGRESS_TRAFFIC_INTERNAL_LOAD_BALANCER, or INGRESS_TRAFFIC_ALL."
  }
}

variable "gen_ai_auth_audience" {
  description = "Expected OIDC token audience for gen-ai-service Bearer authentication"
  type        = string
  default     = ""
}

variable "gen_ai_allowed_principals" {
  description = "Comma-separated allowlist of authenticated email principals allowed to call gen-ai-service"
  type        = string
  default     = ""
}

variable "gen_ai_allowed_domains" {
  description = "Comma-separated allowlist of Google Workspace domains allowed to call gen-ai-service"
  type        = string
  default     = ""
}

variable "gen_ai_allowed_origins" {
  description = "Comma-separated CORS allowed origins for gen-ai-service"
  type        = string
  default     = ""
}

variable "gen_ai_rate_limit_per_minute" {
  description = "Per-principal sliding-window rate limit per minute for paid Gen AI / BigQuery endpoints"
  type        = number
  default     = 60

  validation {
    condition     = var.gen_ai_rate_limit_per_minute > 0
    error_message = "gen_ai_rate_limit_per_minute must be greater than 0."
  }
}

variable "gen_ai_sql_task_rate_limit_per_minute" {
  description = "Per-principal sliding-window rate limit per minute for POST /generate-sql"
  type        = number
  default     = 10

  validation {
    condition     = var.gen_ai_sql_task_rate_limit_per_minute > 0
    error_message = "gen_ai_sql_task_rate_limit_per_minute must be greater than 0."
  }
}

variable "gen_ai_max_concurrent_sql_tasks" {
  description = "Maximum concurrent background SQL generation tasks per gen-ai-service instance"
  type        = number
  default     = 3

  validation {
    condition     = var.gen_ai_max_concurrent_sql_tasks > 0
    error_message = "gen_ai_max_concurrent_sql_tasks must be greater than 0."
  }
}

variable "gen_ai_allowed_image_hosts" {
  description = "Optional comma-separated allowlist of HTTPS hostnames allowed for gen-ai-service product image fetches"
  type        = string
  default     = ""
}

variable "gen_ai_max_image_bytes" {
  description = "Maximum allowed product image payload size in bytes for gen-ai-service (defaults to 10 MiB)"
  type        = number
  default     = 10485760

  validation {
    condition     = var.gen_ai_max_image_bytes > 0 && floor(var.gen_ai_max_image_bytes) == var.gen_ai_max_image_bytes
    error_message = "gen_ai_max_image_bytes must be a positive integer."
  }
}

variable "search_api_invoker_members" {
  description = "Explicit IAM principals granted roles/run.invoker on product-search-api (defaults to none)"
  type        = list(string)
  default     = []

  validation {
    condition     = !contains(var.search_api_invoker_members, "allUsers") && !contains(var.search_api_invoker_members, "allAuthenticatedUsers")
    error_message = "Public principals (allUsers, allAuthenticatedUsers) are not permitted on product-search-api."
  }
}

variable "search_api_ingress" {
  description = "Cloud Run ingress traffic setting for product-search-api"
  type        = string
  default     = "INGRESS_TRAFFIC_INTERNAL_LOAD_BALANCER"

  validation {
    condition     = contains(["INGRESS_TRAFFIC_INTERNAL_ONLY", "INGRESS_TRAFFIC_INTERNAL_LOAD_BALANCER", "INGRESS_TRAFFIC_ALL"], var.search_api_ingress)
    error_message = "search_api_ingress must be one of INGRESS_TRAFFIC_INTERNAL_ONLY, INGRESS_TRAFFIC_INTERNAL_LOAD_BALANCER, or INGRESS_TRAFFIC_ALL."
  }
}

variable "search_api_require_auth" {
  description = "Whether Bearer OIDC authentication is required on product-search-api endpoints"
  type        = string
  default     = "true"
}

variable "serving_auth_audience" {
  description = "Expected OIDC token audience for product-search-api Bearer authentication"
  type        = string
  default     = ""
}

variable "serving_allowed_principals" {
  description = "Comma-separated allowlist of authenticated email principals allowed to call product-search-api"
  type        = string
  default     = ""
}

variable "serving_allowed_domains" {
  description = "Comma-separated allowlist of Google Workspace domains allowed to call product-search-api"
  type        = string
  default     = ""
}

variable "search_api_allowed_origins" {
  description = "Comma-separated CORS allowed origins for product-search-api"
  type        = string
  default     = ""
}

variable "ui_invoker_members" {
  description = "Explicit IAM principals granted roles/run.invoker on psearch-ui (defaults to none)"
  type        = list(string)
  default     = []

  validation {
    condition     = !contains(var.ui_invoker_members, "allUsers") && !contains(var.ui_invoker_members, "allAuthenticatedUsers")
    error_message = "Public principals (allUsers, allAuthenticatedUsers) are not permitted on psearch-ui."
  }
}

variable "ui_ingress" {
  description = "Cloud Run ingress traffic setting for psearch-ui"
  type        = string
  default     = "INGRESS_TRAFFIC_INTERNAL_LOAD_BALANCER"

  validation {
    condition     = contains(["INGRESS_TRAFFIC_INTERNAL_ONLY", "INGRESS_TRAFFIC_INTERNAL_LOAD_BALANCER", "INGRESS_TRAFFIC_ALL"], var.ui_ingress)
    error_message = "ui_ingress must be one of INGRESS_TRAFFIC_INTERNAL_ONLY, INGRESS_TRAFFIC_INTERNAL_LOAD_BALANCER, or INGRESS_TRAFFIC_ALL."
  }
}
