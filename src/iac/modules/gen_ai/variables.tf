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
  description = "The ID of the project where resources will be created"
  type        = string
}

variable "region" {
  description = "The region where resources will be created"
  type        = string
}

variable "service_account_email" {
  description = "The service account email to be used by Cloud Build and Cloud Run"
  type        = string
}

variable "cpu_limit" {
  description = "CPU limit for the Cloud Run service"
  type        = string
  default     = "2.0"
}

variable "memory_limit" {
  description = "Memory limit for the Cloud Run service"
  type        = string
  default     = "4Gi"
}

variable "min_instances" {
  description = "Minimum number of instances for the Cloud Run service"
  type        = number
  default     = 1
}

variable "max_instances" {
  description = "Maximum number of instances for the Cloud Run service"
  type        = number
  default     = 10
}

variable "timeout_seconds" {
  description = "Timeout in seconds for the Cloud Run service"
  type        = number
  default     = 300 # 5 minutes timeout
}

variable "invoker_members" {
  description = "Explicit IAM principals granted roles/run.invoker on gen-ai-service (defaults to none)"
  type        = list(string)
  default     = []

  validation {
    condition     = !contains(var.invoker_members, "allUsers") && !contains(var.invoker_members, "allAuthenticatedUsers")
    error_message = "Public principals (allUsers, allAuthenticatedUsers) are not permitted on gen-ai-service."
  }
}

variable "ingress" {
  description = "Cloud Run ingress traffic setting for gen-ai-service"
  type        = string
  default     = "INGRESS_TRAFFIC_INTERNAL_LOAD_BALANCER"

  validation {
    condition     = contains(["INGRESS_TRAFFIC_INTERNAL_ONLY", "INGRESS_TRAFFIC_INTERNAL_LOAD_BALANCER", "INGRESS_TRAFFIC_ALL"], var.ingress)
    error_message = "ingress must be one of INGRESS_TRAFFIC_INTERNAL_ONLY, INGRESS_TRAFFIC_INTERNAL_LOAD_BALANCER, or INGRESS_TRAFFIC_ALL."
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

variable "allowed_origins" {
  description = "Comma-separated CORS allowed origins for gen-ai-service"
  type        = string
  default     = ""
}

variable "rate_limit_per_minute" {
  description = "Per-principal sliding-window rate limit per minute for paid Gen AI / BigQuery endpoints"
  type        = number
  default     = 60

  validation {
    condition     = var.rate_limit_per_minute > 0
    error_message = "rate_limit_per_minute must be greater than 0."
  }
}

variable "sql_task_rate_limit_per_minute" {
  description = "Per-principal sliding-window rate limit per minute for POST /generate-sql"
  type        = number
  default     = 10

  validation {
    condition     = var.sql_task_rate_limit_per_minute > 0
    error_message = "sql_task_rate_limit_per_minute must be greater than 0."
  }
}

variable "max_concurrent_sql_tasks" {
  description = "Maximum concurrent background SQL generation tasks per Cloud Run instance"
  type        = number
  default     = 3

  validation {
    condition     = var.max_concurrent_sql_tasks > 0
    error_message = "max_concurrent_sql_tasks must be greater than 0."
  }
}
