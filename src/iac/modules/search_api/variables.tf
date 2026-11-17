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

variable "spanner_instance_id" {
  description = "The ID of the Spanner instance"
  type        = string
}

variable "spanner_database_id" {
  description = "The ID of the Spanner database"
  type        = string
}

variable "invoker_members" {
  description = "Explicit IAM principals granted roles/run.invoker on product-search-api (defaults to none)"
  type        = list(string)
  default     = []

  validation {
    condition     = !contains(var.invoker_members, "allUsers") && !contains(var.invoker_members, "allAuthenticatedUsers")
    error_message = "Public principals (allUsers, allAuthenticatedUsers) are not permitted on product-search-api."
  }
}

variable "ingress" {
  description = "Cloud Run ingress traffic setting for product-search-api"
  type        = string
  default     = "INGRESS_TRAFFIC_INTERNAL_LOAD_BALANCER"

  validation {
    condition     = contains(["INGRESS_TRAFFIC_INTERNAL_ONLY", "INGRESS_TRAFFIC_INTERNAL_LOAD_BALANCER", "INGRESS_TRAFFIC_ALL"], var.ingress)
    error_message = "ingress must be one of INGRESS_TRAFFIC_INTERNAL_ONLY, INGRESS_TRAFFIC_INTERNAL_LOAD_BALANCER, or INGRESS_TRAFFIC_ALL."
  }
}

variable "require_api_auth" {
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

variable "allowed_origins" {
  description = "Comma-separated CORS allowed origins for product-search-api"
  type        = string
  default     = ""
}
