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
  description = "The ID of the GCP project where resources will be created"
  type        = string
}

variable "region" {
  description = "The GCP region where resources will be created"
  type        = string
}

variable "service_name" {
  description = "Name of the UI service"
  type        = string
  default     = "psearch-ui"
}

variable "min_instances" {
  description = "Minimum number of instances for the UI service"
  type        = number
  default     = 0
}

variable "max_instances" {
  description = "Maximum number of instances for the UI service"
  type        = number
  default     = 1
}

variable "cpu_limit" {
  description = "CPU limit for the UI service"
  type        = string
  default     = "2.0"
}

variable "memory_limit" {
  description = "Memory limit for the UI service"
  type        = string
  default     = "4Gi"
}

variable "service_account_email" {
  description = "The service account email to be used by the UI service"
  type        = string
}

variable "environment_variables" {
  description = "Environment variables to be set in the UI service"
  type        = map(string)
  default     = {}
}

variable "timeout_seconds" {
  description = "Timeout in seconds for the UI service"
  type        = number
  default     = 300 # 5 minutes
}

variable "search_api_url" {
  description = "The URL of the Search API service"
  type        = string
}

variable "gen_ai_url" {
  description = "The URL of the Gen AI service"
  type        = string
}

variable "ingestion_source_url" {
  description = "The URL of the Ingestion Source service"
  type        = string
}

variable "invoker_members" {
  description = "Explicit IAM principals granted roles/run.invoker on psearch-ui (defaults to none)"
  type        = list(string)
  default     = []

  validation {
    condition     = !contains(var.invoker_members, "allUsers") && !contains(var.invoker_members, "allAuthenticatedUsers")
    error_message = "Public principals (allUsers, allAuthenticatedUsers) are not permitted on psearch-ui."
  }
}

variable "ingress" {
  description = "Cloud Run ingress traffic setting for psearch-ui"
  type        = string
  default     = "INGRESS_TRAFFIC_INTERNAL_LOAD_BALANCER"

  validation {
    condition     = contains(["INGRESS_TRAFFIC_INTERNAL_ONLY", "INGRESS_TRAFFIC_INTERNAL_LOAD_BALANCER", "INGRESS_TRAFFIC_ALL"], var.ingress)
    error_message = "ingress must be one of INGRESS_TRAFFIC_INTERNAL_ONLY, INGRESS_TRAFFIC_INTERNAL_LOAD_BALANCER, or INGRESS_TRAFFIC_ALL."
  }
}
