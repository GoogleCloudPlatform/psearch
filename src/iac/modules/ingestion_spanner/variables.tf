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
  description = "CPU limit for the Cloud Run job"
  type        = string
  default     = "2000m"
}

variable "memory_limit" {
  description = "Memory limit for the Cloud Run job"
  type        = string
  default     = "4Gi"
}

variable "max_retries" {
  description = "Maximum number of retries for the Cloud Run job"
  type        = number
  default     = 0
}

variable "timeout_seconds" {
  description = "Timeout in seconds for the Cloud Run job"
  type        = number
  default     = 86400
}

variable "spanner_instance_id" {
  description = "The ID of the Spanner instance"
  type        = string
}

variable "spanner_database_id" {
  description = "The ID of the Spanner database"
  type        = string
}
