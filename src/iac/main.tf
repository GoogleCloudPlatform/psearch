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

terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = ">= 5.10.0"
    }
  }
}

provider "google" {
  project = var.project_id
  region  = var.region
}

# Enable APIs
resource "google_project_service" "resource_manager" {
  project                    = var.project_id
  service                    = "cloudresourcemanager.googleapis.com"
  disable_dependent_services = true
}

resource "time_sleep" "wait_for_resource_manager" {
  create_duration = "30s"

  depends_on = [
    google_project_service.resource_manager
  ]
}

resource "google_project_service" "required_apis" {
  for_each = toset([
    "aiplatform.googleapis.com",
    "artifactregistry.googleapis.com",
    "bigquery.googleapis.com",
    "bigqueryconnection.googleapis.com",
    "bigquerydatatransfer.googleapis.com",
    "cloudbuild.googleapis.com",
    "cloudfunctions.googleapis.com",
    "containerregistry.googleapis.com",
    "iam.googleapis.com",
    "pubsub.googleapis.com",
    "run.googleapis.com",
    "storage.googleapis.com",
    "eventarc.googleapis.com",
    "secretmanager.googleapis.com",
    "firestore.googleapis.com",
    "spanner.googleapis.com"
  ])

  project                    = var.project_id
  service                    = each.key
  disable_dependent_services = true

  depends_on = [
    time_sleep.wait_for_resource_manager
  ]
}

module "iam" {
  source         = "./modules/iam"
  project_id     = var.project_id
  project_number = var.project_number
  depends_on = [
    google_project_service.required_apis
  ]
}


resource "time_sleep" "wait_for_iam" {
  create_duration = "60s"

  depends_on = [
    module.iam
  ]
}

module "spanner" {
  source                = "./modules/spanner"
  project_id            = var.project_id
  region                = var.region
  service_account_email = module.iam.ingestion_service_account_email
  processing_units      = 1000
  database_name         = "products-db"
  environment           = "development"

  depends_on = [
    time_sleep.wait_for_iam
  ]
}

module "ingestion_spanner" {
  source                      = "./modules/ingestion_spanner"
  project_id                  = var.project_id
  region                      = var.region
  bq_dt_service_account_email = module.iam.ingestion_service_account_email
  spanner_instance_id         = module.spanner.instance_id
  spanner_database_id         = module.spanner.database_id

  depends_on = [
    module.spanner
  ]
}

module "search_api" {
  source                = "./modules/search_api"
  project_id            = var.project_id
  region                = var.region
  service_account_email = module.iam.ingestion_service_account_email
  spanner_instance_id   = module.spanner.instance_id
  spanner_database_id   = module.spanner.database_id

  depends_on = [
    module.spanner
  ]
}

module "ui" {
  source                = "./modules/ui"
  project_id            = var.project_id
  region                = var.region
  service_account_email = module.iam.ingestion_service_account_email # TODO: Create a new Service Account for this service
  search_api_url        = module.search_api.service_url
  gen_ai_url            = module.gen_ai.service_url
  ingestion_source_url  = module.ingestion_source.service_url

  depends_on = [
    module.search_api,
    module.gen_ai,
    module.ingestion_source
  ]
}

module "gen_ai" {
  source                = "./modules/gen_ai"
  project_id            = var.project_id
  region                = var.region
  service_account_email = module.iam.ingestion_service_account_email # TODO: Create a new Service Account for this service

  depends_on = [
    time_sleep.wait_for_iam
  ]
}

module "ingestion_source" {
  source                = "./modules/ingestion_source"
  project_id            = var.project_id
  region                = var.region
  service_account_email = module.iam.ingestion_service_account_email # TODO: Create a new Service Account for this service

  depends_on = [
    time_sleep.wait_for_iam
  ]
}
