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

locals {
  service_name = "product-ingestion-spanner"
}

# Create Cloud Storage bucket for the model artifacts
resource "google_storage_bucket" "model_bucket" {
  name                        = "${var.project_id}-${local.service_name}-models"
  location                    = var.region
  uniform_bucket_level_access = true
  force_destroy               = true
}

# Create BigQuery Dataset
resource "google_bigquery_dataset" "psearch_raw_dataset" {
  project     = var.project_id
  dataset_id  = "psearch_raw"
  location    = var.region
  description = "Dataset for raw product data for psearch"
}

resource "google_bigquery_dataset" "psearch_dataset" {
  project     = var.project_id
  dataset_id  = "psearch"
  location    = var.region
  description = "Dataset for psearch ML models and transformations"
}

# Grant BQ Data Viewer role to the BQ Data Transfer SA on psearch_raw dataset
resource "google_bigquery_dataset_iam_member" "psearch_raw_dataset_viewer" {
  project    = var.project_id
  dataset_id = google_bigquery_dataset.psearch_raw_dataset.dataset_id
  role       = "roles/bigquery.dataViewer"
  member     = "serviceAccount:${var.bq_dt_service_account_email}"
}

# Grant BQ Data Viewer role to the BQ Data Transfer SA on psearch dataset
resource "google_bigquery_dataset_iam_member" "psearch_dataset_viewer" {
  project    = var.project_id
  dataset_id = google_bigquery_dataset.psearch_dataset.dataset_id
  role       = "roles/bigquery.dataViewer"
  member     = "serviceAccount:${var.bq_dt_service_account_email}"
}

# Create BigQuery Connection for Vertex AI
resource "google_bigquery_connection" "vertex_ai_connection" {
  project       = var.project_id
  location      = var.region
  friendly_name = "${local.service_name}-vertex-ai-connection"
  description   = "Connection to Vertex AI for remote models"
  cloud_resource {}
}

# Grant the BQ Data Transfer SA (ingestion_service_account) permission to use the BQ connection
resource "google_bigquery_connection_iam_member" "connection_user" {
  project       = var.project_id
  location      = var.region
  connection_id = google_bigquery_connection.vertex_ai_connection.connection_id
  role          = "roles/bigquery.connectionUser"
  member        = "serviceAccount:${var.bq_dt_service_account_email}"
}

resource "time_sleep" "connection_propagation_delay" {
  create_duration = "180s"

  depends_on = [
    google_bigquery_connection_iam_member.connection_user
  ]
}

# Grant the BQ Connection's own service account roles/vertexai.user
# This allows the connection to call Vertex AI endpoints.
resource "google_project_iam_member" "bq_connection_sa_vertex_user" {
  project = var.project_id
  role    = "roles/aiplatform.user"
  member  = "serviceAccount:${google_bigquery_connection.vertex_ai_connection.cloud_resource.0.service_account_id}"

  depends_on = [
    time_sleep.connection_propagation_delay
  ]
}

resource "time_sleep" "iam_propagation_delay" {
  create_duration = "180s"

  depends_on = [
    google_project_iam_member.bq_connection_sa_vertex_user
  ]
}

resource "google_bigquery_reservation" "export_reservation" {
  project           = var.project_id
  location          = var.region
  name              = "reservation-export"
  edition           = "ENTERPRISE"
  slot_capacity     = 0
  ignore_idle_slots = true
  autoscale {
    max_slots = 50
  }
}

resource "google_bigquery_reservation_assignment" "project_export_assignment" {
  project     = var.project_id
  location    = var.region
  reservation = google_bigquery_reservation.export_reservation.id
  assignee    = "projects/${var.project_id}"
  job_type    = "QUERY"
}

locals {
  rendered_create_model_sql = templatefile("${path.module}/../../bigquery_scripts/create_embedding_model.sql", {
    YOUR_PROJECT_ID    = var.project_id
    YOUR_REGION        = var.region
    YOUR_CONNECTION_ID = google_bigquery_connection.vertex_ai_connection.connection_id
  })
}

# Create BigQuery ML model via gcloud
resource "null_resource" "create_embedding_model_via_gcloud" {
  triggers = {
    sql_content   = filemd5("${path.module}/../../bigquery_scripts/create_embedding_model.sql")
    project_id    = var.project_id
    region        = var.region
    connection_id = google_bigquery_connection.vertex_ai_connection.connection_id
  }

  provisioner "local-exec" {
    command     = "bq --project_id='${var.project_id}' query --use_legacy_sql=false --nouse_cache '${local.rendered_create_model_sql}'"
    interpreter = ["bash", "-c"]
  }

  depends_on = [
    google_bigquery_dataset.psearch_raw_dataset,
    google_bigquery_dataset.psearch_dataset,
    time_sleep.iam_propagation_delay,
    google_bigquery_reservation_assignment.project_export_assignment
  ]
}

resource "null_resource" "create_spanner_export_procedure" {
  triggers = {
    sql_content = filemd5("${path.module}/../../bigquery_scripts/transform_embed_export_to_spanner.sql")
    project_id  = var.project_id
  }

  provisioner "local-exec" {
    command     = <<-EOT
      bq --project_id='${var.project_id}' query --use_legacy_sql=false --nouse_cache <<QUERY_EOF
      ${file("${path.module}/../../bigquery_scripts/transform_embed_export_to_spanner.sql")}
      QUERY_EOF
    EOT
    interpreter = ["bash", "-c"]
  }

  depends_on = [
    google_bigquery_dataset.psearch_dataset,
    null_resource.create_embedding_model_via_gcloud
  ]
}
resource "google_bigquery_data_transfer_config" "spanner_export_schedule" {
  project                = var.project_id
  location               = var.region
  display_name           = "${local.service_name}-spanner-export"
  data_source_id         = "scheduled_query"
  schedule               = "every 1 hours"
  destination_dataset_id = null

  params = {
    query = "CALL psearch.transform_and_export_to_spanner('${var.project_id}', '${var.spanner_instance_id}', '${var.spanner_database_id}');"
  }

  service_account_name = var.bq_dt_service_account_email

  depends_on = [
    null_resource.create_embedding_model_via_gcloud,
    google_bigquery_dataset.psearch_raw_dataset,
    google_bigquery_reservation_assignment.project_export_assignment,
    null_resource.create_spanner_export_procedure
  ]
}
