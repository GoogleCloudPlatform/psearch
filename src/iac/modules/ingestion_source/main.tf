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
  service_name = "ingestion-source-service"
}

# Create Cloud Storage bucket for the source code
resource "google_storage_bucket" "source_bucket" {
  name                        = "${var.project_id}-${local.service_name}-source"
  location                    = var.region
  uniform_bucket_level_access = true
  force_destroy               = true
}

# Create Artifact Registry Repository
resource "google_artifact_registry_repository" "ingestion_source_repo" {
  location      = var.region
  repository_id = "${local.service_name}-repo"
  description   = "Docker repository for ${local.service_name} images"
  format        = "DOCKER"
}

# Create zip file from source code
data "archive_file" "source_zip" {
  type        = "zip"
  source_dir  = "${path.root}/../../src/psearch/ingestion_source"
  output_path = "${path.module}/tmp/source.zip"
}

# Upload source code to bucket
resource "google_storage_bucket_object" "source_code" {
  name   = "source-${data.archive_file.source_zip.output_md5}.zip"
  bucket = google_storage_bucket.source_bucket.name
  source = data.archive_file.source_zip.output_path
}

# Build the container image
resource "null_resource" "build_image" {
  triggers = {
    source_zip_hash = data.archive_file.source_zip.output_md5
  }

  provisioner "local-exec" {
    command = <<EOF
      gcloud builds submit ${data.archive_file.source_zip.output_path} \
        --project=${var.project_id} \
        --region=${var.region} \
        --tag=${var.region}-docker.pkg.dev/${var.project_id}/${google_artifact_registry_repository.ingestion_source_repo.repository_id}/${local.service_name}:latest
    EOF
  }

  depends_on = [
    google_artifact_registry_repository.ingestion_source_repo,
    google_storage_bucket_object.source_code
  ]
}

# Update Cloud Run service to use the built image
locals {
  image_path = "${var.region}-docker.pkg.dev/${var.project_id}/${google_artifact_registry_repository.ingestion_source_repo.repository_id}/${local.service_name}"
}

# Create a Cloud Run service
resource "google_cloud_run_v2_service" "ingestion_source_service" {
  name     = local.service_name
  location = var.region

  template {
    containers {
      image = "${local.image_path}:latest"

      env {
        name  = "PROJECT_ID"
        value = var.project_id
      }
      env {
        name  = "REGION"
        value = var.region
      }

      resources {
        limits = {
          cpu    = var.cpu_limit
          memory = var.memory_limit
        }
      }

      ports {
        container_port = 8080
      }
    }

    service_account = var.service_account_email

    scaling {
      min_instance_count = var.min_instances
      max_instance_count = var.max_instances
    }

    timeout = "${var.timeout_seconds}s"
  }

  depends_on = [
    null_resource.build_image
  ]

  deletion_protection = false
}

# Make the service public
data "google_iam_policy" "noauth" {
  binding {
    role = "roles/run.invoker"
    members = [
      "allUsers",
    ]
  }
}

resource "google_cloud_run_v2_service_iam_policy" "noauth" {
  location    = google_cloud_run_v2_service.ingestion_source_service.location
  name        = google_cloud_run_v2_service.ingestion_source_service.name
  policy_data = data.google_iam_policy.noauth.policy_data
}
