terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.0"
    }
  }
  required_version = ">= 1.3"
}

provider "google" {
  project = var.project_id
  region  = var.region
}

# GCS Bucket for raw data
resource "google_storage_bucket" "raw_bucket" {
  name          = var.gcs_bucket_name
  location      = var.location
  force_destroy = true

  lifecycle_rule {
    condition {
      age = 90
    }
    action {
      type = "Delete"
    }
  }
}

# BigQuery Dataset
resource "google_bigquery_dataset" "steam_dataset" {
  dataset_id  = var.bq_dataset_id
  location    = var.location
  description = "Steam Full Market Dataset 2025"
}