variable "project_id" {
  description = "GCP Project ID"
  type        = string
  default     = "YOUR_GCP_PROJECT_ID"
}

variable "region" {
  description = "GCP Region"
  type        = string
  default     = "us-central1"
}

variable "location" {
  description = "GCP Location for BigQuery"
  type        = string
  default     = "US"
}

variable "gcs_bucket_name" {
  description = "GCS Bucket name for raw data"
  type        = string
  default     = "steam-fullmarket-raw"
}

variable "bq_dataset_id" {
  description = "BigQuery Dataset ID"
  type        = string
  default     = "steam_fullmarket"
}