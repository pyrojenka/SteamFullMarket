output "gcs_bucket_name" {
  description = "GCS Bucket name"
  value       = google_storage_bucket.raw_bucket.name
}

output "gcs_bucket_url" {
  description = "GCS Bucket URL"
  value       = google_storage_bucket.raw_bucket.url
}

output "bigquery_dataset_id" {
  description = "BigQuery Dataset ID"
  value       = google_bigquery_dataset.steam_dataset.dataset_id
}

output "bigquery_dataset_location" {
  description = "BigQuery Dataset Location"
  value       = google_bigquery_dataset.steam_dataset.location
}