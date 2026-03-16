output "datasets" {
  description = "Created datasets"
  value = {
    raw       = google_bigquery_dataset.raw.dataset_id
    staging   = google_bigquery_dataset.staging.dataset_id
    analytics = google_bigquery_dataset.analytics.dataset_id
  }
}

output "tables" {
  description = "Created tables"
  value = [
    "${google_bigquery_dataset.raw.dataset_id}.${google_bigquery_table.raw_funds.table_id}",
    "${google_bigquery_dataset.raw.dataset_id}.${google_bigquery_table.raw_transactions.table_id}",
    "${google_bigquery_dataset.raw.dataset_id}.${google_bigquery_table.raw_users.table_id}",
    "${google_bigquery_dataset.raw.dataset_id}.${google_bigquery_table.raw_nav.table_id}",
    "${google_bigquery_dataset.staging.dataset_id}.${google_bigquery_table.stg_transactions.table_id}",
    "${google_bigquery_dataset.staging.dataset_id}.${google_bigquery_table.stg_users.table_id}",
    "${google_bigquery_dataset.analytics.dataset_id}.${google_bigquery_table.daily_aum.table_id}",
    "${google_bigquery_dataset.analytics.dataset_id}.${google_bigquery_table.user_portfolio.table_id}",
    "${google_bigquery_dataset.analytics.dataset_id}.${google_bigquery_table.cmf_report.table_id}",
  ]
}

output "extractor_service_account" {
  description = "Service account email for extraction (minimum perms)"
  value       = var.create_service_account ? google_service_account.extractor[0].email : "n/a"
}

output "extraction_command" {
  description = "Run this to extract schemas + query logs"
  value       = "./scripts/extract.sh ${var.project_id} ${var.region}"
}

output "extraction_as_sa_command" {
  description = "Run extraction as the minimal-permissions service account"
  value       = var.create_service_account ? "terraform output -raw extractor_sa_key | base64 -d > /tmp/alma-sa-key.json && gcloud auth activate-service-account --key-file=/tmp/alma-sa-key.json && ./scripts/extract.sh ${var.project_id} ${var.region} && rm /tmp/alma-sa-key.json" : "n/a"
}

output "extractor_sa_key" {
  description = "Base64-encoded SA key JSON (decode before use)"
  value       = var.create_service_account ? google_service_account_key.extractor[0].private_key : ""
  sensitive   = true
}
