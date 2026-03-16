variable "project_id" {
  description = "GCP project ID to provision extraction resources in"
  type        = string
}

variable "region" {
  description = "BigQuery dataset region (e.g. US, EU, us-central1). Used as-is for dataset location."
  type        = string
  default     = "US"
}

variable "region_slug" {
  description = "Region slug for INFORMATION_SCHEMA queries (lowercase, e.g. 'us', 'eu', 'us-central1'). Must match dataset location."
  type        = string
  default     = "US"
}

variable "create_service_account" {
  description = "Create a minimal-permissions SA to prove IAM requirements"
  type        = bool
  default     = true
}
