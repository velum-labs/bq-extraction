variable "project_id" {
  description = "GCP project ID to provision demo resources in"
  type        = string
}

variable "region" {
  description = "BigQuery dataset region"
  type        = string
  default     = "US"
}

variable "create_service_account" {
  description = "Create a minimal-permissions SA to prove IAM requirements"
  type        = bool
  default     = true
}
