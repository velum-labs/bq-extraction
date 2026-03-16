terraform {
  required_version = ">= 1.5"
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.0"
    }
  }
}

provider "google" {
  project = var.project_id
}

# ──────────────────────────────────────────────
# Enable required APIs
# ──────────────────────────────────────────────

resource "google_project_service" "bigquery" {
  service            = "bigquery.googleapis.com"
  disable_on_destroy = false
}

resource "google_project_service" "iam" {
  service            = "iam.googleapis.com"
  disable_on_destroy = false
}

# ──────────────────────────────────────────────
# Datasets — mimics a typical fintech warehouse
# ──────────────────────────────────────────────

resource "google_bigquery_dataset" "raw" {
  dataset_id  = "raw"
  description = "Raw ingested data from source systems"
  location    = var.region

  depends_on = [google_project_service.bigquery]

  labels = {
    layer = "raw"
    demo  = "alma-extraction"
  }
}

resource "google_bigquery_dataset" "staging" {
  dataset_id  = "staging"
  description = "Cleaned and transformed staging models"
  location    = var.region

  depends_on = [google_project_service.bigquery]

  labels = {
    layer = "staging"
    demo  = "alma-extraction"
  }
}

resource "google_bigquery_dataset" "analytics" {
  dataset_id  = "analytics"
  description = "Business-ready analytics models"
  location    = var.region

  depends_on = [google_project_service.bigquery]

  labels = {
    layer = "analytics"
    demo  = "alma-extraction"
  }
}

# ──────────────────────────────────────────────
# Raw tables
# ──────────────────────────────────────────────

resource "google_bigquery_table" "raw_funds" {
  dataset_id          = google_bigquery_dataset.raw.dataset_id
  table_id            = "funds"
  description         = "Fund master data from admin system"
  deletion_protection = false

  schema = jsonencode([
    { name = "fund_id", type = "STRING", mode = "REQUIRED", description = "Unique fund identifier" },
    { name = "fund_name", type = "STRING", mode = "REQUIRED", description = "Public fund name" },
    { name = "fund_type", type = "STRING", mode = "NULLABLE", description = "e.g. mutual_fund, etf, money_market" },
    { name = "currency", type = "STRING", mode = "NULLABLE", description = "ISO 4217 currency code" },
    { name = "inception_date", type = "DATE", mode = "NULLABLE", description = "Fund launch date" },
    { name = "is_active", type = "BOOLEAN", mode = "NULLABLE", description = "Whether fund is currently active" },
    { name = "cmf_rut", type = "STRING", mode = "NULLABLE", description = "Chilean CMF regulator identifier" },
    { name = "management_fee_bps", type = "INT64", mode = "NULLABLE", description = "Annual management fee in basis points" },
    { name = "loaded_at", type = "TIMESTAMP", mode = "REQUIRED", description = "Ingestion timestamp" },
  ])
}

resource "google_bigquery_table" "raw_transactions" {
  dataset_id          = google_bigquery_dataset.raw.dataset_id
  table_id            = "transactions"
  description         = "Investment transactions (deposits, withdrawals, switches)"
  deletion_protection = false

  time_partitioning {
    type  = "DAY"
    field = "transaction_date"
  }

  clustering = ["fund_id", "transaction_type"]

  schema = jsonencode([
    { name = "transaction_id", type = "STRING", mode = "REQUIRED", description = "Unique transaction ID" },
    { name = "user_id", type = "STRING", mode = "REQUIRED", description = "Investor user ID" },
    { name = "fund_id", type = "STRING", mode = "REQUIRED", description = "Target fund" },
    { name = "transaction_type", type = "STRING", mode = "REQUIRED", description = "deposit, withdrawal, switch_in, switch_out" },
    { name = "amount_clp", type = "NUMERIC", mode = "REQUIRED", description = "Transaction amount in CLP" },
    { name = "amount_usd", type = "NUMERIC", mode = "NULLABLE", description = "Transaction amount in USD (if applicable)" },
    { name = "shares", type = "FLOAT64", mode = "NULLABLE", description = "Number of fund shares" },
    { name = "nav_at_transaction", type = "FLOAT64", mode = "NULLABLE", description = "NAV per share at time of transaction" },
    { name = "transaction_date", type = "DATE", mode = "REQUIRED", description = "Settlement date" },
    { name = "status", type = "STRING", mode = "NULLABLE", description = "pending, settled, cancelled" },
    { name = "source_system", type = "STRING", mode = "NULLABLE", description = "Originating system" },
    { name = "loaded_at", type = "TIMESTAMP", mode = "REQUIRED", description = "Ingestion timestamp" },
  ])
}

resource "google_bigquery_table" "raw_users" {
  dataset_id          = google_bigquery_dataset.raw.dataset_id
  table_id            = "users"
  description         = "Investor profiles"
  deletion_protection = false

  schema = jsonencode([
    { name = "user_id", type = "STRING", mode = "REQUIRED", description = "Unique user identifier" },
    { name = "email_hash", type = "STRING", mode = "NULLABLE", description = "Hashed email (PII masked)" },
    { name = "registration_date", type = "DATE", mode = "NULLABLE", description = "Account creation date" },
    { name = "country_code", type = "STRING", mode = "NULLABLE", description = "ISO 3166-1 alpha-2" },
    { name = "risk_profile", type = "STRING", mode = "NULLABLE", description = "conservative, moderate, aggressive" },
    { name = "is_qualified_investor", type = "BOOLEAN", mode = "NULLABLE", description = "Chilean qualified investor flag" },
    { name = "kyc_status", type = "STRING", mode = "NULLABLE", description = "pending, approved, rejected" },
    { name = "loaded_at", type = "TIMESTAMP", mode = "REQUIRED", description = "Ingestion timestamp" },
  ])
}

resource "google_bigquery_table" "raw_nav" {
  dataset_id          = google_bigquery_dataset.raw.dataset_id
  table_id            = "nav_history"
  description         = "Daily NAV (net asset value) per fund"
  deletion_protection = false

  time_partitioning {
    type  = "DAY"
    field = "nav_date"
  }

  clustering = ["fund_id"]

  schema = jsonencode([
    { name = "fund_id", type = "STRING", mode = "REQUIRED", description = "Fund identifier" },
    { name = "nav_date", type = "DATE", mode = "REQUIRED", description = "Valuation date" },
    { name = "nav_per_share", type = "FLOAT64", mode = "REQUIRED", description = "NAV per share" },
    { name = "total_aum", type = "NUMERIC", mode = "NULLABLE", description = "Total assets under management (CLP)" },
    { name = "shares_outstanding", type = "FLOAT64", mode = "NULLABLE", description = "Total shares outstanding" },
    { name = "daily_return_pct", type = "FLOAT64", mode = "NULLABLE", description = "Daily return percentage" },
    { name = "loaded_at", type = "TIMESTAMP", mode = "REQUIRED", description = "Ingestion timestamp" },
  ])
}

# ──────────────────────────────────────────────
# Staging tables (what dbt would produce)
# ──────────────────────────────────────────────

resource "google_bigquery_table" "stg_transactions" {
  dataset_id          = google_bigquery_dataset.staging.dataset_id
  table_id            = "stg_transactions"
  description         = "Cleaned transactions — deduped, status filtered, amounts validated"
  deletion_protection = false

  time_partitioning {
    type  = "DAY"
    field = "transaction_date"
  }

  schema = jsonencode([
    { name = "transaction_id", type = "STRING", mode = "REQUIRED" },
    { name = "user_id", type = "STRING", mode = "REQUIRED" },
    { name = "fund_id", type = "STRING", mode = "REQUIRED" },
    { name = "transaction_type", type = "STRING", mode = "REQUIRED" },
    { name = "amount_clp", type = "NUMERIC", mode = "REQUIRED" },
    { name = "shares", type = "FLOAT64", mode = "NULLABLE" },
    { name = "transaction_date", type = "DATE", mode = "REQUIRED" },
    { name = "loaded_at", type = "TIMESTAMP", mode = "REQUIRED" },
  ])
}

resource "google_bigquery_table" "stg_users" {
  dataset_id          = google_bigquery_dataset.staging.dataset_id
  table_id            = "stg_users"
  description         = "Cleaned user profiles — KYC approved only"
  deletion_protection = false

  schema = jsonencode([
    { name = "user_id", type = "STRING", mode = "REQUIRED" },
    { name = "registration_date", type = "DATE", mode = "NULLABLE" },
    { name = "country_code", type = "STRING", mode = "NULLABLE" },
    { name = "risk_profile", type = "STRING", mode = "NULLABLE" },
    { name = "is_qualified_investor", type = "BOOLEAN", mode = "NULLABLE" },
  ])
}

# ──────────────────────────────────────────────
# Analytics tables (business-ready)
# ──────────────────────────────────────────────

resource "google_bigquery_table" "daily_aum" {
  dataset_id          = google_bigquery_dataset.analytics.dataset_id
  table_id            = "daily_aum"
  description         = "Daily AUM per fund — the core metric Fintual reports to CMF"
  deletion_protection = false

  time_partitioning {
    type  = "DAY"
    field = "report_date"
  }

  schema = jsonencode([
    { name = "report_date", type = "DATE", mode = "REQUIRED", description = "Reporting date" },
    { name = "fund_id", type = "STRING", mode = "REQUIRED", description = "Fund identifier" },
    { name = "fund_name", type = "STRING", mode = "NULLABLE", description = "Fund display name" },
    { name = "total_aum_clp", type = "NUMERIC", mode = "REQUIRED", description = "Total AUM in CLP" },
    { name = "total_aum_usd", type = "NUMERIC", mode = "NULLABLE", description = "Total AUM in USD" },
    { name = "investor_count", type = "INT64", mode = "NULLABLE", description = "Number of active investors" },
    { name = "daily_net_flow_clp", type = "NUMERIC", mode = "NULLABLE", description = "Net deposits - withdrawals" },
    { name = "daily_return_pct", type = "FLOAT64", mode = "NULLABLE", description = "Daily return %" },
  ])
}

resource "google_bigquery_table" "user_portfolio" {
  dataset_id          = google_bigquery_dataset.analytics.dataset_id
  table_id            = "user_portfolio"
  description         = "Current portfolio snapshot per user"
  deletion_protection = false

  schema = jsonencode([
    { name = "user_id", type = "STRING", mode = "REQUIRED" },
    { name = "fund_id", type = "STRING", mode = "REQUIRED" },
    { name = "shares_held", type = "FLOAT64", mode = "REQUIRED" },
    { name = "current_value_clp", type = "NUMERIC", mode = "NULLABLE" },
    { name = "total_deposited_clp", type = "NUMERIC", mode = "NULLABLE" },
    { name = "total_withdrawn_clp", type = "NUMERIC", mode = "NULLABLE" },
    { name = "unrealized_gain_clp", type = "NUMERIC", mode = "NULLABLE" },
    { name = "first_investment_date", type = "DATE", mode = "NULLABLE" },
    { name = "last_transaction_date", type = "DATE", mode = "NULLABLE" },
    { name = "snapshot_date", type = "DATE", mode = "REQUIRED" },
  ])
}

resource "google_bigquery_table" "cmf_report" {
  dataset_id          = google_bigquery_dataset.analytics.dataset_id
  table_id            = "cmf_regulatory_report"
  description         = "CMF regulatory report — monthly filing data"
  deletion_protection = false

  schema = jsonencode([
    { name = "report_month", type = "DATE", mode = "REQUIRED", description = "First day of reporting month" },
    { name = "fund_id", type = "STRING", mode = "REQUIRED" },
    { name = "fund_name", type = "STRING", mode = "NULLABLE" },
    { name = "cmf_rut", type = "STRING", mode = "NULLABLE", description = "CMF regulator RUT" },
    { name = "eom_aum_clp", type = "NUMERIC", mode = "REQUIRED", description = "End of month AUM" },
    { name = "eom_investor_count", type = "INT64", mode = "NULLABLE" },
    { name = "monthly_deposits_clp", type = "NUMERIC", mode = "NULLABLE" },
    { name = "monthly_withdrawals_clp", type = "NUMERIC", mode = "NULLABLE" },
    { name = "monthly_return_pct", type = "FLOAT64", mode = "NULLABLE" },
    { name = "management_fee_collected_clp", type = "NUMERIC", mode = "NULLABLE" },
    { name = "generated_at", type = "TIMESTAMP", mode = "REQUIRED" },
  ])
}

# ──────────────────────────────────────────────
# Service account with minimum extraction perms
# ──────────────────────────────────────────────

resource "google_service_account" "extractor" {
  count        = var.create_service_account ? 1 : 0
  account_id   = "alma-extractor"
  display_name = "Alma Schema Extractor (demo)"
  description  = "Minimum permissions needed to extract schemas + query logs"

  depends_on = [google_project_service.iam]
}

resource "google_project_iam_member" "extractor_data_viewer" {
  count   = var.create_service_account ? 1 : 0
  project = var.project_id
  role    = "roles/bigquery.dataViewer"
  member  = "serviceAccount:${google_service_account.extractor[0].email}"
}

resource "google_project_iam_member" "extractor_resource_viewer" {
  count   = var.create_service_account ? 1 : 0
  project = var.project_id
  role    = "roles/bigquery.resourceViewer"
  member  = "serviceAccount:${google_service_account.extractor[0].email}"
}

resource "google_project_iam_member" "extractor_job_user" {
  count   = var.create_service_account ? 1 : 0
  project = var.project_id
  role    = "roles/bigquery.jobUser"
  member  = "serviceAccount:${google_service_account.extractor[0].email}"
}

resource "google_service_account_key" "extractor" {
  count              = var.create_service_account ? 1 : 0
  service_account_id = google_service_account.extractor[0].name
}
