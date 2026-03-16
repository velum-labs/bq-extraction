# BQ Schema & Query Log Extraction — Demo Environment

Reproducible demo: provisions a BigQuery project with realistic fintech schemas, seeds sample data + query traffic, then runs the extraction scripts to produce actual output.

## What This Proves

1. The extraction commands work end-to-end
2. Output format is useful (schemas, DDLs, query logs, access patterns)
3. Minimum IAM permissions are correct
4. Javier can replicate with one script swap (`PROJECT_ID`)

## Prerequisites

```bash
# gcloud CLI
brew install google-cloud-sdk   # or: curl https://sdk.cloud.google.com | bash

# terraform
brew install terraform

# authenticate
gcloud auth login
gcloud auth application-default login
```

## Quick Start

```bash
# 1. Provision
cd terraform
cp terraform.tfvars.example terraform.tfvars
# edit terraform.tfvars with your project ID
terraform init
terraform apply

# 2. Seed data + query traffic
cd ../scripts
./seed_data.sh <project-id>
./seed_queries.sh <project-id>

# 3. Run extraction (the thing we're demoing)
./extract.sh <project-id> us

# 4. Check output
ls -lh output/
cat output/columns.json | head -50

# 5. Teardown
cd ../terraform
terraform destroy
```

## Architecture

```
┌─────────────────────────────────────────────────┐
│ GCP Project                                      │
│                                                  │
│  ┌──────────┐  ┌──────────┐  ┌───────────────┐  │
│  │ raw      │  │ staging  │  │ analytics     │  │
│  │          │  │          │  │               │  │
│  │ funds    │  │ stg_txns │  │ daily_aum     │  │
│  │ txns     │  │ stg_users│  │ user_portfolio│  │
│  │ users    │  │          │  │ cmf_report    │  │
│  │ nav      │  │          │  │               │  │
│  └──────────┘  └──────────┘  └───────────────┘  │
│                                                  │
│  ┌──────────────────────────────────────────┐    │
│  │ INFORMATION_SCHEMA                        │    │
│  │  .COLUMNS  .TABLES  .JOBS                │    │
│  └──────────────────────────────────────────┘    │
│                                                  │
│  ┌──────────────────────┐                        │
│  │ SA: alma-extractor   │ (dataViewer +          │
│  │                      │  resourceViewer)       │
│  └──────────────────────┘                        │
└─────────────────────────────────────────────────┘
```

## Directory Structure

```
bq-extraction-demo/
├── README.md
├── terraform/
│   ├── main.tf              # provider, datasets, tables, IAM
│   ├── variables.tf         # project_id, region
│   ├── outputs.tf           # extraction commands, SA email
│   └── terraform.tfvars.example
├── scripts/
│   ├── seed_data.sh         # INSERT sample rows
│   ├── seed_queries.sh      # runs realistic queries to populate JOBS
│   └── extract.sh           # the extraction script (what Javier runs)
└── sample_output/           # reference output from a successful run
    └── .gitkeep
```

## Cleanup

```bash
cd terraform && terraform destroy
```

Total cost: ~$0. BQ storage for a few KB of demo data + a handful of queries on free tier.
