# BigQuery Discovery-Based Extraction

Reproducible setup: provisions a BigQuery project with realistic fintech schemas, seeds sample data + query traffic, then runs a discovery-first extractor that auto-discovers what metadata is available through BigQuery APIs, SDKs, and scoped `INFORMATION_SCHEMA` capabilities.

## What This Proves

1. The extractor discovers datasets, tables/views, routines, models, and optional jobs metadata without a hardcoded warehouse shape
2. Output format is useful for inventory, schema, DDL, and jobs analysis
3. Location-aware capability probing behaves correctly under partial permissions
4. Javier can replicate with one script swap (`PROJECT_ID`)

## Prerequisites

```bash
# uv (manages Python + dependencies for the extractor)
brew install uv

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
# 1. Install Python dependencies for the extractor
uv sync

# 2. Provision
cd terraform
cp terraform.tfvars.example terraform.tfvars
# edit terraform.tfvars with your project ID
terraform init
terraform apply

# 3. Seed data + query traffic
cd ../scripts
./seed_data.sh <project-id>
./seed_queries.sh <project-id>

# 4. Run extraction
cd ..
uv run python scripts/extract.py --project <project-id>

# 5. Check output
LATEST_OUTPUT="$(ls -td output/* | head -1)"
ls -lh "$LATEST_OUTPUT"
python3 -m json.tool "$LATEST_OUTPUT/tables.json" | head -50

# 6. Teardown
cd terraform
terraform destroy
```

## Architecture

```
BigQuery APIs / Python SDK
  - datasets.list
  - tables.list + tables.get
  - routines.list + routines.get
  - models.list + models.get
          |
          v
Discovered datasets grouped by location
          |
          v
Capability probe
  - INFORMATION_SCHEMA.TABLES for DDLs
  - INFORMATION_SCHEMA.JOBS_BY_PROJECT for jobs-derived recipes
          |
          v
Derived outputs
  - datasets.json
  - tables.json
  - routines.json
  - models.json
  - tables.ddls.json
  - jobs.*.json
```

## Discovery Model

- The extractor uses official BigQuery APIs and the Python SDK first, then falls back to `INFORMATION_SCHEMA` only for metadata the API does not expose equivalently, such as DDL and jobs-derived recipes.
- Extraction is location-aware. Datasets are discovered first, then grouped by location so region-qualified metadata queries run in the correct BigQuery location.
- Metadata capabilities are probed before extraction. If a capability is unavailable because of scope or permissions, it is skipped cleanly and reported.
- Hidden datasets are excluded by default. Use `--include-hidden-datasets` to include them in API-backed discovery, with the caveat that `INFORMATION_SCHEMA` does not expose hidden-dataset metadata consistently.

## Common Flags

- `--locations us,eu,...` limits discovery and extraction to specific BigQuery locations. `--region` is kept as a single-location alias for compatibility with older single-location flows.
- `--families datasets,tables,routines,models,jobs` narrows which object families are emitted.
- `--sources tables.ddls,jobs.query_logs,...` narrows which non-API metadata capabilities run.
- `--dry-run` performs discovery and capability probing without writing output files.

## Directory Structure

```
bq-extraction/
├── README.md
├── pyproject.toml          # uv-managed Python project metadata
├── terraform/
│   ├── main.tf              # provider, datasets, tables, IAM
│   ├── variables.tf         # project_id, region
│   ├── outputs.tf           # extraction commands, SA email
│   └── terraform.tfvars.example
├── scripts/
│   ├── seed_data.sh         # INSERT sample rows
│   ├── seed_queries.sh      # runs realistic queries to populate JOBS
│   └── extract.py           # Python extractor entrypoint
├── src/
│   └── bq_extraction/       # extractor package + capability registry
├── tests/                   # unit + smoke coverage
├── uv.lock                  # pinned dependency lockfile
└── sample_output/           # reference output from a successful run
    └── .gitkeep
```

## Cleanup

```bash
cd terraform && terraform destroy
```

Total cost: ~$0. BQ storage for a few KB of sample data + a handful of queries on free tier.
