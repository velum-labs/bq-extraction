#!/bin/bash
# ═══════════════════════════════════════════════════════
# BQ Schema & Query Log Extraction Script
# This is what Javier would run against Fintual's project
# ═══════════════════════════════════════════════════════
set -euo pipefail

PROJECT_ID="${1:?Usage: $0 <project-id> [region]}"
REGION="${2:-us}"
OUTPUT_DIR="output/$(date +%Y%m%d_%H%M%S)"

mkdir -p "$OUTPUT_DIR"
echo "╔══════════════════════════════════════════════════╗"
echo "║  BQ Schema & Query Log Extraction                ║"
echo "║  Project: $PROJECT_ID"
echo "║  Region:  $REGION"
echo "║  Output:  $OUTPUT_DIR/"
echo "╚══════════════════════════════════════════════════╝"
echo ""

# ── 1. List Datasets ────────────────────────────────────
echo "① Listing datasets..."
bq ls --project_id "$PROJECT_ID" --format=prettyjson > "$OUTPUT_DIR/datasets.json" 2>/dev/null
DATASET_COUNT=$(python3 -c "import json; print(len(json.load(open('$OUTPUT_DIR/datasets.json'))))" 2>/dev/null || echo "?")
echo "   ✓ $DATASET_COUNT datasets → datasets.json"

# ── 2. All Columns + Types ─────────────────────────────
echo "② Extracting column schemas..."
# Try region-level first; fall back to per-dataset union if permissions insufficient
if bq query --use_legacy_sql=false --format=prettyjson --max_rows=200000 --project_id="$PROJECT_ID" "
  SELECT
    table_schema AS dataset,
    table_name,
    column_name,
    ordinal_position,
    data_type,
    is_nullable,
    is_partitioning_column,
    clustering_ordinal_position
  FROM \`$PROJECT_ID\`.\`region-$REGION\`.INFORMATION_SCHEMA.COLUMNS
  ORDER BY table_schema, table_name, ordinal_position
" > "$OUTPUT_DIR/columns.json" 2>/dev/null; then
  true
else
  echo "   (region-level query denied, falling back to per-dataset)"
  # Build UNION ALL across all datasets
  DATASETS=$(bq ls --project_id "$PROJECT_ID" --format=json 2>/dev/null | python3 -c "import json,sys; [print(d['datasetReference']['datasetId']) for d in json.load(sys.stdin)]")
  UNION_SQL=""
  for DS in $DATASETS; do
    [ -n "$UNION_SQL" ] && UNION_SQL="$UNION_SQL UNION ALL "
    UNION_SQL="${UNION_SQL}SELECT table_schema AS dataset, table_name, column_name, ordinal_position, data_type, is_nullable, is_partitioning_column, clustering_ordinal_position FROM \`$PROJECT_ID\`.\`$DS\`.INFORMATION_SCHEMA.COLUMNS"
  done
  bq query --use_legacy_sql=false --format=prettyjson --max_rows=200000 --project_id="$PROJECT_ID" \
    "$UNION_SQL ORDER BY dataset, table_name, ordinal_position" > "$OUTPUT_DIR/columns.json"
fi
COL_COUNT=$(python3 -c "import json; print(len(json.load(open('$OUTPUT_DIR/columns.json'))))" 2>/dev/null || echo "?")
echo "   ✓ $COL_COUNT columns → columns.json"

# ── 2b. Column Descriptions (via COLUMN_FIELD_PATHS) ──
echo "②b Extracting column descriptions..."
DATASETS=${DATASETS:-$(bq ls --project_id "$PROJECT_ID" --format=json 2>/dev/null | python3 -c "import json,sys; [print(d['datasetReference']['datasetId']) for d in json.load(sys.stdin)]")}
UNION_SQL=""
for DS in $DATASETS; do
  [ -n "$UNION_SQL" ] && UNION_SQL="$UNION_SQL UNION ALL "
  UNION_SQL="${UNION_SQL}SELECT table_schema AS dataset, table_name, column_name, field_path, description, data_type FROM \`$PROJECT_ID\`.\`$DS\`.INFORMATION_SCHEMA.COLUMN_FIELD_PATHS"
done
bq query --use_legacy_sql=false --format=prettyjson --max_rows=200000 --project_id="$PROJECT_ID" \
  "$UNION_SQL ORDER BY dataset, table_name, column_name" > "$OUTPUT_DIR/column_descriptions.json" 2>/dev/null || echo "[]" > "$OUTPUT_DIR/column_descriptions.json"
DESC_COUNT=$(python3 -c "import json; d=json.load(open('$OUTPUT_DIR/column_descriptions.json')); print(sum(1 for r in d if r.get('description')))" 2>/dev/null || echo "?")
echo "   ✓ $DESC_COUNT described columns → column_descriptions.json"

# ── 3. Full DDLs ───────────────────────────────────────
echo "③ Extracting DDLs (CREATE TABLE statements)..."
if bq query --use_legacy_sql=false --format=prettyjson --max_rows=10000 --project_id="$PROJECT_ID" "
  SELECT
    table_schema AS dataset,
    table_name,
    table_type,
    ddl
  FROM \`$PROJECT_ID\`.\`region-$REGION\`.INFORMATION_SCHEMA.TABLES
  ORDER BY table_schema, table_name
" > "$OUTPUT_DIR/ddls.json" 2>/dev/null; then
  true
else
  echo "   (region-level query denied, falling back to per-dataset)"
  DATASETS=${DATASETS:-$(bq ls --project_id "$PROJECT_ID" --format=json 2>/dev/null | python3 -c "import json,sys; [print(d['datasetReference']['datasetId']) for d in json.load(sys.stdin)]")}
  UNION_SQL=""
  for DS in $DATASETS; do
    [ -n "$UNION_SQL" ] && UNION_SQL="$UNION_SQL UNION ALL "
    UNION_SQL="${UNION_SQL}SELECT table_schema AS dataset, table_name, table_type, ddl FROM \`$PROJECT_ID\`.\`$DS\`.INFORMATION_SCHEMA.TABLES"
  done
  bq query --use_legacy_sql=false --format=prettyjson --max_rows=10000 --project_id="$PROJECT_ID" \
    "$UNION_SQL ORDER BY dataset, table_name" > "$OUTPUT_DIR/ddls.json"
fi
TBL_COUNT=$(python3 -c "import json; print(len(json.load(open('$OUTPUT_DIR/ddls.json'))))" 2>/dev/null || echo "?")
echo "   ✓ $TBL_COUNT tables → ddls.json"

# ── 4. Query Logs (last 30 days) ──────────────────────
echo "④ Extracting query logs (30 days)..."
bq query --use_legacy_sql=false --format=prettyjson --max_rows=500000 --project_id="$PROJECT_ID" "
  SELECT
    job_id,
    user_email,
    query,
    statement_type,
    creation_time,
    TIMESTAMP_DIFF(end_time, start_time, SECOND) AS duration_seconds,
    total_bytes_processed,
    total_bytes_billed,
    total_slot_ms,
    cache_hit,
    referenced_tables
  FROM \`$PROJECT_ID\`.\`region-$REGION\`.INFORMATION_SCHEMA.JOBS
  WHERE
    creation_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 DAY)
    AND job_type = 'QUERY'
    AND state = 'DONE'
    AND error_result IS NULL
  ORDER BY creation_time DESC
" > "$OUTPUT_DIR/query_logs.json"
QUERY_COUNT=$(python3 -c "import json; print(len(json.load(open('$OUTPUT_DIR/query_logs.json'))))" 2>/dev/null || echo "?")
echo "   ✓ $QUERY_COUNT queries → query_logs.json"

# ── 5. Most Frequent Queries ──────────────────────────
echo "⑤ Analyzing query frequency..."
bq query --use_legacy_sql=false --format=prettyjson --max_rows=1000 --project_id="$PROJECT_ID" "
  SELECT
    query_info.query_hashes.normalized_literals AS query_hash,
    COUNT(*) AS execution_count,
    ARRAY_AGG(query LIMIT 1)[OFFSET(0)] AS sample_query,
    ARRAY_AGG(DISTINCT user_email) AS users,
    ROUND(AVG(total_bytes_processed), 0) AS avg_bytes,
    ROUND(AVG(total_slot_ms), 0) AS avg_slot_ms
  FROM \`$PROJECT_ID\`.\`region-$REGION\`.INFORMATION_SCHEMA.JOBS
  WHERE
    creation_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 DAY)
    AND job_type = 'QUERY'
    AND state = 'DONE'
    AND error_result IS NULL
    AND query_info.query_hashes.normalized_literals IS NOT NULL
  GROUP BY query_hash
  ORDER BY execution_count DESC
  LIMIT 100
" > "$OUTPUT_DIR/frequent_queries.json"
echo "   ✓ frequent_queries.json"

# ── 6. Table Access Patterns ──────────────────────────
echo "⑥ Mapping table access patterns..."
bq query --use_legacy_sql=false --format=prettyjson --max_rows=10000 --project_id="$PROJECT_ID" "
  SELECT
    ref.dataset_id,
    ref.table_id,
    COUNT(*) AS query_count,
    COUNT(DISTINCT user_email) AS distinct_users,
    MIN(creation_time) AS first_accessed,
    MAX(creation_time) AS last_accessed
  FROM \`$PROJECT_ID\`.\`region-$REGION\`.INFORMATION_SCHEMA.JOBS,
    UNNEST(referenced_tables) AS ref
  WHERE
    creation_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 DAY)
    AND job_type = 'QUERY'
    AND state = 'DONE'
    AND error_result IS NULL
  GROUP BY ref.dataset_id, ref.table_id
  ORDER BY query_count DESC
" > "$OUTPUT_DIR/table_access.json"
echo "   ✓ table_access.json"

# ── 7. Per-User Query Stats ───────────────────────────
echo "⑦ Per-user query stats..."
bq query --use_legacy_sql=false --format=prettyjson --max_rows=1000 --project_id="$PROJECT_ID" "
  SELECT
    user_email,
    COUNT(*) AS query_count,
    COUNTIF(statement_type = 'SELECT') AS select_count,
    COUNTIF(statement_type IN ('INSERT', 'UPDATE', 'DELETE', 'MERGE')) AS dml_count,
    ROUND(SUM(total_bytes_processed) / 1e9, 2) AS total_gb_processed,
    ROUND(SUM(total_slot_ms) / 1000 / 3600, 2) AS total_slot_hours
  FROM \`$PROJECT_ID\`.\`region-$REGION\`.INFORMATION_SCHEMA.JOBS
  WHERE
    creation_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 DAY)
    AND job_type = 'QUERY'
    AND state = 'DONE'
  GROUP BY user_email
  ORDER BY query_count DESC
" > "$OUTPUT_DIR/user_stats.json"
echo "   ✓ user_stats.json"

# ── Summary ───────────────────────────────────────────
echo ""
echo "═══════════════════════════════════════════════════"
echo "  EXTRACTION COMPLETE"
echo "═══════════════════════════════════════════════════"
echo ""
ls -lh "$OUTPUT_DIR/"
echo ""
echo "Files:"
echo "  datasets.json             — all datasets in project"
echo "  columns.json              — every column + type across all tables"
echo "  column_descriptions.json  — column descriptions (from COLUMN_FIELD_PATHS)"
echo "  ddls.json                 — full CREATE TABLE DDLs"
echo "  query_logs.json       — all queries (30 days)"
echo "  frequent_queries.json — top queries by frequency (normalized)"
echo "  table_access.json     — which tables get queried most"
echo "  user_stats.json       — per-user query volume + cost"
echo ""
echo "Next: feed columns.json + query_logs.json into Alma"
