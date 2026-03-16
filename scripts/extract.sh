#!/usr/bin/env bash
# ═══════════════════════════════════════════════════════════════════
# BQ Schema & Query Log Extraction
# ═══════════════════════════════════════════════════════════════════
set -euo pipefail

# ── Defaults ──────────────────────────────────────────────────────
PROJECT_ID=""
REGION="us"
OUTPUT_DIR=""
DAYS=30
MAX_ROWS=200000
FORMAT="json"
DATASETS_FILTER=""
SKIP=""
QUIET=false
DRY_RUN=false

# ── Help ──────────────────────────────────────────────────────────
usage() {
  cat <<'USAGE'
Usage: extract.sh --project PROJECT_ID [OPTIONS]

Extract BigQuery schema and query logs for analysis.

Required:
  --project PROJECT_ID      GCP project to extract from

Options:
  --region REGION           INFORMATION_SCHEMA region (default: us)
  --output-dir DIR          Output directory (default: output/YYYYMMDD_HHMMSS)
  --days N                  Query log window in days (default: 30)
  --max-rows N              Max rows per query (default: 200000)
  --format json|csv         Output format for bq query results (default: json)
  --datasets DS1,DS2,...    Only include these datasets (default: all)
  --skip STEP1,STEP2,...    Skip steps. Available steps:
                              datasets, columns, column_descriptions, ddls,
                              query_logs, query_sources, frequent_queries,
                              table_access, user_stats
  --quiet                   Output only the output dir path on success (for CI)
  --dry-run                 Print queries that would run without executing
  --help, -h                Show this help

Examples:
  extract.sh --project my-project --region us
  extract.sh --project my-project --days 7 --skip query_logs,user_stats
  extract.sh --project my-project --datasets sales,marketing
  extract.sh --project my-project --dry-run
  extract.sh --project my-project --quiet
USAGE
}

# ── Argument parsing ──────────────────────────────────────────────
while [[ $# -gt 0 ]]; do
  case "$1" in
    --project)     PROJECT_ID="$2";                                   shift 2 ;;
    --region)      REGION="$(echo "$2" | tr '[:upper:]' '[:lower:]')"; shift 2 ;;
    --output-dir)  OUTPUT_DIR="$2";                                   shift 2 ;;
    --days)        DAYS="$2";                                         shift 2 ;;
    --max-rows)    MAX_ROWS="$2";                                     shift 2 ;;
    --format)      FORMAT="$2";                                       shift 2 ;;
    --datasets)    DATASETS_FILTER="$2";                              shift 2 ;;
    --skip)        SKIP="$2";                                         shift 2 ;;
    --quiet)       QUIET=true;                                        shift   ;;
    --dry-run)     DRY_RUN=true;                                      shift   ;;
    --help|-h)     usage; exit 0 ;;
    *) echo "Unknown option: $1" >&2; usage >&2; exit 1 ;;
  esac
done

# ── Validation ────────────────────────────────────────────────────
if [[ -z "$PROJECT_ID" ]]; then
  echo "Error: --project is required" >&2; usage >&2; exit 1
fi
if [[ "$FORMAT" != "json" && "$FORMAT" != "csv" ]]; then
  echo "Error: --format must be json or csv" >&2; exit 1
fi

# ── Dependency check ──────────────────────────────────────────────
for _cmd in bq python3; do
  if ! command -v "$_cmd" &>/dev/null; then
    echo "Error: '$_cmd' not found on PATH" >&2; exit 1
  fi
done

# ── Setup ─────────────────────────────────────────────────────────
[[ -z "$OUTPUT_DIR" ]] && OUTPUT_DIR="output/$(date +%Y%m%d_%H%M%S)"
EXT="$FORMAT"
BQ_FORMAT="$([[ "$FORMAT" == "csv" ]] && echo "csv" || echo "prettyjson")"
ALL_DATASETS=""   # populated by _init_datasets

[[ "$DRY_RUN" != "true" ]] && mkdir -p "$OUTPUT_DIR"

# ── Logging ───────────────────────────────────────────────────────
log()  { [[ "$QUIET" != "true" ]] && echo "$@" || true; }
warn() { echo "   ⚠ $*" >&2; }

# ── Core helpers ──────────────────────────────────────────────────

should_skip() { [[ ",$SKIP," == *",$1,"* ]]; }

# Run a bq query, writing results to <output>. Returns non-zero on failure.
bq_query() {
  local output="$1" max_rows="$2" sql="$3"
  if [[ "$DRY_RUN" == "true" ]]; then
    log "    [dry-run] → $output"
    log "    $(printf '%s' "$sql" | tr '\n' ' ' | sed 's/  */ /g' | head -c 400) ..."
    return 0
  fi
  local tmp; tmp=$(mktemp)
  if bq query --use_legacy_sql=false --format="$BQ_FORMAT" \
      --max_rows="$max_rows" --project_id="$PROJECT_ID" "$sql" >"$tmp" 2>&1; then
    mv "$tmp" "$output"
  else
    rm -f "$tmp"; return 1
  fi
}

# Count rows in an output file (JSON array or CSV with header).
count_results() {
  local file="$1"
  [[ ! -f "$file" ]] && { echo "?"; return; }
  if [[ "$FORMAT" == "csv" ]]; then
    python3 -c "
with open('$file') as f:
    n = sum(1 for l in f if l.strip())
print(max(0, n - 1))" 2>/dev/null || echo "?"
  else
    python3 -c "import json; print(len(json.load(open('$file'))))" 2>/dev/null || echo "?"
  fi
}

# Count JSON rows where python expression <expr> on row r is truthy.
count_json_where() {
  local file="$1" expr="$2"
  [[ ! -f "$file" ]] && { echo "?"; return; }
  python3 -c "import json; d=json.load(open('$file')); print(sum(1 for r in d if $expr))" 2>/dev/null || echo "?"
}

# Populate global ALL_DATASETS (called once, not in a subshell).
_init_datasets() {
  if [[ "$DRY_RUN" == "true" ]]; then
    ALL_DATASETS="example_dataset"; return 0
  fi
  if [[ -n "$DATASETS_FILTER" ]]; then
    ALL_DATASETS="${DATASETS_FILTER//,/ }"; return 0
  fi
  local raw
  raw=$(bq ls --project_id "$PROJECT_ID" --format=json 2>/dev/null) || return 1
  ALL_DATASETS=$(echo "$raw" | python3 -c \
    "import json,sys; print(' '.join(d['datasetReference']['datasetId'] for d in json.load(sys.stdin)))") || return 1
}

# Try region-level INFORMATION_SCHEMA; fall back to per-dataset UNION ALL.
# Usage: region_or_dataset_query <output> <select_fields> <view> <order_by> [max_rows]
region_or_dataset_query() {
  local output="$1" select_fields="$2" schema_view="$3" order_by="$4"
  local max_rows="${5:-$MAX_ROWS}"
  local region_sql="SELECT $select_fields
  FROM \`$PROJECT_ID\`.\`region-$REGION\`.$schema_view
  ORDER BY $order_by"

  if [[ "$DRY_RUN" == "true" ]]; then
    log "    [dry-run] → $output (region-level, with per-dataset fallback)"
    log "    $(printf '%s' "$region_sql" | tr '\n' ' ' | sed 's/  */ /g')"
    return 0
  fi

  local tmp; tmp=$(mktemp)
  if bq query --use_legacy_sql=false --format="$BQ_FORMAT" \
      --max_rows="$max_rows" --project_id="$PROJECT_ID" "$region_sql" >"$tmp" 2>/dev/null; then
    mv "$tmp" "$output"; return 0
  fi
  rm -f "$tmp"

  log "   (region-level denied, falling back to per-dataset)"
  if [[ -z "$ALL_DATASETS" ]]; then
    warn "no datasets available for fallback"; return 1
  fi
  local union_sql="" ds
  for ds in $ALL_DATASETS; do
    [[ -n "$union_sql" ]] && union_sql+=" UNION ALL "
    union_sql+="SELECT $select_fields FROM \`$PROJECT_ID\`.\`$ds\`.$schema_view"
  done
  bq_query "$output" "$max_rows" "$union_sql ORDER BY $order_by"
}

# Run a per-dataset UNION ALL query (always; no region-level attempt).
# Usage: per_dataset_union_query <output> <select_fields> <view> <order_by> [max_rows]
per_dataset_union_query() {
  local output="$1" select_fields="$2" schema_view="$3" order_by="$4"
  local max_rows="${5:-$MAX_ROWS}"

  if [[ "$DRY_RUN" == "true" ]]; then
    log "    [dry-run] → $output (per-dataset UNION ALL)"
    log "    SELECT $select_fields FROM <project>.<dataset>.$schema_view ... ORDER BY $order_by"
    return 0
  fi
  if [[ -z "$ALL_DATASETS" ]]; then
    warn "no datasets available"; return 1
  fi
  local union_sql="" ds
  for ds in $ALL_DATASETS; do
    [[ -n "$union_sql" ]] && union_sql+=" UNION ALL "
    union_sql+="SELECT $select_fields FROM \`$PROJECT_ID\`.\`$ds\`.$schema_view"
  done
  bq_query "$output" "$max_rows" "$union_sql ORDER BY $order_by"
}

# ── Reusable SQL fragments ─────────────────────────────────────────

# Classifies a job's origin; used verbatim in steps 4 and 4b.
QUERY_SOURCE_CASE="CASE
      WHEN user_email LIKE '%gserviceaccount.com' THEN 'service_account'
      WHEN EXISTS(SELECT 1 FROM UNNEST(labels) l WHERE l.key IN ('airflow_dag_id', 'dbt_invocation_id', 'scheduled_query_id')) THEN 'scheduled'
      WHEN job_creation_reason.code = 'REQUESTED' AND priority = 'BATCH' THEN 'batch'
      ELSE 'ad_hoc'
    END"

# Shared WHERE clause for JOBS queries (note: user_stats intentionally omits error_result filter)
JOBS_WHERE="creation_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL ${DAYS} DAY)
    AND job_type = 'QUERY'
    AND state = 'DONE'
    AND error_result IS NULL"

# ── Initialise dataset list ────────────────────────────────────────
if ! _init_datasets; then
  warn "could not list datasets; per-dataset fallback steps may fail"
fi

# ── Banner ────────────────────────────────────────────────────────
if [[ "$QUIET" != "true" ]]; then
  echo "╔══════════════════════════════════════════════════════╗"
  printf "║  %-52s║\n" "BQ Schema & Query Log Extraction"
  printf "║  %-52s║\n" "Project:  $PROJECT_ID"
  printf "║  %-52s║\n" "Region:   $REGION"
  printf "║  %-52s║\n" "Days:     $DAYS"
  printf "║  %-52s║\n" "Output:   $OUTPUT_DIR/"
  [[ "$DRY_RUN" == "true" ]] && \
  printf "║  %-52s║\n" "*** DRY RUN — no queries will execute ***"
  echo "╚══════════════════════════════════════════════════════╝"
  echo ""
fi

# ── Step 1: List Datasets ─────────────────────────────────────────
log "① Listing datasets..."
if should_skip "datasets"; then
  log "   ↷ skipped"
elif [[ "$DRY_RUN" == "true" ]]; then
  log "    [dry-run] bq ls --project_id $PROJECT_ID → $OUTPUT_DIR/datasets.json"
else
  if bq ls --project_id "$PROJECT_ID" --format=prettyjson >"$OUTPUT_DIR/datasets.json" 2>/dev/null; then
    COUNT=$(python3 -c "import json; print(len(json.load(open('$OUTPUT_DIR/datasets.json'))))" 2>/dev/null || echo "?")
    log "   ✓ $COUNT datasets → datasets.json"
  else
    warn "dataset listing failed (continuing)"
  fi
fi

# ── Step 2: Column Schemas ────────────────────────────────────────
log "② Extracting column schemas..."
if should_skip "columns"; then
  log "   ↷ skipped"
else
  OUT="$OUTPUT_DIR/columns.$EXT"
  if region_or_dataset_query "$OUT" \
      "table_schema AS dataset, table_name, column_name, ordinal_position, data_type, is_nullable, is_partitioning_column, clustering_ordinal_position" \
      "INFORMATION_SCHEMA.COLUMNS" \
      "dataset, table_name, ordinal_position"; then
    [[ "$DRY_RUN" != "true" ]] && COUNT=$(count_results "$OUT") || COUNT="—"
    log "   ✓ $COUNT columns → columns.$EXT"
  else
    warn "column extraction failed (continuing)"
  fi
fi

# ── Step 2b: Column Descriptions ─────────────────────────────────
log "②b Extracting column descriptions..."
if should_skip "column_descriptions"; then
  log "   ↷ skipped"
else
  OUT="$OUTPUT_DIR/column_descriptions.$EXT"
  if per_dataset_union_query "$OUT" \
      "table_schema AS dataset, table_name, column_name, field_path, description, data_type" \
      "INFORMATION_SCHEMA.COLUMN_FIELD_PATHS" \
      "dataset, table_name, column_name"; then
    if [[ "$DRY_RUN" != "true" && -f "$OUT" ]]; then
      if [[ "$FORMAT" == "csv" ]]; then
        COUNT=$(count_results "$OUT")
      else
        COUNT=$(count_json_where "$OUT" "r.get('description')")
      fi
    else
      COUNT="—"
    fi
    log "   ✓ $COUNT described columns → column_descriptions.$EXT"
  else
    [[ "$DRY_RUN" != "true" ]] && echo "[]" >"$OUT"
    warn "column descriptions unavailable (continuing)"
  fi
fi

# ── Step 3: DDLs ──────────────────────────────────────────────────
log "③ Extracting DDLs (CREATE TABLE statements)..."
if should_skip "ddls"; then
  log "   ↷ skipped"
else
  OUT="$OUTPUT_DIR/ddls.$EXT"
  if region_or_dataset_query "$OUT" \
      "table_schema AS dataset, table_name, table_type, ddl" \
      "INFORMATION_SCHEMA.TABLES" \
      "dataset, table_name" \
      10000; then
    [[ "$DRY_RUN" != "true" ]] && COUNT=$(count_results "$OUT") || COUNT="—"
    log "   ✓ $COUNT tables → ddls.$EXT"
  else
    warn "DDL extraction failed (continuing)"
  fi
fi

# ── Step 4: Query Logs ────────────────────────────────────────────
log "④ Extracting query logs ($DAYS days)..."
if should_skip "query_logs"; then
  log "   ↷ skipped"
else
  OUT="$OUTPUT_DIR/query_logs.$EXT"
  if bq_query "$OUT" 500000 "
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
    referenced_tables,
    labels,
    $QUERY_SOURCE_CASE AS query_source
  FROM \`$PROJECT_ID\`.\`region-$REGION\`.INFORMATION_SCHEMA.JOBS
  WHERE
    $JOBS_WHERE
  ORDER BY creation_time DESC"; then
    [[ "$DRY_RUN" != "true" ]] && COUNT=$(count_results "$OUT") || COUNT="—"
    log "   ✓ $COUNT queries → query_logs.$EXT"
  else
    warn "query log extraction failed (continuing)"
  fi
fi

# ── Step 4b: Scheduled vs Ad-hoc Breakdown ───────────────────────
log "④b Scheduled vs ad-hoc breakdown..."
if should_skip "query_sources"; then
  log "   ↷ skipped"
else
  OUT="$OUTPUT_DIR/query_sources.$EXT"
  if bq_query "$OUT" 1000 "
  SELECT
    $QUERY_SOURCE_CASE AS query_source,
    COUNT(*) AS query_count,
    ARRAY_AGG(DISTINCT user_email) AS users,
    ROUND(SUM(total_bytes_processed) / 1e9, 2) AS total_gb_processed,
    ROUND(SUM(total_slot_ms) / 1000 / 3600, 2) AS total_slot_hours
  FROM \`$PROJECT_ID\`.\`region-$REGION\`.INFORMATION_SCHEMA.JOBS
  WHERE
    $JOBS_WHERE
  GROUP BY query_source
  ORDER BY query_count DESC"; then
    log "   ✓ query_sources.$EXT"
  else
    warn "query sources breakdown failed (continuing)"
  fi
fi

# ── Step 5: Frequent Queries ──────────────────────────────────────
log "⑤ Analyzing query frequency..."
if should_skip "frequent_queries"; then
  log "   ↷ skipped"
else
  OUT="$OUTPUT_DIR/frequent_queries.$EXT"
  if bq_query "$OUT" 1000 "
  SELECT
    query_info.query_hashes.normalized_literals AS query_hash,
    COUNT(*) AS execution_count,
    ARRAY_AGG(query LIMIT 1)[OFFSET(0)] AS sample_query,
    ARRAY_AGG(DISTINCT user_email) AS users,
    ROUND(AVG(total_bytes_processed), 0) AS avg_bytes,
    ROUND(AVG(total_slot_ms), 0) AS avg_slot_ms
  FROM \`$PROJECT_ID\`.\`region-$REGION\`.INFORMATION_SCHEMA.JOBS
  WHERE
    $JOBS_WHERE
    AND query_info.query_hashes.normalized_literals IS NOT NULL
  GROUP BY query_hash
  ORDER BY execution_count DESC
  LIMIT 100"; then
    log "   ✓ frequent_queries.$EXT"
  else
    warn "frequent queries extraction failed (continuing)"
  fi
fi

# ── Step 6: Table Access Patterns ────────────────────────────────
log "⑥ Mapping table access patterns..."
if should_skip "table_access"; then
  log "   ↷ skipped"
else
  OUT="$OUTPUT_DIR/table_access.$EXT"
  if bq_query "$OUT" 10000 "
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
    $JOBS_WHERE
  GROUP BY ref.dataset_id, ref.table_id
  ORDER BY query_count DESC"; then
    log "   ✓ table_access.$EXT"
  else
    warn "table access extraction failed (continuing)"
  fi
fi

# ── Step 7: Per-User Stats ─────────────────────────────────────────
log "⑦ Per-user query stats..."
if should_skip "user_stats"; then
  log "   ↷ skipped"
else
  OUT="$OUTPUT_DIR/user_stats.$EXT"
  if bq_query "$OUT" 1000 "
  SELECT
    user_email,
    COUNT(*) AS query_count,
    COUNTIF(statement_type = 'SELECT') AS select_count,
    COUNTIF(statement_type IN ('INSERT', 'UPDATE', 'DELETE', 'MERGE')) AS dml_count,
    ROUND(SUM(total_bytes_processed) / 1e9, 2) AS total_gb_processed,
    ROUND(SUM(total_slot_ms) / 1000 / 3600, 2) AS total_slot_hours
  FROM \`$PROJECT_ID\`.\`region-$REGION\`.INFORMATION_SCHEMA.JOBS
  WHERE
    creation_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL ${DAYS} DAY)
    AND job_type = 'QUERY'
    AND state = 'DONE'
  GROUP BY user_email
  ORDER BY query_count DESC"; then
    log "   ✓ user_stats.$EXT"
  else
    warn "user stats extraction failed (continuing)"
  fi
fi

# ── Done ──────────────────────────────────────────────────────────
if [[ "$DRY_RUN" == "true" ]]; then
  log ""
  log "Dry run complete. No queries were executed."
  exit 0
fi

if [[ "$QUIET" == "true" ]]; then
  echo "$OUTPUT_DIR"
  exit 0
fi

echo ""
echo "═══════════════════════════════════════════════════════"
echo "  EXTRACTION COMPLETE"
echo "═══════════════════════════════════════════════════════"
echo ""
ls -lh "$OUTPUT_DIR/"
echo ""
echo "Output files:"
echo "  datasets.json              — all datasets in project"
echo "  columns.$EXT               — every column + type across all tables"
echo "  column_descriptions.$EXT   — column descriptions (from COLUMN_FIELD_PATHS)"
echo "  ddls.$EXT                  — full CREATE TABLE DDLs"
echo "  query_logs.$EXT            — all queries ($DAYS days) with query_source label"
echo "  query_sources.$EXT         — scheduled vs ad-hoc vs service_account breakdown"
echo "  frequent_queries.$EXT      — top queries by frequency (normalized)"
echo "  table_access.$EXT          — which tables get queried most"
echo "  user_stats.$EXT            — per-user query volume + cost"
echo ""
echo "Next: feed columns.$EXT + query_logs.$EXT into Alma"
