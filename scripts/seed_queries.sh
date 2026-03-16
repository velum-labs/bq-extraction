#!/bin/bash
# Simulate realistic query traffic so INFORMATION_SCHEMA.JOBS has data.
# These mimic what a data team, Grafana dashboards, and Airflow DAGs would run.
# Usage: ./seed_queries.sh <project-id>
set -euo pipefail

PROJECT_ID="${1:?Usage: $0 <project-id> [region]}"
REGION="${2:-US}"

echo "Seeding query traffic into $PROJECT_ID (region: $REGION) ..."
echo "(These populate INFORMATION_SCHEMA.JOBS for the extraction demo)"
echo ""

# Helper: run a query silently, just to create a JOBS entry
run_query() {
  local label="$1"
  local sql="$2"
  echo "  → $label"
  bq query --use_legacy_sql=false --project_id="$PROJECT_ID" --max_rows=0 "$sql" > /dev/null 2>&1 || true
}

# ── Dashboard-style queries (Grafana / Metabase) ──

run_query "Dashboard: Total AUM by fund (latest)" '
SELECT fund_name, total_aum_clp, daily_return_pct
FROM `'"$PROJECT_ID"'.analytics.daily_aum`
WHERE report_date = (SELECT MAX(report_date) FROM `'"$PROJECT_ID"'.analytics.daily_aum`)
ORDER BY total_aum_clp DESC
'

run_query "Dashboard: AUM time series (7 days)" '
SELECT report_date, fund_name, total_aum_clp
FROM `'"$PROJECT_ID"'.analytics.daily_aum`
WHERE report_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)
ORDER BY report_date, fund_name
'

run_query "Dashboard: Net flows by fund" '
SELECT fund_name, SUM(daily_net_flow_clp) AS net_flow_7d
FROM `'"$PROJECT_ID"'.analytics.daily_aum`
WHERE report_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)
GROUP BY fund_name
ORDER BY net_flow_7d DESC
'

run_query "Dashboard: Investor count trend" '
SELECT report_date, SUM(investor_count) AS total_investors
FROM `'"$PROJECT_ID"'.analytics.daily_aum`
GROUP BY report_date
ORDER BY report_date
'

# ── Analyst ad-hoc queries ──

run_query "Analyst: Top investors by portfolio value" '
SELECT user_id, SUM(current_value_clp) AS total_value
FROM `'"$PROJECT_ID"'.analytics.user_portfolio`
GROUP BY user_id
ORDER BY total_value DESC
LIMIT 10
'

run_query "Analyst: Transaction mix by type" '
SELECT transaction_type, COUNT(*) AS cnt, SUM(amount_clp) AS total_clp
FROM `'"$PROJECT_ID"'.staging.stg_transactions`
GROUP BY transaction_type
ORDER BY total_clp DESC
'

run_query "Analyst: User risk profile distribution" '
SELECT risk_profile, COUNT(*) AS user_count
FROM `'"$PROJECT_ID"'.staging.stg_users`
GROUP BY risk_profile
'

run_query "Analyst: Raw vs settled transaction diff" '
SELECT
  (SELECT COUNT(*) FROM `'"$PROJECT_ID"'.raw.transactions`) AS raw_count,
  (SELECT COUNT(*) FROM `'"$PROJECT_ID"'.staging.stg_transactions`) AS staged_count
'

# ── Airflow/dbt-style transformation queries ──

run_query "dbt: stg_transactions rebuild" '
SELECT transaction_id, user_id, fund_id, transaction_type, amount_clp, shares, transaction_date, loaded_at
FROM `'"$PROJECT_ID"'.raw.transactions`
WHERE status = "settled"
'

run_query "dbt: daily_aum calculation" '
SELECT
  n.nav_date AS report_date,
  n.fund_id,
  f.fund_name,
  n.total_aum AS total_aum_clp,
  CAST(n.total_aum / 925 AS INT64) AS total_aum_usd,
  COUNT(DISTINCT t.user_id) AS investor_count,
  SUM(CASE WHEN t.transaction_type = "deposit" THEN t.amount_clp ELSE -t.amount_clp END) AS daily_net_flow_clp,
  n.daily_return_pct
FROM `'"$PROJECT_ID"'.raw.nav_history` n
LEFT JOIN `'"$PROJECT_ID"'.raw.funds` f ON n.fund_id = f.fund_id
LEFT JOIN `'"$PROJECT_ID"'.raw.transactions` t ON n.fund_id = t.fund_id AND n.nav_date = t.transaction_date
GROUP BY n.nav_date, n.fund_id, f.fund_name, n.total_aum, n.daily_return_pct
'

run_query "dbt: cmf_report generation" '
SELECT
  DATE_TRUNC(n.nav_date, MONTH) AS report_month,
  n.fund_id,
  f.fund_name,
  f.cmf_rut,
  MAX(n.total_aum) AS eom_aum_clp
FROM `'"$PROJECT_ID"'.raw.nav_history` n
JOIN `'"$PROJECT_ID"'.raw.funds` f ON n.fund_id = f.fund_id
GROUP BY DATE_TRUNC(n.nav_date, MONTH), n.fund_id, f.fund_name, f.cmf_rut
'

# ── Cross-dataset joins (the kind that cause semantic drift) ──

run_query "Cross-dataset: revenue by fund (definition A — management fee)" '
SELECT
  f.fund_name,
  n.total_aum * f.management_fee_bps / 10000 / 365 AS daily_revenue_clp
FROM `'"$PROJECT_ID"'.raw.nav_history` n
JOIN `'"$PROJECT_ID"'.raw.funds` f ON n.fund_id = f.fund_id
WHERE n.nav_date = "2026-03-15"
'

run_query "Cross-dataset: revenue by fund (definition B — from CMF report)" '
SELECT fund_name, management_fee_collected_clp AS monthly_revenue
FROM `'"$PROJECT_ID"'.analytics.cmf_regulatory_report`
WHERE report_month = "2026-02-01"
'

# ── Schema inspection queries (meta) ──

run_query "Meta: table listing" '
SELECT table_schema, table_name, table_type
FROM `'"$PROJECT_ID"'.`region-'"$REGION"'`.INFORMATION_SCHEMA.TABLES
ORDER BY table_schema, table_name
'

run_query "Meta: column listing" '
SELECT table_schema, table_name, column_name, data_type
FROM `'"$PROJECT_ID"'.`region-'"$REGION"'`.INFORMATION_SCHEMA.COLUMNS
ORDER BY table_schema, table_name, ordinal_position
LIMIT 100
'

echo ""
echo "✅ Query seeding complete! $(date)"
echo "   15 queries executed — visible in INFORMATION_SCHEMA.JOBS"
echo ""
echo "   Highlights for the demo:"
echo "   - 2 competing 'revenue' definitions (management fee calc vs CMF report)"
echo "   - Dashboard queries vs analyst ad-hoc vs dbt transforms"
echo "   - Cross-dataset joins that Alma would flag"
