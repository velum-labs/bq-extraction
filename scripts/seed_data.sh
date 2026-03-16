#!/bin/bash
# Seed sample data into the BQ tables created by Terraform.
# Usage: ./seed_data.sh <project-id>
set -euo pipefail

PROJECT_ID="${1:?Usage: $0 <project-id>}"

echo "Seeding sample data into $PROJECT_ID ..."

# Truncate all tables first (idempotent — safe to re-run)
echo "→ Truncating existing data..."
for TABLE in raw.funds raw.users raw.nav_history raw.transactions \
             staging.stg_transactions staging.stg_users \
             analytics.daily_aum analytics.user_portfolio analytics.cmf_regulatory_report; do
  bq query --use_legacy_sql=false --project_id="$PROJECT_ID" \
    "TRUNCATE TABLE \`$PROJECT_ID.$TABLE\`" > /dev/null 2>&1 || true
done
echo "   ✓ Tables truncated"
echo ""

# ── Raw: Funds ──────────────────────────────────
echo "→ raw.funds"
bq query --use_legacy_sql=false --project_id="$PROJECT_ID" '
INSERT INTO `'"$PROJECT_ID"'.raw.funds`
  (fund_id, fund_name, fund_type, currency, inception_date, is_active, cmf_rut, management_fee_bps, loaded_at)
VALUES
  ("F001", "Fintual Conservative", "mutual_fund", "CLP", "2018-03-15", true, "76.588.827-2", 59, CURRENT_TIMESTAMP()),
  ("F002", "Fintual Moderate", "mutual_fund", "CLP", "2018-03-15", true, "76.588.828-0", 79, CURRENT_TIMESTAMP()),
  ("F003", "Fintual Risky Norris", "mutual_fund", "CLP", "2018-03-15", true, "76.588.829-9", 119, CURRENT_TIMESTAMP()),
  ("F004", "Fintual Very Risky Norris", "mutual_fund", "CLP", "2019-06-01", true, "76.588.830-2", 119, CURRENT_TIMESTAMP()),
  ("F005", "Fintual Dólar", "money_market", "USD", "2020-01-10", true, "76.588.831-0", 39, CURRENT_TIMESTAMP()),
  ("F006", "Fintual APV Conservative", "apv", "CLP", "2019-09-01", true, "76.588.832-9", 59, CURRENT_TIMESTAMP()),
  ("F007", "Fintual Deprecated Fund", "mutual_fund", "CLP", "2017-01-01", false, "76.588.833-7", 99, CURRENT_TIMESTAMP())
'

# ── Raw: Users ──────────────────────────────────
echo "→ raw.users"
bq query --use_legacy_sql=false --project_id="$PROJECT_ID" '
INSERT INTO `'"$PROJECT_ID"'.raw.users`
  (user_id, email_hash, registration_date, country_code, risk_profile, is_qualified_investor, kyc_status, loaded_at)
VALUES
  ("U001", "a1b2c3d4", "2019-05-10", "CL", "moderate", false, "approved", CURRENT_TIMESTAMP()),
  ("U002", "e5f6g7h8", "2020-01-22", "CL", "aggressive", true, "approved", CURRENT_TIMESTAMP()),
  ("U003", "i9j0k1l2", "2021-03-14", "CL", "conservative", false, "approved", CURRENT_TIMESTAMP()),
  ("U004", "m3n4o5p6", "2022-07-08", "PE", "moderate", false, "pending", CURRENT_TIMESTAMP()),
  ("U005", "q7r8s9t0", "2023-11-30", "CL", "aggressive", false, "approved", CURRENT_TIMESTAMP()),
  ("U006", "u1v2w3x4", "2024-02-14", "MX", "conservative", false, "rejected", CURRENT_TIMESTAMP())
'

# ── Raw: NAV History ────────────────────────────
echo "→ raw.nav_history"
bq query --use_legacy_sql=false --project_id="$PROJECT_ID" '
INSERT INTO `'"$PROJECT_ID"'.raw.nav_history`
  (fund_id, nav_date, nav_per_share, total_aum, shares_outstanding, daily_return_pct, loaded_at)
VALUES
  ("F001", "2026-03-14", 1523.45, 85000000000, 55782341.2, 0.02, CURRENT_TIMESTAMP()),
  ("F001", "2026-03-15", 1524.12, 85100000000, 55830000.0, 0.04, CURRENT_TIMESTAMP()),
  ("F002", "2026-03-14", 2341.78, 120000000000, 51244123.5, -0.03, CURRENT_TIMESTAMP()),
  ("F002", "2026-03-15", 2340.11, 119800000000, 51200000.0, -0.07, CURRENT_TIMESTAMP()),
  ("F003", "2026-03-14", 4521.90, 200000000000, 44234567.8, 0.15, CURRENT_TIMESTAMP()),
  ("F003", "2026-03-15", 4530.22, 200500000000, 44280000.0, 0.18, CURRENT_TIMESTAMP()),
  ("F004", "2026-03-14", 3890.44, 95000000000, 24418000.0, 0.22, CURRENT_TIMESTAMP()),
  ("F004", "2026-03-15", 3899.01, 95300000000, 24445000.0, 0.22, CURRENT_TIMESTAMP()),
  ("F005", "2026-03-14", 1001.23, 30000000000, 29963000.0, 0.01, CURRENT_TIMESTAMP()),
  ("F005", "2026-03-15", 1001.34, 30050000000, 30010000.0, 0.01, CURRENT_TIMESTAMP())
'

# ── Raw: Transactions ───────────────────────────
echo "→ raw.transactions"
bq query --use_legacy_sql=false --project_id="$PROJECT_ID" '
INSERT INTO `'"$PROJECT_ID"'.raw.transactions`
  (transaction_id, user_id, fund_id, transaction_type, amount_clp, amount_usd, shares, nav_at_transaction, transaction_date, status, source_system, loaded_at)
VALUES
  ("T001", "U001", "F002", "deposit", 5000000, NULL, 2134.56, 2341.78, "2026-03-14", "settled", "web_app", CURRENT_TIMESTAMP()),
  ("T002", "U002", "F003", "deposit", 10000000, NULL, 2211.89, 4521.90, "2026-03-14", "settled", "mobile_app", CURRENT_TIMESTAMP()),
  ("T003", "U001", "F001", "withdrawal", 2000000, NULL, 1312.89, 1523.45, "2026-03-14", "settled", "web_app", CURRENT_TIMESTAMP()),
  ("T004", "U003", "F005", "deposit", 15000000, 16250.00, 14981.55, 1001.23, "2026-03-15", "settled", "web_app", CURRENT_TIMESTAMP()),
  ("T005", "U002", "F004", "switch_in", 8000000, NULL, 2056.31, 3890.44, "2026-03-15", "settled", "mobile_app", CURRENT_TIMESTAMP()),
  ("T006", "U005", "F003", "deposit", 3000000, NULL, 662.07, 4530.22, "2026-03-15", "pending", "web_app", CURRENT_TIMESTAMP()),
  ("T007", "U004", "F001", "deposit", 1000000, NULL, 656.13, 1524.12, "2026-03-15", "pending", "api", CURRENT_TIMESTAMP()),
  ("T008", "U001", "F002", "withdrawal", 3000000, NULL, 1282.00, 2340.11, "2026-03-15", "cancelled", "web_app", CURRENT_TIMESTAMP())
'

# ── Staging: stg_transactions ───────────────────
echo "→ staging.stg_transactions"
bq query --use_legacy_sql=false --project_id="$PROJECT_ID" '
INSERT INTO `'"$PROJECT_ID"'.staging.stg_transactions`
  (transaction_id, user_id, fund_id, transaction_type, amount_clp, shares, transaction_date, loaded_at)
SELECT transaction_id, user_id, fund_id, transaction_type, amount_clp, shares, transaction_date, loaded_at
FROM `'"$PROJECT_ID"'.raw.transactions`
WHERE status = "settled"
'

# ── Staging: stg_users ──────────────────────────
echo "→ staging.stg_users"
bq query --use_legacy_sql=false --project_id="$PROJECT_ID" '
INSERT INTO `'"$PROJECT_ID"'.staging.stg_users`
  (user_id, registration_date, country_code, risk_profile, is_qualified_investor)
SELECT user_id, registration_date, country_code, risk_profile, is_qualified_investor
FROM `'"$PROJECT_ID"'.raw.users`
WHERE kyc_status = "approved"
'

# ── Analytics: daily_aum ────────────────────────
echo "→ analytics.daily_aum"
bq query --use_legacy_sql=false --project_id="$PROJECT_ID" '
INSERT INTO `'"$PROJECT_ID"'.analytics.daily_aum`
  (report_date, fund_id, fund_name, total_aum_clp, total_aum_usd, investor_count, daily_net_flow_clp, daily_return_pct)
VALUES
  ("2026-03-14", "F001", "Fintual Conservative", 85000000000, 91891891, 12450, -2000000, 0.02),
  ("2026-03-14", "F002", "Fintual Moderate", 120000000000, 129729729, 18230, 5000000, -0.03),
  ("2026-03-14", "F003", "Fintual Risky Norris", 200000000000, 216216216, 31500, 10000000, 0.15),
  ("2026-03-15", "F001", "Fintual Conservative", 85100000000, 92000000, 12455, 1000000, 0.04),
  ("2026-03-15", "F002", "Fintual Moderate", 119800000000, 129513513, 18228, -3000000, -0.07),
  ("2026-03-15", "F003", "Fintual Risky Norris", 200500000000, 216756756, 31510, 3000000, 0.18)
'

# ── Analytics: user_portfolio ───────────────────
echo "→ analytics.user_portfolio"
bq query --use_legacy_sql=false --project_id="$PROJECT_ID" '
INSERT INTO `'"$PROJECT_ID"'.analytics.user_portfolio`
  (user_id, fund_id, shares_held, current_value_clp, total_deposited_clp, total_withdrawn_clp, unrealized_gain_clp, first_investment_date, last_transaction_date, snapshot_date)
VALUES
  ("U001", "F001", 5200.00, 7925424, 8000000, 2000000, -74576, "2020-06-15", "2026-03-14", "2026-03-15"),
  ("U001", "F002", 4300.00, 10062473, 10000000, 0, 62473, "2021-01-10", "2026-03-14", "2026-03-15"),
  ("U002", "F003", 8500.00, 38506870, 35000000, 0, 3506870, "2020-08-22", "2026-03-14", "2026-03-15"),
  ("U002", "F004", 2056.31, 8018653, 8000000, 0, 18653, "2026-03-15", "2026-03-15", "2026-03-15"),
  ("U003", "F005", 14981.55, 15001686, 15000000, 0, 1686, "2026-03-15", "2026-03-15", "2026-03-15")
'

# ── Analytics: cmf_regulatory_report ────────────
echo "→ analytics.cmf_regulatory_report"
bq query --use_legacy_sql=false --project_id="$PROJECT_ID" '
INSERT INTO `'"$PROJECT_ID"'.analytics.cmf_regulatory_report`
  (report_month, fund_id, fund_name, cmf_rut, eom_aum_clp, eom_investor_count, monthly_deposits_clp, monthly_withdrawals_clp, monthly_return_pct, management_fee_collected_clp, generated_at)
VALUES
  ("2026-02-01", "F001", "Fintual Conservative", "76.588.827-2", 84000000000, 12300, 5200000000, 4800000000, 0.35, 41300000, CURRENT_TIMESTAMP()),
  ("2026-02-01", "F002", "Fintual Moderate", "76.588.828-0", 118000000000, 18100, 7800000000, 6200000000, 0.52, 77700000, CURRENT_TIMESTAMP()),
  ("2026-02-01", "F003", "Fintual Risky Norris", "76.588.829-9", 198000000000, 31200, 12000000000, 8500000000, 1.23, 196350000, CURRENT_TIMESTAMP())
'

echo ""
echo "✅ Seed data complete!"
echo "   raw:       funds (7), users (6), nav_history (10), transactions (8)"
echo "   staging:   stg_transactions (5), stg_users (4)"
echo "   analytics: daily_aum (6), user_portfolio (5), cmf_regulatory_report (3)"
