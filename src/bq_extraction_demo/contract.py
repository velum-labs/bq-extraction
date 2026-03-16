"""External contract preserved from the original shell extractor.

This module is intentionally small and declarative. It documents the CLI and
output behavior that callers already depend on while the implementation moves
from Bash to Python.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Final


SUPPORTED_FORMATS: Final[tuple[str, str]] = ("json", "csv")


@dataclass(frozen=True)
class StepContract:
    key: str
    banner: str
    output_name: str


STEP_CONTRACTS: Final[tuple[StepContract, ...]] = (
    StepContract("datasets", "① Listing datasets...", "datasets.json"),
    StepContract("columns", "② Extracting column schemas...", "columns.{ext}"),
    StepContract(
        "column_descriptions",
        "②b Extracting column descriptions...",
        "column_descriptions.{ext}",
    ),
    StepContract("ddls", "③ Extracting DDLs (CREATE TABLE statements)...", "ddls.{ext}"),
    StepContract("query_logs", "④ Extracting query logs ({days} days)...", "query_logs.{ext}"),
    StepContract("query_sources", "④b Scheduled vs ad-hoc breakdown...", "query_sources.{ext}"),
    StepContract("frequent_queries", "⑤ Analyzing query frequency...", "frequent_queries.{ext}"),
    StepContract("table_access", "⑥ Mapping table access patterns...", "table_access.{ext}"),
    StepContract("user_stats", "⑦ Per-user query stats...", "user_stats.{ext}"),
)

STEP_ORDER: Final[tuple[str, ...]] = tuple(step.key for step in STEP_CONTRACTS)
STEP_LOOKUP: Final[dict[str, StepContract]] = {step.key: step for step in STEP_CONTRACTS}


QUERY_SOURCE_CASE: Final[str] = """CASE
      WHEN user_email LIKE '%gserviceaccount.com' THEN 'service_account'
      WHEN EXISTS(
        SELECT 1
        FROM UNNEST(labels) l
        WHERE l.key IN ('airflow_dag_id', 'dbt_invocation_id', 'scheduled_query_id')
      ) THEN 'scheduled'
      WHEN job_creation_reason.code = 'REQUESTED' AND priority = 'BATCH' THEN 'batch'
      ELSE 'ad_hoc'
    END"""


def jobs_where(days: int, *, include_errors: bool = False) -> str:
    clauses = [
        f"creation_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {days} DAY)",
        "job_type = 'QUERY'",
        "state = 'DONE'",
    ]
    if not include_errors:
        clauses.append("error_result IS NULL")
    return "\n    AND ".join(clauses)


def region_information_schema_sql(
    project_id: str,
    region: str,
    select_fields: str,
    schema_view: str,
    order_by: str,
) -> str:
    return f"""SELECT {select_fields}
  FROM `{project_id}`.`region-{region}`.{schema_view}
  ORDER BY {order_by}"""


def per_dataset_information_schema_sql(
    project_id: str,
    datasets: list[str],
    select_fields: str,
    schema_view: str,
    order_by: str,
) -> str:
    unions = [
        f"SELECT {select_fields} FROM `{project_id}`.`{dataset}`.{schema_view}"
        for dataset in datasets
    ]
    return f"{' UNION ALL '.join(unions)} ORDER BY {order_by}"


def query_logs_sql(project_id: str, region: str, days: int) -> str:
    return f"""SELECT
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
    {QUERY_SOURCE_CASE} AS query_source
  FROM `{project_id}`.`region-{region}`.INFORMATION_SCHEMA.JOBS
  WHERE
    {jobs_where(days)}
  ORDER BY creation_time DESC"""


def query_sources_sql(project_id: str, region: str, days: int) -> str:
    return f"""SELECT
    {QUERY_SOURCE_CASE} AS query_source,
    COUNT(*) AS query_count,
    ARRAY_AGG(DISTINCT user_email) AS users,
    ROUND(SUM(total_bytes_processed) / 1e9, 2) AS total_gb_processed,
    ROUND(SUM(total_slot_ms) / 1000 / 3600, 2) AS total_slot_hours
  FROM `{project_id}`.`region-{region}`.INFORMATION_SCHEMA.JOBS
  WHERE
    {jobs_where(days)}
  GROUP BY query_source
  ORDER BY query_count DESC"""


def frequent_queries_sql(project_id: str, region: str, days: int) -> str:
    return f"""SELECT
    query_info.query_hashes.normalized_literals AS query_hash,
    COUNT(*) AS execution_count,
    ARRAY_AGG(query LIMIT 1)[OFFSET(0)] AS sample_query,
    ARRAY_AGG(DISTINCT user_email) AS users,
    ROUND(AVG(total_bytes_processed), 0) AS avg_bytes,
    ROUND(AVG(total_slot_ms), 0) AS avg_slot_ms
  FROM `{project_id}`.`region-{region}`.INFORMATION_SCHEMA.JOBS
  WHERE
    {jobs_where(days)}
    AND query_info.query_hashes.normalized_literals IS NOT NULL
  GROUP BY query_hash
  ORDER BY execution_count DESC
  LIMIT 100"""


def table_access_sql(project_id: str, region: str, days: int) -> str:
    return f"""SELECT
    ref.dataset_id,
    ref.table_id,
    COUNT(*) AS query_count,
    COUNT(DISTINCT user_email) AS distinct_users,
    MIN(creation_time) AS first_accessed,
    MAX(creation_time) AS last_accessed
  FROM `{project_id}`.`region-{region}`.INFORMATION_SCHEMA.JOBS,
    UNNEST(referenced_tables) AS ref
  WHERE
    {jobs_where(days)}
  GROUP BY ref.dataset_id, ref.table_id
  ORDER BY query_count DESC"""


def user_stats_sql(project_id: str, region: str, days: int) -> str:
    return f"""SELECT
    user_email,
    COUNT(*) AS query_count,
    COUNTIF(statement_type = 'SELECT') AS select_count,
    COUNTIF(statement_type IN ('INSERT', 'UPDATE', 'DELETE', 'MERGE')) AS dml_count,
    ROUND(SUM(total_bytes_processed) / 1e9, 2) AS total_gb_processed,
    ROUND(SUM(total_slot_ms) / 1000 / 3600, 2) AS total_slot_hours
  FROM `{project_id}`.`region-{region}`.INFORMATION_SCHEMA.JOBS
  WHERE
    {jobs_where(days, include_errors=True)}
  GROUP BY user_email
  ORDER BY query_count DESC"""

