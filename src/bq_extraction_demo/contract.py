"""Registry and query builders for discovery-based extraction."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Final


SUPPORTED_FORMATS: Final[tuple[str, str]] = ("json", "csv")


@dataclass(frozen=True)
class ObjectFamilySpec:
    key: str
    description: str
    api_backed: bool


OBJECT_FAMILIES: Final[tuple[ObjectFamilySpec, ...]] = (
    ObjectFamilySpec("datasets", "Datasets discovered through the BigQuery API", True),
    ObjectFamilySpec("tables", "Tables and views discovered through the BigQuery API", True),
    ObjectFamilySpec("routines", "Routines discovered through the BigQuery API", True),
    ObjectFamilySpec("models", "BigQuery ML models discovered through the BigQuery API", True),
    ObjectFamilySpec("jobs", "Optional jobs-derived analytics via INFORMATION_SCHEMA", False),
)

OBJECT_FAMILY_KEYS: Final[tuple[str, ...]] = tuple(family.key for family in OBJECT_FAMILIES)
OBJECT_FAMILY_LOOKUP: Final[dict[str, ObjectFamilySpec]] = {
    family.key: family for family in OBJECT_FAMILIES
}


@dataclass(frozen=True)
class CapabilitySpec:
    key: str
    family: str
    description: str
    strategy: str
    required_permissions: tuple[str, ...]
    information_schema_view: str | None = None
    select_fields: str | None = None
    order_by: str | None = None
    fallback_to_dataset: bool = False
    max_rows: int | None = None


CAPABILITY_SPECS: Final[tuple[CapabilitySpec, ...]] = (
    CapabilitySpec(
        key="tables.ddls",
        family="tables",
        description="DDL statements from INFORMATION_SCHEMA.TABLES",
        strategy="information_schema",
        required_permissions=(
            "bigquery.routines.list",
            "bigquery.routines.get",
            "bigquery.tables.list",
            "bigquery.tables.get",
        ),
        information_schema_view="INFORMATION_SCHEMA.TABLES",
        select_fields="table_catalog, table_schema AS dataset, table_name, table_type, ddl",
        order_by="dataset, table_name",
        fallback_to_dataset=True,
    ),
    CapabilitySpec(
        key="jobs.query_logs",
        family="jobs",
        description="Per-query job history from INFORMATION_SCHEMA.JOBS_BY_PROJECT",
        strategy="job_recipe",
        required_permissions=("bigquery.jobs.listAll",),
    ),
    CapabilitySpec(
        key="jobs.query_sources",
        family="jobs",
        description="Query source breakdown from INFORMATION_SCHEMA.JOBS_BY_PROJECT",
        strategy="job_recipe",
        required_permissions=("bigquery.jobs.listAll",),
    ),
    CapabilitySpec(
        key="jobs.frequent_queries",
        family="jobs",
        description="Frequent query rollups from INFORMATION_SCHEMA.JOBS_BY_PROJECT",
        strategy="job_recipe",
        required_permissions=("bigquery.jobs.listAll",),
    ),
    CapabilitySpec(
        key="jobs.table_access",
        family="jobs",
        description="Table access rollups from INFORMATION_SCHEMA.JOBS_BY_PROJECT",
        strategy="job_recipe",
        required_permissions=("bigquery.jobs.listAll",),
    ),
    CapabilitySpec(
        key="jobs.user_stats",
        family="jobs",
        description="Per-user query stats from INFORMATION_SCHEMA.JOBS_BY_PROJECT",
        strategy="job_recipe",
        required_permissions=("bigquery.jobs.listAll",),
    ),
)

CAPABILITY_KEYS: Final[tuple[str, ...]] = tuple(spec.key for spec in CAPABILITY_SPECS)
CAPABILITY_LOOKUP: Final[dict[str, CapabilitySpec]] = {
    spec.key: spec for spec in CAPABILITY_SPECS
}


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


def family_output_name(family_key: str, output_format: str) -> str:
    return f"{family_key}.{output_format}"


def capability_output_name(capability_key: str, output_format: str) -> str:
    return f"{capability_key}.{output_format}"


def location_to_region_qualifier(location: str) -> str:
    return location.lower()


def jobs_where(
    days: int,
    *,
    include_errors: bool = False,
    exclude_script_statements: bool = False,
) -> str:
    clauses = [
        f"creation_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {days} DAY)",
        "job_type = 'QUERY'",
        "state = 'DONE'",
    ]
    if not include_errors:
        clauses.append("error_result IS NULL")
    if exclude_script_statements:
        clauses.append("statement_type != 'SCRIPT'")
    return "\n    AND ".join(clauses)


def region_information_schema_sql(
    project_id: str,
    location: str,
    select_fields: str,
    schema_view: str,
    order_by: str,
) -> str:
    qualifier = location_to_region_qualifier(location)
    return f"""SELECT {select_fields}
  FROM `{project_id}`.`region-{qualifier}`.{schema_view}
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


def probe_sql(sql: str) -> str:
    return f"SELECT * FROM ({sql}) LIMIT 1"


def build_capability_sql(
    spec: CapabilitySpec,
    *,
    project_id: str,
    location: str,
    days: int,
    datasets: list[str] | None = None,
    use_dataset_scope: bool = False,
) -> str:
    if spec.strategy == "information_schema":
        if spec.select_fields is None or spec.information_schema_view is None or spec.order_by is None:
            raise ValueError(f"incomplete INFORMATION_SCHEMA capability: {spec.key}")
        if use_dataset_scope:
            if not datasets:
                raise ValueError(f"dataset-scoped execution requested without datasets for {spec.key}")
            return per_dataset_information_schema_sql(
                project_id,
                datasets,
                spec.select_fields,
                spec.information_schema_view,
                spec.order_by,
            )
        return region_information_schema_sql(
            project_id,
            location,
            spec.select_fields,
            spec.information_schema_view,
            spec.order_by,
        )

    if spec.key == "jobs.query_logs":
        return query_logs_sql(project_id, location, days)
    if spec.key == "jobs.query_sources":
        return query_sources_sql(project_id, location, days)
    if spec.key == "jobs.frequent_queries":
        return frequent_queries_sql(project_id, location, days)
    if spec.key == "jobs.table_access":
        return table_access_sql(project_id, location, days)
    if spec.key == "jobs.user_stats":
        return user_stats_sql(project_id, location, days)
    raise ValueError(f"unknown capability key: {spec.key}")


def jobs_view(project_id: str, location: str) -> str:
    qualifier = location_to_region_qualifier(location)
    return f"`{project_id}`.`region-{qualifier}`.INFORMATION_SCHEMA.JOBS_BY_PROJECT"


def query_logs_sql(project_id: str, location: str, days: int) -> str:
    view = jobs_view(project_id, location)
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
  FROM {view}
  WHERE
    {jobs_where(days)}
  ORDER BY creation_time DESC"""


def query_sources_sql(project_id: str, location: str, days: int) -> str:
    view = jobs_view(project_id, location)
    return f"""SELECT
    {QUERY_SOURCE_CASE} AS query_source,
    COUNT(*) AS query_count,
    ARRAY_AGG(DISTINCT user_email) AS users,
    ROUND(SUM(total_bytes_processed) / 1e9, 2) AS total_gb_processed,
    ROUND(SUM(total_slot_ms) / 1000 / 3600, 2) AS total_slot_hours
  FROM {view}
  WHERE
    {jobs_where(days, exclude_script_statements=True)}
  GROUP BY query_source
  ORDER BY query_count DESC"""


def frequent_queries_sql(project_id: str, location: str, days: int) -> str:
    view = jobs_view(project_id, location)
    return f"""SELECT
    query_info.query_hashes.normalized_literals AS query_hash,
    COUNT(*) AS execution_count,
    ARRAY_AGG(query LIMIT 1)[OFFSET(0)] AS sample_query,
    ARRAY_AGG(DISTINCT user_email) AS users,
    ROUND(AVG(total_bytes_processed), 0) AS avg_bytes,
    ROUND(AVG(total_slot_ms), 0) AS avg_slot_ms
  FROM {view}
  WHERE
    {jobs_where(days, exclude_script_statements=True)}
    AND query_info.query_hashes.normalized_literals IS NOT NULL
  GROUP BY query_hash
  ORDER BY execution_count DESC
  LIMIT 100"""


def table_access_sql(project_id: str, location: str, days: int) -> str:
    view = jobs_view(project_id, location)
    return f"""SELECT
    ref.dataset_id,
    ref.table_id,
    COUNT(*) AS query_count,
    COUNT(DISTINCT user_email) AS distinct_users,
    MIN(creation_time) AS first_accessed,
    MAX(creation_time) AS last_accessed
  FROM {view},
    UNNEST(referenced_tables) AS ref
  WHERE
    {jobs_where(days, exclude_script_statements=True)}
  GROUP BY ref.dataset_id, ref.table_id
  ORDER BY query_count DESC"""


def user_stats_sql(project_id: str, location: str, days: int) -> str:
    view = jobs_view(project_id, location)
    return f"""SELECT
    user_email,
    COUNT(*) AS query_count,
    COUNTIF(statement_type = 'SELECT') AS select_count,
    COUNTIF(statement_type IN ('INSERT', 'UPDATE', 'DELETE', 'MERGE')) AS dml_count,
    ROUND(SUM(total_bytes_processed) / 1e9, 2) AS total_gb_processed,
    ROUND(SUM(total_slot_ms) / 1000 / 3600, 2) AS total_slot_hours
  FROM {view}
  WHERE
    {jobs_where(days, include_errors=True, exclude_script_statements=True)}
  GROUP BY user_email
  ORDER BY query_count DESC"""

