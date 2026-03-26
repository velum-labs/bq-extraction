"""Loader for bq-extraction result directories.

Reads timestamped run folders produced by the extraction pipeline,
normalises JSON artifacts into pandas DataFrames with typed columns,
and degrades gracefully when a run only contains a subset of artifacts.

Usage::

    from bq_extraction.loader import load_runs

    runs = load_runs("output/")
    runs.datasets      # pd.DataFrame | None
    runs.query_logs    # pd.DataFrame | None
    ...

Atlas / Alma bridge
-------------------
The normalised frames are shaped to map into downstream systems:

- ``datasets`` + ``tables`` -> Atlas ``assets`` table (id=project.dataset.table)
- ``frequent_queries`` -> Atlas ``queries`` (fingerprint=query_hash)
- ``user_stats`` -> Atlas ``consumers`` (kind=user|service)
- ``table_access`` -> Atlas ``edges`` + ``consumer_assets``
- ``query_logs`` -> Alma ``QueryEvent`` proto via ``Ingest`` RPC
- ``ddls`` -> Atlas ``schema_snapshots`` / Alma source adapter snapshots

See the notebook section "Atlas / Alma Bridge Notes" for step-by-step
hydration instructions.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from pathlib import Path

import pandas as pd

ARTIFACT_FILES: dict[str, str] = {
    "datasets": "datasets.json",
    "tables": "tables.json",
    "ddls": "tables.ddls.json",
    "models": "models.json",
    "routines": "routines.json",
    "query_logs": "jobs.query_logs.json",
    "query_sources": "jobs.query_sources.json",
    "frequent_queries": "jobs.frequent_queries.json",
    "table_access": "jobs.table_access.json",
    "user_stats": "jobs.user_stats.json",
}

_RUN_DIR_RE = re.compile(r"^\d{8}_\d{6}_.+$")

_PROBE_TABLE_IDS = {
    "INFORMATION_SCHEMA.JOBS_BY_PROJECT",
    "INFORMATION_SCHEMA.TABLES",
    "INFORMATION_SCHEMA.COLUMNS",
    "INFORMATION_SCHEMA.TABLE_OPTIONS",
    "INFORMATION_SCHEMA.TABLE_STORAGE",
}


@dataclass
class ExtractionRuns:
    """Holds concatenated DataFrames across all loaded runs."""

    datasets: pd.DataFrame | None = None
    tables: pd.DataFrame | None = None
    ddls: pd.DataFrame | None = None
    models: pd.DataFrame | None = None
    routines: pd.DataFrame | None = None
    query_logs: pd.DataFrame | None = None
    query_sources: pd.DataFrame | None = None
    frequent_queries: pd.DataFrame | None = None
    table_access: pd.DataFrame | None = None
    user_stats: pd.DataFrame | None = None
    run_summary: pd.DataFrame = field(default_factory=pd.DataFrame)


def _infer_project_id(run_dir_name: str) -> str:
    """Extract the project slug from a run directory name.

    Pattern: ``YYYYMMDD_HHMMSS_<project-slug>``
    """
    parts = run_dir_name.split("_", 2)
    return parts[2] if len(parts) >= 3 else run_dir_name


def _load_json_array(path: Path) -> list[dict]:
    with open(path, encoding="utf-8") as fh:
        data = json.load(fh)
    if not isinstance(data, list):
        return []
    return data


def _normalise_datasets(raw: list[dict], run_id: str, project_id: str) -> pd.DataFrame:
    rows: list[dict] = []
    for d in raw:
        ref = d.get("datasetReference", {})
        labels = d.get("labels", {})
        rows.append({
            "run_id": run_id,
            "project_id": ref.get("projectId", project_id),
            "dataset_id": ref.get("datasetId", ""),
            "location": d.get("location", ""),
            "label_producer": labels.get("producer", ""),
            "label_maturity": labels.get("maturity", ""),
            "labels_raw": labels if labels else None,
        })
    return pd.DataFrame(rows)


def _normalise_tables(raw: list[dict], run_id: str, project_id: str) -> pd.DataFrame:
    rows: list[dict] = []
    for t in raw:
        ref = t.get("tableReference", {})
        labels = t.get("labels", {})
        schema_fields = t.get("schema", {}).get("fields", [])
        view = t.get("view", {})
        rows.append({
            "run_id": run_id,
            "project_id": ref.get("projectId", project_id),
            "dataset_id": ref.get("datasetId", ""),
            "table_id": ref.get("tableId", ""),
            "table_type": t.get("type", ""),
            "location": t.get("location", t.get("dataset_location", "")),
            "num_bytes": _safe_int(t.get("numBytes")),
            "num_rows": _safe_int(t.get("numRows")),
            "column_count": len(schema_fields),
            "schema_fields": schema_fields or None,
            "view_query": view.get("query", ""),
            "view_use_legacy_sql": str(view.get("useLegacySql", "")).lower() == "true",
            "label_maturity": labels.get("maturity", ""),
            "label_producer": labels.get("producer", ""),
        })
    return pd.DataFrame(rows)


def _normalise_ddls(raw: list[dict], run_id: str, project_id: str) -> pd.DataFrame:
    rows: list[dict] = []
    for d in raw:
        rows.append({
            "run_id": run_id,
            "project_id": d.get("table_catalog", project_id),
            "dataset": d.get("dataset", ""),
            "table_name": d.get("table_name", ""),
            "table_type": d.get("table_type", ""),
            "ddl": d.get("ddl", ""),
            "location": d.get("location", ""),
        })
    return pd.DataFrame(rows)


def _normalise_query_logs(raw: list[dict], run_id: str, project_id: str) -> pd.DataFrame:
    rows: list[dict] = []
    for r in raw:
        refs = r.get("referenced_tables", []) or []
        rows.append({
            "run_id": run_id,
            "project_id": project_id,
            "job_id": r.get("job_id", ""),
            "user_email": r.get("user_email", ""),
            "query": r.get("query", ""),
            "statement_type": r.get("statement_type", ""),
            "creation_time": pd.to_datetime(r.get("creation_time"), errors="coerce"),
            "duration_seconds": _safe_float(r.get("duration_seconds")),
            "total_bytes_processed": _safe_float(r.get("total_bytes_processed")),
            "total_bytes_billed": _safe_float(r.get("total_bytes_billed")),
            "total_slot_ms": _safe_float(r.get("total_slot_ms")),
            "cache_hit": str(r.get("cache_hit", "")).lower() == "true",
            "query_source": r.get("query_source", ""),
            "location": r.get("location", ""),
            "referenced_tables": refs,
            "is_probe": _is_probe_query(refs),
        })
    return pd.DataFrame(rows)


def _normalise_query_sources(raw: list[dict], run_id: str, project_id: str) -> pd.DataFrame:
    rows: list[dict] = []
    for r in raw:
        rows.append({
            "run_id": run_id,
            "project_id": project_id,
            "query_source": r.get("query_source", ""),
            "query_count": _safe_int(r.get("query_count")),
            "users": r.get("users", []),
            "total_gb_processed": _safe_float(r.get("total_gb_processed")),
            "total_slot_hours": _safe_float(r.get("total_slot_hours")),
            "location": r.get("location", ""),
        })
    return pd.DataFrame(rows)


def _normalise_frequent_queries(raw: list[dict], run_id: str, project_id: str) -> pd.DataFrame:
    rows: list[dict] = []
    for r in raw:
        rows.append({
            "run_id": run_id,
            "project_id": project_id,
            "query_hash": r.get("query_hash", ""),
            "execution_count": _safe_int(r.get("execution_count")),
            "sample_query": r.get("sample_query", ""),
            "users": r.get("users", []),
            "avg_bytes": _safe_float(r.get("avg_bytes")),
            "avg_slot_ms": _safe_float(r.get("avg_slot_ms")),
            "location": r.get("location", ""),
        })
    return pd.DataFrame(rows)


def _normalise_table_access(raw: list[dict], run_id: str, project_id: str) -> pd.DataFrame:
    rows: list[dict] = []
    for r in raw:
        rows.append({
            "run_id": run_id,
            "project_id": project_id,
            "dataset_id": r.get("dataset_id", ""),
            "table_id": r.get("table_id", ""),
            "query_count": _safe_int(r.get("query_count")),
            "distinct_users": _safe_int(r.get("distinct_users")),
            "first_accessed": pd.to_datetime(r.get("first_accessed"), errors="coerce"),
            "last_accessed": pd.to_datetime(r.get("last_accessed"), errors="coerce"),
            "location": r.get("location", ""),
        })
    return pd.DataFrame(rows)


def _normalise_user_stats(raw: list[dict], run_id: str, project_id: str) -> pd.DataFrame:
    rows: list[dict] = []
    for r in raw:
        rows.append({
            "run_id": run_id,
            "project_id": project_id,
            "user_email": r.get("user_email", ""),
            "query_count": _safe_int(r.get("query_count")),
            "select_count": _safe_int(r.get("select_count")),
            "dml_count": _safe_int(r.get("dml_count")),
            "total_gb_processed": _safe_float(r.get("total_gb_processed")),
            "total_slot_hours": _safe_float(r.get("total_slot_hours")),
            "location": r.get("location", ""),
        })
    return pd.DataFrame(rows)


_NORMALISERS: dict[str, object] = {
    "datasets": _normalise_datasets,
    "tables": _normalise_tables,
    "ddls": _normalise_ddls,
    "query_logs": _normalise_query_logs,
    "query_sources": _normalise_query_sources,
    "frequent_queries": _normalise_frequent_queries,
    "table_access": _normalise_table_access,
    "user_stats": _normalise_user_stats,
}


def _safe_float(v: object) -> float | None:
    if v is None:
        return None
    try:
        return float(v)
    except (ValueError, TypeError):
        return None


def _safe_int(v: object) -> int | None:
    if v is None:
        return None
    try:
        return int(float(v))
    except (ValueError, TypeError):
        return None


def _is_probe_query(referenced_tables: list[dict]) -> bool:
    """Return True if every referenced table is an INFORMATION_SCHEMA object."""
    if not referenced_tables:
        return False
    return all(
        ref.get("table_id", "") in _PROBE_TABLE_IDS
        or ref.get("dataset_id", "").startswith("region-")
        for ref in referenced_tables
    )


def load_run(run_dir: Path) -> dict[str, pd.DataFrame]:
    """Load a single extraction run directory, returning normalised frames."""
    run_id = run_dir.name
    project_id = _infer_project_id(run_id)
    frames: dict[str, pd.DataFrame] = {}

    for key, filename in ARTIFACT_FILES.items():
        path = run_dir / filename
        if not path.exists():
            continue
        raw = _load_json_array(path)
        if not raw:
            continue
        normaliser = _NORMALISERS.get(key)
        if normaliser is not None:
            frames[key] = normaliser(raw, run_id, project_id)
        else:
            df = pd.DataFrame(raw)
            df.insert(0, "run_id", run_id)
            df.insert(1, "project_id", project_id)
            frames[key] = df

    return frames


def discover_runs(results_dir: str | Path) -> list[Path]:
    """Find timestamped run directories inside *results_dir*."""
    results_path = Path(results_dir)
    if not results_path.is_dir():
        return []
    return sorted(
        (p for p in results_path.iterdir() if p.is_dir() and _RUN_DIR_RE.match(p.name)),
        key=lambda p: p.name,
    )


def load_runs(results_dir: str | Path) -> ExtractionRuns:
    """Load all extraction runs and concatenate them into a single object."""
    run_dirs = discover_runs(results_dir)
    if not run_dirs:
        return ExtractionRuns()

    all_frames: dict[str, list[pd.DataFrame]] = {}
    summaries: list[dict] = []

    for run_dir in run_dirs:
        frames = load_run(run_dir)
        for key, df in frames.items():
            all_frames.setdefault(key, []).append(df)
        summaries.append({
            "run_id": run_dir.name,
            "project_id": _infer_project_id(run_dir.name),
            "artifacts": sorted(frames.keys()),
            "artifact_count": len(frames),
            "is_complete": len(frames) >= 8,
        })

    merged: dict[str, pd.DataFrame | None] = {}
    for key in ARTIFACT_FILES:
        parts = all_frames.get(key)
        if parts:
            merged[key] = pd.concat(parts, ignore_index=True)
        else:
            merged[key] = None

    return ExtractionRuns(
        datasets=merged.get("datasets"),
        tables=merged.get("tables"),
        ddls=merged.get("ddls"),
        models=merged.get("models"),
        routines=merged.get("routines"),
        query_logs=merged.get("query_logs"),
        query_sources=merged.get("query_sources"),
        frequent_queries=merged.get("frequent_queries"),
        table_access=merged.get("table_access"),
        user_stats=merged.get("user_stats"),
        run_summary=pd.DataFrame(summaries),
    )


def filter_probe_jobs(df: pd.DataFrame) -> pd.DataFrame:
    """Remove extractor-generated INFORMATION_SCHEMA probe queries."""
    if "is_probe" not in df.columns:
        return df
    return df[~df["is_probe"]].reset_index(drop=True)
