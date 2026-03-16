from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from bq_extraction_demo.config import ExtractionConfig
from bq_extraction_demo.contract import STEP_ORDER
from bq_extraction_demo.extractor import ExtractionRunner
from bq_extraction_demo.service import QueryResult


class FakeService:
    def __init__(
        self,
        *,
        datasets: list[dict[str, Any]] | None = None,
        dataset_ids: list[str] | None = None,
    ) -> None:
        self.datasets = datasets or [
            {"datasetReference": {"datasetId": "analytics"}},
            {"datasetReference": {"datasetId": "raw"}},
        ]
        self.dataset_ids = dataset_ids or ["analytics", "raw"]
        self.queries: list[tuple[str, int]] = []
        self.fail_region_columns = False
        self.fail_column_descriptions = False

    def list_datasets(self) -> list[dict[str, Any]]:
        return self.datasets

    def list_dataset_ids(self) -> list[str]:
        if self.fail_column_descriptions:
            raise RuntimeError("dataset lookup failed")
        return self.dataset_ids

    def run_query(self, sql: str, max_rows: int) -> QueryResult:
        self.queries.append((sql, max_rows))

        if self.fail_region_columns and "region-us" in sql and "INFORMATION_SCHEMA.COLUMNS" in sql:
            raise RuntimeError("region denied")

        if "INFORMATION_SCHEMA.COLUMNS" in sql:
            return QueryResult(
                field_names=(
                    "dataset",
                    "table_name",
                    "column_name",
                    "ordinal_position",
                    "data_type",
                    "is_nullable",
                    "is_partitioning_column",
                    "clustering_ordinal_position",
                ),
                rows=[
                    {
                        "dataset": "analytics",
                        "table_name": "daily_aum",
                        "column_name": "report_date",
                        "ordinal_position": "1",
                        "data_type": "DATE",
                        "is_nullable": "NO",
                        "is_partitioning_column": "NO",
                        "clustering_ordinal_position": None,
                    }
                ],
            )

        if "COLUMN_FIELD_PATHS" in sql:
            return QueryResult(
                field_names=("dataset", "table_name", "column_name", "field_path", "description", "data_type"),
                rows=[
                    {
                        "dataset": "analytics",
                        "table_name": "daily_aum",
                        "column_name": "report_date",
                        "field_path": "report_date",
                        "description": "Reporting date",
                        "data_type": "DATE",
                    },
                    {
                        "dataset": "analytics",
                        "table_name": "daily_aum",
                        "column_name": "fund_id",
                        "field_path": "fund_id",
                        "description": None,
                        "data_type": "STRING",
                    },
                ],
            )

        if "query_source" in sql and "GROUP BY query_source" in sql:
            return QueryResult(
                field_names=("query_source", "query_count", "users", "total_gb_processed", "total_slot_hours"),
                rows=[{"query_source": "ad_hoc", "query_count": "1", "users": ["alen@example.com"], "total_gb_processed": "0.5", "total_slot_hours": "0.0"}],
            )

        if "UNNEST(referenced_tables)" in sql:
            return QueryResult(
                field_names=("dataset_id", "table_id", "query_count", "distinct_users", "first_accessed", "last_accessed"),
                rows=[],
            )

        if "query_info.query_hashes.normalized_literals" in sql:
            return QueryResult(
                field_names=("query_hash", "execution_count", "sample_query", "users", "avg_bytes", "avg_slot_ms"),
                rows=[],
            )

        if "COUNTIF(statement_type = 'SELECT')" in sql:
            return QueryResult(
                field_names=("user_email", "query_count", "select_count", "dml_count", "total_gb_processed", "total_slot_hours"),
                rows=[],
            )

        if "job_id" in sql:
            return QueryResult(
                field_names=("job_id", "labels", "cache_hit"),
                rows=[
                    {
                        "job_id": "job-1",
                        "labels": [{"key": "airflow_dag_id", "value": "demo"}],
                        "cache_hit": "false",
                    }
                ],
            )

        return QueryResult(field_names=("value",), rows=[{"value": "1"}])


def test_dry_run_does_not_create_output_dir(tmp_path: Path, capsys) -> None:
    config = make_config(
        tmp_path,
        dry_run=True,
        skip_steps=skip_all_except("columns"),
    )

    runner = ExtractionRunner(config, service=FakeService())
    runner.run()

    captured = capsys.readouterr()
    assert not config.output_dir.exists()
    assert "[dry-run] ->" in captured.out
    assert "region-level, with per-dataset fallback" in captured.out
    assert "Dry run complete. No queries were executed." in captured.out


def test_quiet_mode_prints_only_output_dir(tmp_path: Path, capsys) -> None:
    config = make_config(
        tmp_path,
        quiet=True,
        skip_steps=frozenset(STEP_ORDER),
    )

    runner = ExtractionRunner(config, service=FakeService())
    runner.run()

    captured = capsys.readouterr()
    assert config.output_dir.exists()
    assert captured.out.strip() == str(config.output_dir)
    assert captured.err == ""


def test_region_query_falls_back_to_per_dataset_union(tmp_path: Path) -> None:
    config = make_config(
        tmp_path,
        skip_steps=skip_all_except("columns"),
    )
    service = FakeService()
    service.fail_region_columns = True

    runner = ExtractionRunner(config, service=service)
    runner.run()

    output_path = config.output_dir / "columns.json"
    payload = json.loads(output_path.read_text(encoding="utf-8"))

    assert len(service.queries) == 2
    assert "region-us" in service.queries[0][0]
    assert "`demo-project`.`analytics`.INFORMATION_SCHEMA.COLUMNS" in service.queries[1][0]
    assert payload[0]["column_name"] == "report_date"


def test_csv_output_serializes_nested_values(tmp_path: Path) -> None:
    config = make_config(
        tmp_path,
        output_format="csv",
        skip_steps=skip_all_except("query_logs"),
    )

    runner = ExtractionRunner(config, service=FakeService())
    runner.run()

    contents = (config.output_dir / "query_logs.csv").read_text(encoding="utf-8")
    assert "job_id,labels,cache_hit" in contents
    assert '"[{""key"":""airflow_dag_id"",""value"":""demo""}]"' in contents
    assert "job-1" in contents


def test_float_values_preserve_decimal_text(tmp_path: Path) -> None:
    config = make_config(
        tmp_path,
        skip_steps=skip_all_except("query_sources"),
    )

    runner = ExtractionRunner(config, service=FakeService())
    runner.run()

    payload = json.loads((config.output_dir / "query_sources.json").read_text(encoding="utf-8"))
    assert payload[0]["total_slot_hours"] == "0.0"


def test_column_description_failure_writes_empty_json(tmp_path: Path) -> None:
    config = make_config(
        tmp_path,
        skip_steps=skip_all_except("column_descriptions"),
    )
    service = FakeService()
    service.fail_column_descriptions = True

    runner = ExtractionRunner(config, service=service)
    runner.run()

    output_path = config.output_dir / "column_descriptions.json"
    assert json.loads(output_path.read_text(encoding="utf-8")) == []


def make_config(
    tmp_path: Path,
    *,
    output_format: str = "json",
    quiet: bool = False,
    dry_run: bool = False,
    skip_steps: frozenset[str] | None = None,
) -> ExtractionConfig:
    return ExtractionConfig(
        project_id="demo-project",
        region="us",
        output_dir=tmp_path / "output",
        days=30,
        max_rows=200000,
        output_format=output_format,
        datasets=(),
        skip_steps=skip_steps or frozenset(),
        quiet=quiet,
        dry_run=dry_run,
    )


def skip_all_except(*step_keys: str) -> frozenset[str]:
    keep = set(step_keys)
    return frozenset(step for step in STEP_ORDER if step not in keep)

