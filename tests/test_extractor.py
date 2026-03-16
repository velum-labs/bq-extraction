from __future__ import annotations

import json
from pathlib import Path

from bq_extraction_demo.config import ExtractionConfig
from bq_extraction_demo.service import DatasetDiscovery, QueryResult
from bq_extraction_demo.extractor import ExtractionRunner


class FakeService:
    def __init__(self) -> None:
        self.datasets = [
            DatasetDiscovery(
                project_id="demo-project",
                dataset_id="analytics",
                location="US",
                payload={
                    "datasetReference": {"projectId": "demo-project", "datasetId": "analytics"},
                    "location": "US",
                },
            ),
            DatasetDiscovery(
                project_id="demo-project",
                dataset_id="staging",
                location="EU",
                payload={
                    "datasetReference": {"projectId": "demo-project", "datasetId": "staging"},
                    "location": "EU",
                },
            ),
        ]
        self.include_hidden_flags: list[bool] = []
        self.probes: list[tuple[str, str]] = []
        self.queries: list[tuple[str, int, str]] = []
        self.fail_region_tables_probe = False
        self.fail_jobs_probe = False
        self.fail_dataset_discovery = False

    def list_datasets(self, *, include_hidden: bool = False) -> list[DatasetDiscovery]:
        self.include_hidden_flags.append(include_hidden)
        if self.fail_dataset_discovery:
            raise RuntimeError("dataset discovery failed")
        return self.datasets

    def list_table_objects(self, dataset: DatasetDiscovery) -> list[dict[str, object]]:
        return [
            {
                "tableReference": {
                    "projectId": dataset.project_id,
                    "datasetId": dataset.dataset_id,
                    "tableId": f"{dataset.dataset_id}_table",
                },
                "tableType": "TABLE",
                "dataset_location": dataset.location,
                "schema": {
                    "fields": [
                        {"name": "id", "type": "INT64", "mode": "REQUIRED"},
                        {"name": "note", "type": "STRING", "mode": "NULLABLE"},
                    ]
                },
            }
        ]

    def list_routine_objects(self, dataset: DatasetDiscovery) -> list[dict[str, object]]:
        return [
            {
                "routineReference": {
                    "projectId": dataset.project_id,
                    "datasetId": dataset.dataset_id,
                    "routineId": f"{dataset.dataset_id}_routine",
                },
                "routineType": "SCALAR_FUNCTION",
                "dataset_location": dataset.location,
            }
        ]

    def list_model_objects(self, dataset: DatasetDiscovery) -> list[dict[str, object]]:
        return [
            {
                "modelReference": {
                    "projectId": dataset.project_id,
                    "datasetId": dataset.dataset_id,
                    "modelId": f"{dataset.dataset_id}_model",
                },
                "modelType": "LINEAR_REGRESSION",
                "dataset_location": dataset.location,
            }
        ]

    def probe_query(self, sql: str, *, location: str) -> None:
        self.probes.append((sql, location))
        if self.fail_region_tables_probe and "region-us" in sql and "INFORMATION_SCHEMA.TABLES" in sql:
            raise RuntimeError("region denied")
        if self.fail_jobs_probe and "JOBS_BY_PROJECT" in sql:
            raise RuntimeError("missing jobs permission")

    def run_query(self, sql: str, max_rows: int, *, location: str) -> QueryResult:
        self.queries.append((sql, max_rows, location))

        if "INFORMATION_SCHEMA.TABLES" in sql:
            return QueryResult(
                field_names=(
                    "table_catalog",
                    "dataset",
                    "table_name",
                    "table_type",
                    "ddl",
                ),
                rows=[
                    {
                        "table_catalog": "demo-project",
                        "dataset": "analytics",
                        "table_name": "daily_aum",
                        "table_type": "BASE TABLE",
                        "ddl": "CREATE TABLE `demo-project.analytics.daily_aum` (id INT64)",
                    }
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


def test_dry_run_discovers_and_probes_without_writing_output_dir(tmp_path: Path, capsys) -> None:
    config = make_config(
        tmp_path,
        dry_run=True,
        include_families=frozenset({"datasets", "tables"}),
        include_sources=frozenset({"tables.ddls"}),
    )

    service = FakeService()
    runner = ExtractionRunner(config, service=service)
    runner.run()

    captured = capsys.readouterr()
    assert not config.output_dir.exists()
    assert "Mode: dry run" in captured.out
    assert f"  - would write {config.output_dir / 'datasets.json'}" in captured.out
    assert "  OK tables.ddls @ US (region scope)" in captured.out
    assert "Dry run complete. No output files were written." in captured.out
    assert service.probes


def test_standard_run_logs_header_steps_and_summary(tmp_path: Path, capsys) -> None:
    config = make_config(
        tmp_path,
        include_families=frozenset({"datasets", "tables"}),
        include_sources=frozenset({"tables.ddls"}),
        exclude_families=frozenset({"routines", "models", "jobs"}),
    )

    runner = ExtractionRunner(config, service=FakeService())
    runner.run()

    captured = capsys.readouterr()
    assert "BigQuery Discovery Extractor" in captured.out
    assert "Project: demo-project" in captured.out
    assert "Step 1/4: Discover datasets" in captured.out
    assert "Step 2/4: Extract API-backed object families" in captured.out
    assert "Step 3/4: Probe metadata capabilities" in captured.out
    assert "Step 4/4: Extract discovered metadata capabilities" in captured.out
    assert "  OK wrote datasets.json (2 rows)" in captured.out
    assert "Extraction complete" in captured.out
    assert "Generated files:" in captured.out
    assert captured.err == ""


def test_quiet_mode_prints_only_output_dir(tmp_path: Path, capsys) -> None:
    config = make_config(
        tmp_path,
        quiet=True,
        include_families=frozenset({"datasets"}),
        exclude_families=frozenset({"tables", "routines", "models", "jobs"}),
    )

    runner = ExtractionRunner(config, service=FakeService())
    runner.run()

    captured = capsys.readouterr()
    assert config.output_dir.exists()
    assert captured.out.strip() == str(config.output_dir)
    assert captured.err == ""
    assert (config.output_dir / "datasets.json").exists()


def test_dataset_discovery_warning_is_sent_to_stderr(tmp_path: Path, capsys) -> None:
    config = make_config(
        tmp_path,
        include_families=frozenset({"datasets"}),
        exclude_families=frozenset({"tables", "routines", "models", "jobs"}),
    )
    service = FakeService()
    service.fail_dataset_discovery = True

    runner = ExtractionRunner(config, service=service)
    runner.run()

    captured = capsys.readouterr()
    assert "WARN dataset discovery unavailable: dataset discovery failed" in captured.err
    assert "  SKIP no datasets discovered" in captured.out
    assert (config.output_dir / "datasets.json").exists()


def test_tables_capability_falls_back_to_dataset_scope(tmp_path: Path) -> None:
    config = make_config(
        tmp_path,
        include_families=frozenset({"datasets", "tables"}),
        include_sources=frozenset({"tables.ddls"}),
        exclude_families=frozenset({"routines", "models", "jobs"}),
    )
    service = FakeService()
    service.fail_region_tables_probe = True

    runner = ExtractionRunner(config, service=service)
    runner.run()

    output_path = config.output_dir / "tables.ddls.json"
    payload = json.loads(output_path.read_text(encoding="utf-8"))

    assert any("region-us" in probe[0] for probe in service.probes)
    assert any("`demo-project`.`analytics`.INFORMATION_SCHEMA.TABLES" in query[0] for query in service.queries)
    assert any(row["location"] == "US" for row in payload)
    assert any(row["table_name"] == "daily_aum" for row in payload)


def test_derived_output_names_are_written_for_selected_families_and_sources(tmp_path: Path) -> None:
    config = make_config(
        tmp_path,
        include_families=frozenset({"datasets", "tables", "routines", "models", "jobs"}),
        include_sources=frozenset({"tables.ddls", "jobs.query_logs"}),
    )

    runner = ExtractionRunner(config, service=FakeService())
    runner.run()

    assert (config.output_dir / "datasets.json").exists()
    assert (config.output_dir / "tables.json").exists()
    assert (config.output_dir / "routines.json").exists()
    assert (config.output_dir / "models.json").exists()
    assert (config.output_dir / "tables.ddls.json").exists()
    assert (config.output_dir / "jobs.query_logs.json").exists()


def test_unavailable_capability_is_skipped_cleanly(tmp_path: Path) -> None:
    config = make_config(
        tmp_path,
        include_families=frozenset({"datasets", "jobs"}),
        include_sources=frozenset({"jobs.query_logs"}),
        exclude_families=frozenset({"tables", "routines", "models"}),
    )
    service = FakeService()
    service.fail_jobs_probe = True

    runner = ExtractionRunner(config, service=service)
    runner.run()

    assert (config.output_dir / "datasets.json").exists()
    assert not (config.output_dir / "jobs.query_logs.json").exists()


def test_csv_output_serializes_nested_query_rows(tmp_path: Path) -> None:
    config = make_config(
        tmp_path,
        output_format="csv",
        include_families=frozenset({"jobs"}),
        include_sources=frozenset({"jobs.query_logs"}),
        exclude_families=frozenset({"datasets", "tables", "routines", "models"}),
    )

    runner = ExtractionRunner(config, service=FakeService())
    runner.run()

    contents = (config.output_dir / "jobs.query_logs.csv").read_text(encoding="utf-8")
    assert "location,job_id,labels,cache_hit" in contents
    assert '"[{""key"":""airflow_dag_id"",""value"":""demo""}]"' in contents
    assert "job-1" in contents


def test_location_filters_limit_discovery_and_probing(tmp_path: Path) -> None:
    config = make_config(
        tmp_path,
        location_filters=("us",),
        include_families=frozenset({"datasets", "tables"}),
        include_sources=frozenset({"tables.ddls"}),
        exclude_families=frozenset({"routines", "models", "jobs"}),
    )
    service = FakeService()

    runner = ExtractionRunner(config, service=service)
    runner.run()

    datasets_payload = json.loads((config.output_dir / "datasets.json").read_text(encoding="utf-8"))
    assert len(datasets_payload) == 1
    assert datasets_payload[0]["datasetReference"]["datasetId"] == "analytics"
    assert all(location == "US" for _, location in service.probes)


def test_hyphenated_locations_are_canonicalized_for_grouping(tmp_path: Path) -> None:
    config = make_config(
        tmp_path,
        location_filters=("us-central1",),
        include_families=frozenset({"datasets", "tables"}),
        include_sources=frozenset({"tables.ddls"}),
        exclude_families=frozenset({"routines", "models", "jobs"}),
    )
    service = FakeService()
    service.datasets = [
        DatasetDiscovery(
            project_id="demo-project",
            dataset_id="regional",
            location="US-CENTRAL1",
            payload={
                "datasetReference": {"projectId": "demo-project", "datasetId": "regional"},
                "location": "US-CENTRAL1",
            },
        )
    ]

    runner = ExtractionRunner(config, service=service)
    runner.run()

    datasets_payload = json.loads((config.output_dir / "datasets.json").read_text(encoding="utf-8"))
    assert len(datasets_payload) == 1
    assert all(location == "us-central1" for _, _, location in service.queries)


def test_include_hidden_datasets_flag_is_forwarded(tmp_path: Path) -> None:
    config = make_config(
        tmp_path,
        dry_run=True,
        include_hidden_datasets=True,
        include_families=frozenset({"datasets"}),
        exclude_families=frozenset({"tables", "routines", "models", "jobs"}),
    )
    service = FakeService()

    runner = ExtractionRunner(config, service=service)
    runner.run()

    assert service.include_hidden_flags == [True]


def make_config(
    tmp_path: Path,
    *,
    output_format: str = "json",
    quiet: bool = False,
    dry_run: bool = False,
    include_families: frozenset[str] | None = None,
    exclude_families: frozenset[str] | None = None,
    include_sources: frozenset[str] | None = None,
    exclude_sources: frozenset[str] | None = None,
    include_hidden_datasets: bool = False,
    location_filters: tuple[str, ...] = (),
) -> ExtractionConfig:
    return ExtractionConfig(
        project_id="demo-project",
        location_filters=location_filters,
        output_dir=tmp_path / "output",
        days=30,
        max_rows=200000,
        output_format=output_format,
        datasets=(),
        include_families=include_families or frozenset(),
        exclude_families=exclude_families or frozenset(),
        include_sources=include_sources or frozenset(),
        exclude_sources=exclude_sources or frozenset(),
        include_hidden_datasets=include_hidden_datasets,
        quiet=quiet,
        dry_run=dry_run,
    )

