from __future__ import annotations

import logging
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Protocol

from bq_extraction_demo.config import ExtractionConfig
from bq_extraction_demo.contract import (
    STEP_CONTRACTS,
    STEP_LOOKUP,
    frequent_queries_sql,
    per_dataset_information_schema_sql,
    query_logs_sql,
    query_sources_sql,
    region_information_schema_sql,
    table_access_sql,
    user_stats_sql,
)
from bq_extraction_demo.service import BigQueryService, QueryResult
from bq_extraction_demo.writer import write_csv, write_json


class QueryService(Protocol):
    def list_datasets(self) -> list[dict[str, Any]]:
        ...

    def list_dataset_ids(self) -> list[str]:
        ...

    def run_query(self, sql: str, max_rows: int) -> QueryResult:
        ...


class _MaxLevelFilter(logging.Filter):
    def __init__(self, max_level: int) -> None:
        super().__init__()
        self._max_level = max_level

    def filter(self, record: logging.LogRecord) -> bool:
        return record.levelno <= self._max_level


@dataclass(frozen=True)
class RegionQuerySpec:
    select_fields: str
    schema_view: str
    order_by: str
    max_rows: int | None = None


def build_logger(*, quiet: bool) -> logging.Logger:
    logger = logging.getLogger("bq_extraction_demo")
    logger.handlers.clear()
    logger.setLevel(logging.INFO)
    logger.propagate = False

    if not quiet:
        stdout_handler = logging.StreamHandler(sys.stdout)
        stdout_handler.setLevel(logging.INFO)
        stdout_handler.addFilter(_MaxLevelFilter(logging.INFO))
        stdout_handler.setFormatter(logging.Formatter("%(message)s"))
        logger.addHandler(stdout_handler)

    stderr_handler = logging.StreamHandler(sys.stderr)
    stderr_handler.setLevel(logging.WARNING)
    stderr_handler.setFormatter(logging.Formatter("%(message)s"))
    logger.addHandler(stderr_handler)
    return logger


class ExtractionRunner:
    def __init__(
        self,
        config: ExtractionConfig,
        *,
        service: QueryService | None = None,
        logger: logging.Logger | None = None,
    ) -> None:
        self.config = config
        self.service = service or BigQueryService(
            config.project_id,
            query_location=config.query_location,
        )
        self.logger = logger or build_logger(quiet=config.quiet)

    def run(self) -> Path:
        if not self.config.dry_run:
            self._prepare_output_dir()

        self._log_banner()
        self._run_step("datasets", self._step_datasets, "dataset listing failed (continuing)")
        self._run_step(
            "columns",
            lambda: self._run_region_or_dataset_step(
                step_key="columns",
                spec=RegionQuerySpec(
                    select_fields=(
                        "table_schema AS dataset, table_name, column_name, ordinal_position, "
                        "data_type, is_nullable, is_partitioning_column, "
                        "clustering_ordinal_position"
                    ),
                    schema_view="INFORMATION_SCHEMA.COLUMNS",
                    order_by="dataset, table_name, ordinal_position",
                ),
                success_label="columns",
            ),
            "column extraction failed (continuing)",
        )
        self._run_step(
            "column_descriptions",
            lambda: self._run_per_dataset_step(
                step_key="column_descriptions",
                spec=RegionQuerySpec(
                    select_fields=(
                        "table_schema AS dataset, table_name, column_name, field_path, "
                        "description, data_type"
                    ),
                    schema_view="INFORMATION_SCHEMA.COLUMN_FIELD_PATHS",
                    order_by="dataset, table_name, column_name",
                ),
                success_label="described columns",
                count_fn=self._count_descriptions,
                write_empty_on_failure=True,
            ),
            "column descriptions unavailable (continuing)",
        )
        self._run_step(
            "ddls",
            lambda: self._run_region_or_dataset_step(
                step_key="ddls",
                spec=RegionQuerySpec(
                    select_fields="table_schema AS dataset, table_name, table_type, ddl",
                    schema_view="INFORMATION_SCHEMA.TABLES",
                    order_by="dataset, table_name",
                    max_rows=10000,
                ),
                success_label="tables",
            ),
            "DDL extraction failed (continuing)",
        )
        self._run_step(
            "query_logs",
            lambda: self._run_query_step(
                sql=query_logs_sql(self.config.project_id, self.config.region, self.config.days),
                step_key="query_logs",
                max_rows=500000,
                success_label="queries",
            ),
            "query log extraction failed (continuing)",
        )
        self._run_step(
            "query_sources",
            lambda: self._run_query_step(
                sql=query_sources_sql(self.config.project_id, self.config.region, self.config.days),
                step_key="query_sources",
                max_rows=1000,
            ),
            "query sources breakdown failed (continuing)",
        )
        self._run_step(
            "frequent_queries",
            lambda: self._run_query_step(
                sql=frequent_queries_sql(self.config.project_id, self.config.region, self.config.days),
                step_key="frequent_queries",
                max_rows=1000,
            ),
            "frequent queries extraction failed (continuing)",
        )
        self._run_step(
            "table_access",
            lambda: self._run_query_step(
                sql=table_access_sql(self.config.project_id, self.config.region, self.config.days),
                step_key="table_access",
                max_rows=10000,
            ),
            "table access extraction failed (continuing)",
        )
        self._run_step(
            "user_stats",
            lambda: self._run_query_step(
                sql=user_stats_sql(self.config.project_id, self.config.region, self.config.days),
                step_key="user_stats",
                max_rows=1000,
            ),
            "user stats extraction failed (continuing)",
        )

        if self.config.dry_run:
            self.logger.info("")
            self.logger.info("Dry run complete. No queries were executed.")
            return self.config.output_dir

        if self.config.quiet:
            print(self.config.output_dir)
        else:
            self._log_summary()
        return self.config.output_dir

    def _run_step(self, step_key: str, action: Callable[[], None], failure_message: str) -> None:
        contract = STEP_LOOKUP[step_key]
        self.logger.info(contract.banner.format(days=self.config.days))
        if self.config.should_skip(step_key):
            self.logger.info("   ↷ skipped")
            return
        try:
            action()
        except Exception as exc:
            self.logger.warning(f"   ! {failure_message}: {exc}")

    def _step_datasets(self) -> None:
        output_path = self._output_path("datasets")
        if self.config.dry_run:
            self.logger.info(f"    [dry-run] -> {output_path}")
            self.logger.info("    list datasets via BigQuery API")
            return

        datasets = self.service.list_datasets()
        write_json(output_path, datasets)
        self.logger.info(f"   ✓ {len(datasets)} datasets -> {output_path.name}")

    def _run_region_or_dataset_step(
        self,
        *,
        step_key: str,
        spec: RegionQuerySpec,
        success_label: str,
        count_fn: Callable[[QueryResult], int] | None = None,
    ) -> None:
        output_path = self._output_path(step_key)
        region_sql = region_information_schema_sql(
            self.config.project_id,
            self.config.region,
            spec.select_fields,
            spec.schema_view,
            spec.order_by,
        )

        if self.config.dry_run:
            self.logger.info(f"    [dry-run] -> {output_path} (region-level, with per-dataset fallback)")
            self.logger.info(f"    {self._preview_sql(region_sql)}")
            return

        max_rows = spec.max_rows or self.config.max_rows
        try:
            result = self.service.run_query(region_sql, max_rows)
        except Exception as exc:
            self.logger.info(f"   (region-level query failed: {exc}; falling back to per-dataset)")
            datasets = self._resolve_dataset_ids()
            if not datasets:
                raise RuntimeError("no datasets available for per-dataset fallback") from exc
            fallback_sql = per_dataset_information_schema_sql(
                self.config.project_id,
                datasets,
                spec.select_fields,
                spec.schema_view,
                spec.order_by,
            )
            result = self.service.run_query(fallback_sql, max_rows)

        self._write_query_output(output_path, result)
        count = count_fn(result) if count_fn is not None else len(result.rows)
        self.logger.info(f"   ✓ {count} {success_label} -> {output_path.name}")

    def _run_per_dataset_step(
        self,
        *,
        step_key: str,
        spec: RegionQuerySpec,
        success_label: str,
        count_fn: Callable[[QueryResult], int] | None = None,
        write_empty_on_failure: bool = False,
    ) -> None:
        output_path = self._output_path(step_key)
        preview_sql = (
            f"SELECT {spec.select_fields} FROM <project>.<dataset>.{spec.schema_view} "
            f"... ORDER BY {spec.order_by}"
        )

        if self.config.dry_run:
            self.logger.info(f"    [dry-run] -> {output_path} (per-dataset UNION ALL)")
            self.logger.info(f"    {self._preview_sql(preview_sql)}")
            return

        try:
            dataset_ids = self._resolve_dataset_ids()
            if not dataset_ids:
                raise RuntimeError("no datasets available")
            sql = per_dataset_information_schema_sql(
                self.config.project_id,
                dataset_ids,
                spec.select_fields,
                spec.schema_view,
                spec.order_by,
            )
            result = self.service.run_query(sql, spec.max_rows or self.config.max_rows)
        except Exception:
            if write_empty_on_failure:
                self._write_empty_output(output_path)
            raise

        self._write_query_output(output_path, result)
        count = count_fn(result) if count_fn is not None else len(result.rows)
        self.logger.info(f"   ✓ {count} {success_label} -> {output_path.name}")

    def _run_query_step(
        self,
        *,
        sql: str,
        step_key: str,
        max_rows: int,
        success_label: str | None = None,
    ) -> None:
        output_path = self._output_path(step_key)
        if self.config.dry_run:
            self.logger.info(f"    [dry-run] -> {output_path}")
            self.logger.info(f"    {self._preview_sql(sql)}")
            return

        result = self.service.run_query(sql, max_rows)
        self._write_query_output(output_path, result)
        if success_label is None:
            self.logger.info(f"   ✓ {output_path.name}")
        else:
            self.logger.info(f"   ✓ {len(result.rows)} {success_label} -> {output_path.name}")

    def _write_query_output(self, output_path: Path, result: QueryResult) -> None:
        if self.config.output_format == "json":
            write_json(output_path, result.rows)
            return
        write_csv(output_path, result.field_names, result.rows)

    def _write_empty_output(self, output_path: Path) -> None:
        if self.config.output_format == "json":
            write_json(output_path, [])
            return
        output_path.write_text("", encoding="utf-8")

    def _resolve_dataset_ids(self) -> list[str]:
        if self.config.dry_run:
            return list(self.config.datasets) if self.config.datasets else ["example_dataset"]
        if self.config.datasets:
            return list(self.config.datasets)
        return self.service.list_dataset_ids()

    def _prepare_output_dir(self) -> None:
        if self.config.output_dir.exists() and not self.config.output_dir.is_dir():
            raise RuntimeError(f"output path is not a directory: {self.config.output_dir}")
        self.config.output_dir.mkdir(parents=True, exist_ok=True)

    def _log_banner(self) -> None:
        if self.config.quiet:
            return
        self.logger.info("╔══════════════════════════════════════════════════════╗")
        self.logger.info(f"║  {'BQ Schema & Query Log Extraction':<52}║")
        self.logger.info(f"║  {'Project:  ' + self.config.project_id:<52}║")
        self.logger.info(f"║  {'Region:   ' + self.config.region:<52}║")
        self.logger.info(f"║  {'Days:     ' + str(self.config.days):<52}║")
        self.logger.info(f"║  {'Output:   ' + str(self.config.output_dir) + '/':<52}║")
        if self.config.dry_run:
            self.logger.info(f"║  {'*** DRY RUN - no queries will execute ***':<52}║")
        self.logger.info("╚══════════════════════════════════════════════════════╝")
        self.logger.info("")

    def _log_summary(self) -> None:
        self.logger.info("")
        self.logger.info("═══════════════════════════════════════════════════════")
        self.logger.info("  EXTRACTION COMPLETE")
        self.logger.info("═══════════════════════════════════════════════════════")
        self.logger.info("")
        self.logger.info("Output files:")
        for contract in STEP_CONTRACTS:
            if contract.key == "datasets":
                name = "datasets.json"
            else:
                name = contract.output_name.format(ext=self.config.output_extension, days=self.config.days)
            self.logger.info(f"  {name}")
        self.logger.info("")
        self.logger.info(
            f"Next: feed columns.{self.config.output_extension} + "
            f"query_logs.{self.config.output_extension} into Alma"
        )

    def _output_path(self, step_key: str) -> Path:
        contract = STEP_LOOKUP[step_key]
        filename = contract.output_name.format(ext=self.config.output_extension, days=self.config.days)
        return self.config.output_dir / filename

    @staticmethod
    def _preview_sql(sql: str) -> str:
        compact = " ".join(sql.split())
        if len(compact) <= 400:
            return compact
        return f"{compact[:400]} ..."

    def _count_descriptions(self, result: QueryResult) -> int:
        if self.config.output_format == "csv":
            return len(result.rows)
        return sum(1 for row in result.rows if row.get("description"))

