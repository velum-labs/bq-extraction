from __future__ import annotations

from collections import defaultdict
import logging
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Protocol

from bq_extraction_demo.config import ExtractionConfig
from bq_extraction_demo.contract import (
    CAPABILITY_SPECS,
    OBJECT_FAMILIES,
    CapabilitySpec,
    build_capability_sql,
    capability_output_name,
    family_output_name,
    probe_sql,
)
from bq_extraction_demo.service import BigQueryService, DatasetDiscovery, QueryResult
from bq_extraction_demo.writer import derive_field_names, write_rows


class DiscoveryService(Protocol):
    def list_datasets(self, *, include_hidden: bool = False) -> list[DatasetDiscovery]:
        ...

    def list_table_objects(self, dataset: DatasetDiscovery) -> list[dict[str, Any]]:
        ...

    def list_routine_objects(self, dataset: DatasetDiscovery) -> list[dict[str, Any]]:
        ...

    def list_model_objects(self, dataset: DatasetDiscovery) -> list[dict[str, Any]]:
        ...

    def probe_query(self, sql: str, *, location: str) -> None:
        ...

    def run_query(self, sql: str, max_rows: int, *, location: str) -> QueryResult:
        ...


class _MaxLevelFilter(logging.Filter):
    def __init__(self, max_level: int) -> None:
        super().__init__()
        self._max_level = max_level

    def filter(self, record: logging.LogRecord) -> bool:
        return record.levelno <= self._max_level


@dataclass(frozen=True)
class LocationGroup:
    location: str
    datasets: tuple[DatasetDiscovery, ...]

    @property
    def dataset_ids(self) -> tuple[str, ...]:
        return tuple(dataset.dataset_id for dataset in self.datasets)

    @property
    def query_location(self) -> str:
        return canonical_query_location(self.location)


@dataclass(frozen=True)
class DiscoverySnapshot:
    datasets: tuple[DatasetDiscovery, ...]
    locations: tuple[LocationGroup, ...]


@dataclass(frozen=True)
class CapabilityPlan:
    spec: CapabilitySpec
    location: str
    query_location: str
    dataset_ids: tuple[str, ...]
    use_dataset_scope: bool


@dataclass(frozen=True)
class GeneratedOutput:
    key: str
    path: Path
    row_count: int


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


def canonical_query_location(location: str) -> str:
    normalized = location.strip()
    if "-" in normalized:
        return normalized.lower()
    return normalized.upper()


def format_error(exc: Exception) -> str:
    return str(exc).splitlines()[0]


class ExtractionRunner:
    def __init__(
        self,
        config: ExtractionConfig,
        *,
        service: DiscoveryService | None = None,
        logger: logging.Logger | None = None,
    ) -> None:
        self.config = config
        self.service = service or BigQueryService(config.project_id)
        self.logger = logger or build_logger(quiet=config.quiet)

    def run(self) -> Path:
        if not self.config.dry_run:
            self._prepare_output_dir()

        self._log_banner()
        snapshot = self._discover_snapshot()
        outputs: list[GeneratedOutput] = []
        outputs.extend(self._extract_api_families(snapshot))

        capability_plans, unavailable = self._probe_capabilities(snapshot)
        if not self.config.dry_run:
            outputs.extend(self._extract_capabilities(capability_plans))

        if self.config.dry_run:
            self.logger.info("")
            self.logger.info("Dry run complete. No output files were written.")
            return self.config.output_dir

        if self.config.quiet:
            print(self.config.output_dir)
        else:
            self._log_summary(outputs, unavailable)
        return self.config.output_dir

    def _discover_snapshot(self) -> DiscoverySnapshot:
        self.logger.info("① Discovering datasets...")
        try:
            datasets = self.service.list_datasets(
                include_hidden=self.config.include_hidden_datasets
            )
        except Exception as exc:
            self.logger.warning(f"   ! dataset discovery unavailable: {format_error(exc)}")
            datasets = []

        filtered = [
            dataset
            for dataset in datasets
            if (not self.config.datasets or dataset.dataset_id in self.config.datasets)
            and (not dataset.location or self.config.location_allowed(dataset.location))
        ]

        if filtered:
            self.logger.info(f"   ✓ {len(filtered)} datasets discovered")
        else:
            self.logger.info("   ↷ no datasets discovered")

        location_map: dict[str, list[DatasetDiscovery]] = defaultdict(list)
        for dataset in filtered:
            if dataset.location:
                location_map[canonical_query_location(dataset.location)].append(dataset)

        for location_filter in self.config.location_filters:
            location_map.setdefault(canonical_query_location(location_filter), [])

        locations = tuple(
            LocationGroup(
                location=location,
                datasets=tuple(sorted(location_map[location], key=lambda dataset: dataset.dataset_id)),
            )
            for location in sorted(location_map, key=str.lower)
        )
        return DiscoverySnapshot(datasets=tuple(filtered), locations=locations)

    def _extract_api_families(self, snapshot: DiscoverySnapshot) -> list[GeneratedOutput]:
        self.logger.info("② Extracting API-backed object families...")
        outputs: list[GeneratedOutput] = []

        for family in OBJECT_FAMILIES:
            if family.key == "jobs":
                continue
            if not self.config.wants_family(family.key):
                self.logger.info(f"   ↷ {family.key}: excluded by family selection")
                continue

            output_path = self.config.output_dir / family_output_name(
                family.key,
                self.config.output_extension,
            )
            if self.config.dry_run:
                self.logger.info(f"   [dry-run] -> {output_path}")
                if family.key == "datasets":
                    self.logger.info(f"   discovered datasets: {len(snapshot.datasets)}")
                else:
                    self.logger.info(f"   datasets in scope: {len(snapshot.datasets)}")
                continue

            rows = self._collect_family_rows(family.key, snapshot)
            write_rows(
                output_path,
                output_format=self.config.output_format,
                rows=rows,
            )
            self.logger.info(f"   ✓ {len(rows)} {family.key} rows -> {output_path.name}")
            outputs.append(
                GeneratedOutput(
                    key=family.key,
                    path=output_path,
                    row_count=len(rows),
                )
            )
        return outputs

    def _collect_family_rows(
        self,
        family_key: str,
        snapshot: DiscoverySnapshot,
    ) -> list[dict[str, Any]]:
        if family_key == "datasets":
            return [dataset.payload for dataset in snapshot.datasets]

        rows: list[dict[str, Any]] = []
        for dataset in snapshot.datasets:
            try:
                if family_key == "tables":
                    rows.extend(self.service.list_table_objects(dataset))
                elif family_key == "routines":
                    rows.extend(self.service.list_routine_objects(dataset))
                elif family_key == "models":
                    rows.extend(self.service.list_model_objects(dataset))
                else:
                    raise ValueError(f"unknown family key: {family_key}")
            except Exception as exc:
                self.logger.warning(
                    f"   ! {family_key}: could not enumerate {dataset.qualified_id}: {format_error(exc)}"
                )
        return rows

    def _probe_capabilities(
        self,
        snapshot: DiscoverySnapshot,
    ) -> tuple[list[CapabilityPlan], list[str]]:
        self.logger.info("③ Probing metadata capabilities...")
        plans: list[CapabilityPlan] = []
        unavailable: list[str] = []

        for spec in CAPABILITY_SPECS:
            if not self.config.wants_source(spec.key):
                self.logger.info(f"   ↷ {spec.key}: excluded by source selection")
                continue
            if not snapshot.locations:
                message = f"{spec.key}: no locations discovered"
                self.logger.info(f"   ↷ {message}")
                unavailable.append(message)
                continue

            for location_group in snapshot.locations:
                try:
                    plan = self._probe_capability(spec, location_group)
                    plans.append(plan)
                    mode_label = "dataset scope" if plan.use_dataset_scope else "region scope"
                    self.logger.info(f"   ✓ {spec.key} @ {plan.location} ({mode_label})")
                except Exception as exc:
                    message = f"{spec.key} @ {location_group.location}: {format_error(exc)}"
                    self.logger.info(f"   ↷ unavailable: {message}")
                    unavailable.append(message)

        return plans, unavailable

    def _probe_capability(
        self,
        spec: CapabilitySpec,
        location_group: LocationGroup,
    ) -> CapabilityPlan:
        query_location = location_group.query_location
        region_sql = build_capability_sql(
            spec,
            project_id=self.config.project_id,
            location=location_group.location,
            days=self.config.days,
        )
        try:
            self.service.probe_query(probe_sql(region_sql), location=query_location)
            return CapabilityPlan(
                spec=spec,
                location=location_group.location,
                query_location=query_location,
                dataset_ids=location_group.dataset_ids,
                use_dataset_scope=False,
            )
        except Exception as region_exc:
            if not spec.fallback_to_dataset or not location_group.dataset_ids:
                raise RuntimeError(f"region probe failed: {region_exc}") from region_exc

            last_dataset_exc: Exception | None = None
            for dataset_id in location_group.dataset_ids:
                dataset_sql = build_capability_sql(
                    spec,
                    project_id=self.config.project_id,
                    location=location_group.location,
                    days=self.config.days,
                    datasets=[dataset_id],
                    use_dataset_scope=True,
                )
                try:
                    self.service.probe_query(probe_sql(dataset_sql), location=query_location)
                    return CapabilityPlan(
                        spec=spec,
                        location=location_group.location,
                        query_location=query_location,
                        dataset_ids=location_group.dataset_ids,
                        use_dataset_scope=True,
                    )
                except Exception as dataset_exc:
                    last_dataset_exc = dataset_exc

            detail = f"region probe failed: {region_exc}"
            if last_dataset_exc is not None:
                detail += f"; dataset probe failed: {last_dataset_exc}"
            raise RuntimeError(detail) from region_exc

    def _extract_capabilities(self, plans: list[CapabilityPlan]) -> list[GeneratedOutput]:
        self.logger.info("④ Extracting discovered metadata capabilities...")
        rows_by_key: dict[str, list[dict[str, Any]]] = defaultdict(list)
        field_names_by_key: dict[str, list[str]] = {}

        for plan in plans:
            try:
                rows, field_names = self._run_capability(plan)
            except Exception as exc:
                self.logger.warning(
                    f"   ! {plan.spec.key} @ {plan.location}: extraction failed: {format_error(exc)}"
                )
                continue

            rows_by_key[plan.spec.key].extend(rows)
            if plan.spec.key not in field_names_by_key:
                field_names_by_key[plan.spec.key] = field_names
            else:
                merged = field_names_by_key[plan.spec.key]
                for field_name in field_names:
                    if field_name not in merged:
                        merged.append(field_name)

        outputs: list[GeneratedOutput] = []
        for spec in CAPABILITY_SPECS:
            if not self.config.wants_source(spec.key):
                continue
            if spec.key not in rows_by_key:
                continue
            output_path = self.config.output_dir / capability_output_name(
                spec.key,
                self.config.output_extension,
            )
            rows = rows_by_key[spec.key]
            write_rows(
                output_path,
                output_format=self.config.output_format,
                rows=rows,
                field_names=field_names_by_key.get(spec.key) or derive_field_names(rows),
            )
            self.logger.info(f"   ✓ {len(rows)} rows -> {output_path.name}")
            outputs.append(
                GeneratedOutput(
                    key=spec.key,
                    path=output_path,
                    row_count=len(rows),
                )
            )
        return outputs

    def _run_capability(
        self,
        plan: CapabilityPlan,
    ) -> tuple[list[dict[str, Any]], list[str]]:
        if plan.use_dataset_scope:
            return self._run_dataset_scoped_capability(plan)

        sql = build_capability_sql(
            plan.spec,
            project_id=self.config.project_id,
            location=plan.location,
            days=self.config.days,
        )
        result = self.service.run_query(
            sql,
            plan.spec.max_rows or self.config.max_rows,
            location=plan.query_location,
        )
        return self._augment_query_rows(result, plan.location)

    def _run_dataset_scoped_capability(
        self,
        plan: CapabilityPlan,
    ) -> tuple[list[dict[str, Any]], list[str]]:
        rows: list[dict[str, Any]] = []
        field_names: list[str] = []
        for dataset_id in plan.dataset_ids:
            try:
                sql = build_capability_sql(
                    plan.spec,
                    project_id=self.config.project_id,
                    location=plan.location,
                    days=self.config.days,
                    datasets=[dataset_id],
                    use_dataset_scope=True,
                )
                result = self.service.run_query(
                    sql,
                    plan.spec.max_rows or self.config.max_rows,
                    location=plan.query_location,
                )
            except Exception as exc:
                self.logger.warning(
                    f"   ! {plan.spec.key} @ {plan.location}: dataset {dataset_id} failed: {format_error(exc)}"
                )
                continue

            augmented_rows, augmented_field_names = self._augment_query_rows(result, plan.location)
            rows.extend(augmented_rows)
            for field_name in augmented_field_names:
                if field_name not in field_names:
                    field_names.append(field_name)
        return rows, field_names

    def _augment_query_rows(
        self,
        result: QueryResult,
        location: str,
    ) -> tuple[list[dict[str, Any]], list[str]]:
        rows = [{"location": location, **row} for row in result.rows]
        field_names = ["location", *result.field_names]
        return rows, field_names

    def _prepare_output_dir(self) -> None:
        if self.config.output_dir.exists() and not self.config.output_dir.is_dir():
            raise RuntimeError(f"output path is not a directory: {self.config.output_dir}")
        self.config.output_dir.mkdir(parents=True, exist_ok=True)

    def _log_banner(self) -> None:
        if self.config.quiet:
            return
        self.logger.info("╔══════════════════════════════════════════════════════╗")
        self.logger.info(f"║  {'BigQuery Discovery Extractor':<52}║")
        self.logger.info(f"║  {'Project:  ' + self.config.project_id:<52}║")
        locations = ",".join(self.config.location_filters) if self.config.location_filters else "auto-discover"
        self.logger.info(f"║  {'Locations: ' + locations:<52}║")
        self.logger.info(f"║  {'Days:     ' + str(self.config.days):<52}║")
        self.logger.info(f"║  {'Output:   ' + str(self.config.output_dir) + '/':<52}║")
        if self.config.dry_run:
            self.logger.info(f"║  {'*** DRY RUN - no output files will be written ***':<52}║")
        self.logger.info("╚══════════════════════════════════════════════════════╝")
        self.logger.info("")

    def _log_summary(self, outputs: list[GeneratedOutput], unavailable: list[str]) -> None:
        self.logger.info("")
        self.logger.info("═══════════════════════════════════════════════════════")
        self.logger.info("  EXTRACTION COMPLETE")
        self.logger.info("═══════════════════════════════════════════════════════")
        self.logger.info("")
        self.logger.info("Output files:")
        for output in outputs:
            self.logger.info(f"  {output.path.name} ({output.row_count} rows)")
        if unavailable:
            self.logger.info("")
            self.logger.info("Skipped or unavailable:")
            for item in unavailable:
                self.logger.info(f"  {item}")


