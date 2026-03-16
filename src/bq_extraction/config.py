from __future__ import annotations

import argparse
import re
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Sequence

from bq_extraction.contract import (
    CAPABILITY_KEYS,
    OBJECT_FAMILY_KEYS,
    SUPPORTED_FORMATS,
)


DATASET_RE = re.compile(r"^[A-Za-z0-9_]+$")


@dataclass(frozen=True)
class ExtractionConfig:
    project_id: str
    location_filters: tuple[str, ...]
    output_dir: Path
    days: int
    max_rows: int
    output_format: str
    datasets: tuple[str, ...]
    include_families: frozenset[str]
    exclude_families: frozenset[str]
    include_sources: frozenset[str]
    exclude_sources: frozenset[str]
    include_hidden_datasets: bool
    quiet: bool
    dry_run: bool

    @property
    def output_extension(self) -> str:
        return self.output_format

    def wants_family(self, family_key: str) -> bool:
        if self.include_families and family_key not in self.include_families:
            return False
        return family_key not in self.exclude_families

    def wants_source(self, source_key: str) -> bool:
        family_key = source_key.split(".", 1)[0]
        if not self.wants_family(family_key):
            return False
        if self.include_sources and source_key not in self.include_sources:
            return False
        return source_key not in self.exclude_sources

    def location_allowed(self, location: str) -> bool:
        if not self.location_filters:
            return True
        return location.lower() in self.location_filters


def default_output_dir(now: datetime | None = None) -> Path:
    current = now or datetime.now()
    return Path("output") / current.strftime("%Y%m%d_%H%M%S")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="extract.py",
        description="Discover and extract BigQuery metadata through APIs, SDKs, and scoped metadata views.",
    )
    parser.add_argument("--project", required=True, dest="project_id", help="GCP project to extract from")
    parser.add_argument(
        "--region",
        help="Deprecated alias for a single location filter",
    )
    parser.add_argument(
        "--locations",
        help="Only include these locations (comma-separated, for example: us,eu,us-central1)",
    )
    parser.add_argument(
        "--output-dir",
        help="Output directory (default: output/YYYYMMDD_HHMMSS)",
    )
    parser.add_argument("--days", type=int, default=30, help="Query log window in days (default: 30)")
    parser.add_argument(
        "--max-rows",
        type=int,
        default=200000,
        help="Max rows per query (default: 200000)",
    )
    parser.add_argument(
        "--format",
        dest="output_format",
        choices=SUPPORTED_FORMATS,
        default="json",
        help="Output format for query results (default: json)",
    )
    parser.add_argument(
        "--datasets",
        help="Only include these datasets (comma-separated)",
    )
    parser.add_argument(
        "--families",
        help=(
            "Only include these object families: "
            + ", ".join(OBJECT_FAMILY_KEYS)
        ),
    )
    parser.add_argument(
        "--exclude-families",
        help="Exclude these object families (comma-separated)",
    )
    parser.add_argument(
        "--sources",
        help="Only include these metadata sources: " + ", ".join(CAPABILITY_KEYS),
    )
    parser.add_argument(
        "--exclude-sources",
        help="Exclude these metadata sources (comma-separated)",
    )
    parser.add_argument(
        "--skip",
        help="Deprecated alias for --exclude-sources",
    )
    parser.add_argument(
        "--include-hidden-datasets",
        action="store_true",
        help="Include hidden datasets in API-backed dataset discovery",
    )
    parser.add_argument("--quiet", action="store_true", help="Output only the output dir path on success")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Perform discovery and capability probing without writing output files",
    )
    return parser


def parse_args(argv: Sequence[str] | None = None, now: datetime | None = None) -> ExtractionConfig:
    parser = build_parser()
    namespace = parser.parse_args(argv)

    output_dir = Path(namespace.output_dir) if namespace.output_dir else default_output_dir(now)
    days = namespace.days
    max_rows = namespace.max_rows
    if days <= 0:
        parser.error("--days must be greater than 0")
    if max_rows <= 0:
        parser.error("--max-rows must be greater than 0")
    if namespace.region and namespace.locations:
        parser.error("use either --region or --locations, not both")

    datasets = _parse_csv_values(namespace.datasets, label="dataset", parser=parser, validator=DATASET_RE.fullmatch)
    family_values = _parse_keywords(
        namespace.families,
        valid_values=OBJECT_FAMILY_KEYS,
        flag_name="--families",
        parser=parser,
    )
    excluded_families = _parse_keywords(
        namespace.exclude_families,
        valid_values=OBJECT_FAMILY_KEYS,
        flag_name="--exclude-families",
        parser=parser,
    )
    include_sources = _parse_keywords(
        namespace.sources,
        valid_values=CAPABILITY_KEYS,
        flag_name="--sources",
        parser=parser,
    )
    excluded_sources = set(
        _parse_keywords(
            namespace.exclude_sources,
            valid_values=CAPABILITY_KEYS,
            flag_name="--exclude-sources",
            parser=parser,
        )
    )
    excluded_sources.update(
        _parse_keywords(
            namespace.skip,
            valid_values=CAPABILITY_KEYS,
            flag_name="--skip",
            parser=parser,
        )
    )

    locations_raw = namespace.locations or namespace.region
    location_filters = tuple(value.lower() for value in _parse_csv_values(locations_raw, label="location", parser=parser, validator=None))

    return ExtractionConfig(
        project_id=namespace.project_id,
        location_filters=location_filters,
        output_dir=output_dir,
        days=days,
        max_rows=max_rows,
        output_format=namespace.output_format,
        datasets=datasets,
        include_families=frozenset(family_values),
        exclude_families=frozenset(excluded_families),
        include_sources=frozenset(include_sources),
        exclude_sources=frozenset(excluded_sources),
        include_hidden_datasets=namespace.include_hidden_datasets,
        quiet=namespace.quiet,
        dry_run=namespace.dry_run,
    )

def _parse_keywords(
    raw_value: str | None,
    *,
    valid_values: Sequence[str],
    flag_name: str,
    parser: argparse.ArgumentParser,
) -> tuple[str, ...]:
    values = _parse_csv_values(raw_value, label="value", parser=parser, validator=None)
    invalid = [value for value in values if value not in valid_values]
    if invalid:
        parser.error(
            f"{flag_name} contains invalid values: "
            + ", ".join(invalid)
            + ". Valid values: "
            + ", ".join(valid_values)
        )
    return values


def _parse_csv_values(
    raw_value: str | None,
    *,
    label: str,
    parser: argparse.ArgumentParser,
    validator,
) -> tuple[str, ...]:
    if raw_value is None:
        return ()

    values = tuple(part.strip() for part in raw_value.split(",") if part.strip())
    if not values:
        parser.error(f"--{label}s must include at least one value")

    if validator is not None:
        invalid = [value for value in values if validator(value) is None]
        if invalid:
            parser.error(
                f"--{label}s contains invalid names: "
                + ", ".join(invalid)
                + ". BigQuery dataset names may only contain letters, numbers, and underscores."
            )
    return values

