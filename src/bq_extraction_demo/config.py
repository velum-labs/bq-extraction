from __future__ import annotations

import argparse
import re
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Sequence

from bq_extraction_demo.contract import STEP_ORDER, SUPPORTED_FORMATS


DATASET_RE = re.compile(r"^[A-Za-z0-9_]+$")


@dataclass(frozen=True)
class ExtractionConfig:
    project_id: str
    region: str
    output_dir: Path
    days: int
    max_rows: int
    output_format: str
    datasets: tuple[str, ...]
    skip_steps: frozenset[str]
    quiet: bool
    dry_run: bool

    @property
    def output_extension(self) -> str:
        return self.output_format

    @property
    def query_location(self) -> str:
        return self.region.upper()

    def should_skip(self, step_key: str) -> bool:
        return step_key in self.skip_steps


def default_output_dir(now: datetime | None = None) -> Path:
    current = now or datetime.now()
    return Path("output") / current.strftime("%Y%m%d_%H%M%S")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="extract.py",
        description="Extract BigQuery schema and query logs for analysis.",
    )
    parser.add_argument("--project", required=True, dest="project_id", help="GCP project to extract from")
    parser.add_argument(
        "--region",
        default="us",
        help="INFORMATION_SCHEMA region (default: us)",
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
        "--skip",
        help=(
            "Skip steps. Available steps: "
            + ", ".join(STEP_ORDER)
        ),
    )
    parser.add_argument("--quiet", action="store_true", help="Output only the output dir path on success")
    parser.add_argument("--dry-run", action="store_true", help="Print queries without executing")
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

    datasets = _parse_csv_values(namespace.datasets, label="dataset", parser=parser, validator=DATASET_RE.fullmatch)
    skip_steps = frozenset(_parse_skip_steps(namespace.skip, parser))

    return ExtractionConfig(
        project_id=namespace.project_id,
        region=namespace.region.lower(),
        output_dir=output_dir,
        days=days,
        max_rows=max_rows,
        output_format=namespace.output_format,
        datasets=datasets,
        skip_steps=skip_steps,
        quiet=namespace.quiet,
        dry_run=namespace.dry_run,
    )


def _parse_skip_steps(raw_value: str | None, parser: argparse.ArgumentParser) -> list[str]:
    values = _parse_csv_values(raw_value, label="step", parser=parser, validator=None)
    invalid = [value for value in values if value not in STEP_ORDER]
    if invalid:
        parser.error(
            "--skip contains invalid steps: "
            + ", ".join(invalid)
            + ". Valid steps: "
            + ", ".join(STEP_ORDER)
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

