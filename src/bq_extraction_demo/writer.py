from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Any, Iterable

from bq_extraction_demo.service import BigQueryService


def write_json(path: Path, payload: Any) -> None:
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=True) + "\n", encoding="utf-8")


def write_csv(path: Path, field_names: Iterable[str], rows: list[dict[str, Any]]) -> None:
    field_names = list(field_names)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=field_names)
        writer.writeheader()
        for row in rows:
            writer.writerow(BigQueryService.csv_row(row, field_names))


def write_rows(
    path: Path,
    *,
    output_format: str,
    rows: list[dict[str, Any]],
    field_names: Iterable[str] | None = None,
) -> None:
    if output_format == "json":
        write_json(path, rows)
        return
    write_csv(path, field_names or derive_field_names(rows), rows)


def derive_field_names(rows: list[dict[str, Any]]) -> list[str]:
    ordered: list[str] = []
    seen: set[str] = set()
    for row in rows:
        for key in row:
            if key in seen:
                continue
            seen.add(key)
            ordered.append(key)
    return ordered

