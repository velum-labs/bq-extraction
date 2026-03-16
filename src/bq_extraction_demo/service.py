from __future__ import annotations

import base64
import json
from dataclasses import dataclass
from datetime import date, datetime, time, timezone
from decimal import Decimal
from typing import Any, Iterable

from google.cloud import bigquery
from google.cloud.bigquery.table import Row


@dataclass(frozen=True)
class QueryResult:
    field_names: tuple[str, ...]
    rows: list[dict[str, Any]]


class BigQueryService:
    def __init__(self, project_id: str, *, query_location: str | None = None) -> None:
        self._client = bigquery.Client(project=project_id)
        self._query_location = query_location

    def list_datasets(self) -> list[dict[str, Any]]:
        datasets = sorted(self._client.list_datasets(), key=lambda dataset: dataset.dataset_id)
        return [dataset.to_api_repr() for dataset in datasets]

    def list_dataset_ids(self) -> list[str]:
        return [dataset["datasetReference"]["datasetId"] for dataset in self.list_datasets()]

    def run_query(self, sql: str, max_rows: int) -> QueryResult:
        query_job = self._client.query(sql, location=self._query_location)
        row_iterator = query_job.result(max_results=max_rows)
        field_names = tuple(field.name for field in query_job.schema)
        rows = [self._normalize_row(row, field_names) for row in row_iterator]
        return QueryResult(field_names=field_names, rows=rows)

    @staticmethod
    def csv_row(row: dict[str, Any], field_names: Iterable[str]) -> dict[str, str]:
        return {field_name: serialize_csv_cell(row.get(field_name)) for field_name in field_names}

    def _normalize_row(self, row: Row, field_names: tuple[str, ...]) -> dict[str, Any]:
        return {field_name: normalize_value(row[field_name]) for field_name in field_names}


def normalize_value(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, Decimal):
        return format(value, "f")
    if isinstance(value, int):
        return str(value)
    if isinstance(value, float):
        return str(value)
    if isinstance(value, datetime):
        return _format_datetime(value)
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, time):
        return value.isoformat()
    if isinstance(value, (bytes, bytearray)):
        return base64.b64encode(bytes(value)).decode("ascii")
    if isinstance(value, Row):
        return {key: normalize_value(item) for key, item in value.items()}
    if isinstance(value, dict):
        return {key: normalize_value(item) for key, item in value.items()}
    if isinstance(value, list):
        return [normalize_value(item) for item in value]
    if isinstance(value, tuple):
        return [normalize_value(item) for item in value]
    return value


def serialize_csv_cell(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, (dict, list)):
        return json.dumps(value, separators=(",", ":"), ensure_ascii=True)
    return str(value)


def _format_datetime(value: datetime) -> str:
    if value.tzinfo is not None:
        value = value.astimezone(timezone.utc).replace(tzinfo=None)
    timespec = "microseconds" if value.microsecond else "seconds"
    return value.isoformat(sep=" ", timespec=timespec)

