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


@dataclass(frozen=True)
class DatasetDiscovery:
    project_id: str
    dataset_id: str
    location: str
    payload: dict[str, Any]

    @property
    def qualified_id(self) -> str:
        return f"{self.project_id}.{self.dataset_id}"


class BigQueryService:
    def __init__(self, project_id: str) -> None:
        self._client = bigquery.Client(project=project_id)
        self._project_id = project_id

    def list_datasets(self, *, include_hidden: bool = False) -> list[DatasetDiscovery]:
        dataset_items = sorted(
            self._client.list_datasets(project=self._project_id, include_all=include_hidden),
            key=lambda dataset: dataset.dataset_id,
        )
        discoveries: list[DatasetDiscovery] = []
        for dataset in dataset_items:
            payload = normalize_api_resource(dataset.to_api_repr())
            reference = payload.get("datasetReference", {})
            discoveries.append(
                DatasetDiscovery(
                    project_id=str(reference.get("projectId", self._project_id)),
                    dataset_id=str(reference["datasetId"]),
                    location=str(payload.get("location", "")),
                    payload=payload,
                )
            )
        return discoveries

    def list_table_objects(self, dataset: DatasetDiscovery) -> list[dict[str, Any]]:
        table_items = sorted(
            self._client.list_tables(dataset.qualified_id),
            key=lambda item: item.table_id,
        )
        objects: list[dict[str, Any]] = []
        for table_item in table_items:
            table = self._client.get_table(table_item.reference)
            payload = normalize_api_resource(table.to_api_repr())
            payload["dataset_location"] = dataset.location
            objects.append(payload)
        return objects

    def list_routine_objects(self, dataset: DatasetDiscovery) -> list[dict[str, Any]]:
        routine_items = sorted(
            self._client.list_routines(dataset.qualified_id),
            key=lambda item: item.reference.routine_id,
        )
        objects: list[dict[str, Any]] = []
        for routine_item in routine_items:
            routine = self._client.get_routine(routine_item.reference)
            payload = normalize_api_resource(routine.to_api_repr())
            payload["dataset_location"] = dataset.location
            objects.append(payload)
        return objects

    def list_model_objects(self, dataset: DatasetDiscovery) -> list[dict[str, Any]]:
        model_items = sorted(
            self._client.list_models(dataset.qualified_id),
            key=lambda item: item.reference.model_id,
        )
        objects: list[dict[str, Any]] = []
        for model_item in model_items:
            model = self._client.get_model(model_item.reference)
            payload = normalize_api_resource(model.to_api_repr())
            payload["dataset_location"] = dataset.location
            objects.append(payload)
        return objects

    def probe_query(self, sql: str, *, location: str) -> None:
        query_job = self._client.query(sql, location=location)
        query_job.result(max_results=1)

    def run_query(self, sql: str, max_rows: int, *, location: str) -> QueryResult:
        query_job = self._client.query(sql, location=location)
        row_iterator = query_job.result(max_results=max_rows)
        field_names = tuple(field.name for field in query_job.schema)
        rows = [self._normalize_row(row, field_names) for row in row_iterator]
        return QueryResult(field_names=field_names, rows=rows)

    @staticmethod
    def csv_row(row: dict[str, Any], field_names: Iterable[str]) -> dict[str, str]:
        return {field_name: serialize_csv_cell(row.get(field_name)) for field_name in field_names}

    def _normalize_row(self, row: Row, field_names: tuple[str, ...]) -> dict[str, Any]:
        return {field_name: normalize_value(row[field_name]) for field_name in field_names}


def normalize_api_resource(payload: dict[str, Any]) -> dict[str, Any]:
    return {key: normalize_value(value) for key, value in payload.items()}


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

