from __future__ import annotations

from unittest.mock import Mock, patch

from bq_extraction_demo.service import BigQueryService, normalize_value


def test_bigquery_service_passes_query_location() -> None:
    fake_job = Mock()
    fake_job.result.return_value = []
    fake_job.schema = []

    fake_client = Mock()
    fake_client.query.return_value = fake_job

    with patch("bq_extraction_demo.service.bigquery.Client", return_value=fake_client):
        service = BigQueryService("demo-project")
        service.run_query("SELECT 1", 10, location="EU")

    fake_client.query.assert_called_once_with("SELECT 1", location="EU")


def test_list_datasets_forwards_include_hidden_flag() -> None:
    dataset_item = Mock()
    dataset_item.dataset_id = "analytics"
    dataset_item._properties = {
        "datasetReference": {"projectId": "demo-project", "datasetId": "analytics"},
        "location": "US",
    }

    fake_client = Mock()
    fake_client.list_datasets.return_value = [dataset_item]

    with patch("bq_extraction_demo.service.bigquery.Client", return_value=fake_client):
        service = BigQueryService("demo-project")
        datasets = service.list_datasets(include_hidden=True)

    fake_client.list_datasets.assert_called_once_with(
        project="demo-project",
        include_all=True,
    )
    assert datasets[0].dataset_id == "analytics"
    assert datasets[0].location == "US"


def test_normalize_value_keeps_float_decimal_form() -> None:
    assert normalize_value(0.0) == "0.0"
    assert normalize_value(12.5) == "12.5"

