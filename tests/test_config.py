from __future__ import annotations

from datetime import datetime

import pytest

from bq_extraction.config import default_output_dir, parse_args


def test_default_output_dir_uses_timestamp() -> None:
    output_dir = default_output_dir(datetime(2026, 3, 16, 12, 34, 56))
    assert str(output_dir) == "output/20260316_123456"


def test_parse_args_preserves_discovery_defaults() -> None:
    config = parse_args(["--project", "example-project"], now=datetime(2026, 3, 16, 12, 34, 56))

    assert config.project_id == "example-project"
    assert config.location_filters == ()
    assert str(config.output_dir) == "output/20260316_123456"
    assert config.days == 30
    assert config.max_rows == 200000
    assert config.output_format == "json"
    assert config.datasets == ()
    assert config.include_families == frozenset()
    assert config.exclude_families == frozenset()
    assert config.include_sources == frozenset()
    assert config.exclude_sources == frozenset()
    assert config.include_hidden_datasets is False
    assert config.quiet is False
    assert config.dry_run is False


def test_parse_args_normalizes_locations_and_selectors() -> None:
    config = parse_args(
        [
            "--project",
            "example-project",
            "--locations",
            "US,eu,us-central1",
            "--datasets",
            "raw,analytics",
            "--families",
            "tables,jobs",
            "--sources",
            "tables.ddls,jobs.query_logs",
            "--exclude-sources",
            "jobs.user_stats",
            "--include-hidden-datasets",
        ]
    )

    assert config.location_filters == ("us", "eu", "us-central1")
    assert config.datasets == ("raw", "analytics")
    assert config.include_families == frozenset({"tables", "jobs"})
    assert config.include_sources == frozenset({"tables.ddls", "jobs.query_logs"})
    assert config.exclude_sources == frozenset({"jobs.user_stats"})
    assert config.include_hidden_datasets is True


def test_parse_args_supports_region_alias_for_single_location() -> None:
    config = parse_args(["--project", "example-project", "--region", "US"])
    assert config.location_filters == ("us",)


def test_parse_args_rejects_invalid_dataset_names() -> None:
    with pytest.raises(SystemExit):
        parse_args(["--project", "example-project", "--datasets", "raw,bad-name"])


def test_parse_args_rejects_invalid_family_names() -> None:
    with pytest.raises(SystemExit):
        parse_args(["--project", "example-project", "--families", "made_up_family"])


def test_parse_args_rejects_invalid_source_names() -> None:
    with pytest.raises(SystemExit):
        parse_args(["--project", "example-project", "--sources", "jobs.made_up"])


def test_parse_args_rejects_region_and_locations_together() -> None:
    with pytest.raises(SystemExit):
        parse_args(["--project", "example-project", "--region", "us", "--locations", "eu"])

