from __future__ import annotations

from datetime import datetime

import pytest

from bq_extraction_demo.config import default_output_dir, parse_args


def test_default_output_dir_uses_timestamp() -> None:
    output_dir = default_output_dir(datetime(2026, 3, 16, 12, 34, 56))
    assert str(output_dir) == "output/20260316_123456"


def test_parse_args_preserves_contract_defaults() -> None:
    config = parse_args(["--project", "demo-project"], now=datetime(2026, 3, 16, 12, 34, 56))

    assert config.project_id == "demo-project"
    assert config.region == "us"
    assert str(config.output_dir) == "output/20260316_123456"
    assert config.days == 30
    assert config.max_rows == 200000
    assert config.output_format == "json"
    assert config.datasets == ()
    assert config.skip_steps == frozenset()
    assert config.quiet is False
    assert config.dry_run is False


def test_parse_args_normalizes_region_and_lists() -> None:
    config = parse_args(
        [
            "--project",
            "demo-project",
            "--region",
            "US-CENTRAL1",
            "--datasets",
            "raw,analytics",
            "--skip",
            "query_logs,user_stats",
        ]
    )

    assert config.region == "us-central1"
    assert config.query_location == "US-CENTRAL1"
    assert config.datasets == ("raw", "analytics")
    assert config.skip_steps == frozenset({"query_logs", "user_stats"})


def test_parse_args_rejects_invalid_dataset_names() -> None:
    with pytest.raises(SystemExit):
        parse_args(["--project", "demo-project", "--datasets", "raw,bad-name"])


def test_parse_args_rejects_invalid_skip_steps() -> None:
    with pytest.raises(SystemExit):
        parse_args(["--project", "demo-project", "--skip", "made_up_step"])

