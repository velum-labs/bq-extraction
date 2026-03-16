from __future__ import annotations

import os
import subprocess
from pathlib import Path

import pytest


ROOT = Path(__file__).resolve().parents[1]


def test_cli_dry_run_smoke(tmp_path: Path) -> None:
    command = [
        "uv",
        "run",
        "python",
        "scripts/extract.py",
        "--project",
        "demo-project",
        "--region",
        "us",
        "--output-dir",
        str(tmp_path / "dry-run-output"),
        "--dry-run",
    ]

    result = subprocess.run(command, cwd=ROOT, capture_output=True, text=True, check=False)

    assert result.returncode == 0, result.stderr
    assert "Dry run complete. No output files were written." in result.stdout
    assert not (tmp_path / "dry-run-output").exists()


@pytest.mark.skipif(
    not os.environ.get("BQ_EXTRACTION_SMOKE_PROJECT"),
    reason="set BQ_EXTRACTION_SMOKE_PROJECT to run the live smoke test",
)
def test_cli_live_smoke(tmp_path: Path) -> None:
    project_id = os.environ["BQ_EXTRACTION_SMOKE_PROJECT"]
    region = os.environ.get("BQ_EXTRACTION_SMOKE_REGION", "us")
    output_dir = tmp_path / "live-output"
    command = [
        "uv",
        "run",
        "python",
        "scripts/extract.py",
        "--project",
        project_id,
        "--region",
        region,
        "--output-dir",
        str(output_dir),
        "--days",
        "1",
    ]

    result = subprocess.run(command, cwd=ROOT, capture_output=True, text=True, check=False)

    assert result.returncode == 0, result.stderr
    assert output_dir.exists()
    assert (output_dir / "datasets.json").exists()
    assert (output_dir / "tables.json").exists()

