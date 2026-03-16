#!/usr/bin/env python3

from __future__ import annotations

import sys

from bq_extraction.config import parse_args
from bq_extraction.extractor import ExtractionRunner, format_error


def main(argv: list[str] | None = None) -> int:
    config = parse_args(argv)
    runner = ExtractionRunner(config)
    runner.run()
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main(sys.argv[1:]))
    except KeyboardInterrupt:
        print("Extraction interrupted.", file=sys.stderr)
        raise SystemExit(130)
    except Exception as exc:
        print(f"Extraction failed: {format_error(exc)}", file=sys.stderr)
        raise SystemExit(1)

