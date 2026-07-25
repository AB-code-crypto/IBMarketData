from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from ibmd.catalog.cme_schedules import (
    CmeScheduleError,
    build_qualified_cme_session_calendar_from_files,
)
from ibmd.foundation.atomic_json import atomic_write_json


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Build a production-qualified target session calendar from an "
            "official CME Reference Data API Trading Schedules JSON export. "
            "The script performs no network requests and never guesses holiday "
            "hours."
        )
    )
    parser.add_argument(
        "--base-calendar",
        type=Path,
        default=ROOT / "catalog" / "sessions.v1.json",
    )
    parser.add_argument("--cme-export", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--session-id", default="CME_EQUITY_INDEX")
    parser.add_argument("--globex-group-code", required=True)
    parser.add_argument("--trading-schedule-id", default=None)
    parser.add_argument("--coverage-start-date", required=True)
    parser.add_argument("--coverage-end-date", required=True)
    parser.add_argument("--calendar-version", required=True)
    parser.add_argument(
        "--force",
        action="store_true",
        help="replace an existing output file",
    )
    return parser


def run(arguments: argparse.Namespace) -> int:
    output = arguments.output.resolve()
    if output.exists() and not arguments.force:
        raise CmeScheduleError(
            f"output already exists; pass --force to replace it: {output}"
        )
    result = build_qualified_cme_session_calendar_from_files(
        base_calendar_path=arguments.base_calendar.resolve(),
        source_export_path=arguments.cme_export.resolve(),
        session_id=str(arguments.session_id),
        globex_group_code=str(arguments.globex_group_code),
        coverage_start_date=str(arguments.coverage_start_date),
        coverage_end_date=str(arguments.coverage_end_date),
        calendar_version=str(arguments.calendar_version),
        trading_schedule_id=(
            None
            if arguments.trading_schedule_id is None
            else str(arguments.trading_schedule_id)
        ),
    )
    atomic_write_json(output, result.calendar.to_dict())
    print(
        json.dumps(
            {
                "output_path": str(output),
                "calendar_version": result.calendar.calendar_version,
                "content_hash": result.calendar.content_hash,
                "trading_schedule_id": result.trading_schedule_id,
                "globex_group_code": result.globex_group_code,
                "coverage_start_date": result.coverage_start_date,
                "coverage_end_date": result.coverage_end_date,
                "generated_exception_count": (
                    result.generated_exception_count
                ),
                "source_sha256": result.source_sha256,
                "production_qualified": True,
                "legacy_database_compatibility_required": False,
            },
            ensure_ascii=False,
            sort_keys=True,
            indent=2,
        )
    )
    return 0


def main(argv: list[str] | None = None) -> int:
    arguments = build_parser().parse_args(argv)
    try:
        return run(arguments)
    except (CmeScheduleError, OSError, ValueError) as exc:
        print(
            f"CME session calendar build failed: {type(exc).__name__}: {exc}",
            file=sys.stderr,
        )
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
