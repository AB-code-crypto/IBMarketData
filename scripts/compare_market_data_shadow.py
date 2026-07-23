from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from ibmd.foundation.atomic_json import atomic_write_json
from ibmd.operations.parity.market_data import compare_market_data_databases


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Compare complete legacy and target BID/ASK bars over one UTC interval. "
            "Both databases are opened read-only."
        )
    )
    parser.add_argument("--legacy-database", type=Path, required=True)
    parser.add_argument("--legacy-table", default="MNQ_5s")
    parser.add_argument("--target-database", type=Path, required=True)
    parser.add_argument("--instrument", default="MNQ")
    parser.add_argument("--start-utc", required=True)
    parser.add_argument("--end-utc", required=True)
    parser.add_argument("--tolerance", type=float, default=1e-9)
    parser.add_argument("--max-samples", type=int, default=20)
    parser.add_argument("--output-json", type=Path, default=None)
    return parser


def main(argv: list[str] | None = None) -> int:
    arguments = build_parser().parse_args(argv)
    try:
        report = compare_market_data_databases(
            legacy_database_path=arguments.legacy_database,
            legacy_table_name=arguments.legacy_table,
            target_database_path=arguments.target_database,
            instrument_id=arguments.instrument,
            start_utc=arguments.start_utc,
            end_utc=arguments.end_utc,
            tolerance=arguments.tolerance,
            max_samples=arguments.max_samples,
        )
    except Exception as exc:
        print(
            json.dumps(
                {
                    "is_match": False,
                    "error": f"{type(exc).__name__}: {exc}",
                },
                ensure_ascii=False,
                sort_keys=True,
            ),
            file=sys.stderr,
        )
        return 2

    payload = report.to_dict()
    print(json.dumps(payload, ensure_ascii=False, sort_keys=True, indent=2))
    if arguments.output_json is not None:
        atomic_write_json(arguments.output_json, payload)
    return 0 if report.is_match else 1


if __name__ == "__main__":
    raise SystemExit(main())
