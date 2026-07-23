from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from ibmd.catalog import load_catalog_bundle
from ibmd.foundation.identity import new_id
from ibmd.foundation.process_lock import ServiceProcessLock
from ibmd.foundation.time import format_utc, parse_utc
from ibmd.market_data.adapters import SQLiteMarketBarStore
from ibmd.operations.migrations.legacy_market_data import (
    LegacyContractMapping,
    import_legacy_complete_bars,
)

SERVICE_NAME = "market_data"


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Import complete legacy BID/ASK bars into the target market-data "
            "store over one explicit UTC interval.\n"
            "The legacy database is opened read-only.\n"
            "Without --apply, this command performs only a validated dry run."
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--legacy-database", type=Path, required=True)
    parser.add_argument("--legacy-table", default="MNQ_5s")
    parser.add_argument("--target-database", type=Path, required=True)
    parser.add_argument("--catalog-root", type=Path, default=ROOT / "catalog")
    parser.add_argument("--instrument", default="MNQ")
    parser.add_argument("--start-utc", required=True)
    parser.add_argument("--end-utc", required=True)
    parser.add_argument(
        "--apply",
        action="store_true",
        help="write validated missing complete bars into the target store",
    )
    return parser


def _required_environment(name: str) -> str:
    value = str(os.environ.get(name, "") or "").strip()
    if not value:
        raise ValueError(f"required environment value is missing: {name}")
    return value


def _market_data_lock() -> ServiceProcessLock:
    deployment_id = _required_environment("IBMD_DEPLOYMENT_ID")
    data_root = Path(
        _required_environment("IBMD_DATA_ROOT")
    ).expanduser()
    if not data_root.is_absolute():
        data_root = Path.cwd() / data_root
    lock_file = (
        data_root.resolve()
        / "runtime"
        / "locks"
        / f"{SERVICE_NAME}.lock"
    )
    return ServiceProcessLock(
        lock_file,
        service_name=SERVICE_NAME,
        deployment_id=deployment_id,
        instance_id=new_id("instance"),
    )


def main(argv: list[str] | None = None) -> int:
    arguments = build_parser().parse_args(argv)
    instrument_id = str(arguments.instrument or "").strip()
    bundle = load_catalog_bundle(arguments.catalog_root.resolve())
    instrument = bundle.instrument_master.require(instrument_id)
    if bundle.contract_calendar.instrument_id != instrument_id:
        raise ValueError(
            "target contract calendar belongs to another instrument: "
            f"requested={instrument_id}, "
            f"calendar={bundle.contract_calendar.instrument_id}"
        )
    if instrument.default_bar_size_seconds != 5:
        raise ValueError(
            "legacy importer currently supports the existing five-second "
            f"profile only: instrument={instrument_id}"
        )

    start = parse_utc(arguments.start_utc)
    end = parse_utc(arguments.end_utc)
    if start < bundle.contract_calendar.coverage_start:
        raise ValueError(
            "import start precedes target contract-calendar coverage: "
            f"start={format_utc(start)}, "
            f"coverage_start={format_utc(bundle.contract_calendar.coverage_start)}"
        )
    if end > bundle.contract_calendar.coverage_end:
        raise ValueError(
            "import end exceeds target contract-calendar coverage: "
            f"end={format_utc(end)}, "
            f"coverage_end={format_utc(bundle.contract_calendar.coverage_end)}"
        )

    target_database = arguments.target_database.resolve()
    SQLiteMarketBarStore(
        target_database,
        instrument_id=instrument_id,
    ).validate_schema()
    mappings = tuple(
        LegacyContractMapping(
            local_symbol=contract.local_symbol,
            con_id=contract.con_id,
        )
        for contract in bundle.contract_calendar.contracts
    )

    def execute() -> dict[str, object]:
        result = import_legacy_complete_bars(
            legacy_database_path=arguments.legacy_database.resolve(),
            legacy_table_name=arguments.legacy_table,
            target_database_path=target_database,
            instrument_id=instrument_id,
            mappings=mappings,
            start_utc=arguments.start_utc,
            end_utc=arguments.end_utc,
            bar_duration_seconds=instrument.default_bar_size_seconds,
            apply=bool(arguments.apply),
        )
        return result.to_dict()

    if arguments.apply:
        with _market_data_lock():
            payload = execute()
    else:
        payload = execute()

    print(
        json.dumps(
            payload,
            ensure_ascii=False,
            sort_keys=True,
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
