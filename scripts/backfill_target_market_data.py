from __future__ import annotations

import argparse
import asyncio
import json
import logging
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from ibmd.catalog import load_catalog_bundle
from ibmd.foundation.config import load_deployment_settings
from ibmd.foundation.identity import new_id
from ibmd.foundation.process_lock import ServiceProcessLock
from ibmd.foundation.time import format_utc, parse_utc, utc_now
from ibmd.ib_gateway.ib_async_market_data import (
    IBAsyncMarketDataReader,
    IBMarketDataConnectionSettings,
)
from ibmd.market_data.adapters import (
    MarketDataSchemaError,
    SQLiteMarketBarStore,
)
from ibmd.market_data.application import (
    MarketDataShadowConfig,
    MarketDataShadowService,
    backfill_contract_history,
)
from ibmd.operations.health import ServiceHealthFile
from ibmd.public_contracts.market_data import MarketDataContractV1

SERVICE_NAME = "market_data"
DEFAULT_CLIENT_ID_OFFSET = 80


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Backfill the target MNQ shadow market-data store over one explicit "
            "UTC interval. The legacy price database is never modified."
        )
    )
    parser.add_argument("--start-utc", required=True)
    parser.add_argument("--end-utc", required=True)
    parser.add_argument("--database", type=Path, default=None)
    parser.add_argument("--catalog-root", type=Path, default=ROOT / "catalog")
    parser.add_argument("--instrument", default="MNQ")
    parser.add_argument("--client-id-offset", type=int, default=DEFAULT_CLIENT_ID_OFFSET)
    parser.add_argument("--chunk-seconds", type=int, default=3_600)
    parser.add_argument("--request-spacing-seconds", type=float, default=11.0)
    parser.add_argument("--connect-timeout-seconds", type=float, default=15.0)
    parser.add_argument("--account-timeout-seconds", type=float, default=5.0)
    parser.add_argument("--historical-timeout-seconds", type=float, default=30.0)
    parser.add_argument(
        "--use-rth",
        action="store_true",
        help="request RTH-only data; default preserves baseline useRTH=false",
    )
    return parser


def build_contract(instrument, contract) -> MarketDataContractV1:
    return MarketDataContractV1(
        instrument_id=instrument.instrument_id,
        sec_type=instrument.sec_type,
        symbol=instrument.instrument_id,
        exchange=instrument.exchange,
        currency=instrument.currency,
        trading_class=instrument.trading_class,
        multiplier=instrument.multiplier,
        con_id=contract.con_id,
        local_symbol=contract.local_symbol,
        last_trade_date=contract.last_trade_date,
    )


def contract_windows(calendar, *, start_utc: str, end_utc: str):
    start = parse_utc(start_utc)
    end = parse_utc(end_utc)
    if end <= start:
        raise ValueError("backfill interval must be positive")
    duration = 5
    if int(start.timestamp()) % duration or int(end.timestamp()) % duration:
        raise ValueError("backfill boundaries must be aligned to 5 seconds")
    if start < calendar.coverage_start or end > calendar.coverage_end:
        raise ValueError(
            "backfill interval is outside target contract-calendar coverage: "
            f"coverage={format_utc(calendar.coverage_start)}.."
            f"{format_utc(calendar.coverage_end)}"
        )
    values = []
    for contract in calendar.contracts:
        window_start = max(start, contract.active_from)
        window_end = min(end, contract.active_to)
        if window_start < window_end:
            values.append(
                (
                    contract,
                    format_utc(window_start),
                    format_utc(window_end),
                )
            )
    return tuple(values)


async def run(arguments: argparse.Namespace) -> int:
    settings = load_deployment_settings()
    bundle = load_catalog_bundle(arguments.catalog_root.resolve())
    instrument_id = str(arguments.instrument).strip()
    instrument = bundle.instrument_master.require(instrument_id)
    if bundle.contract_calendar.instrument_id != instrument_id:
        raise ValueError(
            "contract calendar belongs to another instrument: "
            f"{bundle.contract_calendar.instrument_id}"
        )
    windows = contract_windows(
        bundle.contract_calendar,
        start_utc=arguments.start_utc,
        end_utc=arguments.end_utc,
    )
    if not windows:
        raise ValueError("backfill interval contains no active contract window")

    database_path = (
        arguments.database.resolve()
        if arguments.database is not None
        else settings.data_root / "market_data" / instrument.database_name
    )
    repository = SQLiteMarketBarStore(
        database_path,
        instrument_id=instrument_id,
    )
    try:
        repository.validate_schema()
    except MarketDataSchemaError as exc:
        raise RuntimeError(
            "target market-data store is not migrated: "
            f"{database_path}; {exc}"
        ) from exc

    instance_id = new_id("instance")
    config = MarketDataShadowConfig(
        instrument_id=instrument_id,
        deployment_id=settings.deployment_id,
        instance_id=instance_id,
        application_version=settings.application_version,
        configuration_hash=settings.configuration_hash,
        bar_duration_seconds=instrument.default_bar_size_seconds,
        history_lookback_seconds=arguments.chunk_seconds,
        history_chunk_seconds=arguments.chunk_seconds,
        history_chunk_spacing_seconds=arguments.request_spacing_seconds,
        bar_max_age_seconds=60.0,
        realtime_read_timeout_seconds=1.0,
        writer_queue_maxsize=10_000,
        use_rth=bool(arguments.use_rth),
    )
    client_id = settings.ib_client_id + int(arguments.client_id_offset)
    if client_id < 0:
        raise ValueError(f"resolved market-data client id is negative: {client_id}")
    reader = IBAsyncMarketDataReader(
        IBMarketDataConnectionSettings(
            host=settings.ib_host,
            port=settings.ib_port,
            client_id=client_id,
            account_id=settings.ib_account_id,
            connect_timeout_seconds=arguments.connect_timeout_seconds,
            account_timeout_seconds=arguments.account_timeout_seconds,
            historical_timeout_seconds=arguments.historical_timeout_seconds,
            historical_request_spacing_seconds=arguments.request_spacing_seconds,
        )
    )
    paths = settings.paths_for(SERVICE_NAME)
    service = MarketDataShadowService(
        config=config,
        reader=reader,
        repository=repository,
        health_publisher=ServiceHealthFile(
            paths.health_file,
            expected_service=SERVICE_NAME,
        ),
    )

    results = []
    with ServiceProcessLock(
        paths.lock_file,
        service_name=SERVICE_NAME,
        deployment_id=settings.deployment_id,
        instance_id=instance_id,
    ):
        service.publish_starting()
        await service.start(publish_initial=False)
        failed = False
        try:
            for contract_spec, window_start, window_end in windows:
                result = await backfill_contract_history(
                    service=service,
                    contract=build_contract(instrument, contract_spec),
                    start_utc=window_start,
                    end_utc=window_end,
                )
                results.append(result)
        except Exception:
            failed = True
            raise
        finally:
            await service.stop(publish_lifecycle=not failed)
            await reader.close()

    payload = {
        "instrument_id": instrument_id,
        "requested_start_utc": format_utc(parse_utc(arguments.start_utc)),
        "requested_end_utc": format_utc(parse_utc(arguments.end_utc)),
        "completed_at_utc": format_utc(utc_now()),
        "contract_windows": [
            {
                "con_id": item.con_id,
                "local_symbol": item.local_symbol,
                "start_utc": item.start_utc,
                "end_utc": item.end_utc,
                "chunk_count": item.chunk_count,
                "fragment_count": item.fragment_count,
                "changed_bar_count": item.changed_bar_count,
            }
            for item in results
        ],
        "total_chunks": sum(item.chunk_count for item in results),
        "total_fragments": sum(item.fragment_count for item in results),
        "total_changed_bars": sum(item.changed_bar_count for item in results),
    }
    print(json.dumps(payload, ensure_ascii=False, sort_keys=True, indent=2))
    return 0


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    arguments = build_parser().parse_args(argv)
    try:
        return asyncio.run(run(arguments))
    except KeyboardInterrupt:
        return 130


if __name__ == "__main__":
    raise SystemExit(main())
