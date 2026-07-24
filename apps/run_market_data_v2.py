from __future__ import annotations

import argparse
import asyncio
import hashlib
import logging
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from ibmd.catalog import (
    ActiveContractStatus,
    load_catalog_bundle,
    resolve_active_contract,
)
from ibmd.foundation.atomic_json import canonical_json_text
from ibmd.foundation.config import load_deployment_settings
from ibmd.foundation.identity import new_id
from ibmd.foundation.process_lock import ServiceProcessLock
from ibmd.foundation.time import parse_utc, utc_now
from ibmd.ib_gateway.ib_async_market_data import (
    IBAsyncMarketDataReader,
    IBMarketDataConnectionSettings,
)
from ibmd.market_data import MarketDataShadowConfig, MarketDataShadowService
from ibmd.market_data.adapters import (
    MarketDataSchemaError,
    SQLiteMarketBarStore,
)
from ibmd.operations.health import ServiceHealthFile
from ibmd.public_contracts.market_data import MarketDataContractV1

SERVICE_NAME = "market_data"
DEFAULT_CLIENT_ID_OFFSET = 80


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Run the target MNQ market-data service in shadow mode. "
            "It writes only its own target SQLite database and never feeds "
            "the legacy signal, trader or execution processes."
        )
    )
    parser.add_argument(
        "--validate-store-only",
        action="store_true",
        help="validate the pre-migrated target store and exit without IB",
    )
    parser.add_argument(
        "--history-only",
        action="store_true",
        help="load one recent complete BID/ASK history window and exit",
    )
    parser.add_argument(
        "--database",
        type=Path,
        default=None,
        help=(
            "target price database path; default is "
            "<IBMD_DATA_ROOT>/market_data/<instrument database_name>"
        ),
    )
    parser.add_argument(
        "--catalog-root",
        type=Path,
        default=ROOT / "catalog",
    )
    parser.add_argument("--instrument", default="MNQ")
    parser.add_argument(
        "--client-id-offset",
        type=int,
        default=DEFAULT_CLIENT_ID_OFFSET,
    )
    parser.add_argument(
        "--history-lookback-seconds",
        type=int,
        default=3_600,
    )
    parser.add_argument(
        "--bar-max-age-seconds",
        type=float,
        default=60.0,
    )
    parser.add_argument(
        "--realtime-read-timeout-seconds",
        type=float,
        default=1.0,
    )
    parser.add_argument(
        "--writer-queue-maxsize",
        type=int,
        default=10_000,
    )
    parser.add_argument(
        "--reconnect-delay-seconds",
        type=float,
        default=5.0,
    )
    parser.add_argument(
        "--connect-timeout-seconds",
        type=float,
        default=15.0,
    )
    parser.add_argument(
        "--account-timeout-seconds",
        type=float,
        default=5.0,
    )
    parser.add_argument(
        "--historical-timeout-seconds",
        type=float,
        default=30.0,
    )
    parser.add_argument(
        "--historical-request-spacing-seconds",
        type=float,
        default=1.0,
    )
    parser.add_argument(
        "--use-rth",
        action="store_true",
        help="request regular-trading-hours data only; default preserves useRTH=false",
    )
    return parser


def migration_command(
    *,
    database_path: Path,
    application_version: str,
) -> str:
    manifest = ROOT / "migrations" / "market_data.v1.json"
    return (
        f"{sys.executable} {ROOT / 'scripts' / 'run_target_migrations.py'} "
        f"--manifest {manifest} "
        f"--database {database_path} "
        f"--application-version {application_version} --apply"
    )


def effective_configuration_hash(
    *,
    deployment_hash: str,
    catalog_hash: str,
    instrument_id: str,
    arguments: argparse.Namespace,
) -> str:
    payload = {
        "deployment_hash": deployment_hash,
        "catalog_hash": catalog_hash,
        "instrument_id": instrument_id,
        "bar_duration_seconds": 5,
        "history_lookback_seconds": arguments.history_lookback_seconds,
        "bar_max_age_seconds": arguments.bar_max_age_seconds,
        "realtime_read_timeout_seconds": arguments.realtime_read_timeout_seconds,
        "use_rth": bool(arguments.use_rth),
    }
    return hashlib.sha256(
        canonical_json_text(payload).encode("utf-8")
    ).hexdigest()


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


async def run(arguments: argparse.Namespace) -> int:
    reconnect_delay = float(arguments.reconnect_delay_seconds)
    if reconnect_delay <= 0.0:
        raise ValueError("reconnect-delay-seconds must be positive")

    settings = load_deployment_settings()
    catalog_root = arguments.catalog_root.resolve()
    bundle = load_catalog_bundle(catalog_root)
    instrument_id = str(arguments.instrument).strip()
    instrument = bundle.instrument_master.require(instrument_id)
    if bundle.contract_calendar.instrument_id != instrument_id:
        raise ValueError(
            "target catalog currently contains a futures calendar for another "
            f"instrument: requested={instrument_id}, "
            f"calendar={bundle.contract_calendar.instrument_id}"
        )
    if instrument.default_bar_size_seconds != 5:
        raise ValueError(
            "market-data shadow v1 requires a 5-second instrument profile"
        )

    paths = settings.paths_for(SERVICE_NAME)
    database_path = (
        arguments.database.resolve()
        if arguments.database is not None
        else settings.data_root / "market_data" / instrument.database_name
    )
    instance_id = new_id("instance")
    config_hash = effective_configuration_hash(
        deployment_hash=settings.configuration_hash,
        catalog_hash=bundle.bundle_hash,
        instrument_id=instrument_id,
        arguments=arguments,
    )
    config = MarketDataShadowConfig(
        instrument_id=instrument_id,
        deployment_id=settings.deployment_id,
        instance_id=instance_id,
        application_version=settings.application_version,
        configuration_hash=config_hash,
        bar_duration_seconds=instrument.default_bar_size_seconds,
        history_lookback_seconds=arguments.history_lookback_seconds,
        bar_max_age_seconds=arguments.bar_max_age_seconds,
        realtime_read_timeout_seconds=arguments.realtime_read_timeout_seconds,
        writer_queue_maxsize=arguments.writer_queue_maxsize,
        use_rth=bool(arguments.use_rth),
    )
    repository = SQLiteMarketBarStore(
        database_path,
        instrument_id=instrument_id,
    )
    health_file = ServiceHealthFile(
        paths.health_file,
        expected_service=SERVICE_NAME,
    )

    client_id = settings.ib_client_id + int(arguments.client_id_offset)
    if client_id < 0:
        raise ValueError(
            f"resolved market-data client id must be non-negative: {client_id}"
        )
    reader = IBAsyncMarketDataReader(
        IBMarketDataConnectionSettings(
            host=settings.ib_host,
            port=settings.ib_port,
            client_id=client_id,
            account_id=settings.ib_account_id,
            connect_timeout_seconds=arguments.connect_timeout_seconds,
            account_timeout_seconds=arguments.account_timeout_seconds,
            historical_timeout_seconds=arguments.historical_timeout_seconds,
            historical_request_spacing_seconds=(
                arguments.historical_request_spacing_seconds
            ),
        )
    )
    service = MarketDataShadowService(
        config=config,
        reader=reader,
        repository=repository,
        health_publisher=health_file,
    )

    writer_started = False
    failed = False
    with ServiceProcessLock(
        paths.lock_file,
        service_name=SERVICE_NAME,
        deployment_id=settings.deployment_id,
        instance_id=instance_id,
    ):
        service.publish_starting()
        try:
            repository.validate_schema()
        except MarketDataSchemaError as exc:
            reason = f"market-data target store is not ready: {exc}"
            service.publish_blocked(reason)
            print(reason, file=sys.stderr)
            print(
                "Apply the explicit offline migration before startup:",
                file=sys.stderr,
            )
            print(
                migration_command(
                    database_path=database_path,
                    application_version=settings.application_version,
                ),
                file=sys.stderr,
            )
            await reader.close()
            return 2

        await service.start(publish_initial=False)
        writer_started = True
        if arguments.validate_store_only:
            print(f"market-data store is compatible: {database_path}")
            await service.stop()
            await reader.close()
            return 0

        try:
            while True:
                now = utc_now()
                resolution = resolve_active_contract(
                    bundle.contract_calendar,
                    now,
                )
                if resolution.status != ActiveContractStatus.ACTIVE:
                    reason = (
                        "active futures contract is unavailable: "
                        f"instrument={instrument_id}, "
                        f"status={resolution.status.value}, "
                        f"observed_at={resolution.observed_at_utc}"
                    )
                    service.publish_blocked(reason)
                    if arguments.history_only:
                        print(reason, file=sys.stderr)
                        return 3
                    delay = reconnect_delay
                    if resolution.gap is not None:
                        remaining = (
                            parse_utc(resolution.gap.end_utc) - now
                        ).total_seconds()
                        if remaining > 0.0:
                            delay = min(delay, remaining)
                    await asyncio.sleep(max(0.05, delay))
                    continue

                contract_spec = resolution.contract
                if contract_spec is None:
                    raise RuntimeError("ACTIVE resolution has no contract")
                contract = build_contract(instrument, contract_spec)
                logging.info(
                    "market-data shadow uses contract=%s conId=%s active_to=%s",
                    contract.local_symbol,
                    contract.con_id,
                    contract_spec.active_to_utc,
                )
                try:
                    changed = await service.run_active_contract(
                        contract=contract,
                        active_to_utc=contract_spec.active_to_utc,
                        history_only=bool(arguments.history_only),
                    )
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    logging.warning(
                        "market-data shadow session failed: %s: %s",
                        type(exc).__name__,
                        exc,
                    )
                    if arguments.history_only:
                        raise
                    await asyncio.sleep(reconnect_delay)
                    continue

                if arguments.history_only:
                    latest = repository.read_latest_complete(instrument_id)
                    print(
                        "market-data recent history published: "
                        f"instrument={instrument_id}, changed={len(changed)}, "
                        f"latest_bar_id={None if latest is None else latest.bar_id}, "
                        f"latest_end={None if latest is None else latest.bar_end_utc}"
                    )
                    return 0
                logging.info(
                    "active contract interval ended; resolving the next catalog state"
                )
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            failed = True
            try:
                service.publish_failed(f"{type(exc).__name__}: {exc}")
            except Exception:
                pass
            raise
        finally:
            if writer_started:
                await service.stop(publish_lifecycle=not failed)
            await reader.close()

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
