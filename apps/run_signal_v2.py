from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import logging
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from ibmd.catalog import load_catalog_bundle
from ibmd.foundation.atomic_json import canonical_json_text
from ibmd.foundation.config import load_deployment_settings
from ibmd.foundation.identity import new_id
from ibmd.foundation.process_lock import ServiceProcessLock
from ibmd.foundation.time import parse_utc
from ibmd.operations.health import ServiceHealthFile
from ibmd.signal import SignalShadowConfig, SignalShadowService
from ibmd.signal.adapters import (
    SQLiteSignalMarketDataReader,
    SQLiteSignalStore,
    SignalMarketDataError,
    SignalSchemaError,
)

SERVICE_NAME = "signal"


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Run the target rolling signal in shadow mode. It reads only "
            "target market-data, writes only its own signal database and "
            "never creates trade commands."
        )
    )
    parser.add_argument(
        "--validate-store-only",
        action="store_true",
        help="validate target market-data and signal stores, then exit",
    )
    parser.add_argument(
        "--once-at",
        default=None,
        help=(
            "calculate one explicit UTC signal time, for example "
            "2026-07-23T17:24:00Z, then exit"
        ),
    )
    parser.add_argument(
        "--market-database",
        type=Path,
        default=None,
    )
    parser.add_argument(
        "--signal-database",
        type=Path,
        default=None,
    )
    parser.add_argument(
        "--catalog-root",
        type=Path,
        default=ROOT / "catalog",
    )
    parser.add_argument("--instrument", default="MNQ")
    parser.add_argument(
        "--poll-interval-seconds",
        type=float,
        default=1.0,
    )
    return parser


def migration_command(
    *,
    database_path: Path,
    application_version: str,
) -> str:
    return (
        f"{sys.executable} {ROOT / 'scripts' / 'run_target_migrations.py'} "
        f"--manifest {ROOT / 'migrations' / 'signal.v1.json'} "
        f"--database {database_path} "
        f"--application-version {application_version} --apply"
    )


def service_configuration_hash(
    *,
    deployment_hash: str,
    catalog_hash: str,
    market_database: Path,
    signal_database: Path,
    poll_interval_seconds: float,
) -> str:
    payload = {
        "deployment_hash": deployment_hash,
        "catalog_hash": catalog_hash,
        "market_database": str(market_database),
        "signal_database": str(signal_database),
        "poll_interval_seconds": float(poll_interval_seconds),
    }
    return hashlib.sha256(
        canonical_json_text(payload).encode("utf-8")
    ).hexdigest()


async def run(arguments: argparse.Namespace) -> int:
    settings = load_deployment_settings()
    bundle = load_catalog_bundle(arguments.catalog_root.resolve())
    instrument_id = str(arguments.instrument or "").strip()
    instrument = bundle.instrument_master.require(instrument_id)
    policy = bundle.strategy_policy.require(instrument_id)
    signal_policy = policy.signal

    market_database = (
        arguments.market_database.resolve()
        if arguments.market_database is not None
        else settings.data_root
        / "market_data"
        / instrument.database_name
    )
    signal_database = (
        arguments.signal_database.resolve()
        if arguments.signal_database is not None
        else settings.data_root / "signal" / "signal.sqlite3"
    )
    instance_id = new_id("instance")
    config = SignalShadowConfig(
        deployment_id=settings.deployment_id,
        instance_id=instance_id,
        application_version=settings.application_version,
        service_configuration_hash=service_configuration_hash(
            deployment_hash=settings.configuration_hash,
            catalog_hash=bundle.bundle_hash,
            market_database=market_database,
            signal_database=signal_database,
            poll_interval_seconds=arguments.poll_interval_seconds,
        ),
        strategy_id=bundle.strategy_policy.strategy_id,
        strategy_version=bundle.strategy_policy.strategy_version,
        signal_configuration_hash=(
            bundle.strategy_policy.content_hash
        ),
        instrument_id=instrument_id,
        price_precision=instrument.price_precision,
        source_bar_size_seconds=(
            signal_policy.source_bar_size_seconds
        ),
        max_complete_bar_lag_seconds=(
            signal_policy.max_complete_bar_lag_seconds
        ),
        rolling_step_seconds=signal_policy.rolling_step_seconds,
        pattern_lookback_minutes=(
            signal_policy.pattern_lookback_minutes
        ),
        potential_horizon_minutes=(
            signal_policy.potential_horizon_minutes
        ),
        historical_lookback_days=(
            signal_policy.historical_lookback_days
        ),
        pearson_minimum=signal_policy.pearson_minimum,
        minmax_hard_filter_max_ratio=(
            signal_policy.minmax_hard_filter_max_ratio
        ),
        score_pearson_weight=(
            signal_policy.score_pearson_weight
        ),
        score_end_delta_weight=(
            signal_policy.score_end_delta_weight
        ),
        score_minmax_weight=signal_policy.score_minmax_weight,
        potential_candidate_min_count=(
            signal_policy.potential_candidate_min_count
        ),
        potential_candidate_max_count=(
            signal_policy.potential_candidate_max_count
        ),
        minimum_abs_potential_end_delta_points=(
            signal_policy.minimum_abs_potential_end_delta_points
        ),
        candidate_hour_profile=(
            signal_policy.candidate_hour_profile
        ),
        poll_interval_seconds=arguments.poll_interval_seconds,
    )
    market_data = SQLiteSignalMarketDataReader(
        market_database,
        instrument_id=instrument_id,
        price_precision=instrument.price_precision,
    )
    repository = SQLiteSignalStore(signal_database)
    paths = settings.paths_for(SERVICE_NAME)
    health = ServiceHealthFile(
        paths.health_file,
        expected_service=SERVICE_NAME,
    )
    service = SignalShadowService(
        config=config,
        market_data=market_data,
        repository=repository,
        health_publisher=health,
    )

    with ServiceProcessLock(
        paths.lock_file,
        service_name=SERVICE_NAME,
        deployment_id=settings.deployment_id,
        instance_id=instance_id,
    ):
        service.publish_starting()
        try:
            try:
                repository.validate_schema()
            except SignalSchemaError as exc:
                reason = f"target signal store is not ready: {exc}"
                service.publish_blocked(reason)
                print(reason, file=sys.stderr)
                print(
                    "Apply the explicit offline migration before startup:",
                    file=sys.stderr,
                )
                print(
                    migration_command(
                        database_path=signal_database,
                        application_version=settings.application_version,
                    ),
                    file=sys.stderr,
                )
                return 2

            try:
                market_data.validate_schema()
            except SignalMarketDataError as exc:
                reason = f"target market-data is not ready: {exc}"
                service.publish_blocked(reason)
                print(reason, file=sys.stderr)
                return 3

            if arguments.validate_store_only:
                print(
                    "signal dependencies are compatible: "
                    f"market={market_database}, signal={signal_database}"
                )
                service.publish_stopped()
                return 0

            if arguments.once_at is not None:
                signal_time = parse_utc(arguments.once_at)
                signal_ts = int(signal_time.timestamp())
                if (
                    signal_ts
                    % signal_policy.rolling_step_seconds
                    != 0
                ):
                    raise ValueError(
                        "--once-at must be aligned to the rolling "
                        f"{signal_policy.rolling_step_seconds}s step"
                    )
                calculation = await asyncio.to_thread(
                    service.calculate_at,
                    signal_ts,
                )
                print(
                    json.dumps(
                        calculation.to_dict(),
                        ensure_ascii=False,
                        sort_keys=True,
                        indent=2,
                    )
                )
                service.publish_stopped()
                return 0

            await service.run_forever()
            service.publish_stopped()
            return 0
        except asyncio.CancelledError:
            service.publish_stopped()
            raise
        except Exception as exc:
            try:
                service.publish_failed(
                    f"{type(exc).__name__}: {exc}"
                )
            except Exception:
                pass
            raise


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
