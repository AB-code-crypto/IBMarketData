from __future__ import annotations

import argparse
import asyncio
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from ibmd.catalog import load_catalog_bundle
from ibmd.execution.adapters import (
    ExecutionPositionFeedError,
    ExecutionStateReadError,
    ProtectionSchemaError,
    ProtectionStoreError,
    SQLiteExecutionPositionFeedReader,
    SQLiteExecutionStateReader,
    SQLiteProtectionReader,
)
from ibmd.execution.adapters.sqlite_protective_lifecycle import (
    SQLiteProtectiveLifecycleStore,
)
from ibmd.execution.application.protective_lifecycle import (
    ProtectiveLifecycleService,
    ProtectiveLifecycleServiceError,
    protective_lifecycle_payload,
)
from ibmd.execution.domain.protective_lifecycle import (
    ProtectiveLifecycleError,
    ProtectiveLifecyclePolicyV1,
)
from ibmd.foundation.config import load_deployment_settings
from ibmd.foundation.identity import new_id
from ibmd.foundation.process_lock import ServiceProcessLock
from ibmd.foundation.time import format_utc, utc_now
from ibmd.ib_gateway.broker_reconciliation import BrokerReconciliationReadError
from ibmd.ib_gateway.ib_async_broker_reconciliation import (
    IBAsyncBrokerReconciliationReader,
    IBBrokerReconciliationConnectionSettings,
)

SERVICE_NAME = "execution"


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Reconcile protective fills, commissions, OCA sibling state and "
            "position-episode closure. This entrypoint is read-only at IB and "
            "never calls placeOrder or cancelOrder."
        )
    )
    parser.add_argument("--validate-store-only", action="store_true")
    parser.add_argument("--once-position-episode-id", default=None)
    parser.add_argument("--execution-database", type=Path, default=None)
    parser.add_argument("--position-feed-database", type=Path, default=None)
    parser.add_argument(
        "--catalog-root",
        type=Path,
        default=ROOT / "catalog",
    )
    parser.add_argument("--instrument", default="MNQ")
    parser.add_argument(
        "--reconciliation-client-id-offset",
        type=int,
        default=100,
    )
    parser.add_argument(
        "--position-max-age-seconds",
        type=float,
        default=10.0,
    )
    parser.add_argument(
        "--commission-wait-seconds",
        type=float,
        default=2.0,
    )
    return parser


def schema_commands(
    *,
    database_path: Path,
    application_version: str,
) -> tuple[str, str]:
    base = (
        f"{sys.executable} {ROOT / 'scripts' / 'run_target_migrations.py'} "
        f"--manifest {ROOT / 'migrations' / 'execution.v1.json'} "
        f"--database {database_path} "
        f"--application-version {application_version} --apply"
    )
    lifecycle = (
        f"{sys.executable} "
        f"{ROOT / 'scripts' / 'run_execution_protective_lifecycle_schema.py'} "
        f"--database {database_path} "
        f"--application-version {application_version} --apply"
    )
    return base, lifecycle


async def run(arguments: argparse.Namespace) -> int:
    settings = load_deployment_settings()
    execution_database = (
        arguments.execution_database.resolve()
        if arguments.execution_database is not None
        else settings.data_root / "execution" / "execution.sqlite3"
    )
    position_feed_database = (
        arguments.position_feed_database.resolve()
        if arguments.position_feed_database is not None
        else settings.data_root
        / "position_feed"
        / "broker_positions.sqlite3"
    )

    protection_source = SQLiteProtectionReader(execution_database)
    execution_state = SQLiteExecutionStateReader(execution_database)
    position_source = SQLiteExecutionPositionFeedReader(
        position_feed_database
    )
    lifecycle_store = SQLiteProtectiveLifecycleStore(execution_database)
    try:
        protection_source.validate_schema()
        execution_state.validate_schema()
        position_source.validate_schema()
        lifecycle_store.validate_schema()
    except (
        ExecutionPositionFeedError,
        ExecutionStateReadError,
        ProtectionSchemaError,
    ) as exc:
        print(
            f"protective lifecycle store is not ready: {exc}",
            file=sys.stderr,
        )
        print("Apply the explicit offline schema commands:", file=sys.stderr)
        for command in schema_commands(
            database_path=execution_database,
            application_version=settings.application_version,
        ):
            print(command, file=sys.stderr)
        return 2

    if arguments.validate_store_only:
        print(
            "protective lifecycle dependencies are compatible: "
            f"execution={execution_database}, "
            f"position_feed={position_feed_database}"
        )
        return 0

    episode_id = str(arguments.once_position_episode_id or "").strip()
    if not episode_id:
        raise ValueError("--once-position-episode-id is required")

    bundle = load_catalog_bundle(arguments.catalog_root.resolve())
    instrument_id = str(arguments.instrument or "").strip()
    instrument = bundle.instrument_master.require(instrument_id)
    if instrument.sec_type != "FUT":
        raise ValueError(
            "protective lifecycle currently supports futures only"
        )

    reconciliation_client_id = (
        settings.ib_client_id
        + int(arguments.reconciliation_client_id_offset)
    )
    broker_reader = IBAsyncBrokerReconciliationReader(
        IBBrokerReconciliationConnectionSettings(
            host=settings.ib_host,
            port=settings.ib_port,
            client_id=reconciliation_client_id,
            account_id=settings.ib_account_id,
            commission_wait_seconds=float(
                arguments.commission_wait_seconds
            ),
        )
    )
    service = ProtectiveLifecycleService(
        policy=ProtectiveLifecyclePolicyV1(
            account_id=settings.ib_account_id,
            strategy_id=bundle.strategy_policy.strategy_id,
            strategy_version=bundle.strategy_policy.strategy_version,
            deployment_id=settings.deployment_id,
            instrument_id=instrument.instrument_id,
            position_max_age_seconds=float(
                arguments.position_max_age_seconds
            ),
        ),
        protection_source=protection_source,
        execution_state_source=execution_state,
        position_snapshot_source=position_source,
        broker_snapshot_source=broker_reader,
        repository=lifecycle_store,
    )
    try:
        with ServiceProcessLock(
            settings.paths_for(SERVICE_NAME).lock_file,
            service_name=SERVICE_NAME,
            deployment_id=settings.deployment_id,
            instance_id=new_id("instance"),
        ):
            update = await service.run_once(
                position_episode_id=episode_id,
                observed_at_utc=format_utc(utc_now()),
            )
        fills = lifecycle_store.read_fills(episode_id)
        pending = lifecycle_store.read_commission_pending_exec_ids(episode_id)
        payload = protective_lifecycle_payload(
            update,
            fills=fills,
            commission_pending_exec_ids=pending,
        )
        payload["reconciliation_client_id"] = reconciliation_client_id
        print(
            json.dumps(
                payload,
                ensure_ascii=False,
                sort_keys=True,
                indent=2,
            )
        )
        return 0
    finally:
        await broker_reader.close()


def main(argv: list[str] | None = None) -> int:
    arguments = build_parser().parse_args(argv)
    selected = int(bool(arguments.validate_store_only)) + int(
        arguments.once_position_episode_id is not None
    )
    if selected != 1:
        print(
            "protective lifecycle requires exactly one of "
            "--validate-store-only or --once-position-episode-id",
            file=sys.stderr,
        )
        return 2
    try:
        return asyncio.run(run(arguments))
    except (
        BrokerReconciliationReadError,
        ExecutionPositionFeedError,
        ExecutionStateReadError,
        ProtectiveLifecycleError,
        ProtectiveLifecycleServiceError,
        ProtectionStoreError,
        ValueError,
    ) as exc:
        print(
            "protective lifecycle failed: "
            f"{type(exc).__name__}: {exc}",
            file=sys.stderr,
        )
        return 2
    except KeyboardInterrupt:
        return 130


if __name__ == "__main__":
    raise SystemExit(main())
