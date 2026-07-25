from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from ibmd.catalog import load_catalog_bundle
from ibmd.execution.adapters import (
    BrokerAttemptSchemaError,
    BrokerAttemptStoreError,
    BrokerReconciliationSchemaError,
    BrokerReconciliationStoreError,
    ExecutionPositionFeedError,
    ExecutionSchemaError,
    ExecutionStateReadError,
    ProtectionSchemaError,
    ProtectionStoreError,
    SQLiteBrokerAttemptStore,
    SQLiteBrokerReconciliationReader,
    SQLiteExecutionPositionFeedReader,
    SQLiteExecutionStateReader,
    SQLiteExecutionStore,
    SQLiteProtectionStore,
)
from ibmd.execution.application.protection import (
    PositionEpisodeProtectionService,
    ProtectionPlanningServiceError,
    protection_plan_payload,
)
from ibmd.execution.domain.protection import (
    ProtectionPlanningError,
    ProtectionPlanningPolicyV1,
)
from ibmd.foundation.config import load_deployment_settings
from ibmd.foundation.identity import new_id
from ibmd.foundation.process_lock import ServiceProcessLock
from ibmd.foundation.time import format_utc, utc_now
from ibmd.public_contracts.protection import PositionEpisodePolicyV1

SERVICE_NAME = "execution"


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Create one durable PositionEpisodeV1 and STOP-first protective "
            "plan from a proven SUCCEEDED strategic operation. This entrypoint "
            "never connects to IB and never submits/cancels broker orders."
        )
    )
    parser.add_argument(
        "--validate-store-only",
        action="store_true",
        help="validate execution/position-feed stores and exit",
    )
    parser.add_argument(
        "--plan-from-operation",
        default=None,
        help=(
            "broker_operation id whose proven fill opens/reverses the "
            "position episode"
        ),
    )
    parser.add_argument("--execution-database", type=Path, default=None)
    parser.add_argument("--position-feed-database", type=Path, default=None)
    parser.add_argument(
        "--catalog-root",
        type=Path,
        default=ROOT / "catalog",
    )
    parser.add_argument("--instrument", default="MNQ")
    parser.add_argument(
        "--position-max-age-seconds",
        type=float,
        default=10.0,
    )
    return parser


def migration_command(
    *,
    database_path: Path,
    application_version: str,
) -> str:
    return (
        f"{sys.executable} {ROOT / 'scripts' / 'run_target_migrations.py'} "
        f"--manifest {ROOT / 'migrations' / 'execution.v1.json'} "
        f"--database {database_path} "
        f"--application-version {application_version} --apply"
    )


def run(arguments: argparse.Namespace) -> int:
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

    execution_store = SQLiteExecutionStore(execution_database)
    operation_source = SQLiteBrokerAttemptStore(execution_database)
    fill_source = SQLiteBrokerReconciliationReader(execution_database)
    execution_state = SQLiteExecutionStateReader(execution_database)
    protection_store = SQLiteProtectionStore(execution_database)
    position_source = SQLiteExecutionPositionFeedReader(
        position_feed_database
    )
    try:
        execution_store.validate_schema()
        operation_source.validate_schema()
        fill_source.validate_schema()
        execution_state.validate_schema()
        protection_store.validate_schema()
        position_source.validate_schema()
    except (
        BrokerAttemptSchemaError,
        BrokerReconciliationSchemaError,
        ExecutionPositionFeedError,
        ExecutionSchemaError,
        ExecutionStateReadError,
        ProtectionSchemaError,
    ) as exc:
        print(
            f"execution protection store is not ready: {exc}",
            file=sys.stderr,
        )
        print(
            "Apply the explicit offline execution migration:",
            file=sys.stderr,
        )
        print(
            migration_command(
                database_path=execution_database,
                application_version=settings.application_version,
            ),
            file=sys.stderr,
        )
        return 2

    if arguments.validate_store_only:
        print(
            "execution protection dependencies are compatible: "
            f"execution={execution_database}, "
            f"position_feed={position_feed_database}"
        )
        return 0

    operation_id = str(arguments.plan_from_operation or "").strip()
    if not operation_id:
        raise ValueError("--plan-from-operation is required")

    bundle = load_catalog_bundle(arguments.catalog_root.resolve())
    instrument_id = str(arguments.instrument or "").strip()
    instrument = bundle.instrument_master.require(instrument_id)
    instrument_policy = bundle.strategy_policy.require(instrument_id)
    protective = instrument_policy.protective
    service = PositionEpisodeProtectionService(
        policy=ProtectionPlanningPolicyV1(
            account_id=settings.ib_account_id,
            strategy_id=bundle.strategy_policy.strategy_id,
            strategy_version=bundle.strategy_policy.strategy_version,
            deployment_id=settings.deployment_id,
            instrument_id=instrument.instrument_id,
            strategy_policy_hash=bundle.strategy_policy.content_hash,
            position_max_age_seconds=float(
                arguments.position_max_age_seconds
            ),
            protective_policy=PositionEpisodePolicyV1(
                price_tick=instrument.price_tick,
                stop_required=protective.stop_required,
                take_profit_enabled=protective.take_profit_enabled,
                stop_loss_points=protective.stop_loss_points,
                take_profit_points=protective.take_profit_points,
                time_in_force=protective.time_in_force,
                stop_outside_rth=protective.stop_outside_rth,
                take_profit_outside_rth=(
                    protective.take_profit_outside_rth
                ),
                price_watchdog_enabled=(
                    protective.price_watchdog_enabled
                ),
                stale_feed_market_close_enabled=(
                    protective.stale_feed_market_close_enabled
                ),
                price_stale_max_seconds=(
                    protective.price_stale_max_seconds
                ),
            ),
        ),
        operation_source=operation_source,
        command_source=execution_store,
        fill_source=fill_source,
        position_snapshot_source=position_source,
        execution_state_source=execution_state,
        protection_repository=protection_store,
    )
    with ServiceProcessLock(
        settings.paths_for(SERVICE_NAME).lock_file,
        service_name=SERVICE_NAME,
        deployment_id=settings.deployment_id,
        instance_id=new_id("instance"),
    ):
        plan = service.plan_from_operation(
            operation_id=operation_id,
            observed_at_utc=format_utc(utc_now()),
        )
    print(
        json.dumps(
            protection_plan_payload(plan),
            ensure_ascii=False,
            sort_keys=True,
            indent=2,
        )
    )
    return 0


def main(argv: list[str] | None = None) -> int:
    arguments = build_parser().parse_args(argv)
    selected = int(bool(arguments.validate_store_only)) + int(
        arguments.plan_from_operation is not None
    )
    if selected != 1:
        print(
            "execution protection requires exactly one of "
            "--validate-store-only or --plan-from-operation",
            file=sys.stderr,
        )
        return 2
    try:
        return run(arguments)
    except (
        BrokerAttemptStoreError,
        BrokerReconciliationStoreError,
        ExecutionPositionFeedError,
        ProtectionPlanningError,
        ProtectionPlanningServiceError,
        ProtectionStoreError,
        ValueError,
    ) as exc:
        print(
            f"execution protection failed: {type(exc).__name__}: {exc}",
            file=sys.stderr,
        )
        return 2
    except KeyboardInterrupt:
        return 130


if __name__ == "__main__":
    raise SystemExit(main())
