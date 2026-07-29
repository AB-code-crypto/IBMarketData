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

from ibmd.catalog import CatalogError, load_catalog_bundle
from ibmd.execution.adapters import (
    ExecutionDecisionSourceError,
    ExecutionPositionFeedError,
    ExecutionStateReadError,
    ProtectionSchemaError,
    ProtectionStoreError,
    SQLiteExecutionDecisionReader,
    SQLiteExecutionPositionFeedReader,
    SQLiteExecutionStateReader,
    SQLiteExecutionStore,
    SQLiteProtectionReader,
    SQLiteProtectiveLifecycleStore,
    SQLiteProtectiveSubmitStore,
)
from ibmd.execution.adapters.sqlite_liquidation import (
    LiquidationSchemaError,
    LiquidationStoreError,
    SQLiteLiquidationStore,
)
from ibmd.execution.application.protective_lifecycle import (
    ProtectiveLifecyclePolicyV1,
    ProtectiveLifecycleService,
    ProtectiveLifecycleServiceError,
)
from ibmd.execution.application.reverse_handoff import (
    PaperReverseHandoffCoordinator,
    PaperReverseHandoffError,
    PaperReverseHandoffPolicyV1,
    paper_reverse_handoff_payload,
)
from ibmd.execution.domain.protective_lifecycle import ProtectiveLifecycleError
from ibmd.execution.domain.reverse_handoff import ReverseHandoffError
from ibmd.foundation.config import load_deployment_settings
from ibmd.foundation.identity import new_id
from ibmd.foundation.process_lock import ServiceProcessLock
from ibmd.ib_gateway.broker_reconciliation import BrokerReconciliationReadError
from ibmd.ib_gateway.ib_async_broker_reconciliation import (
    IBAsyncBrokerReconciliationReader,
    IBBrokerReconciliationConnectionSettings,
)
from ibmd.ib_gateway.ib_async_paper_cancellations import (
    IBAsyncPaperOrderCancellationGateway,
    IBPaperCancellationConnectionSettings,
)
from ibmd.ib_gateway.paper_cancellations import BrokerOrderCancelError

SERVICE_NAME = "execution"


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Cancel and reconcile the current position episode's protective exits "
            "before a REVERSE command can reach the MARKET submit boundary. "
            "At most one cancelOrder call is made per invocation."
        )
    )
    parser.add_argument("--validate-store-only", action="store_true")
    parser.add_argument("--once-command-id", default=None)
    parser.add_argument("--confirm-paper-account", default=None)
    parser.add_argument("--decision-database", type=Path, default=None)
    parser.add_argument("--execution-database", type=Path, default=None)
    parser.add_argument("--position-feed-database", type=Path, default=None)
    parser.add_argument(
        "--catalog-root",
        type=Path,
        default=ROOT / "catalog",
    )
    parser.add_argument("--instrument", default="MNQ")
    parser.add_argument("--position-max-age-seconds", type=float, default=10.0)
    parser.add_argument("--cancel-client-id-offset", type=int, default=140)
    parser.add_argument(
        "--reconciliation-client-id-offset",
        type=int,
        default=100,
    )
    parser.add_argument("--connect-timeout-seconds", type=float, default=15.0)
    parser.add_argument("--request-timeout-seconds", type=float, default=15.0)
    parser.add_argument("--commission-wait-seconds", type=float, default=2.0)
    return parser


def _paths(arguments: argparse.Namespace, settings):
    execution_database = (
        arguments.execution_database.resolve()
        if arguments.execution_database is not None
        else settings.data_root / "execution" / "execution.sqlite3"
    )
    decision_database = (
        arguments.decision_database.resolve()
        if arguments.decision_database is not None
        else settings.data_root / "decision" / "decision.sqlite3"
    )
    position_feed_database = (
        arguments.position_feed_database.resolve()
        if arguments.position_feed_database is not None
        else settings.data_root
        / "position_feed"
        / "broker_positions.sqlite3"
    )
    return execution_database, decision_database, position_feed_database


def _dependencies(arguments: argparse.Namespace, settings):
    execution_database, decision_database, position_feed_database = _paths(
        arguments,
        settings,
    )
    execution_store = SQLiteExecutionStore(execution_database)
    execution_state = SQLiteExecutionStateReader(execution_database)
    protection_reader = SQLiteProtectionReader(execution_database)
    lifecycle_store = SQLiteProtectiveLifecycleStore(execution_database)
    submit_store = SQLiteProtectiveSubmitStore(execution_database)
    liquidation_store = SQLiteLiquidationStore(execution_database)
    decision_source = SQLiteExecutionDecisionReader(decision_database)
    position_source = SQLiteExecutionPositionFeedReader(position_feed_database)
    execution_store.validate_schema()
    execution_state.validate_schema()
    protection_reader.validate_schema()
    lifecycle_store.validate_schema()
    submit_store.validate_schema()
    liquidation_store.validate_schema()
    decision_source.validate_schema()
    position_source.validate_schema()
    return (
        execution_database,
        decision_database,
        position_feed_database,
        execution_store,
        execution_state,
        protection_reader,
        lifecycle_store,
        submit_store,
        liquidation_store,
        decision_source,
        position_source,
    )


async def run(arguments: argparse.Namespace) -> int:
    settings = load_deployment_settings()
    bundle = load_catalog_bundle(arguments.catalog_root.resolve())
    instrument_id = str(arguments.instrument or "").strip()
    if not instrument_id:
        raise ValueError("--instrument is required")
    instrument = bundle.instrument_master.require(instrument_id)
    strategy_policy = bundle.strategy_policy.require(instrument_id)
    (
        execution_database,
        decision_database,
        position_feed_database,
        execution_store,
        execution_state,
        protection_reader,
        lifecycle_store,
        submit_store,
        liquidation_store,
        decision_source,
        position_source,
    ) = _dependencies(arguments, settings)
    if arguments.validate_store_only:
        print(
            "execution reverse-handoff dependencies are compatible: "
            f"execution={execution_database}, "
            f"decision={decision_database}, "
            f"position_feed={position_feed_database}"
        )
        return 0
    command_id = str(arguments.once_command_id or "").strip()
    confirmed_account = str(arguments.confirm_paper_account or "").strip()
    if not command_id:
        raise ValueError("--once-command-id is required")
    if not confirmed_account:
        raise ValueError("--confirm-paper-account is required")
    cancel_client_id = settings.ib_client_id + int(
        arguments.cancel_client_id_offset
    )
    reconciliation_client_id = settings.ib_client_id + int(
        arguments.reconciliation_client_id_offset
    )
    if min(cancel_client_id, reconciliation_client_id) < 0:
        raise ValueError("resolved IB client ids must be non-negative")
    if cancel_client_id == reconciliation_client_id:
        raise ValueError(
            "reverse cancel and reconciliation client IDs must be distinct"
        )
    reconciliation = IBAsyncBrokerReconciliationReader(
        IBBrokerReconciliationConnectionSettings(
            host=settings.ib_host,
            port=settings.ib_port,
            client_id=reconciliation_client_id,
            account_id=settings.ib_account_id,
            connect_timeout_seconds=float(
                arguments.connect_timeout_seconds
            ),
            open_orders_timeout_seconds=float(
                arguments.request_timeout_seconds
            ),
            completed_orders_timeout_seconds=float(
                arguments.request_timeout_seconds
            ),
            executions_timeout_seconds=float(
                arguments.request_timeout_seconds
            ),
            commission_wait_seconds=float(
                arguments.commission_wait_seconds
            ),
        )
    )
    cancellation = IBAsyncPaperOrderCancellationGateway(
        IBPaperCancellationConnectionSettings(
            host=settings.ib_host,
            port=settings.ib_port,
            client_id=cancel_client_id,
            account_id=settings.ib_account_id,
            connect_timeout_seconds=float(
                arguments.connect_timeout_seconds
            ),
        )
    )
    lifecycle_service = ProtectiveLifecycleService(
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
        protection_source=protection_reader,
        execution_state_source=execution_state,
        position_snapshot_source=position_source,
        broker_snapshot_source=reconciliation,
        repository=lifecycle_store,
    )
    coordinator = PaperReverseHandoffCoordinator(
        policy=PaperReverseHandoffPolicyV1(
            account_id=settings.ib_account_id,
            environment=settings.environment,
            confirmed_paper_account_id=confirmed_account,
            strategy_id=bundle.strategy_policy.strategy_id,
            strategy_version=bundle.strategy_policy.strategy_version,
            deployment_id=settings.deployment_id,
            instrument_id=instrument.instrument_id,
            policy_hash=bundle.strategy_policy.content_hash,
            position_max_age_seconds=float(
                arguments.position_max_age_seconds
            ),
        ),
        command_state_source=execution_store,
        command_request_source=decision_source,
        execution_state_source=execution_state,
        protection_state_source=protection_reader,
        position_snapshot_source=position_source,
        protection_repository=submit_store,
        liquidation_state_source=liquidation_store,
        lifecycle_service=lifecycle_service,
        cancellation_gateway=cancellation,
    )
    try:
        with ServiceProcessLock(
            settings.paths_for(SERVICE_NAME).lock_file,
            service_name=SERVICE_NAME,
            deployment_id=settings.deployment_id,
            instance_id=new_id("instance"),
        ):
            result = await coordinator.run_once(command_id=command_id)
        payload = paper_reverse_handoff_payload(result)
        payload["client_ids"] = {
            "protective_cancel": cancel_client_id,
            "read_only_reconciliation": reconciliation_client_id,
        }
        payload["instrument"] = {
            "instrument_id": instrument.instrument_id,
            "sec_type": instrument.sec_type,
            "target_quantity": strategy_policy.target_quantity,
        }
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
        await reconciliation.close()
        await cancellation.close()


def main(argv: list[str] | None = None) -> int:
    arguments = build_parser().parse_args(argv)
    selected = int(bool(arguments.validate_store_only)) + int(
        arguments.once_command_id is not None
    )
    if selected != 1:
        print(
            "execution reverse handoff requires exactly one mode: "
            "--validate-store-only or --once-command-id",
            file=sys.stderr,
        )
        return 2
    try:
        return asyncio.run(run(arguments))
    except (
        BrokerOrderCancelError,
        BrokerReconciliationReadError,
        CatalogError,
        ExecutionDecisionSourceError,
        ExecutionPositionFeedError,
        ExecutionStateReadError,
        LiquidationSchemaError,
        LiquidationStoreError,
        PaperReverseHandoffError,
        ProtectionSchemaError,
        ProtectionStoreError,
        ProtectiveLifecycleError,
        ProtectiveLifecycleServiceError,
        ReverseHandoffError,
        ValueError,
    ) as exc:
        print(
            "execution reverse handoff failed: "
            f"{type(exc).__name__}: {exc}",
            file=sys.stderr,
        )
        return 2
    except KeyboardInterrupt:
        return 130


if __name__ == "__main__":
    raise SystemExit(main())
