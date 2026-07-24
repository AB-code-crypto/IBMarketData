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

from ibmd.catalog import (
    ActiveContractStatus,
    load_catalog_bundle,
    resolve_active_contract,
    resolve_session,
)
from ibmd.execution import (
    PaperOrderSubmitCoordinator,
    PaperSubmitError,
    PaperSubmitPolicy,
    RegisteredFuturesContractV1,
    paper_submit_payload,
)
from ibmd.execution.adapters import (
    BrokerAttemptSchemaError,
    BrokerAttemptStoreError,
    BrokerReconciliationSchemaError,
    BrokerReconciliationStoreError,
    ExecutionDecisionSourceError,
    ExecutionSchemaError,
    ExecutionStateReadError,
    SQLiteBrokerAttemptStore,
    SQLiteBrokerReconciliationStore,
    SQLiteExecutionDecisionReader,
    SQLiteExecutionStateReader,
    SQLiteExecutionStore,
)
from ibmd.execution.application.new_risk_window import (
    NewRiskWindowError,
    NewRiskWindowV1,
)
from ibmd.execution.domain import (
    BrokerAttemptDomainError,
    BrokerReconciliationDomainError,
)
from ibmd.foundation.config import load_deployment_settings
from ibmd.foundation.identity import new_id
from ibmd.foundation.process_lock import ServiceProcessLock
from ibmd.foundation.time import format_utc, utc_now
from ibmd.ib_gateway import (
    BrokerOrderSubmitError,
    BrokerReconciliationReadError,
    IBAsyncBrokerReconciliationReader,
    IBAsyncPaperOrderGateway,
    IBBrokerReconciliationConnectionSettings,
    IBPaperOrderConnectionSettings,
    PaperOrderRoute,
)

SERVICE_NAME = "execution"


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Submit at most one explicit target MARKET order through an Interactive "
            "Brokers paper account. SUBMITTING and the allocated order id are "
            "persisted before placeOrder; uncertain outcomes are never retried."
        )
    )
    parser.add_argument(
        "--validate-store-only",
        action="store_true",
        help="validate execution migration v2 and exit without connecting to IB",
    )
    parser.add_argument(
        "--once-command-id",
        default=None,
        help="submit or reconcile one explicit ADMITTED strategy command id",
    )
    parser.add_argument(
        "--confirm-paper-account",
        default=None,
        help=(
            "mandatory exact repetition of IB_ACCOUNT_ID for the mutating mode"
        ),
    )
    parser.add_argument("--decision-database", type=Path, default=None)
    parser.add_argument("--execution-database", type=Path, default=None)
    parser.add_argument(
        "--catalog-root",
        type=Path,
        default=ROOT / "catalog",
    )
    parser.add_argument("--instrument", default="MNQ")
    parser.add_argument("--submit-client-id-offset", type=int, default=120)
    parser.add_argument("--reconciliation-client-id-offset", type=int, default=100)
    parser.add_argument("--connect-timeout-seconds", type=float, default=15.0)
    parser.add_argument("--request-timeout-seconds", type=float, default=15.0)
    parser.add_argument("--commission-wait-seconds", type=float, default=2.0)
    parser.add_argument("--reconciliation-read-attempts", type=int, default=5)
    parser.add_argument("--reconciliation-poll-seconds", type=float, default=1.0)
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


async def run(arguments: argparse.Namespace) -> int:
    settings = load_deployment_settings()
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

    execution_store = SQLiteExecutionStore(execution_database)
    execution_state = SQLiteExecutionStateReader(execution_database)
    attempt_store = SQLiteBrokerAttemptStore(execution_database)
    reconciliation_store = SQLiteBrokerReconciliationStore(execution_database)
    try:
        execution_store.validate_schema()
        execution_state.validate_schema()
        attempt_store.validate_schema()
        reconciliation_store.validate_schema()
    except (
        ExecutionSchemaError,
        ExecutionStateReadError,
        BrokerAttemptSchemaError,
        BrokerReconciliationSchemaError,
    ) as exc:
        print(
            f"paper execution store is not ready: {exc}",
            file=sys.stderr,
        )
        print(
            "Apply the explicit offline migration before startup:",
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
            "paper submit execution store is compatible: "
            f"execution={execution_database}"
        )
        return 0

    command_id = str(arguments.once_command_id or "").strip()
    confirmed_account = str(arguments.confirm_paper_account or "").strip()
    if not command_id:
        raise ValueError("--once-command-id is required")
    if not confirmed_account:
        raise ValueError(
            "--confirm-paper-account is mandatory for broker mutation"
        )

    bundle = load_catalog_bundle(arguments.catalog_root.resolve())
    instrument_id = str(arguments.instrument or "").strip()
    instrument = bundle.instrument_master.require(instrument_id)
    instrument_policy = bundle.strategy_policy.require(instrument_id)
    session_id = instrument_policy.daily_flat.session_id
    daily_flat_session = bundle.session_calendar.require(session_id)
    new_risk_window = NewRiskWindowV1(
        enabled=instrument_policy.daily_flat.enabled,
        timezone_name=daily_flat_session.timezone,
        liquidation_start_local=(
            instrument_policy.daily_flat.liquidation_start_local
        ),
        risk_blocked_until_local=(
            instrument_policy.daily_flat.risk_blocked_until_local
        ),
    )

    observed = utc_now()
    resolution = resolve_active_contract(bundle.contract_calendar, observed)
    active_contract = None
    route = None
    if (
        resolution.status == ActiveContractStatus.ACTIVE
        and resolution.contract is not None
    ):
        contract = resolution.contract
        active_contract = RegisteredFuturesContractV1(
            con_id=contract.con_id,
            local_symbol=contract.local_symbol,
            contract_is_active=True,
        )
        route = PaperOrderRoute(
            instrument_id=instrument.instrument_id,
            con_id=contract.con_id,
            local_symbol=contract.local_symbol,
            last_trade_date=contract.last_trade_date,
            sec_type=instrument.sec_type,
            exchange=instrument.exchange,
            currency=instrument.currency,
            trading_class=instrument.trading_class,
            multiplier=instrument.multiplier,
        )

    submit_client_id = settings.ib_client_id + int(
        arguments.submit_client_id_offset
    )
    reconciliation_client_id = settings.ib_client_id + int(
        arguments.reconciliation_client_id_offset
    )
    if min(submit_client_id, reconciliation_client_id) < 0:
        raise ValueError("resolved IB client ids must be non-negative")
    if submit_client_id == reconciliation_client_id:
        raise ValueError(
            "submit and reconciliation IB client ids must be different"
        )

    decision_source = SQLiteExecutionDecisionReader(decision_database)
    decision_source.validate_schema()
    submit_gateway = IBAsyncPaperOrderGateway(
        IBPaperOrderConnectionSettings(
            host=settings.ib_host,
            port=settings.ib_port,
            client_id=submit_client_id,
            account_id=settings.ib_account_id,
            connect_timeout_seconds=float(
                arguments.connect_timeout_seconds
            ),
        )
    )
    reconciliation_source = IBAsyncBrokerReconciliationReader(
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
    coordinator = PaperOrderSubmitCoordinator(
        policy=PaperSubmitPolicy(
            account_id=settings.ib_account_id,
            environment=settings.environment,
            confirmed_paper_account_id=confirmed_account,
            strategy_id=bundle.strategy_policy.strategy_id,
            strategy_version=bundle.strategy_policy.strategy_version,
            deployment_id=settings.deployment_id,
            instrument_id=instrument.instrument_id,
            policy_hash=bundle.strategy_policy.content_hash,
            daily_risk_timezone=instrument_policy.daily_pnl.timezone,
            active_contract=active_contract,
            order_route=route,
            reconciliation_read_attempts=int(
                arguments.reconciliation_read_attempts
            ),
            reconciliation_poll_seconds=float(
                arguments.reconciliation_poll_seconds
            ),
        ),
        command_state_source=execution_store,
        command_request_source=decision_source,
        execution_state_source=execution_state,
        attempt_repository=attempt_store,
        reconciliation_repository=reconciliation_store,
        order_gateway=submit_gateway,
        broker_snapshot_source=reconciliation_source,
    )

    instance_id = new_id("instance")
    lock_file = settings.paths_for(SERVICE_NAME).lock_file
    try:
        with ServiceProcessLock(
            lock_file,
            service_name=SERVICE_NAME,
            deployment_id=settings.deployment_id,
            instance_id=instance_id,
        ):
            submit_observed = utc_now()
            submit_observed_text = format_utc(submit_observed)
            new_risk_window.require_allows_new_risk(
                observed_at_utc=submit_observed_text,
                lead_seconds=60,
            )
            session_resolution = resolve_session(
                bundle.session_calendar,
                session_id=session_id,
                at_utc=submit_observed,
            )
            if not session_resolution.is_trading_open:
                raise PaperSubmitError(
                    "paper submission requires an open catalog session: "
                    f"phase={session_resolution.phase.value}, "
                    f"local={session_resolution.local_date} "
                    f"{session_resolution.local_time}, "
                    f"reason={session_resolution.reason}"
                )
            result = await coordinator.run_once(command_id=command_id)
            payload = paper_submit_payload(result)
            payload["session"] = {
                "session_id": session_resolution.session_id,
                "phase": session_resolution.phase.value,
                "local_date": session_resolution.local_date,
                "local_time": session_resolution.local_time,
                "reason": session_resolution.reason,
                "production_qualified": (
                    session_resolution.production_qualified
                ),
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
        await reconciliation_source.close()
        await submit_gateway.close()


def main(argv: list[str] | None = None) -> int:
    arguments = build_parser().parse_args(argv)
    selected = int(bool(arguments.validate_store_only)) + int(
        arguments.once_command_id is not None
    )
    if selected != 1:
        print(
            "paper execution submit requires exactly one of "
            "--validate-store-only or --once-command-id",
            file=sys.stderr,
        )
        return 2
    try:
        return asyncio.run(run(arguments))
    except (
        BrokerAttemptDomainError,
        BrokerAttemptStoreError,
        BrokerOrderSubmitError,
        BrokerReconciliationDomainError,
        BrokerReconciliationReadError,
        BrokerReconciliationStoreError,
        ExecutionDecisionSourceError,
        NewRiskWindowError,
        PaperSubmitError,
        ValueError,
    ) as exc:
        print(
            f"paper execution submit failed: {type(exc).__name__}: {exc}",
            file=sys.stderr,
        )
        return 2
    except KeyboardInterrupt:
        return 130


if __name__ == "__main__":
    raise SystemExit(main())
