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
    SQLiteProtectiveSubmitStore,
)
from ibmd.execution.application.protective_submit import (
    PaperProtectiveSubmitCoordinator,
    PaperProtectiveSubmitError,
    PaperProtectiveSubmitPolicy,
    paper_protective_submit_payload,
)
from ibmd.execution.domain.protective_submission import (
    ProtectiveSubmissionDomainError,
)
from ibmd.foundation.config import load_deployment_settings
from ibmd.foundation.identity import new_id
from ibmd.foundation.process_lock import ServiceProcessLock
from ibmd.ib_gateway.broker_reconciliation import BrokerReconciliationReadError
from ibmd.ib_gateway.ib_async_broker_reconciliation import (
    IBAsyncBrokerReconciliationReader,
    IBBrokerReconciliationConnectionSettings,
)
from ibmd.ib_gateway.ib_async_paper_orders import (
    IBAsyncPaperOrderGateway,
    IBPaperOrderConnectionSettings,
)
from ibmd.ib_gateway.paper_orders import (
    BrokerOrderSubmitError,
    PaperOrderRoute,
)
from ibmd.operations.restart_probe import (
    CrashAfterSuccessfulSubmitGateway,
    RestartProbeError,
    require_restart_probe_checkpoint,
)

SERVICE_NAME = "execution"


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Submit or reconcile at most one paper protective order for one "
            "durable position episode. STOP is always first; TAKE PROFIT is "
            "ineligible until a fresh broker proof confirms STOP LIVE."
        )
    )
    parser.add_argument("--validate-store-only", action="store_true")
    parser.add_argument("--once-position-episode-id", default=None)
    parser.add_argument("--confirm-paper-account", default=None)
    parser.add_argument("--execution-database", type=Path, default=None)
    parser.add_argument("--position-feed-database", type=Path, default=None)
    parser.add_argument(
        "--catalog-root",
        type=Path,
        default=ROOT / "catalog",
    )
    parser.add_argument("--instrument", default="MNQ")
    parser.add_argument("--submit-client-id-offset", type=int, default=140)
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
        "--proof-max-age-seconds",
        type=float,
        default=15.0,
    )
    parser.add_argument(
        "--reconciliation-read-attempts",
        type=int,
        default=5,
    )
    parser.add_argument(
        "--reconciliation-poll-seconds",
        type=float,
        default=1.0,
    )
    parser.add_argument(
        "--commission-wait-seconds",
        type=float,
        default=2.0,
    )
    parser.add_argument(
        "--drill-crash-after-submit",
        action="store_true",
        help=(
            "paper-drill only: terminate after a successful protective broker "
            "call and before reconciliation"
        ),
    )
    parser.add_argument(
        "--drill-crash-checkpoint-file",
        type=Path,
        default=None,
        help="atomic checkpoint written immediately before the intentional exit",
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


def _route(bundle, *, instrument_id: str, episode) -> PaperOrderRoute:
    instrument = bundle.instrument_master.require(instrument_id)
    matches = [
        item
        for item in bundle.contract_calendar.contracts
        if item.con_id == episode.con_id
        and item.local_symbol == episode.local_symbol
    ]
    if len(matches) != 1:
        raise PaperProtectiveSubmitError(
            "position episode contract is absent or ambiguous in catalog: "
            f"con_id={episode.con_id}, local_symbol={episode.local_symbol}"
        )
    contract = matches[0]
    return PaperOrderRoute(
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


async def run_once(arguments: argparse.Namespace) -> int:
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
    protection_store = SQLiteProtectiveSubmitStore(execution_database)
    execution_state = SQLiteExecutionStateReader(execution_database)
    position_source = SQLiteExecutionPositionFeedReader(
        position_feed_database
    )
    try:
        protection_source.validate_schema()
        protection_store.validate_schema()
        execution_state.validate_schema()
        position_source.validate_schema()
    except (
        ExecutionPositionFeedError,
        ExecutionStateReadError,
        ProtectionSchemaError,
    ) as exc:
        print(
            f"paper protective submit store is not ready: {exc}",
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
            "paper protective submit dependencies are compatible: "
            f"execution={execution_database}, "
            f"position_feed={position_feed_database}"
        )
        return 0

    episode_id = str(arguments.once_position_episode_id or "").strip()
    if not episode_id:
        raise ValueError("--once-position-episode-id is required")
    confirmed_account = str(arguments.confirm_paper_account or "").strip()
    if not confirmed_account:
        raise ValueError("--confirm-paper-account is required")

    episode = protection_source.read_episode(episode_id)
    if episode is None:
        raise PaperProtectiveSubmitError(
            f"position episode does not exist: {episode_id}"
        )
    bundle = load_catalog_bundle(arguments.catalog_root.resolve())
    instrument_id = str(arguments.instrument or "").strip()
    route = _route(bundle, instrument_id=instrument_id, episode=episode)

    submit_client_id = (
        settings.ib_client_id + int(arguments.submit_client_id_offset)
    )
    reconciliation_client_id = (
        settings.ib_client_id
        + int(arguments.reconciliation_client_id_offset)
    )
    if submit_client_id == reconciliation_client_id:
        raise ValueError(
            "submit and reconciliation client IDs must differ"
        )

    gateway = IBAsyncPaperOrderGateway(
        IBPaperOrderConnectionSettings(
            host=settings.ib_host,
            port=settings.ib_port,
            client_id=submit_client_id,
            account_id=settings.ib_account_id,
        )
    )
    if arguments.drill_crash_after_submit:
        checkpoint = require_restart_probe_checkpoint(
            environment=settings.environment,
            deployment_id=settings.deployment_id,
            data_root=settings.data_root,
            checkpoint_file=arguments.drill_crash_checkpoint_file,
        )
        gateway = CrashAfterSuccessfulSubmitGateway(
            inner=gateway,
            checkpoint_file=checkpoint,
        )
    elif arguments.drill_crash_checkpoint_file is not None:
        raise RestartProbeError(
            "--drill-crash-checkpoint-file requires "
            "--drill-crash-after-submit"
        )
    reconciliation = IBAsyncBrokerReconciliationReader(
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
    coordinator = PaperProtectiveSubmitCoordinator(
        policy=PaperProtectiveSubmitPolicy(
            account_id=settings.ib_account_id,
            environment=settings.environment,
            confirmed_paper_account_id=confirmed_account,
            strategy_id=bundle.strategy_policy.strategy_id,
            strategy_version=bundle.strategy_policy.strategy_version,
            deployment_id=settings.deployment_id,
            instrument_id=instrument_id,
            order_route=route,
            position_max_age_seconds=float(
                arguments.position_max_age_seconds
            ),
            proof_max_age_seconds=float(arguments.proof_max_age_seconds),
            reconciliation_read_attempts=int(
                arguments.reconciliation_read_attempts
            ),
            reconciliation_poll_seconds=float(
                arguments.reconciliation_poll_seconds
            ),
        ),
        protection_source=protection_source,
        protection_repository=protection_store,
        execution_state_source=execution_state,
        position_snapshot_source=position_source,
        order_gateway=gateway,
        broker_snapshot_source=reconciliation,
    )
    try:
        with ServiceProcessLock(
            settings.paths_for(SERVICE_NAME).lock_file,
            service_name=SERVICE_NAME,
            deployment_id=settings.deployment_id,
            instance_id=new_id("instance"),
        ):
            result = await coordinator.run_once(
                position_episode_id=episode_id
            )
        payload = paper_protective_submit_payload(result)
        payload["submit_client_id"] = submit_client_id
        payload["reconciliation_client_id"] = reconciliation_client_id
        payload["route"] = {
            "con_id": route.con_id,
            "local_symbol": route.local_symbol,
            "last_trade_date": route.last_trade_date,
            "exchange": route.exchange,
            "currency": route.currency,
            "trading_class": route.trading_class,
            "multiplier": route.multiplier,
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
        await gateway.close()


def main(argv: list[str] | None = None) -> int:
    arguments = build_parser().parse_args(argv)
    selected = int(bool(arguments.validate_store_only)) + int(
        arguments.once_position_episode_id is not None
    )
    if selected != 1:
        print(
            "paper protective submit requires exactly one of "
            "--validate-store-only or --once-position-episode-id",
            file=sys.stderr,
        )
        return 2
    try:
        return asyncio.run(run_once(arguments))
    except (
        BrokerOrderSubmitError,
        BrokerReconciliationReadError,
        ExecutionPositionFeedError,
        ExecutionStateReadError,
        PaperProtectiveSubmitError,
        ProtectionStoreError,
        ProtectiveSubmissionDomainError,
        RestartProbeError,
        ValueError,
    ) as exc:
        print(
            "paper protective submit failed: "
            f"{type(exc).__name__}: {exc}",
            file=sys.stderr,
        )
        return 2
    except KeyboardInterrupt:
        return 130


if __name__ == "__main__":
    raise SystemExit(main())
