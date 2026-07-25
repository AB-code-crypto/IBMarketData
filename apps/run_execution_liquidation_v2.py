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
    SQLiteExecutionPositionFeedReader,
    SQLiteExecutionStateReader,
    SQLiteProtectionReader,
)
from ibmd.execution.adapters.sqlite_liquidation import (
    LiquidationSchemaError,
    LiquidationStoreError,
    SQLiteLiquidationStore,
)
from ibmd.execution.application.liquidation import (
    LiquidationFoundationService,
    LiquidationPolicyV1,
    LiquidationServiceError,
    liquidation_foundation_payload,
)
from ibmd.execution.application.paper_liquidation import (
    PaperLiquidationCoordinator,
    PaperLiquidationError,
    PaperLiquidationPolicy,
    paper_liquidation_payload,
)
from ibmd.execution.domain.liquidation import LiquidationDomainError
from ibmd.execution.domain.liquidation_completion import (
    LiquidationCompletionError,
)
from ibmd.execution.domain.liquidation_exits import LiquidationExitError
from ibmd.execution.domain.liquidation_position import (
    LiquidationPositionError,
)
from ibmd.execution.domain.liquidation_reconciliation import (
    LiquidationReconciliationError,
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
from ibmd.ib_gateway.ib_async_paper_cancellations import (
    IBAsyncPaperOrderCancellationGateway,
    IBPaperCancellationConnectionSettings,
)
from ibmd.ib_gateway.ib_async_paper_orders import (
    IBAsyncPaperOrderGateway,
    IBPaperOrderConnectionSettings,
)
from ibmd.ib_gateway.paper_cancellations import BrokerOrderCancelError
from ibmd.ib_gateway.paper_orders import BrokerOrderSubmitError, PaperOrderRoute
from ibmd.public_contracts.liquidation import LiquidationReason

SERVICE_NAME = "execution"


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Own one durable liquidation operation per position episode. "
            "Request/advance modes are broker-free. Paper mode performs at most "
            "one external broker action per invocation."
        )
    )
    parser.add_argument("--validate-store-only", action="store_true")
    parser.add_argument("--request-position-episode-id", default=None)
    parser.add_argument("--advance-position-episode-id", default=None)
    parser.add_argument("--once-paper-position-episode-id", default=None)
    parser.add_argument(
        "--reason",
        choices=[item.value for item in LiquidationReason],
        default=None,
    )
    parser.add_argument("--source-ref", default=None)
    parser.add_argument("--confirm-paper-account", default=None)
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
    parser.add_argument("--submit-client-id-offset", type=int, default=160)
    parser.add_argument(
        "--reconciliation-client-id-offset",
        type=int,
        default=100,
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
    return parser


def base_migration_command(
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


def liquidation_schema_command(
    *,
    database_path: Path,
    application_version: str,
) -> str:
    return (
        f"{sys.executable} "
        f"{ROOT / 'scripts' / 'run_execution_liquidation_schema.py'} "
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
        raise PaperLiquidationError(
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


def _paths(arguments: argparse.Namespace, settings):
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
    return execution_database, position_feed_database


def _dependencies(arguments: argparse.Namespace, settings):
    execution_database, position_feed_database = _paths(arguments, settings)
    liquidation_store = SQLiteLiquidationStore(execution_database)
    protection_source = SQLiteProtectionReader(execution_database)
    execution_state = SQLiteExecutionStateReader(execution_database)
    position_source = SQLiteExecutionPositionFeedReader(position_feed_database)
    liquidation_store.validate_schema()
    protection_source.validate_schema()
    execution_state.validate_schema()
    position_source.validate_schema()
    return (
        execution_database,
        position_feed_database,
        liquidation_store,
        protection_source,
        execution_state,
        position_source,
    )


def _policy(arguments: argparse.Namespace, settings, bundle) -> LiquidationPolicyV1:
    instrument_id = str(arguments.instrument or "").strip()
    return LiquidationPolicyV1(
        account_id=settings.ib_account_id,
        strategy_id=bundle.strategy_policy.strategy_id,
        strategy_version=bundle.strategy_policy.strategy_version,
        deployment_id=settings.deployment_id,
        instrument_id=instrument_id,
        position_max_age_seconds=float(arguments.position_max_age_seconds),
    )


def _foundation_service(
    *,
    arguments: argparse.Namespace,
    settings,
    bundle,
    liquidation_store,
    protection_source,
    execution_state,
    position_source,
) -> LiquidationFoundationService:
    return LiquidationFoundationService(
        policy=_policy(arguments, settings, bundle),
        protection_source=protection_source,
        execution_state_source=execution_state,
        position_snapshot_source=position_source,
        repository=liquidation_store,
    )


def run_non_mutating(arguments: argparse.Namespace) -> int:
    settings = load_deployment_settings()
    bundle = load_catalog_bundle(arguments.catalog_root.resolve())
    (
        execution_database,
        position_feed_database,
        liquidation_store,
        protection_source,
        execution_state,
        position_source,
    ) = _dependencies(arguments, settings)
    if arguments.validate_store_only:
        print(
            "execution liquidation dependencies are compatible: "
            f"execution={execution_database}, "
            f"position_feed={position_feed_database}"
        )
        return 0
    service = _foundation_service(
        arguments=arguments,
        settings=settings,
        bundle=bundle,
        liquidation_store=liquidation_store,
        protection_source=protection_source,
        execution_state=execution_state,
        position_source=position_source,
    )
    with ServiceProcessLock(
        settings.paths_for(SERVICE_NAME).lock_file,
        service_name=SERVICE_NAME,
        deployment_id=settings.deployment_id,
        instance_id=new_id("instance"),
    ):
        if arguments.request_position_episode_id is not None:
            if arguments.reason is None:
                raise ValueError("--reason is required for liquidation request")
            source_ref = str(arguments.source_ref or "").strip()
            if not source_ref:
                raise ValueError("--source-ref is required for liquidation request")
            result = service.request(
                position_episode_id=arguments.request_position_episode_id,
                reason=LiquidationReason(arguments.reason),
                source_ref=source_ref,
                observed_at_utc=format_utc(utc_now()),
            )
        else:
            result = service.advance(
                position_episode_id=arguments.advance_position_episode_id,
                observed_at_utc=format_utc(utc_now()),
                allow_proven_retry=False,
            )
    print(
        json.dumps(
            liquidation_foundation_payload(result),
            ensure_ascii=False,
            sort_keys=True,
            indent=2,
        )
    )
    return 0


async def run_paper(arguments: argparse.Namespace) -> int:
    settings = load_deployment_settings()
    bundle = load_catalog_bundle(arguments.catalog_root.resolve())
    (
        _execution_database,
        _position_feed_database,
        liquidation_store,
        protection_source,
        execution_state,
        position_source,
    ) = _dependencies(arguments, settings)
    episode_id = str(arguments.once_paper_position_episode_id or "").strip()
    if not episode_id:
        raise ValueError("--once-paper-position-episode-id is required")
    confirmed_account = str(arguments.confirm_paper_account or "").strip()
    if not confirmed_account:
        raise ValueError("--confirm-paper-account is required")
    episode = protection_source.read_episode(episode_id)
    if episode is None:
        raise PaperLiquidationError(
            f"position episode does not exist: {episode_id}"
        )
    instrument_id = str(arguments.instrument or "").strip()
    route = _route(bundle, instrument_id=instrument_id, episode=episode)
    cancel_client_id = (
        settings.ib_client_id + int(arguments.cancel_client_id_offset)
    )
    submit_client_id = (
        settings.ib_client_id + int(arguments.submit_client_id_offset)
    )
    reconciliation_client_id = (
        settings.ib_client_id
        + int(arguments.reconciliation_client_id_offset)
    )
    if len({cancel_client_id, submit_client_id, reconciliation_client_id}) != 3:
        raise ValueError(
            "cancel, submit and reconciliation client IDs must be distinct"
        )
    order_gateway = IBAsyncPaperOrderGateway(
        IBPaperOrderConnectionSettings(
            host=settings.ib_host,
            port=settings.ib_port,
            client_id=submit_client_id,
            account_id=settings.ib_account_id,
        )
    )
    cancellation_gateway = IBAsyncPaperOrderCancellationGateway(
        IBPaperCancellationConnectionSettings(
            host=settings.ib_host,
            port=settings.ib_port,
            client_id=cancel_client_id,
            account_id=settings.ib_account_id,
        )
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
    coordinator = PaperLiquidationCoordinator(
        policy=PaperLiquidationPolicy(
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
            reconciliation_read_attempts=int(
                arguments.reconciliation_read_attempts
            ),
            reconciliation_poll_seconds=float(
                arguments.reconciliation_poll_seconds
            ),
        ),
        protection_source=protection_source,
        execution_state_source=execution_state,
        position_snapshot_source=position_source,
        repository=liquidation_store,
        order_gateway=order_gateway,
        cancellation_gateway=cancellation_gateway,
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
        payload = paper_liquidation_payload(result)
        payload["client_ids"] = {
            "protective_cancel": cancel_client_id,
            "market_close_submit": submit_client_id,
            "read_only_reconciliation": reconciliation_client_id,
        }
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
        await cancellation_gateway.close()
        await order_gateway.close()


def main(argv: list[str] | None = None) -> int:
    arguments = build_parser().parse_args(argv)
    selected = sum(
        (
            bool(arguments.validate_store_only),
            arguments.request_position_episode_id is not None,
            arguments.advance_position_episode_id is not None,
            arguments.once_paper_position_episode_id is not None,
        )
    )
    if selected != 1:
        print(
            "execution liquidation requires exactly one mode: "
            "--validate-store-only, --request-position-episode-id, "
            "--advance-position-episode-id or "
            "--once-paper-position-episode-id",
            file=sys.stderr,
        )
        return 2
    try:
        if arguments.once_paper_position_episode_id is not None:
            return asyncio.run(run_paper(arguments))
        return run_non_mutating(arguments)
    except (
        BrokerOrderCancelError,
        BrokerOrderSubmitError,
        BrokerReconciliationReadError,
        ExecutionPositionFeedError,
        ExecutionStateReadError,
        LiquidationCompletionError,
        LiquidationDomainError,
        LiquidationExitError,
        LiquidationPositionError,
        LiquidationReconciliationError,
        LiquidationSchemaError,
        LiquidationServiceError,
        LiquidationStoreError,
        PaperLiquidationError,
        ProtectionSchemaError,
        ValueError,
    ) as exc:
        print(
            f"execution liquidation failed: {type(exc).__name__}: {exc}",
            file=sys.stderr,
        )
        settings = load_deployment_settings()
        execution_database, _ = _paths(arguments, settings)
        print("Apply fresh target schemas if needed:", file=sys.stderr)
        print(
            base_migration_command(
                database_path=execution_database,
                application_version=settings.application_version,
            ),
            file=sys.stderr,
        )
        print(
            liquidation_schema_command(
                database_path=execution_database,
                application_version=settings.application_version,
            ),
            file=sys.stderr,
        )
        return 2
    except KeyboardInterrupt:
        return 130


if __name__ == "__main__":
    raise SystemExit(main())
