from __future__ import annotations

import argparse
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
)
from ibmd.decision.adapters import (
    DecisionSchemaError,
    DecisionStoreError,
    SQLiteDecisionStore,
)
from ibmd.execution.adapters import (
    BrokerAttemptSchemaError,
    BrokerAttemptStoreError,
    ExecutionPositionFeedError,
    ExecutionSchemaError,
    ExecutionStateReadError,
    ExecutionStoreError,
    SQLiteBrokerAttemptStore,
    SQLiteExecutionPositionFeedReader,
    SQLiteExecutionStateReader,
    SQLiteExecutionStore,
)
from ibmd.execution.application.new_risk_window import (
    NewRiskWindowError,
    NewRiskWindowV1,
)
from ibmd.execution.application.paper_drill import (
    PaperDrillPolicy,
    PaperDrillPreparationError,
    PaperExecutionDrillPreparer,
)
from ibmd.execution.domain import (
    ExecutionDomainError,
    PositionProjectionError,
    RegisteredFuturesContractV1,
)
from ibmd.foundation.config import ConfigurationError, load_deployment_settings
from ibmd.foundation.identity import new_id
from ibmd.foundation.process_lock import ServiceProcessLock
from ibmd.foundation.time import format_utc, utc_now
from ibmd.public_contracts.decision import DesiredTargetSide

SERVICE_DECISION = "decision"
SERVICE_EXECUTION = "execution"


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Prepare one short-lived, broker-free paper execution drill command. "
            "The tool requires a fresh COMPLETE position-feed snapshot proving MNQ "
            "FLAT, writes only the decision/execution stores and never connects to IB."
        )
    )
    parser.add_argument(
        "--validate-store-only",
        action="store_true",
        help="validate decision, execution and position-feed stores without writes",
    )
    parser.add_argument(
        "--prepare",
        action="store_true",
        help="stage one paper-drill decision command and ADMITTED execution state",
    )
    parser.add_argument("--drill-id", default=None)
    parser.add_argument(
        "--target-side",
        choices=("LONG", "SHORT"),
        default=None,
        help="explicit target side for the one-contract OPEN drill",
    )
    parser.add_argument(
        "--confirm-paper-account",
        default=None,
        help="mandatory exact repetition of IB_ACCOUNT_ID in prepare mode",
    )
    parser.add_argument("--decision-database", type=Path, default=None)
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
        default=30.0,
    )
    parser.add_argument(
        "--command-ttl-seconds",
        type=int,
        default=120,
    )
    return parser


def migration_command(
    *,
    manifest: Path,
    database_path: Path,
    application_version: str,
) -> str:
    return (
        f"{sys.executable} {ROOT / 'scripts' / 'run_target_migrations.py'} "
        f"--manifest {manifest} --database {database_path} "
        f"--application-version {application_version} --apply"
    )


def _paths(arguments: argparse.Namespace, data_root: Path) -> tuple[Path, Path, Path]:
    decision_database = (
        arguments.decision_database.resolve()
        if arguments.decision_database is not None
        else data_root / "decision" / "decision.sqlite3"
    )
    execution_database = (
        arguments.execution_database.resolve()
        if arguments.execution_database is not None
        else data_root / "execution" / "execution.sqlite3"
    )
    position_feed_database = (
        arguments.position_feed_database.resolve()
        if arguments.position_feed_database is not None
        else data_root / "position_feed" / "broker_positions.sqlite3"
    )
    return decision_database, execution_database, position_feed_database


def _validate_stores(
    *,
    decision_store: SQLiteDecisionStore,
    execution_store: SQLiteExecutionStore,
    execution_state: SQLiteExecutionStateReader,
    attempt_store: SQLiteBrokerAttemptStore,
    position_source: SQLiteExecutionPositionFeedReader,
) -> None:
    decision_store.validate_schema()
    execution_store.validate_schema()
    execution_state.validate_schema()
    attempt_store.validate_schema()
    position_source.validate_schema()


def run(arguments: argparse.Namespace) -> int:
    settings = load_deployment_settings()
    decision_database, execution_database, position_feed_database = _paths(
        arguments,
        settings.data_root,
    )

    decision_store = SQLiteDecisionStore(decision_database)
    execution_store = SQLiteExecutionStore(execution_database)
    execution_state = SQLiteExecutionStateReader(execution_database)
    attempt_store = SQLiteBrokerAttemptStore(execution_database)
    position_source = SQLiteExecutionPositionFeedReader(position_feed_database)

    try:
        _validate_stores(
            decision_store=decision_store,
            execution_store=execution_store,
            execution_state=execution_state,
            attempt_store=attempt_store,
            position_source=position_source,
        )
    except (
        DecisionSchemaError,
        ExecutionSchemaError,
        ExecutionStateReadError,
        BrokerAttemptSchemaError,
        ExecutionPositionFeedError,
    ) as exc:
        print(f"paper drill stores are not ready: {exc}", file=sys.stderr)
        print("Apply the explicit offline migrations:", file=sys.stderr)
        print(
            migration_command(
                manifest=ROOT / "migrations" / "decision.v1.json",
                database_path=decision_database,
                application_version=settings.application_version,
            ),
            file=sys.stderr,
        )
        print(
            migration_command(
                manifest=ROOT / "migrations" / "execution.v1.json",
                database_path=execution_database,
                application_version=settings.application_version,
            ),
            file=sys.stderr,
        )
        print(
            migration_command(
                manifest=ROOT / "migrations" / "position_feed.v1.json",
                database_path=position_feed_database,
                application_version=settings.application_version,
            ),
            file=sys.stderr,
        )
        return 2

    if arguments.validate_store_only:
        print(
            "paper drill stores are compatible: "
            f"decision={decision_database}, execution={execution_database}, "
            f"position_feed={position_feed_database}"
        )
        return 0

    drill_id = str(arguments.drill_id or "").strip()
    confirmed_account = str(arguments.confirm_paper_account or "").strip()
    if not drill_id:
        raise ValueError("--drill-id is required with --prepare")
    if arguments.target_side is None:
        raise ValueError("--target-side is required with --prepare")
    if not confirmed_account:
        raise ValueError(
            "--confirm-paper-account is required with --prepare"
        )

    bundle = load_catalog_bundle(arguments.catalog_root.resolve())
    instrument_id = str(arguments.instrument or "").strip()
    instrument = bundle.instrument_master.require(instrument_id)
    instrument_policy = bundle.strategy_policy.require(instrument_id)
    if instrument.sec_type != "FUT":
        raise ValueError("paper execution drill currently supports FUT only")

    observed = utc_now()
    observed_text = format_utc(observed)
    daily_flat_session = bundle.session_calendar.require(
        instrument_policy.daily_flat.session_id
    )
    NewRiskWindowV1(
        enabled=instrument_policy.daily_flat.enabled,
        timezone_name=daily_flat_session.timezone,
        liquidation_start_local=(
            instrument_policy.daily_flat.liquidation_start_local
        ),
        risk_blocked_until_local=(
            instrument_policy.daily_flat.risk_blocked_until_local
        ),
    ).require_allows_new_risk(
        observed_at_utc=observed_text,
        lead_seconds=60,
    )

    resolution = resolve_active_contract(bundle.contract_calendar, observed)
    if (
        resolution.status != ActiveContractStatus.ACTIVE
        or resolution.contract is None
    ):
        raise PaperDrillPreparationError(
            "paper drill requires one active contract; "
            f"catalog_status={resolution.status.value}"
        )
    active_con_id = resolution.contract.con_id
    registry = tuple(
        RegisteredFuturesContractV1(
            con_id=item.con_id,
            local_symbol=item.local_symbol,
            contract_is_active=item.con_id == active_con_id,
        )
        for item in bundle.contract_calendar.contracts
    )
    active_contract = next(
        item for item in registry if item.contract_is_active
    )

    preparer = PaperExecutionDrillPreparer(
        policy=PaperDrillPolicy(
            drill_id=drill_id,
            account_id=settings.ib_account_id,
            environment=settings.environment,
            confirmed_paper_account_id=confirmed_account,
            strategy_id=bundle.strategy_policy.strategy_id,
            strategy_version=bundle.strategy_policy.strategy_version,
            deployment_id=settings.deployment_id,
            instrument_id=instrument.instrument_id,
            policy_hash=bundle.strategy_policy.content_hash,
            target_side=DesiredTargetSide(arguments.target_side),
            target_quantity=instrument_policy.target_quantity,
            command_ttl_seconds=int(arguments.command_ttl_seconds),
            position_max_age_seconds=float(
                arguments.position_max_age_seconds
            ),
            daily_risk_timezone=instrument_policy.daily_pnl.timezone,
            daily_risk_target=instrument_policy.daily_pnl.target_usd,
            active_contract=active_contract,
        ),
        decision_repository=decision_store,
        execution_repository=execution_store,
        execution_state_source=execution_state,
        position_snapshot_source=position_source,
        broker_attempt_source=attempt_store,
        contract_registry=registry,
    )

    decision_lock = settings.paths_for(SERVICE_DECISION).lock_file
    execution_lock = settings.paths_for(SERVICE_EXECUTION).lock_file
    with ServiceProcessLock(
        decision_lock,
        service_name=SERVICE_DECISION,
        deployment_id=settings.deployment_id,
        instance_id=new_id("instance"),
    ):
        with ServiceProcessLock(
            execution_lock,
            service_name=SERVICE_EXECUTION,
            deployment_id=settings.deployment_id,
            instance_id=new_id("instance"),
        ):
            result = preparer.prepare(observed_at_utc=observed_text)

    payload = result.to_dict()
    payload["databases"] = {
        "decision": str(decision_database),
        "execution": str(execution_database),
        "position_feed": str(position_feed_database),
    }
    payload["next_step"] = {
        "entrypoint": str(ROOT / "apps" / "run_execution_submit_v2.py"),
        "once_command_id": result.command.command_id,
        "confirm_paper_account": settings.ib_account_id,
        "warning": (
            "No broker order has been sent. Review the JSON, keep position-feed "
            "fresh, then invoke the submit entrypoint explicitly before "
            "submit_before_utc."
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


def main(argv: list[str] | None = None) -> int:
    arguments = build_parser().parse_args(argv)
    selected = int(bool(arguments.validate_store_only)) + int(
        bool(arguments.prepare)
    )
    if selected != 1:
        print(
            "paper drill preparer requires exactly one of "
            "--validate-store-only or --prepare",
            file=sys.stderr,
        )
        return 2
    try:
        return run(arguments)
    except (
        BrokerAttemptStoreError,
        ConfigurationError,
        DecisionStoreError,
        ExecutionDomainError,
        ExecutionPositionFeedError,
        ExecutionStoreError,
        NewRiskWindowError,
        PaperDrillPreparationError,
        PositionProjectionError,
        ValueError,
    ) as exc:
        print(
            f"paper drill preparation failed: {type(exc).__name__}: {exc}",
            file=sys.stderr,
        )
        return 2
    except KeyboardInterrupt:
        return 130


if __name__ == "__main__":
    raise SystemExit(main())
