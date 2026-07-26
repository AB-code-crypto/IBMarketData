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
    resolve_session,
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
    ProtectionSchemaError,
    SQLiteBrokerAttemptStore,
    SQLiteExecutionPositionFeedReader,
    SQLiteExecutionStateReader,
    SQLiteExecutionStore,
    SQLiteProtectionReader,
)
from ibmd.execution.adapters.sqlite_liquidation import (
    LiquidationSchemaError,
    LiquidationStoreError,
    SQLiteLiquidationStore,
)
from ibmd.execution.application.new_risk_window import (
    NewRiskWindowError,
    NewRiskWindowV1,
)
from ibmd.execution.application.paper_reverse_drill import (
    PaperReverseDrillPolicyV1,
    PaperReverseDrillPreparationError,
    PaperReverseExecutionDrillPreparer,
)
from ibmd.execution.domain import RegisteredFuturesContractV1
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
            "Prepare one short-lived broker-free paper REVERSE command from a "
            "fresh broker-proven protected OPEN position. The tool writes only "
            "decision/execution state and never connects to IB."
        )
    )
    parser.add_argument("--validate-store-only", action="store_true")
    parser.add_argument("--prepare", action="store_true")
    parser.add_argument("--drill-id", default=None)
    parser.add_argument(
        "--target-side",
        choices=("LONG", "SHORT"),
        default=None,
    )
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
    parser.add_argument("--position-max-age-seconds", type=float, default=30.0)
    parser.add_argument("--command-ttl-seconds", type=int, default=600)
    return parser


def _paths(arguments: argparse.Namespace, data_root: Path):
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
    protection_source = SQLiteProtectionReader(execution_database)
    liquidation_source = SQLiteLiquidationStore(execution_database)
    position_source = SQLiteExecutionPositionFeedReader(position_feed_database)
    decision_store.validate_schema()
    execution_store.validate_schema()
    execution_state.validate_schema()
    attempt_store.validate_schema()
    protection_source.validate_schema()
    liquidation_source.validate_schema()
    position_source.validate_schema()
    if arguments.validate_store_only:
        print(
            "paper reverse drill dependencies are compatible: "
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
    strategy_policy = bundle.strategy_policy.require(instrument_id)
    if instrument.sec_type != "FUT":
        raise ValueError("paper reverse drill currently supports FUT only")

    observed = utc_now()
    observed_text = format_utc(observed)
    session_id = strategy_policy.daily_flat.session_id
    session = bundle.session_calendar.require(session_id)
    NewRiskWindowV1(
        enabled=strategy_policy.daily_flat.enabled,
        timezone_name=session.timezone,
        liquidation_start_local=(
            strategy_policy.daily_flat.liquidation_start_local
        ),
        risk_blocked_until_local=(
            strategy_policy.daily_flat.risk_blocked_until_local
        ),
    ).require_allows_new_risk(
        observed_at_utc=observed_text,
        lead_seconds=60,
    )
    session_resolution = resolve_session(
        bundle.session_calendar,
        session_id=session_id,
        at_utc=observed,
    )
    if not session_resolution.is_trading_open:
        raise PaperReverseDrillPreparationError(
            "paper reverse drill requires an open catalog session: "
            f"phase={session_resolution.phase.value}, "
            f"local={session_resolution.local_date} "
            f"{session_resolution.local_time}, "
            f"reason={session_resolution.reason}"
        )
    resolution = resolve_active_contract(bundle.contract_calendar, observed)
    if (
        resolution.status != ActiveContractStatus.ACTIVE
        or resolution.contract is None
    ):
        raise PaperReverseDrillPreparationError(
            "paper reverse drill requires one active contract; "
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
    active_contract = next(item for item in registry if item.contract_is_active)
    preparer = PaperReverseExecutionDrillPreparer(
        policy=PaperReverseDrillPolicyV1(
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
            target_quantity=strategy_policy.target_quantity,
            command_ttl_seconds=int(arguments.command_ttl_seconds),
            position_max_age_seconds=float(
                arguments.position_max_age_seconds
            ),
            active_contract=active_contract,
        ),
        decision_repository=decision_store,
        execution_repository=execution_store,
        execution_state_source=execution_state,
        protection_source=protection_source,
        position_snapshot_source=position_source,
        broker_attempt_source=attempt_store,
        liquidation_source=liquidation_source,
        contract_registry=registry,
    )
    with ServiceProcessLock(
        settings.paths_for(SERVICE_DECISION).lock_file,
        service_name=SERVICE_DECISION,
        deployment_id=settings.deployment_id,
        instance_id=new_id("instance"),
    ):
        with ServiceProcessLock(
            settings.paths_for(SERVICE_EXECUTION).lock_file,
            service_name=SERVICE_EXECUTION,
            deployment_id=settings.deployment_id,
            instance_id=new_id("instance"),
        ):
            result = preparer.prepare(observed_at_utc=observed_text)
    payload = result.to_dict()
    payload["session"] = {
        "session_id": session_resolution.session_id,
        "phase": session_resolution.phase.value,
        "local_date": session_resolution.local_date,
        "local_time": session_resolution.local_time,
        "reason": session_resolution.reason,
        "production_qualified": session_resolution.production_qualified,
    }
    payload["databases"] = {
        "decision": str(decision_database),
        "execution": str(execution_database),
        "position_feed": str(position_feed_database),
    }
    payload["next_step"] = {
        "entrypoint": str(ROOT / "apps" / "run_execution_reverse_handoff_v2.py"),
        "once_command_id": result.command.command_id,
        "confirm_paper_account": settings.ib_account_id,
        "warning": (
            "No broker action has been performed. The next stage must cancel "
            "the current episode's exits before REVERSE submission."
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
            "paper reverse drill requires exactly one mode: "
            "--validate-store-only or --prepare",
            file=sys.stderr,
        )
        return 2
    try:
        return run(arguments)
    except (
        BrokerAttemptSchemaError,
        BrokerAttemptStoreError,
        ConfigurationError,
        DecisionSchemaError,
        DecisionStoreError,
        ExecutionPositionFeedError,
        ExecutionSchemaError,
        ExecutionStateReadError,
        ExecutionStoreError,
        LiquidationSchemaError,
        LiquidationStoreError,
        NewRiskWindowError,
        PaperReverseDrillPreparationError,
        ProtectionSchemaError,
        ValueError,
    ) as exc:
        print(
            "paper reverse drill preparation failed: "
            f"{type(exc).__name__}: {exc}",
            file=sys.stderr,
        )
        return 2
    except KeyboardInterrupt:
        return 130


if __name__ == "__main__":
    raise SystemExit(main())
