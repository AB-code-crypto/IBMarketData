from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import os
import sys
from pathlib import Path
from zoneinfo import ZoneInfo

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from ibmd.catalog import (
    ActiveContractStatus,
    load_catalog_bundle,
    resolve_active_contract,
)
from ibmd.execution.adapters import (
    SQLiteBrokerAttemptStore,
    SQLiteBrokerReconciliationReader,
    SQLiteBrokerReconciliationStore,
    SQLiteExecutionDecisionReader,
    SQLiteExecutionPositionFeedReader,
    SQLiteExecutionStateReader,
    SQLiteExecutionStore,
    SQLiteLiquidationStore,
    SQLiteProtectionReader,
    SQLiteProtectiveLifecycleStore,
)
from ibmd.execution.adapters.sqlite_daily_risk_sources import (
    SQLiteDailyRiskExecutionReader,
    SQLiteDailyRiskMarketDataReader,
)
from ibmd.execution.adapters.sqlite_daily_risk_store import SQLiteDailyRiskStore
from ibmd.execution.adapters.sqlite_liquidation_triggers import (
    SQLiteLiquidationTriggerReader,
)
from ibmd.execution.adapters.sqlite_reverse_finalization import (
    SQLiteReverseFinalizationStore,
)
from ibmd.execution.adapters.sqlite_runtime import SQLiteExecutionRuntimeReader
from ibmd.execution.application.daily_risk import DailyRiskService
from ibmd.execution.application.liquidation_triggers import (
    LiquidationTriggerProducerPolicyV1,
    LiquidationTriggerProducerService,
)
from ibmd.execution.application.protection import PositionEpisodeProtectionService
from ibmd.execution.application.protective_lifecycle import (
    ProtectiveLifecycleService,
)
from ibmd.execution.application.read_only_reconciliation import (
    ReadOnlyBrokerReconciliationService,
)
from ibmd.execution.application.runtime import (
    EXECUTION_RUNTIME_STAGE_ORDER,
    ExecutionRuntimeCoordinator,
    ExecutionRuntimeStage,
    ExecutionRuntimeStageResultV1,
    ExecutionRuntimeTickStatus,
)
from ibmd.execution.application.runtime_steps import (
    CallableExecutionRuntimeStage,
    DisabledMutationExecutionRuntimeStage,
)
from ibmd.execution.application.reverse_finalization import (
    ReverseFinalizationService,
)
from ibmd.execution.domain import (
    ExecutionFoundationFixtureV1,
    ExecutionFoundationPolicyV1,
    PositionProjectionPolicyV1,
    ProtectionPlanningPolicyV1,
    ProtectiveLifecyclePolicyV1,
    RegisteredFuturesContractV1,
    admit_strategy_command,
    merge_position_projection_readiness,
    project_strategy_position,
)
from ibmd.execution.domain.daily_risk import DailyRiskPolicyV1
from ibmd.execution.domain.reverse_finalization import (
    ReverseFinalizationPolicyV1,
)
from ibmd.execution.domain.reverse_handoff import (
    ReverseHandoffAction,
    assess_reverse_handoff,
)
from ibmd.foundation.atomic_json import canonical_json_text
from ibmd.foundation.config import load_deployment_settings
from ibmd.foundation.identity import new_id
from ibmd.foundation.process_lock import ServiceProcessLock
from ibmd.foundation.time import format_utc, parse_utc, utc_now
from ibmd.ib_gateway.ib_async_broker_reconciliation import (
    IBAsyncBrokerReconciliationReader,
    IBBrokerReconciliationConnectionSettings,
)
from ibmd.operations.health import ServiceHealthFile
from ibmd.public_contracts.execution import (
    DailyRiskCleanupStatus,
    DailyRiskStateV1,
    DailyRiskStatus,
)
from ibmd.public_contracts.health import (
    DependencyStatusV1,
    Liveness,
    Readiness,
    ServiceHealthV1,
)
from ibmd.public_contracts.protection import PositionEpisodePolicyV1

SERVICE_NAME = "execution"


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Run the single-owner execution control loop in broker-safe mode. "
            "The loop performs read-only reconciliation and broker-free state "
            "transitions, while every broker-mutating stage remains disabled "
            "until the controlled paper acceptance gate is completed."
        )
    )
    parser.add_argument("--validate-store-only", action="store_true")
    parser.add_argument("--once", action="store_true")
    parser.add_argument("--continuous", action="store_true")
    parser.add_argument("--decision-database", type=Path, default=None)
    parser.add_argument("--execution-database", type=Path, default=None)
    parser.add_argument("--position-feed-database", type=Path, default=None)
    parser.add_argument("--market-database", type=Path, default=None)
    parser.add_argument(
        "--catalog-root",
        type=Path,
        default=ROOT / "catalog",
    )
    parser.add_argument("--instrument", default="MNQ")
    parser.add_argument("--poll-interval-seconds", type=float, default=1.0)
    parser.add_argument("--position-max-age-seconds", type=float, default=10.0)
    parser.add_argument("--market-max-age-seconds", type=float, default=None)
    parser.add_argument("--missing-stop-grace-seconds", type=float, default=30.0)
    parser.add_argument("--reconciliation-client-id-offset", type=int, default=100)
    parser.add_argument("--connect-timeout-seconds", type=float, default=15.0)
    parser.add_argument("--request-timeout-seconds", type=float, default=15.0)
    parser.add_argument("--commission-wait-seconds", type=float, default=2.0)
    parser.add_argument(
        "--allow-unqualified-session",
        action="store_true",
        help="Development/paper override for daily-flat trigger evaluation.",
    )
    return parser


def _configuration_hash(
    *,
    settings,
    bundle,
    paths: tuple[Path, ...],
    poll_interval_seconds: float,
) -> str:
    payload = {
        "deployment_hash": settings.configuration_hash,
        "catalog_hash": bundle.bundle_hash,
        "paths": [str(item) for item in paths],
        "poll_interval_seconds": float(poll_interval_seconds),
        "broker_mutations_enabled": False,
        "runtime_stage_order": [item.value for item in EXECUTION_RUNTIME_STAGE_ORDER],
    }
    return hashlib.sha256(
        canonical_json_text(payload).encode("utf-8")
    ).hexdigest()


def _protective_policy(instrument, strategy_policy) -> PositionEpisodePolicyV1:
    protective = strategy_policy.protective
    return PositionEpisodePolicyV1(
        price_tick=instrument.price_tick,
        stop_required=protective.stop_required,
        take_profit_enabled=protective.take_profit_enabled,
        stop_loss_points=protective.stop_loss_points,
        take_profit_points=protective.take_profit_points,
        time_in_force=protective.time_in_force,
        stop_outside_rth=protective.stop_outside_rth,
        take_profit_outside_rth=protective.take_profit_outside_rth,
        price_watchdog_enabled=protective.price_watchdog_enabled,
        stale_feed_market_close_enabled=(
            protective.stale_feed_market_close_enabled
        ),
        price_stale_max_seconds=protective.price_stale_max_seconds,
    )


def _safe_daily_risk(
    *,
    account_id: str,
    strategy_id: str,
    deployment_id: str,
    timezone_name: str,
    target_pnl: float,
    observed_at_utc: str,
) -> DailyRiskStateV1:
    day = (
        parse_utc(observed_at_utc)
        .astimezone(ZoneInfo(timezone_name))
        .date()
        .isoformat()
    )
    return DailyRiskStateV1(
        account_id=account_id,
        strategy_id=strategy_id,
        deployment_id=deployment_id,
        trading_day=day,
        status=DailyRiskStatus.NOT_READY,
        realized_pnl=None,
        unrealized_pnl=None,
        total_pnl=None,
        target_pnl=target_pnl,
        pnl_ready=False,
        cleanup_status=DailyRiskCleanupStatus.NOT_REQUIRED,
        updated_at_utc=observed_at_utc,
    )


async def run(arguments: argparse.Namespace) -> int:
    settings = load_deployment_settings()
    bundle = load_catalog_bundle(
        arguments.catalog_root.resolve(),
        require_production_sessions=False,
    )
    instrument_id = str(arguments.instrument or "").strip()
    instrument = bundle.instrument_master.require(instrument_id)
    strategy_policy = bundle.strategy_policy.require(instrument_id)
    if instrument.sec_type != "FUT":
        raise ValueError("execution runtime currently supports futures only")
    if not strategy_policy.trading_enabled:
        raise ValueError(f"strategy trading is disabled for {instrument_id}")

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
        else settings.data_root / "position_feed" / "broker_positions.sqlite3"
    )
    market_database = (
        arguments.market_database.resolve()
        if arguments.market_database is not None
        else settings.data_root / "market_data" / instrument.database_name
    )

    execution_store = SQLiteExecutionStore(execution_database)
    execution_state = SQLiteExecutionStateReader(execution_database)
    decision_source = SQLiteExecutionDecisionReader(decision_database)
    position_source = SQLiteExecutionPositionFeedReader(position_feed_database)
    attempt_store = SQLiteBrokerAttemptStore(execution_database)
    reconciliation_store = SQLiteBrokerReconciliationStore(execution_database)
    fill_source = SQLiteBrokerReconciliationReader(execution_database)
    protection_reader = SQLiteProtectionReader(execution_database)
    lifecycle_store = SQLiteProtectiveLifecycleStore(execution_database)
    liquidation_store = SQLiteLiquidationStore(execution_database)
    reverse_store = SQLiteReverseFinalizationStore(execution_database)
    daily_evidence = SQLiteDailyRiskExecutionReader(execution_database)
    daily_store = SQLiteDailyRiskStore(execution_database)
    trigger_source = SQLiteLiquidationTriggerReader(execution_database)
    runtime_reader = SQLiteExecutionRuntimeReader(
        execution_database,
        decision_database,
    )
    market_source = SQLiteDailyRiskMarketDataReader(
        market_database,
        instrument_id=instrument.instrument_id,
        price_precision=instrument.price_precision,
    )

    validators = (
        execution_store,
        execution_state,
        decision_source,
        position_source,
        attempt_store,
        reconciliation_store,
        fill_source,
        protection_reader,
        lifecycle_store,
        liquidation_store,
        reverse_store,
        daily_evidence,
        daily_store,
        trigger_source,
        runtime_reader,
        market_source,
    )
    for value in validators:
        value.validate_schema()

    if arguments.validate_store_only:
        print(
            json.dumps(
                {
                    "execution_runtime_dependencies_compatible": True,
                    "execution_database": str(execution_database),
                    "decision_database": str(decision_database),
                    "position_feed_database": str(position_feed_database),
                    "market_database": str(market_database),
                    "broker_mutations_enabled": False,
                    "stage_order": [
                        item.value for item in EXECUTION_RUNTIME_STAGE_ORDER
                    ],
                },
                ensure_ascii=False,
                sort_keys=True,
                indent=2,
            )
        )
        return 0

    interval = float(arguments.poll_interval_seconds)
    if interval <= 0.0:
        raise ValueError("poll_interval_seconds must be positive")
    position_max_age = float(arguments.position_max_age_seconds)
    if position_max_age <= 0.0:
        raise ValueError("position_max_age_seconds must be positive")
    market_max_age = (
        float(arguments.market_max_age_seconds)
        if arguments.market_max_age_seconds is not None
        else float(strategy_policy.signal.max_complete_bar_lag_seconds)
    )

    account_id = settings.ib_account_id
    strategy_id = bundle.strategy_policy.strategy_id
    strategy_version = bundle.strategy_policy.strategy_version
    deployment_id = settings.deployment_id
    scope = {
        "account_id": account_id,
        "strategy_id": strategy_id,
        "deployment_id": deployment_id,
        "instrument_id": instrument.instrument_id,
    }
    protective_policy = _protective_policy(instrument, strategy_policy)
    foundation_policy = ExecutionFoundationPolicyV1(
        account_id=account_id,
        strategy_id=strategy_id,
        strategy_version=strategy_version,
        deployment_id=deployment_id,
        instrument_id=instrument.instrument_id,
        policy_hash=bundle.strategy_policy.content_hash,
    )
    projection_policy = PositionProjectionPolicyV1(
        account_id=account_id,
        strategy_id=strategy_id,
        deployment_id=deployment_id,
        instrument_id=instrument.instrument_id,
        max_snapshot_age_seconds=position_max_age,
    )

    reconciliation_client_id = (
        settings.ib_client_id + int(arguments.reconciliation_client_id_offset)
    )
    if reconciliation_client_id < 0:
        raise ValueError("resolved reconciliation client id must be non-negative")
    timeout = float(arguments.request_timeout_seconds)
    broker_reader = IBAsyncBrokerReconciliationReader(
        IBBrokerReconciliationConnectionSettings(
            host=settings.ib_host,
            port=settings.ib_port,
            client_id=reconciliation_client_id,
            account_id=account_id,
            connect_timeout_seconds=float(arguments.connect_timeout_seconds),
            open_orders_timeout_seconds=timeout,
            completed_orders_timeout_seconds=timeout,
            executions_timeout_seconds=timeout,
            commission_wait_seconds=float(arguments.commission_wait_seconds),
        )
    )

    strategic_reconciliation = ReadOnlyBrokerReconciliationService(
        account_id=account_id,
        broker_source=broker_reader,
        attempt_source=attempt_store,
        reconciliation_store=reconciliation_store,
    )
    protective_lifecycle = ProtectiveLifecycleService(
        policy=ProtectiveLifecyclePolicyV1(
            account_id=account_id,
            strategy_id=strategy_id,
            strategy_version=strategy_version,
            deployment_id=deployment_id,
            instrument_id=instrument.instrument_id,
            position_max_age_seconds=position_max_age,
        ),
        protection_source=protection_reader,
        execution_state_source=execution_state,
        position_snapshot_source=position_source,
        broker_snapshot_source=broker_reader,
        repository=lifecycle_store,
    )
    open_finalizer = PositionEpisodeProtectionService(
        policy=ProtectionPlanningPolicyV1(
            account_id=account_id,
            strategy_id=strategy_id,
            strategy_version=strategy_version,
            deployment_id=deployment_id,
            instrument_id=instrument.instrument_id,
            strategy_policy_hash=bundle.strategy_policy.content_hash,
            position_max_age_seconds=position_max_age,
            protective_policy=protective_policy,
        ),
        operation_source=attempt_store,
        command_source=execution_store,
        fill_source=fill_source,
        position_snapshot_source=position_source,
        execution_state_source=execution_state,
        protection_repository=lifecycle_store,
    )
    reverse_finalizer = ReverseFinalizationService(
        policy=ReverseFinalizationPolicyV1(
            account_id=account_id,
            strategy_id=strategy_id,
            strategy_version=strategy_version,
            deployment_id=deployment_id,
            instrument_id=instrument.instrument_id,
            strategy_policy_hash=bundle.strategy_policy.content_hash,
            position_max_age_seconds=position_max_age,
            protective_policy=protective_policy,
        ),
        operation_source=attempt_store,
        command_state_source=execution_store,
        fill_source=fill_source,
        position_snapshot_source=position_source,
        execution_state_source=execution_state,
        protection_state_source=protection_reader,
        liquidation_state_source=liquidation_store,
        repository=reverse_store,
    )
    daily_risk = DailyRiskService(
        policy=DailyRiskPolicyV1(
            account_id=account_id,
            strategy_id=strategy_id,
            strategy_version=strategy_version,
            deployment_id=deployment_id,
            instrument_id=instrument.instrument_id,
            timezone_name=strategy_policy.daily_pnl.timezone,
            target_pnl=strategy_policy.daily_pnl.target_usd,
            contract_multiplier=instrument.multiplier,
            market_max_age_seconds=market_max_age,
        ),
        execution_state_source=execution_state,
        episode_source=protection_reader,
        owned_fill_source=daily_evidence,
        market_mark_source=market_source,
        repository=daily_store,
    )
    trigger_service = LiquidationTriggerProducerService(
        policy=LiquidationTriggerProducerPolicyV1(
            account_id=account_id,
            strategy_id=strategy_id,
            strategy_version=strategy_version,
            deployment_id=deployment_id,
            instrument_id=instrument.instrument_id,
            missing_stop_grace_seconds=float(
                arguments.missing_stop_grace_seconds
            ),
            require_production_session=(
                not bool(arguments.allow_unqualified_session)
            ),
        ),
        bundle=bundle,
        state_source=trigger_source,
        repository=liquidation_store,
    )

    async def strategic_reconciliation_step(
        observed_at_utc: str,
    ) -> ExecutionRuntimeStageResultV1:
        candidates = attempt_store.read_unresolved()
        pending_commissions = (
            reconciliation_store.read_commission_pending_operation_ids()
        )
        if not candidates and not pending_commissions:
            return ExecutionRuntimeStageResultV1.no_action(
                ExecutionRuntimeStage.STRATEGIC_RECONCILIATION,
                observed_at_utc=observed_at_utc,
            )
        result = await strategic_reconciliation.run_once()
        return ExecutionRuntimeStageResultV1.updated(
            ExecutionRuntimeStage.STRATEGIC_RECONCILIATION,
            observed_at_utc=observed_at_utc,
            subject_id=(
                result.reconciled[0].after.operation.operation_id
                if result.reconciled
                else None
            ),
            detail=(
                f"reconciled={len(result.reconciled)}, "
                f"skipped={len(result.skipped_operation_ids)}"
            ),
        )

    async def protective_reconciliation_step(
        observed_at_utc: str,
    ) -> ExecutionRuntimeStageResultV1:
        episode_ids = runtime_reader.list_open_episode_ids(**scope)
        if not episode_ids:
            return ExecutionRuntimeStageResultV1.no_action(
                ExecutionRuntimeStage.PROTECTIVE_RECONCILIATION,
                observed_at_utc=observed_at_utc,
            )
        if len(episode_ids) != 1:
            return ExecutionRuntimeStageResultV1.blocked(
                ExecutionRuntimeStage.PROTECTIVE_RECONCILIATION,
                observed_at_utc=observed_at_utc,
                subject_id=episode_ids[0],
                detail=f"multiple open position episodes: {episode_ids}",
            )
        update = await protective_lifecycle.run_once(
            position_episode_id=episode_ids[0],
            observed_at_utc=observed_at_utc,
        )
        if not update.evidence and not update.episode_closed:
            return ExecutionRuntimeStageResultV1.no_action(
                ExecutionRuntimeStage.PROTECTIVE_RECONCILIATION,
                observed_at_utc=observed_at_utc,
                detail="no protective broker evidence changed",
            )
        return ExecutionRuntimeStageResultV1.updated(
            ExecutionRuntimeStage.PROTECTIVE_RECONCILIATION,
            observed_at_utc=observed_at_utc,
            subject_id=episode_ids[0],
            detail=(
                f"evidence={len(update.evidence)}, "
                f"episode_closed={update.episode_closed}"
            ),
        )

    def liquidation_pending(_observed_at_utc: str):
        value = runtime_reader.read_active_liquidation(**scope)
        return (
            (None, None)
            if value is None
            else (value.subject_id, value.detail)
        )

    def finalization_step(observed_at_utc: str) -> ExecutionRuntimeStageResultV1:
        candidate = runtime_reader.read_next_finalization_candidate(**scope)
        if candidate is None:
            return ExecutionRuntimeStageResultV1.no_action(
                ExecutionRuntimeStage.POSITION_FINALIZATION,
                observed_at_utc=observed_at_utc,
            )
        if candidate.command_kind.value == "REVERSE":
            result = reverse_finalizer.finalize_from_operation(
                operation_id=candidate.operation_id,
                observed_at_utc=observed_at_utc,
            )
            detail = (
                f"reverse_created={result.finalization_created}, "
                f"commission_refreshed={result.commission_completion_refreshed}"
            )
        else:
            plan = open_finalizer.plan_from_operation(
                operation_id=candidate.operation_id,
                observed_at_utc=observed_at_utc,
            )
            detail = f"episode={plan.episode.position_episode_id}"
        return ExecutionRuntimeStageResultV1.updated(
            ExecutionRuntimeStage.POSITION_FINALIZATION,
            observed_at_utc=observed_at_utc,
            subject_id=candidate.operation_id,
            detail=detail,
        )

    def projection_step(observed_at_utc: str) -> ExecutionRuntimeStageResultV1:
        resolution = resolve_active_contract(
            bundle.contract_calendar,
            parse_utc(observed_at_utc),
        )
        active_con_id = (
            resolution.contract.con_id
            if resolution.status == ActiveContractStatus.ACTIVE
            and resolution.contract is not None
            else None
        )
        registry = tuple(
            RegisteredFuturesContractV1(
                con_id=item.con_id,
                local_symbol=item.local_symbol,
                contract_is_active=(item.con_id == active_con_id),
            )
            for item in bundle.contract_calendar.contracts
        )
        previous_position = execution_state.read_position(**scope)
        previous_readiness = execution_state.read_readiness(**scope)
        snapshot = position_source.read_latest_complete()
        projection = project_strategy_position(
            snapshot=snapshot,
            previous=previous_position,
            policy=projection_policy,
            registry=registry,
            observed_at_utc=observed_at_utc,
            active_contract_available=(active_con_id is not None),
        )
        readiness = merge_position_projection_readiness(
            previous=previous_readiness,
            projection=projection,
            policy=projection_policy,
            observed_at_utc=observed_at_utc,
        )
        risk = execution_state.read_latest_daily_risk(
            account_id=account_id,
            strategy_id=strategy_id,
            deployment_id=deployment_id,
        )
        if risk is None:
            risk = _safe_daily_risk(
                account_id=account_id,
                strategy_id=strategy_id,
                deployment_id=deployment_id,
                timezone_name=strategy_policy.daily_pnl.timezone,
                target_pnl=strategy_policy.daily_pnl.target_usd,
                observed_at_utc=observed_at_utc,
            )
        changed = (
            previous_position is None
            or previous_readiness is None
            or previous_position.to_dict() != projection.position.to_dict()
            or previous_readiness.to_dict() != readiness.to_dict()
        )
        execution_store.publish_fixture(
            ExecutionFoundationFixtureV1(
                observed_at_utc=observed_at_utc,
                readiness=readiness,
                position=projection.position,
                daily_risk=risk,
            )
        )
        if not changed:
            return ExecutionRuntimeStageResultV1.no_action(
                ExecutionRuntimeStage.POSITION_PROJECTION,
                observed_at_utc=observed_at_utc,
            )
        return ExecutionRuntimeStageResultV1.updated(
            ExecutionRuntimeStage.POSITION_PROJECTION,
            observed_at_utc=observed_at_utc,
            subject_id=projection.position.position_episode_id,
            detail=(
                f"projection={projection.position.projection_status.value}, "
                f"active_contract={active_con_id}"
            ),
        )

    def daily_risk_step(observed_at_utc: str) -> ExecutionRuntimeStageResultV1:
        result = daily_risk.run_once(observed_at_utc=observed_at_utc)
        return ExecutionRuntimeStageResultV1.updated(
            ExecutionRuntimeStage.DAILY_RISK,
            observed_at_utc=observed_at_utc,
            subject_id=result.update.calculation.calculation_id,
            detail=(
                f"status={result.update.state.status.value}, "
                f"pnl_ready={result.update.state.pnl_ready}"
            ),
        )

    def trigger_step(observed_at_utc: str) -> ExecutionRuntimeStageResultV1:
        result = trigger_service.run_once(observed_at_utc=observed_at_utc)
        if not result.trigger_created_count and not result.operation_created_count:
            return ExecutionRuntimeStageResultV1.no_action(
                ExecutionRuntimeStage.LIQUIDATION_TRIGGERS,
                observed_at_utc=observed_at_utc,
                detail=f"candidates={result.candidate_count}",
            )
        return ExecutionRuntimeStageResultV1.updated(
            ExecutionRuntimeStage.LIQUIDATION_TRIGGERS,
            observed_at_utc=observed_at_utc,
            detail=(
                f"operations_created={result.operation_created_count}, "
                f"triggers_created={result.trigger_created_count}"
            ),
        )

    def reverse_handoff_pending(_observed_at_utc: str):
        pending = runtime_reader.read_pending_reverse_handoff(
            strategy_id=strategy_id,
            deployment_id=deployment_id,
            instrument_id=instrument.instrument_id,
        )
        if pending is None:
            return None, None
        position = execution_state.read_position(**scope)
        if position is None or position.position_episode_id is None:
            return pending.subject_id, "source position episode is unavailable"
        protection = protection_reader.read_protection_by_episode(
            position.position_episode_id
        )
        if protection is None:
            return pending.subject_id, "source protection state is unavailable"
        assessment = assess_reverse_handoff(protection)
        if assessment.action == ReverseHandoffAction.READY_TO_SUBMIT:
            return None, None
        return (
            pending.subject_id,
            f"action={assessment.action.value}, reason={assessment.blocking_reason}",
        )

    def protective_submission_pending(_observed_at_utc: str):
        value = runtime_reader.read_pending_protective_submission(**scope)
        return (
            (None, None)
            if value is None
            else (value.subject_id, value.detail)
        )

    def command_admission_step(
        observed_at_utc: str,
    ) -> ExecutionRuntimeStageResultV1:
        command_id = runtime_reader.read_next_decision_command_id(
            strategy_id=strategy_id,
            strategy_version=strategy_version,
            deployment_id=deployment_id,
            instrument_id=instrument.instrument_id,
        )
        if command_id is None:
            return ExecutionRuntimeStageResultV1.no_action(
                ExecutionRuntimeStage.COMMAND_ADMISSION,
                observed_at_utc=observed_at_utc,
            )
        command = decision_source.read_command(command_id)
        if command is None:
            raise RuntimeError(f"decision command disappeared: {command_id}")
        position = execution_state.read_position(**scope)
        readiness = execution_state.read_readiness(**scope)
        risk = execution_state.read_latest_daily_risk(
            account_id=account_id,
            strategy_id=strategy_id,
            deployment_id=deployment_id,
        )
        if position is None or readiness is None or risk is None:
            raise RuntimeError(
                "execution position/readiness/daily-risk is incomplete for admission"
            )
        fixture = ExecutionFoundationFixtureV1(
            observed_at_utc=observed_at_utc,
            readiness=readiness,
            position=position,
            daily_risk=risk,
        )
        admission = admit_strategy_command(
            command=command,
            policy=foundation_policy,
            fixture=fixture,
        )
        state = execution_store.publish_admission(admission)
        return ExecutionRuntimeStageResultV1.updated(
            ExecutionRuntimeStage.COMMAND_ADMISSION,
            observed_at_utc=observed_at_utc,
            subject_id=command_id,
            detail=f"state={state.state.value}, reason={state.blocking_reason}",
        )

    def strategic_submission_pending(_observed_at_utc: str):
        value = runtime_reader.read_pending_strategic_submission(
            strategy_id=strategy_id,
            deployment_id=deployment_id,
            instrument_id=instrument.instrument_id,
        )
        return (
            (None, None)
            if value is None
            else (value.subject_id, value.detail)
        )

    stages = (
        CallableExecutionRuntimeStage(
            ExecutionRuntimeStage.STRATEGIC_RECONCILIATION,
            strategic_reconciliation_step,
        ),
        CallableExecutionRuntimeStage(
            ExecutionRuntimeStage.PROTECTIVE_RECONCILIATION,
            protective_reconciliation_step,
        ),
        DisabledMutationExecutionRuntimeStage(
            ExecutionRuntimeStage.LIQUIDATION_ADVANCE,
            pending=liquidation_pending,
            disabled_reason=(
                "continuous liquidation broker mutations are disabled until "
                "the paper acceptance gate passes"
            ),
        ),
        CallableExecutionRuntimeStage(
            ExecutionRuntimeStage.POSITION_FINALIZATION,
            finalization_step,
        ),
        CallableExecutionRuntimeStage(
            ExecutionRuntimeStage.POSITION_PROJECTION,
            projection_step,
        ),
        CallableExecutionRuntimeStage(
            ExecutionRuntimeStage.DAILY_RISK,
            daily_risk_step,
        ),
        CallableExecutionRuntimeStage(
            ExecutionRuntimeStage.LIQUIDATION_TRIGGERS,
            trigger_step,
        ),
        DisabledMutationExecutionRuntimeStage(
            ExecutionRuntimeStage.REVERSE_HANDOFF,
            pending=reverse_handoff_pending,
            disabled_reason=(
                "continuous reverse-handoff cancellation is disabled until "
                "the paper acceptance gate passes"
            ),
        ),
        DisabledMutationExecutionRuntimeStage(
            ExecutionRuntimeStage.PROTECTIVE_SUBMISSION,
            pending=protective_submission_pending,
            disabled_reason=(
                "continuous protective submission is disabled until the paper "
                "acceptance gate passes"
            ),
        ),
        CallableExecutionRuntimeStage(
            ExecutionRuntimeStage.COMMAND_ADMISSION,
            command_admission_step,
        ),
        DisabledMutationExecutionRuntimeStage(
            ExecutionRuntimeStage.STRATEGIC_SUBMISSION,
            pending=strategic_submission_pending,
            disabled_reason=(
                "continuous strategic submission is disabled until the paper "
                "acceptance gate passes"
            ),
        ),
    )
    coordinator = ExecutionRuntimeCoordinator(
        stages=stages,
        broker_mutations_enabled=False,
    )

    instance_id = new_id("instance")
    config_hash = _configuration_hash(
        settings=settings,
        bundle=bundle,
        paths=(
            execution_database,
            decision_database,
            position_feed_database,
            market_database,
        ),
        poll_interval_seconds=interval,
    )
    health_file = ServiceHealthFile(
        settings.paths_for(SERVICE_NAME).health_file,
        expected_service=SERVICE_NAME,
    )
    health = ServiceHealthV1.starting(
        service=SERVICE_NAME,
        deployment_id=deployment_id,
        instance_id=instance_id,
        pid=os.getpid(),
        application_version=settings.application_version,
        configuration_hash=config_hash,
    )

    async def one_tick():
        nonlocal health
        tick = await coordinator.run_tick()
        last = tick.results[-1] if tick.results else None
        if tick.status == ExecutionRuntimeTickStatus.FAILED:
            readiness = Readiness.BLOCKED
            blocking = None if last is None else last.detail
        elif tick.status == ExecutionRuntimeTickStatus.BLOCKED:
            readiness = Readiness.DEGRADED
            blocking = None if last is None else last.detail
        else:
            readiness = Readiness.READY
            blocking = None
        observed = tick.finished_at_utc
        health = health.heartbeat(
            now_utc=observed,
            liveness=Liveness.RUNNING,
            readiness=readiness,
            last_success_at_utc=(
                health.last_success_at_utc
                if tick.status == ExecutionRuntimeTickStatus.FAILED
                else observed
            ),
            dependency_status=tuple(
                DependencyStatusV1(
                    name=item.stage.value.lower(),
                    status=item.status.value,
                    detail=item.detail,
                    observed_at_utc=item.observed_at_utc,
                )
                for item in tick.results
            ),
            blocking_reason=blocking,
        )
        health_file.publish(health)
        return tick

    try:
        with ServiceProcessLock(
            settings.paths_for(SERVICE_NAME).lock_file,
            service_name=SERVICE_NAME,
            deployment_id=deployment_id,
            instance_id=instance_id,
        ):
            health_file.publish(health)
            if arguments.once:
                tick = await one_tick()
                print(
                    json.dumps(
                        tick.to_dict(),
                        ensure_ascii=False,
                        sort_keys=True,
                        indent=2,
                    )
                )
                return 0 if tick.status != ExecutionRuntimeTickStatus.FAILED else 2
            while True:
                tick = await one_tick()
                print(
                    json.dumps(
                        tick.to_dict(),
                        ensure_ascii=False,
                        sort_keys=True,
                    ),
                    flush=True,
                )
                await asyncio.sleep(interval)
    finally:
        await broker_reader.close()


def main(argv: list[str] | None = None) -> int:
    arguments = build_parser().parse_args(argv)
    selected = sum(
        int(value)
        for value in (
            arguments.validate_store_only,
            arguments.once,
            arguments.continuous,
        )
    )
    if selected != 1:
        print(
            "execution runtime requires exactly one mode: "
            "--validate-store-only, --once or --continuous",
            file=sys.stderr,
        )
        return 2
    try:
        return asyncio.run(run(arguments))
    except KeyboardInterrupt:
        return 130
    except Exception as exc:
        print(
            f"execution runtime failed: {type(exc).__name__}: {exc}",
            file=sys.stderr,
        )
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
