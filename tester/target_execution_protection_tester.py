from __future__ import annotations

import tempfile
import unittest
from dataclasses import replace
from pathlib import Path

from ibmd.execution.adapters import SQLiteProtectionStore
from ibmd.execution.domain import (
    apply_broker_observation,
    begin_reconciliation,
    mark_attempt_submitting,
    plan_broker_operation,
)
from ibmd.execution.domain.position_projection import RegisteredFuturesContractV1
from ibmd.execution.domain.protection import (
    ProtectionPlanningError,
    ProtectionPlanningPolicyV1,
    apply_protective_observation,
    create_position_episode_protection_plan,
)
from ibmd.foundation.identity import new_id
from ibmd.operations.migrations import SQLiteMigrationRunner, load_migration_manifest
from ibmd.public_contracts.broker_execution import (
    BrokerObservationOutcome,
    BrokerOrderObservationV1,
    BrokerOrderSide,
)
from ibmd.public_contracts.broker_reconciliation import BrokerFillFactV1
from ibmd.public_contracts.decision import DesiredTargetSide, StrategyCommandKind
from ibmd.public_contracts.execution import (
    ExecutionCommandState,
    ExecutionCommandStateV1,
    ExecutionReadinessStatus,
    ExecutionReadinessV1,
    StrategyPositionSide,
    StrategyPositionStatus,
    StrategyPositionV1,
)
from ibmd.public_contracts.positions import (
    BrokerPositionRowV1,
    BrokerPositionSnapshotV1,
)
from ibmd.public_contracts.protection import (
    PositionEpisodePolicyV1,
    PositionEpisodeV1,
    ProtectionSetStatus,
    ProtectionStateV1,
    ProtectiveOrderKind,
    ProtectiveOrderState,
)

ROOT = Path(__file__).resolve().parents[1]
MANIFEST = ROOT / "migrations" / "execution.v1.json"
ACCOUNT = "DU000000"
STRATEGY = "IBMarketData.rolling"
DEPLOYMENT = "paper-drill-protection"
INSTRUMENT = "MNQ"
CON_ID = 793_356_225
LOCAL_SYMBOL = "MNQU6"
POLICY_HASH = "a" * 64
T0 = "2026-07-27T10:00:00Z"
T1 = "2026-07-27T10:00:01Z"
T2 = "2026-07-27T10:00:02Z"
T3 = "2026-07-27T10:00:03Z"


def apply_schema(path: Path, *, through: int | None = None) -> None:
    store_name, migrations = load_migration_manifest(MANIFEST)
    selected = migrations if through is None else migrations[:through]
    SQLiteMigrationRunner(
        database_path=path,
        store_name=store_name,
        migrations=selected,
        application_version="test",
    ).apply()


def command() -> ExecutionCommandStateV1:
    return ExecutionCommandStateV1(
        command_id=new_id("strategy_command"),
        strategy_id=STRATEGY,
        strategy_version=1,
        deployment_id=DEPLOYMENT,
        instrument_id=INSTRUMENT,
        command_kind=StrategyCommandKind.OPEN,
        desired_target_side=DesiredTargetSide.LONG,
        desired_target_quantity=1,
        state=ExecutionCommandState.ADMITTED,
        requested_qty=1,
        filled_qty=0,
        remaining_qty=1,
        latest_attempt_id=None,
        blocking_reason=None,
        received_at_utc=T0,
        updated_at_utc=T0,
        terminal_at_utc=None,
    )


def flat_position() -> StrategyPositionV1:
    return StrategyPositionV1(
        account_id=ACCOUNT,
        strategy_id=STRATEGY,
        deployment_id=DEPLOYMENT,
        instrument_id=INSTRUMENT,
        position_episode_id=None,
        side=StrategyPositionSide.FLAT,
        quantity=0,
        contracts=(),
        projection_status=StrategyPositionStatus.FLAT,
        broker_snapshot_id=new_id("position_snapshot"),
        updated_at_utc=T0,
        source_freshness_seconds=1.0,
    )


def readiness() -> ExecutionReadinessV1:
    return ExecutionReadinessV1(
        account_id=ACCOUNT,
        strategy_id=STRATEGY,
        deployment_id=DEPLOYMENT,
        instrument_id=INSTRUMENT,
        status=ExecutionReadinessStatus.READY,
        command_intake_enabled=True,
        broker_actions_enabled=True,
        reconciliation_complete=True,
        clock_healthy=True,
        blocking_reasons=(),
        updated_at_utc=T0,
    )


def succeeded_operation(command_state: ExecutionCommandStateV1):
    planned = plan_broker_operation(
        command=command_state,
        position=flat_position(),
        active_contract=RegisteredFuturesContractV1(
            con_id=CON_ID,
            local_symbol=LOCAL_SYMBOL,
            contract_is_active=True,
        ),
        account_id=ACCOUNT,
        observed_at_utc=T0,
    )
    submitting = mark_attempt_submitting(
        planned,
        observed_at_utc=T1,
        broker_order_id=7001,
    )
    reconciling = begin_reconciliation(
        submitting,
        observed_at_utc=T2,
    )
    return apply_broker_observation(
        reconciling,
        observation=BrokerOrderObservationV1(
            order_ref=submitting.attempt.order_ref,
            outcome=BrokerObservationOutcome.FILLED,
            observed_at_utc=T2,
            broker_order_id=7001,
            broker_perm_id=9001,
            broker_status="Filled",
            requested_qty=1,
            filled_qty=1,
            remaining_qty=0,
            detail=None,
        ),
    )


def fill(operation) -> BrokerFillFactV1:
    return BrokerFillFactV1(
        exec_id="exec-entry-1",
        account_id=ACCOUNT,
        order_ref=operation.attempt.order_ref,
        broker_order_id=7001,
        broker_perm_id=9001,
        client_id=320,
        con_id=CON_ID,
        local_symbol=LOCAL_SYMBOL,
        side=BrokerOrderSide.BUY,
        shares=1,
        price=28_600.0,
        cumulative_qty=1,
        average_price=28_600.0,
        exchange="CME",
        executed_at_utc=T2,
        observed_at_utc=T2,
        commission=None,
    )


def broker_snapshot(
    *,
    captured_at: str = T2,
) -> BrokerPositionSnapshotV1:
    return BrokerPositionSnapshotV1.complete(
        snapshot_id=new_id("position_snapshot"),
        account_id=ACCOUNT,
        captured_at_utc=captured_at,
        published_at_utc=captured_at,
        source_session_id=new_id("ib_session"),
        rows=(
            BrokerPositionRowV1(
                con_id=CON_ID,
                local_symbol=LOCAL_SYMBOL,
                symbol=INSTRUMENT,
                sec_type="FUT",
                exchange="CME",
                currency="USD",
                signed_quantity=1,
                average_cost=57_200.0,
            ),
        ),
    )


def protective_policy() -> PositionEpisodePolicyV1:
    return PositionEpisodePolicyV1(
        price_tick=0.25,
        stop_required=True,
        take_profit_enabled=True,
        stop_loss_points=150.0,
        take_profit_points=75.0,
        time_in_force="DAY",
        stop_outside_rth=True,
        take_profit_outside_rth=False,
        price_watchdog_enabled=True,
        stale_feed_market_close_enabled=False,
        price_stale_max_seconds=600,
    )


def planning_policy() -> ProtectionPlanningPolicyV1:
    return ProtectionPlanningPolicyV1(
        account_id=ACCOUNT,
        strategy_id=STRATEGY,
        strategy_version=1,
        deployment_id=DEPLOYMENT,
        instrument_id=INSTRUMENT,
        strategy_policy_hash=POLICY_HASH,
        position_max_age_seconds=10.0,
        protective_policy=protective_policy(),
    )


def plan():
    command_state = command()
    operation = succeeded_operation(command_state)
    return create_position_episode_protection_plan(
        operation=operation,
        command=command_state,
        fills=(fill(operation),),
        broker_snapshot=broker_snapshot(),
        previous_position=flat_position(),
        current_readiness=readiness(),
        policy=planning_policy(),
        observed_at_utc=T3,
    )


def observation(order, outcome, *, detail=None):
    return BrokerOrderObservationV1(
        order_ref=order.order_ref,
        outcome=outcome,
        observed_at_utc=T3,
        broker_order_id=8100 + order.planned_sequence,
        broker_perm_id=9100 + order.planned_sequence,
        broker_status=(
            "Submitted"
            if outcome == BrokerObservationOutcome.LIVE
            else outcome.value
        ),
        requested_qty=order.quantity,
        filled_qty=(
            order.quantity
            if outcome == BrokerObservationOutcome.FILLED
            else 0
        ),
        remaining_qty=(
            0
            if outcome == BrokerObservationOutcome.FILLED
            else order.quantity
        ),
        detail=detail,
    )


class PositionEpisodeProtectionTest(unittest.TestCase):
    def test_plan_creates_episode_and_stop_before_take_profit(self) -> None:
        value = plan()
        self.assertEqual(value.episode.side, StrategyPositionSide.LONG)
        self.assertEqual(value.episode.entry_average_price, 28_600.0)
        self.assertEqual(value.episode.status.value, "OPEN")
        self.assertEqual(
            value.strategy_position.position_episode_id,
            value.episode.position_episode_id,
        )
        self.assertEqual(
            value.execution_readiness.status,
            ExecutionReadinessStatus.BLOCKED,
        )
        self.assertFalse(
            value.execution_readiness.command_intake_enabled
        )
        self.assertTrue(
            value.execution_readiness.broker_actions_enabled
        )
        self.assertIn(
            "protection:stop_not_proven",
            value.execution_readiness.blocking_reasons,
        )

        stop, take_profit = value.protection.orders
        self.assertEqual(stop.kind, ProtectiveOrderKind.STOP_LOSS)
        self.assertEqual(stop.planned_sequence, 1)
        self.assertEqual(stop.stop_price, 28_450.0)
        self.assertTrue(stop.outside_rth)
        self.assertEqual(
            take_profit.kind,
            ProtectiveOrderKind.TAKE_PROFIT,
        )
        self.assertEqual(take_profit.planned_sequence, 2)
        self.assertEqual(take_profit.limit_price, 28_675.0)
        self.assertFalse(take_profit.outside_rth)
        self.assertEqual(stop.oca_group, take_profit.oca_group)
        self.assertLessEqual(len(stop.order_ref), 64)
        self.assertLessEqual(len(take_profit.order_ref), 64)
        self.assertEqual(
            PositionEpisodeV1.from_dict(value.episode.to_dict()),
            value.episode,
        )
        self.assertEqual(
            ProtectionStateV1.from_dict(value.protection.to_dict()),
            value.protection,
        )

    def test_store_migrates_v2_to_v3_and_is_idempotent(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "execution.sqlite3"
            apply_schema(database, through=2)
            apply_schema(database)
            store = SQLiteProtectionStore(database)
            store.validate_schema()
            value = plan()
            first = store.publish_plan(value)
            second = store.publish_plan(value)
            self.assertEqual(first.episode, second.episode)
            self.assertEqual(
                store.reader.read_episode_by_operation(
                    value.episode.source_operation_id
                ),
                value.episode,
            )
            self.assertEqual(
                store.reader.read_protection_by_episode(
                    value.episode.position_episode_id
                ),
                value.protection,
            )
            self.assertEqual(
                store.reader.read_transition_states(
                    value.protection.protection_set_id
                ),
                ("PLANNED",),
            )

    def test_live_stop_then_failed_tp_remains_protected(self) -> None:
        value = plan().protection
        stop = value.stop_order
        after_stop = apply_protective_observation(
            protection=value,
            kind=ProtectiveOrderKind.STOP_LOSS,
            observation=observation(
                stop,
                BrokerObservationOutcome.LIVE,
            ),
            position_open=True,
        )
        self.assertEqual(
            after_stop.status,
            ProtectionSetStatus.STOP_LIVE,
        )
        tp = after_stop.take_profit_order
        self.assertIsNotNone(tp)
        after_tp = apply_protective_observation(
            protection=after_stop,
            kind=ProtectiveOrderKind.TAKE_PROFIT,
            observation=observation(
                tp,
                BrokerObservationOutcome.REJECTED,
                detail="TP rejected",
            ),
            position_open=True,
        )
        self.assertEqual(
            after_tp.status,
            ProtectionSetStatus.PROTECTED,
        )
        self.assertTrue(after_tp.stop_proven_live)
        self.assertEqual(
            after_tp.take_profit_order.state,
            ProtectiveOrderState.REJECTED,
        )

    def test_held_399_stop_is_unprotected(self) -> None:
        value = plan().protection
        stop = value.stop_order
        result = apply_protective_observation(
            protection=value,
            kind=ProtectiveOrderKind.STOP_LOSS,
            observation=observation(
                stop,
                BrokerObservationOutcome.LIVE,
                detail=(
                    "Error 399: order held until regular trading hours"
                ),
            ),
            position_open=True,
        )
        self.assertEqual(
            result.status,
            ProtectionSetStatus.UNPROTECTED,
        )
        self.assertEqual(
            result.stop_order.state,
            ProtectiveOrderState.REJECTED,
        )
        self.assertIn("399", result.blocking_reason)

    def test_stale_position_snapshot_blocks_episode(self) -> None:
        command_state = command()
        operation = succeeded_operation(command_state)
        with self.assertRaisesRegex(ProtectionPlanningError, "stale"):
            create_position_episode_protection_plan(
                operation=operation,
                command=command_state,
                fills=(fill(operation),),
                broker_snapshot=broker_snapshot(captured_at=T0),
                previous_position=flat_position(),
                current_readiness=readiness(),
                policy=replace(
                    planning_policy(),
                    position_max_age_seconds=1.0,
                ),
                observed_at_utc=T3,
            )


if __name__ == "__main__":
    unittest.main()
