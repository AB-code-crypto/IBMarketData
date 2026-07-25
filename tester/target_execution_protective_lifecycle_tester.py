from __future__ import annotations

import json
import sqlite3
import tempfile
import unittest
from dataclasses import replace
from pathlib import Path

from ibmd.execution.adapters import (
    SQLiteProtectionStore,
    SQLiteProtectiveLifecycleStore,
    SQLiteProtectiveSubmitStore,
)
from ibmd.execution.domain.protection import (
    PositionEpisodeProtectionPlan,
    apply_protective_observation,
)
from ibmd.execution.domain.protective_lifecycle import (
    ProtectiveLifecyclePolicyV1,
    reconcile_protective_lifecycle,
)
from ibmd.execution.domain.protective_submission import (
    mark_protective_order_submitting,
)
from ibmd.execution.domain.protective_uncertainty import readiness_for_protection
from ibmd.operations.migrations import SQLiteMigrationRunner, load_migration_manifest
from ibmd.public_contracts.broker_execution import (
    BrokerObservationOutcome,
    BrokerOrderObservationV1,
)
from ibmd.public_contracts.broker_reconciliation import (
    BrokerCommissionFactV1,
    BrokerFillFactV1,
    BrokerOrderFactV1,
    BrokerOrderSource,
    BrokerReconciliationSnapshotV1,
)
from ibmd.public_contracts.execution import (
    ExecutionReadinessStatus,
    StrategyPositionStatus,
)
from ibmd.public_contracts.positions import BrokerPositionSnapshotV1
from ibmd.public_contracts.protection import (
    PositionEpisodeStatus,
    ProtectionSetStatus,
    ProtectiveOrderKind,
    ProtectiveOrderState,
    ProtectiveOrderType,
)
from tester.target_execution_protective_submit_tester import (
    ACCOUNT,
    CON_ID,
    DEPLOYMENT,
    INSTRUMENT,
    LOCAL_SYMBOL,
    STRATEGY,
    T0,
    T1,
    T2,
    T3,
    blocked_readiness,
    episode_and_protection,
    position_snapshot,
    strategy_position,
)

ROOT = Path(__file__).resolve().parents[1]
BASE_MANIFEST = ROOT / "migrations" / "execution.v1.json"
LIFECYCLE_MANIFEST = (
    ROOT / "migrations" / "execution.protective_lifecycle.v1.json"
)


def observation(order, outcome, *, observed_at, status=None):
    return BrokerOrderObservationV1(
        order_ref=order.order_ref,
        outcome=outcome,
        observed_at_utc=observed_at,
        broker_order_id=(
            order.broker_order_id
            if order.broker_order_id is not None
            else 7000 + order.planned_sequence
        ),
        broker_perm_id=9000 + order.planned_sequence,
        broker_status=status or outcome.value,
        requested_qty=order.quantity,
        filled_qty=(order.quantity if outcome == BrokerObservationOutcome.FILLED else 0),
        remaining_qty=(0 if outcome == BrokerObservationOutcome.FILLED else order.quantity),
        detail=None,
    )


def live_protection():
    episode, planned = episode_and_protection()
    stop_submitting = mark_protective_order_submitting(
        planned,
        kind=ProtectiveOrderKind.STOP_LOSS,
        broker_order_id=7001,
        observed_at_utc=T1,
    )
    stop_live = apply_protective_observation(
        protection=stop_submitting,
        kind=ProtectiveOrderKind.STOP_LOSS,
        observation=observation(
            stop_submitting.stop_order,
            BrokerObservationOutcome.LIVE,
            observed_at=T1,
            status="Submitted",
        ),
        position_open=True,
    )
    tp_submitting = mark_protective_order_submitting(
        stop_live,
        kind=ProtectiveOrderKind.TAKE_PROFIT,
        broker_order_id=7002,
        observed_at_utc=T2,
    )
    protected = apply_protective_observation(
        protection=tp_submitting,
        kind=ProtectiveOrderKind.TAKE_PROFIT,
        observation=observation(
            tp_submitting.take_profit_order,
            BrokerObservationOutcome.LIVE,
            observed_at=T2,
            status="Submitted",
        ),
        position_open=True,
    )
    readiness = readiness_for_protection(
        blocked_readiness(),
        protection=protected,
        observed_at_utc=T2,
    )
    return episode, protected, strategy_position(episode), readiness


def completed_order(order, *, state, filled, remaining, captured_at):
    return BrokerOrderFactV1(
        account_id=ACCOUNT,
        order_ref=order.order_ref,
        broker_order_id=order.broker_order_id,
        broker_perm_id=9000 + order.planned_sequence,
        client_id=340,
        con_id=CON_ID,
        local_symbol=LOCAL_SYMBOL,
        side=order.side,
        order_type=(
            "STP"
            if order.order_type == ProtectiveOrderType.STOP
            else "LMT"
        ),
        requested_qty=order.quantity,
        filled_qty=filled,
        remaining_qty=remaining,
        status=state,
        source=BrokerOrderSource.COMPLETED,
        observed_at_utc=captured_at,
        completed_status=state,
        warning_text=None,
    )


def open_order(order, *, captured_at):
    return BrokerOrderFactV1(
        account_id=ACCOUNT,
        order_ref=order.order_ref,
        broker_order_id=order.broker_order_id,
        broker_perm_id=9000 + order.planned_sequence,
        client_id=340,
        con_id=CON_ID,
        local_symbol=LOCAL_SYMBOL,
        side=order.side,
        order_type=(
            "STP"
            if order.order_type == ProtectiveOrderType.STOP
            else "LMT"
        ),
        requested_qty=order.quantity,
        filled_qty=0,
        remaining_qty=order.quantity,
        status="Submitted",
        source=BrokerOrderSource.OPEN,
        observed_at_utc=captured_at,
        completed_status=None,
        warning_text=None,
    )


def protective_fill(order, *, exec_id, commission=None):
    return BrokerFillFactV1(
        exec_id=exec_id,
        account_id=ACCOUNT,
        order_ref=order.order_ref,
        broker_order_id=order.broker_order_id,
        broker_perm_id=9000 + order.planned_sequence,
        client_id=340,
        con_id=CON_ID,
        local_symbol=LOCAL_SYMBOL,
        side=order.side,
        shares=order.quantity,
        price=28_450.0 if order.kind == ProtectiveOrderKind.STOP_LOSS else 28_675.0,
        cumulative_qty=order.quantity,
        average_price=28_450.0 if order.kind == ProtectiveOrderKind.STOP_LOSS else 28_675.0,
        exchange="CME",
        executed_at_utc=T2,
        observed_at_utc=T3,
        commission=commission,
    )


def broker_snapshot(*, open_orders=(), completed_orders=(), fills=()):
    return BrokerReconciliationSnapshotV1(
        source_session_id="ib_session_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        account_id=ACCOUNT,
        captured_at_utc=T3,
        open_orders=tuple(open_orders),
        completed_orders=tuple(completed_orders),
        fills=tuple(fills),
        requests_complete=True,
    )


def flat_snapshot():
    return BrokerPositionSnapshotV1.complete(
        snapshot_id="position_snapshot_bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
        account_id=ACCOUNT,
        captured_at_utc=T3,
        published_at_utc=T3,
        source_session_id="ib_session_cccccccccccccccccccccccccccccccc",
        rows=(),
    )


def lifecycle_policy():
    return ProtectiveLifecyclePolicyV1(
        account_id=ACCOUNT,
        strategy_id=STRATEGY,
        strategy_version=1,
        deployment_id=DEPLOYMENT,
        instrument_id=INSTRUMENT,
        position_max_age_seconds=10.0,
    )


def apply_schema(database: Path) -> None:
    store_name, migrations = load_migration_manifest(BASE_MANIFEST)
    SQLiteMigrationRunner(
        database_path=database,
        store_name=store_name,
        migrations=migrations,
        application_version="test",
    ).apply()
    component = json.loads(LIFECYCLE_MANIFEST.read_text(encoding="utf-8"))
    connection = sqlite3.connect(str(database))
    try:
        connection.execute("PRAGMA foreign_keys = ON")
        connection.execute("BEGIN IMMEDIATE")
        connection.execute(
            """
            CREATE TABLE execution_target_schema_components (
                component_name TEXT PRIMARY KEY,
                component_version INTEGER NOT NULL,
                checksum TEXT NOT NULL,
                applied_at_utc TEXT NOT NULL,
                application_version TEXT NOT NULL
            )
            """
        )
        for statement in component["statements"]:
            connection.execute(statement)
        connection.execute(
            "INSERT INTO execution_target_schema_components VALUES (?, ?, ?, ?, ?)",
            (
                component["component_name"],
                component["component_version"],
                "test-checksum",
                T0,
                "test",
            ),
        )
        connection.commit()
    finally:
        connection.close()


class ProtectiveLifecycleDomainTest(unittest.TestCase):
    def test_stop_fill_and_terminal_oca_sibling_wait_for_flat(self) -> None:
        episode, protection, position, readiness = live_protection()
        stop = protection.stop_order
        tp = protection.take_profit_order
        update = reconcile_protective_lifecycle(
            episode=episode,
            protection=protection,
            strategy_position=position,
            execution_readiness=readiness,
            broker_snapshot=broker_snapshot(
                completed_orders=(
                    completed_order(
                        stop,
                        state="Filled",
                        filled=1,
                        remaining=0,
                        captured_at=T3,
                    ),
                    completed_order(
                        tp,
                        state="Cancelled",
                        filled=0,
                        remaining=1,
                        captured_at=T3,
                    ),
                ),
                fills=(protective_fill(stop, exec_id="stop-exec-1"),),
            ),
            position_snapshot=position_snapshot(),
            policy=lifecycle_policy(),
            observed_at_utc=T3,
        )
        self.assertEqual(update.protection.status, ProtectionSetStatus.EXITED)
        self.assertFalse(update.episode_closed)
        self.assertEqual(
            update.strategy_position.projection_status,
            StrategyPositionStatus.OPEN,
        )
        self.assertEqual(
            update.execution_readiness.status,
            ExecutionReadinessStatus.BLOCKED,
        )
        self.assertEqual(update.protection.stop_order.state, ProtectiveOrderState.FILLED)
        self.assertEqual(update.protection.take_profit_order.state, ProtectiveOrderState.CANCELLED)

    def test_flat_position_closes_episode_after_oca_terminal(self) -> None:
        episode, protection, position, readiness = live_protection()
        stop = protection.stop_order
        tp = protection.take_profit_order
        first = reconcile_protective_lifecycle(
            episode=episode,
            protection=protection,
            strategy_position=position,
            execution_readiness=readiness,
            broker_snapshot=broker_snapshot(
                completed_orders=(
                    completed_order(stop, state="Filled", filled=1, remaining=0, captured_at=T3),
                    completed_order(tp, state="Cancelled", filled=0, remaining=1, captured_at=T3),
                ),
                fills=(protective_fill(stop, exec_id="stop-exec-1"),),
            ),
            position_snapshot=position_snapshot(),
            policy=lifecycle_policy(),
            observed_at_utc=T3,
        )
        second = reconcile_protective_lifecycle(
            episode=first.episode,
            protection=first.protection,
            strategy_position=first.strategy_position,
            execution_readiness=first.execution_readiness,
            broker_snapshot=broker_snapshot(
                completed_orders=(
                    completed_order(first.protection.stop_order, state="Filled", filled=1, remaining=0, captured_at=T3),
                    completed_order(first.protection.take_profit_order, state="Cancelled", filled=0, remaining=1, captured_at=T3),
                ),
                fills=(protective_fill(first.protection.stop_order, exec_id="stop-exec-1"),),
            ),
            position_snapshot=flat_snapshot(),
            policy=lifecycle_policy(),
            observed_at_utc=T3,
        )
        self.assertTrue(second.episode_closed)
        self.assertEqual(second.episode.status, PositionEpisodeStatus.CLOSED)
        self.assertEqual(second.protection.status, ProtectionSetStatus.CLOSED)
        self.assertEqual(second.strategy_position.projection_status, StrategyPositionStatus.FLAT)
        self.assertEqual(second.execution_readiness.status, ExecutionReadinessStatus.READY)
        self.assertTrue(second.execution_readiness.command_intake_enabled)

    def test_live_oca_sibling_after_fill_requires_operator(self) -> None:
        episode, protection, position, readiness = live_protection()
        stop = protection.stop_order
        tp = protection.take_profit_order
        update = reconcile_protective_lifecycle(
            episode=episode,
            protection=protection,
            strategy_position=position,
            execution_readiness=readiness,
            broker_snapshot=broker_snapshot(
                open_orders=(open_order(tp, captured_at=T3),),
                completed_orders=(
                    completed_order(stop, state="Filled", filled=1, remaining=0, captured_at=T3),
                ),
                fills=(protective_fill(stop, exec_id="stop-exec-1"),),
            ),
            position_snapshot=flat_snapshot(),
            policy=lifecycle_policy(),
            observed_at_utc=T3,
        )
        self.assertEqual(update.protection.status, ProtectionSetStatus.OPERATOR_REQUIRED)
        self.assertFalse(update.episode_closed)
        self.assertIn("oca_sibling", update.protection.blocking_reason)
        self.assertEqual(update.strategy_position.projection_status, StrategyPositionStatus.UNKNOWN)

    def test_manual_flat_before_submission_marks_orders_not_required(self) -> None:
        episode, protection = episode_and_protection()
        update = reconcile_protective_lifecycle(
            episode=episode,
            protection=protection,
            strategy_position=strategy_position(episode),
            execution_readiness=blocked_readiness(),
            broker_snapshot=broker_snapshot(),
            position_snapshot=flat_snapshot(),
            policy=lifecycle_policy(),
            observed_at_utc=T3,
        )
        self.assertTrue(update.episode_closed)
        self.assertEqual(update.protection.status, ProtectionSetStatus.CLOSED)
        self.assertTrue(
            all(
                order.state == ProtectiveOrderState.NOT_REQUIRED
                for order in update.protection.orders
            )
        )


class ProtectiveLifecyclePersistenceTest(unittest.TestCase):
    def test_fill_is_immutable_and_late_commission_is_appended(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            database = Path(temp) / "execution.sqlite3"
            apply_schema(database)
            episode, planned = episode_and_protection()
            position = strategy_position(episode)
            initial_readiness = blocked_readiness()
            SQLiteProtectionStore(database).publish_plan(
                PositionEpisodeProtectionPlan(
                    episode=episode,
                    strategy_position=position,
                    execution_readiness=initial_readiness,
                    protection=planned,
                )
            )
            live_episode, live, _, ready = live_protection()
            live_episode = replace(
                live_episode,
                position_episode_id=episode.position_episode_id,
                source_command_id=episode.source_command_id,
                source_operation_id=episode.source_operation_id,
                source_attempt_id=episode.source_attempt_id,
            )
            live = replace(
                live,
                protection_set_id=planned.protection_set_id,
                position_episode_id=episode.position_episode_id,
                orders=tuple(
                    replace(
                        item,
                        protection_set_id=planned.protection_set_id,
                        position_episode_id=episode.position_episode_id,
                        protective_order_id=planned.orders[index].protective_order_id,
                        order_ref=planned.orders[index].order_ref,
                        oca_group=planned.orders[index].oca_group,
                    )
                    for index, item in enumerate(live.orders)
                ),
            )
            ready = replace(
                ready,
                deployment_id=episode.deployment_id,
            )
            SQLiteProtectiveSubmitStore(database).publish_state_and_readiness(
                current=planned,
                updated=live,
                readiness=ready,
            )
            stop = live.stop_order
            tp = live.take_profit_order
            snapshot_without_commission = broker_snapshot(
                completed_orders=(
                    completed_order(stop, state="Filled", filled=1, remaining=0, captured_at=T3),
                    completed_order(tp, state="Cancelled", filled=0, remaining=1, captured_at=T3),
                ),
                fills=(protective_fill(stop, exec_id="stop-exec-1"),),
            )
            first = reconcile_protective_lifecycle(
                episode=episode,
                protection=live,
                strategy_position=position,
                execution_readiness=ready,
                broker_snapshot=snapshot_without_commission,
                position_snapshot=position_snapshot(),
                policy=lifecycle_policy(),
                observed_at_utc=T3,
            )
            store = SQLiteProtectiveLifecycleStore(database)
            store.validate_schema()
            store.publish_lifecycle(
                current_episode=episode,
                current_protection=live,
                current_position=position,
                current_readiness=ready,
                update=first,
            )
            self.assertEqual(store.read_commission_pending_exec_ids(episode.position_episode_id), ("stop-exec-1",))
            stored_fill = store.read_fills(episode.position_episode_id)[0]
            self.assertIsNone(stored_fill.commission)

            commission = BrokerCommissionFactV1(
                exec_id="stop-exec-1",
                commission=0.62,
                currency="USD",
                realized_pnl=-300.0,
                reported_at_utc=T3,
            )
            second = reconcile_protective_lifecycle(
                episode=first.episode,
                protection=first.protection,
                strategy_position=first.strategy_position,
                execution_readiness=first.execution_readiness,
                broker_snapshot=broker_snapshot(
                    completed_orders=(
                        completed_order(first.protection.stop_order, state="Filled", filled=1, remaining=0, captured_at=T3),
                        completed_order(first.protection.take_profit_order, state="Cancelled", filled=0, remaining=1, captured_at=T3),
                    ),
                    fills=(protective_fill(first.protection.stop_order, exec_id="stop-exec-1", commission=commission),),
                ),
                position_snapshot=position_snapshot(),
                policy=lifecycle_policy(),
                observed_at_utc=T3,
            )
            store.publish_lifecycle(
                current_episode=first.episode,
                current_protection=first.protection,
                current_position=first.strategy_position,
                current_readiness=first.execution_readiness,
                update=second,
            )
            values = store.read_fills(episode.position_episode_id)
            self.assertEqual(len(values), 1)
            self.assertEqual(values[0].exec_id, "stop-exec-1")
            self.assertIsNotNone(values[0].commission)
            self.assertEqual(values[0].commission.commission, 0.62)
            self.assertEqual(store.read_commission_pending_exec_ids(episode.position_episode_id), ())


if __name__ == "__main__":
    unittest.main()
