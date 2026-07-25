from __future__ import annotations

import json
import sqlite3
import tempfile
import unittest
from dataclasses import replace
from pathlib import Path

from ibmd.execution.adapters.sqlite_liquidation import SQLiteLiquidationStore
from ibmd.execution.adapters.sqlite_protection import SQLiteProtectionStore
from ibmd.execution.domain.liquidation import (
    LiquidationSnapshot,
    apply_close_observation,
    assess_next_action,
    liquidation_readiness,
    mark_broker_flat,
    mark_close_submitting,
    plan_close_attempt,
    request_liquidation,
)
from ibmd.execution.domain.liquidation_completion import (
    complete_liquidation_after_flat,
)
from ibmd.execution.domain.liquidation_position import (
    prove_liquidation_broker_position,
)
from ibmd.execution.domain.protection import (
    PositionEpisodeProtectionPlan,
    apply_protective_observation,
)
from ibmd.execution.domain.protective_submission import (
    mark_protective_order_submitting,
)
from ibmd.operations.migrations import SQLiteMigrationRunner, load_migration_manifest
from ibmd.public_contracts.broker_execution import (
    BrokerObservationOutcome,
    BrokerOrderObservationV1,
)
from ibmd.public_contracts.execution import ExecutionReadinessStatus
from ibmd.public_contracts.liquidation import (
    LiquidationAttemptState,
    LiquidationNextAction,
    LiquidationOperationState,
    LiquidationReason,
)
from ibmd.public_contracts.positions import BrokerPositionSnapshotV1
from ibmd.public_contracts.protection import (
    PositionEpisodeStatus,
    ProtectionSetStatus,
    ProtectiveOrderKind,
    ProtectiveOrderState,
)
from tester.target_execution_protective_submit_tester import (
    ACCOUNT,
    T0,
    T1,
    T2,
    T3,
    blocked_readiness,
    episode_and_protection,
    strategy_position,
)

ROOT = Path(__file__).resolve().parents[1]
BASE_MANIFEST = ROOT / "migrations" / "execution.v1.json"
LIQUIDATION_MANIFEST = ROOT / "migrations" / "execution.liquidation.v1.json"


def exact_observation(
    snapshot: LiquidationSnapshot,
    *,
    outcome: BrokerObservationOutcome,
    filled: int,
    remaining: int,
    observed_at: str = T2,
) -> BrokerOrderObservationV1:
    attempt = snapshot.attempt
    if attempt is None or attempt.broker_order_id is None:
        raise AssertionError("liquidation attempt is not submitted")
    return BrokerOrderObservationV1(
        order_ref=attempt.order_ref,
        outcome=outcome,
        observed_at_utc=observed_at,
        broker_order_id=attempt.broker_order_id,
        broker_perm_id=9001,
        broker_status=(
            "Submitted"
            if outcome == BrokerObservationOutcome.LIVE
            else outcome.value
        ),
        requested_qty=attempt.requested_qty,
        filled_qty=filled,
        remaining_qty=remaining,
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
        observation=BrokerOrderObservationV1(
            order_ref=stop_submitting.stop_order.order_ref,
            outcome=BrokerObservationOutcome.LIVE,
            observed_at_utc=T1,
            broker_order_id=7001,
            broker_perm_id=9001,
            broker_status="Submitted",
            requested_qty=1,
            filled_qty=0,
            remaining_qty=1,
            detail=None,
        ),
        position_open=True,
    )
    tp_submitting = mark_protective_order_submitting(
        stop_live,
        kind=ProtectiveOrderKind.TAKE_PROFIT,
        broker_order_id=7002,
        observed_at_utc=T2,
    )
    protection = apply_protective_observation(
        protection=tp_submitting,
        kind=ProtectiveOrderKind.TAKE_PROFIT,
        observation=BrokerOrderObservationV1(
            order_ref=tp_submitting.take_profit_order.order_ref,
            outcome=BrokerObservationOutcome.LIVE,
            observed_at_utc=T2,
            broker_order_id=7002,
            broker_perm_id=9002,
            broker_status="Submitted",
            requested_qty=1,
            filled_qty=0,
            remaining_qty=1,
            detail=None,
        ),
        position_open=True,
    )
    return episode, protection


def request_snapshot(
    *,
    episode=None,
    existing=None,
    reason=LiquidationReason.DAILY_FLAT,
    source="a",
):
    if episode is None:
        episode, _ = episode_and_protection()
    return request_liquidation(
        episode=episode,
        position=strategy_position(episode),
        readiness=blocked_readiness(),
        reason=reason,
        source_ref=source,
        observed_at_utc=T1,
        existing=existing,
    )


def flat_snapshot() -> BrokerPositionSnapshotV1:
    return BrokerPositionSnapshotV1.complete(
        snapshot_id="position_snapshot_bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
        account_id=ACCOUNT,
        captured_at_utc=T3,
        published_at_utc=T3,
        source_session_id="ib_session_cccccccccccccccccccccccccccccccc",
        rows=(),
    )


def apply_schema(database: Path) -> None:
    store_name, migrations = load_migration_manifest(BASE_MANIFEST)
    SQLiteMigrationRunner(
        database_path=database,
        store_name=store_name,
        migrations=migrations,
        application_version="test",
    ).apply()
    component = json.loads(LIQUIDATION_MANIFEST.read_text(encoding="utf-8"))
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


class LiquidationDomainTest(unittest.TestCase):
    def test_repeated_and_concurrent_reasons_share_one_operation(self) -> None:
        episode, _ = episode_and_protection()
        first = request_snapshot(episode=episode)
        repeated = request_snapshot(episode=episode, existing=first.snapshot)
        second_reason = request_snapshot(
            episode=episode,
            existing=repeated.snapshot,
            reason=LiquidationReason.MISSING_STOP,
            source="missing-stop",
        )
        self.assertTrue(first.operation_created)
        self.assertFalse(repeated.operation_created)
        self.assertFalse(repeated.trigger_created)
        self.assertEqual(
            first.snapshot.operation.liquidation_operation_id,
            second_reason.snapshot.operation.liquidation_operation_id,
        )
        self.assertEqual(len(second_reason.snapshot.triggers), 2)
        self.assertEqual(
            set(second_reason.snapshot.operation.trigger_reasons),
            {LiquidationReason.DAILY_FLAT, LiquidationReason.MISSING_STOP},
        )

    def test_unknown_outcome_never_plans_second_attempt(self) -> None:
        episode, protection = episode_and_protection()
        requested = request_snapshot(episode=episode).snapshot
        planned = plan_close_attempt(
            requested,
            broker_quantity=1,
            observed_at_utc=T1,
        )
        submitting = mark_close_submitting(
            planned,
            broker_order_id=8001,
            observed_at_utc=T2,
        )
        unknown = apply_close_observation(
            submitting,
            observation=BrokerOrderObservationV1(
                order_ref=submitting.attempt.order_ref,
                outcome=BrokerObservationOutcome.NOT_FOUND,
                observed_at_utc=T3,
                broker_order_id=None,
                broker_perm_id=None,
                broker_status=None,
                requested_qty=None,
                filled_qty=None,
                remaining_qty=None,
                detail="complete snapshot did not prove the order",
            ),
        )
        self.assertEqual(
            unknown.attempt.state,
            LiquidationAttemptState.UNKNOWN_OUTCOME,
        )
        self.assertEqual(
            unknown.operation.state,
            LiquidationOperationState.RECONCILING,
        )
        assessed = assess_next_action(
            snapshot=unknown,
            protection=protection,
            broker_position_state="OPEN",
            observed_at_utc=T3,
        )
        self.assertEqual(
            assessed.operation.next_action,
            LiquidationNextAction.RECONCILE_MARKET_CLOSE,
        )
        with self.assertRaisesRegex(ValueError, "cannot plan"):
            plan_close_attempt(
                unknown,
                broker_quantity=1,
                observed_at_utc=T3,
            )

    def test_terminal_no_fill_allows_only_explicit_attempt_two(self) -> None:
        planned = plan_close_attempt(
            request_snapshot().snapshot,
            broker_quantity=1,
            observed_at_utc=T1,
        )
        submitting = mark_close_submitting(
            planned,
            broker_order_id=8001,
            observed_at_utc=T2,
        )
        cancelled = apply_close_observation(
            submitting,
            observation=exact_observation(
                submitting,
                outcome=BrokerObservationOutcome.CANCELLED,
                filled=0,
                remaining=1,
            ),
        )
        self.assertEqual(
            cancelled.operation.state,
            LiquidationOperationState.FAILED_RETRYABLE,
        )
        retry = plan_close_attempt(
            cancelled,
            broker_quantity=1,
            observed_at_utc=T3,
        )
        self.assertEqual(retry.attempt.attempt_no, 2)
        self.assertEqual(retry.attempt.requested_qty, 1)
        self.assertNotEqual(
            retry.attempt.liquidation_attempt_id,
            cancelled.attempt.liquidation_attempt_id,
        )

    def test_already_flat_creates_no_close_attempt(self) -> None:
        requested = request_snapshot().snapshot
        completed = mark_broker_flat(requested, observed_at_utc=T2)
        self.assertIsNone(completed.attempt)
        self.assertEqual(
            completed.operation.state,
            LiquidationOperationState.CANCELLED_AS_ALREADY_FLAT,
        )
        self.assertEqual(
            completed.operation.next_action,
            LiquidationNextAction.NONE,
        )

    def test_take_profit_is_cancelled_before_stop(self) -> None:
        episode, protection = live_protection()
        requested = request_liquidation(
            episode=episode,
            position=strategy_position(episode),
            readiness=blocked_readiness(),
            reason=LiquidationReason.DAILY_HALT,
            source_ref="risk",
            observed_at_utc=T3,
        ).snapshot
        first = assess_next_action(
            snapshot=requested,
            protection=protection,
            broker_position_state="OPEN",
            observed_at_utc=T3,
        )
        self.assertEqual(
            first.operation.next_action,
            LiquidationNextAction.CANCEL_TAKE_PROFIT,
        )
        tp = protection.take_profit_order
        cancelled_tp = replace(
            protection,
            orders=tuple(
                replace(
                    item,
                    state=ProtectiveOrderState.CANCELLED,
                    updated_at_utc=T3,
                    terminal_at_utc=T3,
                    last_broker_proof_at_utc=T3,
                    broker_terminal_proven=True,
                )
                if item.protective_order_id == tp.protective_order_id
                else item
                for item in protection.orders
            ),
        )
        second = assess_next_action(
            snapshot=first,
            protection=cancelled_tp,
            broker_position_state="OPEN",
            observed_at_utc=T3,
        )
        self.assertEqual(
            second.operation.next_action,
            LiquidationNextAction.CANCEL_STOP,
        )

    def test_completion_requires_terminal_exits_and_fresh_flat(self) -> None:
        episode, protection = episode_and_protection()
        requested = request_snapshot(episode=episode).snapshot
        terminal = mark_broker_flat(requested, observed_at_utc=T3)
        proof = prove_liquidation_broker_position(
            snapshot=flat_snapshot(),
            episode=episode,
            observed_at_utc=T3,
            max_age_seconds=10.0,
        )
        completion = complete_liquidation_after_flat(
            liquidation=terminal,
            episode=episode,
            protection=protection,
            current_position=strategy_position(episode),
            current_readiness=blocked_readiness(),
            position_proof=proof,
            observed_at_utc=T3,
        )
        self.assertEqual(completion.episode.status, PositionEpisodeStatus.CLOSED)
        self.assertEqual(
            completion.protection.status,
            ProtectionSetStatus.CLOSED,
        )
        self.assertTrue(
            all(
                item.state == ProtectiveOrderState.NOT_REQUIRED
                for item in completion.protection.orders
            )
        )
        self.assertEqual(
            completion.execution_readiness.status,
            ExecutionReadinessStatus.READY,
        )


class LiquidationPersistenceTest(unittest.TestCase):
    def test_operation_trigger_and_attempt_are_durable(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            database = Path(temp) / "execution.sqlite3"
            apply_schema(database)
            episode, protection = episode_and_protection()
            position = strategy_position(episode)
            readiness = blocked_readiness()
            SQLiteProtectionStore(database).publish_plan(
                PositionEpisodeProtectionPlan(
                    episode=episode,
                    strategy_position=position,
                    execution_readiness=readiness,
                    protection=protection,
                )
            )
            store = SQLiteLiquidationStore(database)
            store.validate_schema()
            result = request_liquidation(
                episode=episode,
                position=position,
                readiness=readiness,
                reason=LiquidationReason.DAILY_FLAT,
                source_ref="daily-flat-2026-07-27",
                observed_at_utc=T1,
            )
            stored = store.publish_request(current=None, result=result)
            self.assertEqual(
                store.read_snapshot_by_episode(episode.position_episode_id),
                stored,
            )
            planned = plan_close_attempt(
                stored,
                broker_quantity=1,
                observed_at_utc=T2,
            )
            planned_readiness = liquidation_readiness(
                result.execution_readiness,
                operation=planned.operation,
                observed_at_utc=T2,
            )
            stored_planned = store.publish_state(
                current=stored,
                updated=planned,
                readiness=planned_readiness,
            )
            self.assertEqual(stored_planned.attempt.attempt_no, 1)
            restarted = SQLiteLiquidationStore(database)
            self.assertEqual(
                restarted.read_snapshot_by_episode(episode.position_episode_id),
                stored_planned,
            )


if __name__ == "__main__":
    unittest.main()
