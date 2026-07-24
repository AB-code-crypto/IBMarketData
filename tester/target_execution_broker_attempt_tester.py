from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from ibmd.execution.adapters import SQLiteBrokerAttemptStore
from ibmd.execution.domain import (
    BrokerAttemptDomainError,
    apply_broker_observation,
    begin_reconciliation,
    mark_attempt_submitting,
    plan_broker_operation,
    prepare_next_attempt,
)
from ibmd.execution.domain.position_projection import RegisteredFuturesContractV1
from ibmd.foundation.identity import new_id
from ibmd.operations.migrations import (
    SQLiteMigrationRunner,
    load_migration_manifest,
)
from ibmd.public_contracts.broker_execution import (
    BrokerAttemptState,
    BrokerObservationOutcome,
    BrokerOperationState,
    BrokerOrderAttemptV1,
    BrokerOrderObservationV1,
    BrokerOrderOperationV1,
    BrokerOrderSide,
)
from ibmd.public_contracts.decision import (
    DesiredTargetSide,
    StrategyCommandKind,
)
from ibmd.public_contracts.execution import (
    ExecutionCommandState,
    ExecutionCommandStateV1,
    PositionContractV1,
    StrategyPositionSide,
    StrategyPositionStatus,
    StrategyPositionV1,
)

ROOT = Path(__file__).resolve().parents[1]
MANIFEST = ROOT / "migrations" / "execution.v1.json"
ACCOUNT = "U0000000"
STRATEGY = "IBMarketData.rolling"
DEPLOYMENT = "shadow-mnq-account1"
INSTRUMENT = "MNQ"
CON_ID = 793356225
LOCAL_SYMBOL = "MNQU6"
T0 = "2026-07-24T10:00:00Z"
T1 = "2026-07-24T10:00:01Z"
T2 = "2026-07-24T10:00:02Z"
T3 = "2026-07-24T10:00:03Z"
T4 = "2026-07-24T10:00:04Z"


def apply_schema(path: Path) -> None:
    store_name, migrations = load_migration_manifest(MANIFEST)
    SQLiteMigrationRunner(
        database_path=path,
        store_name=store_name,
        migrations=migrations,
        application_version="test",
    ).apply()


def active_contract() -> RegisteredFuturesContractV1:
    return RegisteredFuturesContractV1(
        con_id=CON_ID,
        local_symbol=LOCAL_SYMBOL,
        contract_is_active=True,
    )


def command_state(
    *,
    kind: StrategyCommandKind = StrategyCommandKind.OPEN,
    side: DesiredTargetSide = DesiredTargetSide.LONG,
    target_quantity: int = 1,
    command_id: str | None = None,
) -> ExecutionCommandStateV1:
    return ExecutionCommandStateV1(
        command_id=command_id or new_id("strategy_command"),
        strategy_id=STRATEGY,
        strategy_version=1,
        deployment_id=DEPLOYMENT,
        instrument_id=INSTRUMENT,
        command_kind=kind,
        desired_target_side=side,
        desired_target_quantity=target_quantity,
        state=ExecutionCommandState.ADMITTED,
        requested_qty=target_quantity,
        filled_qty=0,
        remaining_qty=target_quantity,
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


def open_position(
    *,
    side: StrategyPositionSide,
    quantity: int,
) -> StrategyPositionV1:
    signed = quantity if side == StrategyPositionSide.LONG else -quantity
    return StrategyPositionV1(
        account_id=ACCOUNT,
        strategy_id=STRATEGY,
        deployment_id=DEPLOYMENT,
        instrument_id=INSTRUMENT,
        position_episode_id=new_id("position_episode"),
        side=side,
        quantity=quantity,
        contracts=(
            PositionContractV1(
                con_id=CON_ID,
                local_symbol=LOCAL_SYMBOL,
                signed_quantity=signed,
                contract_is_active=True,
            ),
        ),
        projection_status=StrategyPositionStatus.OPEN,
        broker_snapshot_id=new_id("position_snapshot"),
        updated_at_utc=T0,
        source_freshness_seconds=1.0,
    )


def exact_observation(
    snapshot,
    *,
    outcome: BrokerObservationOutcome,
    filled: int,
    remaining: int,
    observed_at: str,
    order_id: int = 7001,
    status: str | None = None,
    detail: str | None = None,
) -> BrokerOrderObservationV1:
    return BrokerOrderObservationV1(
        order_ref=snapshot.attempt.order_ref,
        outcome=outcome,
        observed_at_utc=observed_at,
        broker_order_id=order_id,
        broker_perm_id=9001,
        broker_status=status or outcome.value,
        requested_qty=snapshot.attempt.requested_qty,
        filled_qty=filled,
        remaining_qty=remaining,
        detail=detail,
    )


class BrokerAttemptContractTest(unittest.TestCase):
    def test_public_contracts_round_trip_and_reject_false_filled_state(self) -> None:
        planned = plan_broker_operation(
            command=command_state(),
            position=flat_position(),
            active_contract=active_contract(),
            account_id=ACCOUNT,
            observed_at_utc=T0,
        )
        self.assertEqual(
            BrokerOrderOperationV1.from_dict(planned.operation.to_dict()),
            planned.operation,
        )
        self.assertEqual(
            BrokerOrderAttemptV1.from_dict(planned.attempt.to_dict()),
            planned.attempt,
        )
        invalid = planned.attempt.to_dict()
        invalid.update(
            {
                "state": "FILLED",
                "broker_order_id": 1,
                "broker_status": "Filled",
                "broker_terminal_proven": True,
                "terminal_at_utc": T1,
                "last_broker_proof_at_utc": T1,
            }
        )
        with self.assertRaisesRegex(ValueError, "remaining_qty=0"):
            BrokerOrderAttemptV1.from_dict(invalid)

    def test_operation_and_attempt_identities_are_stable(self) -> None:
        command = command_state(command_id=new_id("strategy_command"))
        first = plan_broker_operation(
            command=command,
            position=flat_position(),
            active_contract=active_contract(),
            account_id=ACCOUNT,
            observed_at_utc=T0,
        )
        second = plan_broker_operation(
            command=command,
            position=flat_position(),
            active_contract=active_contract(),
            account_id=ACCOUNT,
            observed_at_utc=T0,
        )
        self.assertEqual(first, second)
        self.assertLessEqual(len(first.attempt.order_ref), 64)
        self.assertIn(first.operation.operation_id, first.attempt.order_ref)


class BrokerOperationPlanningTest(unittest.TestCase):
    def test_open_uses_target_quantity_and_active_contract(self) -> None:
        planned = plan_broker_operation(
            command=command_state(target_quantity=2),
            position=flat_position(),
            active_contract=active_contract(),
            account_id=ACCOUNT,
            observed_at_utc=T0,
        )
        self.assertEqual(planned.operation.side, BrokerOrderSide.BUY)
        self.assertEqual(planned.operation.requested_qty, 2)
        self.assertEqual(planned.operation.con_id, CON_ID)
        self.assertEqual(planned.attempt.state, BrokerAttemptState.PREPARING)

    def test_reverse_long_two_to_short_one_plans_sell_three(self) -> None:
        planned = plan_broker_operation(
            command=command_state(
                kind=StrategyCommandKind.REVERSE,
                side=DesiredTargetSide.SHORT,
                target_quantity=1,
            ),
            position=open_position(
                side=StrategyPositionSide.LONG,
                quantity=2,
            ),
            active_contract=active_contract(),
            account_id=ACCOUNT,
            observed_at_utc=T0,
        )
        self.assertEqual(planned.operation.side, BrokerOrderSide.SELL)
        self.assertEqual(planned.operation.requested_qty, 3)
        self.assertEqual(planned.attempt.requested_qty, 3)


class BrokerAttemptReconciliationTest(unittest.TestCase):
    def test_not_found_after_submission_is_unknown_and_suppresses_retry(self) -> None:
        planned = plan_broker_operation(
            command=command_state(),
            position=flat_position(),
            active_contract=active_contract(),
            account_id=ACCOUNT,
            observed_at_utc=T0,
        )
        submitting = mark_attempt_submitting(
            planned,
            observed_at_utc=T1,
            broker_order_id=7001,
        )
        unknown = apply_broker_observation(
            submitting,
            observation=BrokerOrderObservationV1(
                order_ref=submitting.attempt.order_ref,
                outcome=BrokerObservationOutcome.NOT_FOUND,
                observed_at_utc=T2,
                broker_order_id=None,
                broker_perm_id=None,
                broker_status=None,
                requested_qty=None,
                filled_qty=None,
                remaining_qty=None,
                detail="complete broker query returned no exact orderRef match",
            ),
        )
        self.assertEqual(
            unknown.operation.state,
            BrokerOperationState.UNKNOWN_OUTCOME,
        )
        self.assertEqual(
            unknown.attempt.state,
            BrokerAttemptState.UNKNOWN_OUTCOME,
        )
        with self.assertRaisesRegex(
            BrokerAttemptDomainError,
            "FAILED_RETRYABLE",
        ):
            prepare_next_attempt(unknown, observed_at_utc=T3)

    def test_terminal_partial_cancel_retries_only_fresh_remaining(self) -> None:
        planned = plan_broker_operation(
            command=command_state(
                kind=StrategyCommandKind.REVERSE,
                side=DesiredTargetSide.SHORT,
            ),
            position=open_position(
                side=StrategyPositionSide.LONG,
                quantity=2,
            ),
            active_contract=active_contract(),
            account_id=ACCOUNT,
            observed_at_utc=T0,
        )
        submitting = mark_attempt_submitting(
            planned,
            observed_at_utc=T1,
            broker_order_id=7001,
        )
        cancelled = apply_broker_observation(
            submitting,
            observation=exact_observation(
                submitting,
                outcome=BrokerObservationOutcome.CANCELLED,
                filled=2,
                remaining=1,
                observed_at=T2,
                status="Cancelled",
            ),
        )
        self.assertEqual(
            cancelled.operation.state,
            BrokerOperationState.FAILED_RETRYABLE,
        )
        self.assertEqual(cancelled.operation.filled_qty, 2)
        self.assertEqual(cancelled.operation.remaining_qty, 1)
        retry = prepare_next_attempt(cancelled, observed_at_utc=T3)
        self.assertEqual(retry.attempt.attempt_no, 2)
        self.assertEqual(retry.attempt.requested_qty, 1)
        self.assertEqual(retry.operation.requested_qty, 3)
        self.assertEqual(retry.operation.filled_qty, 2)

    def test_full_fill_succeeds_and_is_terminal(self) -> None:
        planned = plan_broker_operation(
            command=command_state(),
            position=flat_position(),
            active_contract=active_contract(),
            account_id=ACCOUNT,
            observed_at_utc=T0,
        )
        submitting = mark_attempt_submitting(
            planned,
            observed_at_utc=T1,
            broker_order_id=7001,
        )
        filled = apply_broker_observation(
            submitting,
            observation=exact_observation(
                submitting,
                outcome=BrokerObservationOutcome.FILLED,
                filled=1,
                remaining=0,
                observed_at=T2,
                status="Filled",
            ),
        )
        self.assertEqual(filled.operation.state, BrokerOperationState.SUCCEEDED)
        self.assertEqual(filled.attempt.state, BrokerAttemptState.FILLED)
        self.assertEqual(filled.operation.remaining_qty, 0)
        self.assertEqual(filled.operation.terminal_at_utc, T2.replace("Z", ".000000Z"))


class BrokerAttemptPersistenceTest(unittest.TestCase):
    def test_existing_v1_database_upgrades_to_v2(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            database = Path(temp) / "execution.sqlite3"
            store_name, migrations = load_migration_manifest(MANIFEST)
            SQLiteMigrationRunner(
                database_path=database,
                store_name=store_name,
                migrations=migrations[:1],
                application_version="old",
            ).apply()
            plan = SQLiteMigrationRunner(
                database_path=database,
                store_name=store_name,
                migrations=migrations,
                application_version="new",
            ).apply()
            self.assertEqual(plan.current_version, 2)
            SQLiteBrokerAttemptStore(database).validate_schema()

    def test_restart_adopts_live_order_by_same_order_ref(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            database = Path(temp) / "execution.sqlite3"
            apply_schema(database)
            store = SQLiteBrokerAttemptStore(database)
            store.validate_schema()
            planned = plan_broker_operation(
                command=command_state(),
                position=flat_position(),
                active_contract=active_contract(),
                account_id=ACCOUNT,
                observed_at_utc=T0,
            )
            store.publish_initial(planned)
            submitting = mark_attempt_submitting(
                planned,
                observed_at_utc=T1,
                broker_order_id=7001,
            )
            store.publish_state(submitting)

            restarted = SQLiteBrokerAttemptStore(database)
            unresolved = restarted.read_unresolved()
            self.assertEqual(len(unresolved), 1)
            self.assertEqual(
                unresolved[0].operation.operation_id,
                planned.operation.operation_id,
            )
            reconciling = begin_reconciliation(
                unresolved[0],
                observed_at_utc=T2,
            )
            restarted.publish_state(reconciling)
            observation = exact_observation(
                reconciling,
                outcome=BrokerObservationOutcome.LIVE,
                filled=0,
                remaining=1,
                observed_at=T3,
                status="Submitted",
            )
            live = apply_broker_observation(
                reconciling,
                observation=observation,
            )
            restarted.publish_state(live, observation=observation)

            after_second_restart = SQLiteBrokerAttemptStore(database)
            restored = after_second_restart.read_snapshot(
                planned.operation.operation_id
            )
            self.assertIsNotNone(restored)
            self.assertEqual(restored.operation.state, BrokerOperationState.LIVE)
            self.assertEqual(restored.attempt.attempt_id, planned.attempt.attempt_id)
            self.assertEqual(restored.attempt.order_ref, planned.attempt.order_ref)
            self.assertEqual(restored.attempt.broker_order_id, 7001)
            self.assertEqual(after_second_restart.observation_count(restored.attempt.attempt_id), 1)

            after_second_restart.publish_state(restored, observation=observation)
            self.assertEqual(after_second_restart.observation_count(restored.attempt.attempt_id), 1)
            self.assertEqual(
                after_second_restart.transition_counts(restored),
                (4, 3),
            )

    def test_unknown_restart_state_cannot_create_second_attempt(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            database = Path(temp) / "execution.sqlite3"
            apply_schema(database)
            store = SQLiteBrokerAttemptStore(database)
            planned = plan_broker_operation(
                command=command_state(),
                position=flat_position(),
                active_contract=active_contract(),
                account_id=ACCOUNT,
                observed_at_utc=T0,
            )
            store.publish_initial(planned)
            submitting = mark_attempt_submitting(
                planned,
                observed_at_utc=T1,
            )
            store.publish_state(submitting)
            observation = BrokerOrderObservationV1(
                order_ref=submitting.attempt.order_ref,
                outcome=BrokerObservationOutcome.AMBIGUOUS,
                observed_at_utc=T2,
                broker_order_id=None,
                broker_perm_id=None,
                broker_status=None,
                requested_qty=None,
                filled_qty=None,
                remaining_qty=None,
                detail="two broker orders share incomplete local evidence",
            )
            unknown = apply_broker_observation(
                submitting,
                observation=observation,
            )
            store.publish_state(unknown, observation=observation)

            restarted = SQLiteBrokerAttemptStore(database)
            restored = restarted.read_snapshot(planned.operation.operation_id)
            self.assertEqual(
                restored.operation.state,
                BrokerOperationState.UNKNOWN_OUTCOME,
            )
            with self.assertRaisesRegex(
                BrokerAttemptDomainError,
                "FAILED_RETRYABLE",
            ):
                prepare_next_attempt(restored, observed_at_utc=T3)
            self.assertEqual(restored.operation.current_attempt_no, 1)

    def test_terminal_no_fill_persists_second_attempt_without_duplicates(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            database = Path(temp) / "execution.sqlite3"
            apply_schema(database)
            store = SQLiteBrokerAttemptStore(database)
            planned = plan_broker_operation(
                command=command_state(),
                position=flat_position(),
                active_contract=active_contract(),
                account_id=ACCOUNT,
                observed_at_utc=T0,
            )
            store.publish_initial(planned)
            submitting = mark_attempt_submitting(
                planned,
                observed_at_utc=T1,
                broker_order_id=7001,
            )
            store.publish_state(submitting)
            observation = exact_observation(
                submitting,
                outcome=BrokerObservationOutcome.REJECTED,
                filled=0,
                remaining=1,
                observed_at=T2,
                status="Inactive",
                detail="broker rejected before fill",
            )
            rejected = apply_broker_observation(
                submitting,
                observation=observation,
            )
            store.publish_state(rejected, observation=observation)
            retry = prepare_next_attempt(rejected, observed_at_utc=T3)
            store.publish_state(retry)
            store.publish_state(retry)

            restored = store.read_snapshot(planned.operation.operation_id)
            self.assertEqual(restored.operation.current_attempt_no, 2)
            self.assertEqual(restored.attempt.attempt_no, 2)
            self.assertEqual(restored.attempt.state, BrokerAttemptState.PREPARING)
            self.assertEqual(store.transition_counts(restored), (4, 1))


if __name__ == "__main__":
    unittest.main()
