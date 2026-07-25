from __future__ import annotations

import asyncio
import unittest
from dataclasses import replace

from ibmd.execution.application.paper_liquidation import (
    PaperLiquidationCoordinator,
    PaperLiquidationError,
    PaperLiquidationPolicy,
)
from ibmd.execution.domain.liquidation import (
    mark_close_submitting,
    plan_close_attempt,
    request_liquidation,
)
from ibmd.execution.domain.protective_uncertainty import readiness_for_protection
from ibmd.foundation.identity import new_id
from ibmd.ib_gateway.fake_paper_cancellations import (
    ScriptedPaperOrderCancellationGateway,
)
from ibmd.ib_gateway.fake_paper_orders import ScriptedPaperOrderGateway
from ibmd.public_contracts.broker_execution import BrokerOrderSide
from ibmd.public_contracts.broker_reconciliation import (
    BrokerOrderFactV1,
    BrokerOrderSource,
    BrokerReconciliationSnapshotV1,
)
from ibmd.public_contracts.execution import ExecutionReadinessStatus
from ibmd.public_contracts.liquidation import (
    LiquidationAttemptState,
    LiquidationNextAction,
    LiquidationOperationState,
    LiquidationReason,
)
from ibmd.public_contracts.protection import ProtectiveOrderState
from tester.target_execution_liquidation_tester import (
    flat_snapshot,
    live_protection,
)
from tester.target_execution_protective_lifecycle_tester import (
    _broker_snapshot as broker_snapshot,
    _completed_order as completed_order,
    _open_order as open_order,
)
from tester.target_execution_protective_submit_tester import (
    ACCOUNT,
    DEPLOYMENT,
    INSTRUMENT,
    STRATEGY,
    T2,
    T3,
    blocked_readiness,
    clock,
    episode_and_protection,
    position_snapshot,
    route,
    strategy_position,
)


class MemoryRepository:
    def __init__(self, *, episode, protection, position, readiness, liquidation) -> None:
        self.episode = episode
        self.protection = protection
        self.position = position
        self.readiness = readiness
        self.liquidation = liquidation
        self.published = []

    def read_episode(self, position_episode_id):
        return (
            self.episode
            if position_episode_id == self.episode.position_episode_id
            else None
        )

    def read_protection_by_episode(self, position_episode_id):
        return (
            self.protection
            if position_episode_id == self.episode.position_episode_id
            else None
        )

    def read_position(self, **_scope):
        return self.position

    def read_readiness(self, **_scope):
        return self.readiness

    def read_snapshot_by_episode(self, position_episode_id):
        return (
            self.liquidation
            if position_episode_id == self.episode.position_episode_id
            else None
        )

    def publish_state(
        self,
        *,
        current,
        updated,
        readiness,
        current_protection=None,
        updated_protection=None,
        episode=None,
        strategy_position=None,
        observation=None,
        source_session_id=None,
        captured_at_utc=None,
        fills=(),
    ):
        if current != self.liquidation:
            raise AssertionError("liquidation current state mismatch")
        if current_protection is not None:
            if current_protection != self.protection:
                raise AssertionError("protection current state mismatch")
            self.protection = updated_protection
        self.liquidation = updated
        self.readiness = readiness
        if episode is not None:
            self.episode = episode
        if strategy_position is not None:
            self.position = strategy_position
        self.published.append(
            {
                "liquidation": updated,
                "readiness": readiness,
                "observation": observation,
                "source_session_id": source_session_id,
                "captured_at_utc": captured_at_utc,
                "fills": tuple(fills),
            }
        )
        return updated


class PositionSource:
    def __init__(self, snapshot) -> None:
        self.snapshot = snapshot

    def read_latest_complete(self):
        return self.snapshot


class BrokerSource:
    def __init__(self, factory) -> None:
        self.factory = factory
        self.reads = 0

    async def read_snapshot(self, *, account_id):
        if account_id != ACCOUNT:
            raise AssertionError("unexpected broker account")
        self.reads += 1
        return self.factory()


def policy(*, environment="paper") -> PaperLiquidationPolicy:
    return PaperLiquidationPolicy(
        account_id=ACCOUNT,
        environment=environment,
        confirmed_paper_account_id=ACCOUNT,
        strategy_id=STRATEGY,
        strategy_version=1,
        deployment_id=DEPLOYMENT,
        instrument_id=INSTRUMENT,
        order_route=route(),
        position_max_age_seconds=10.0,
        reconciliation_read_attempts=2,
        reconciliation_poll_seconds=0.0,
    )


def requested_state(*, live_exits=False):
    if live_exits:
        episode, protection = live_protection()
        readiness = readiness_for_protection(
            blocked_readiness(),
            protection=protection,
            observed_at_utc=T2,
        )
    else:
        episode, protection = episode_and_protection()
        readiness = blocked_readiness()
    position = strategy_position(episode)
    liquidation = request_liquidation(
        episode=episode,
        position=position,
        readiness=readiness,
        reason=LiquidationReason.DAILY_FLAT,
        source_ref="paper-liquidation-test",
        observed_at_utc=T2,
    ).snapshot
    repository = MemoryRepository(
        episode=episode,
        protection=protection,
        position=position,
        readiness=readiness,
        liquidation=liquidation,
    )
    return repository


def close_order_fact(repository: MemoryRepository, *, filled: bool):
    attempt = repository.liquidation.attempt
    if attempt is None or attempt.broker_order_id is None:
        raise AssertionError("close attempt is not exposed")
    return BrokerOrderFactV1(
        account_id=ACCOUNT,
        order_ref=attempt.order_ref,
        broker_order_id=attempt.broker_order_id,
        broker_perm_id=9901,
        client_id=360,
        con_id=attempt.con_id,
        local_symbol=attempt.local_symbol,
        side=attempt.side,
        order_type="MARKET",
        requested_qty=attempt.requested_qty,
        filled_qty=(attempt.requested_qty if filled else 0),
        remaining_qty=(0 if filled else attempt.requested_qty),
        status=("Filled" if filled else "Submitted"),
        source=(BrokerOrderSource.COMPLETED if filled else BrokerOrderSource.OPEN),
        observed_at_utc=T3,
        completed_status=("Filled" if filled else None),
        warning_text=None,
    )


def close_snapshot(repository: MemoryRepository, *, filled: bool):
    order = close_order_fact(repository, filled=filled)
    return BrokerReconciliationSnapshotV1(
        source_session_id=new_id("ib_session"),
        account_id=ACCOUNT,
        captured_at_utc=T3,
        open_orders=(() if filled else (order,)),
        completed_orders=((order,) if filled else ()),
        fills=(),
        requests_complete=True,
    )


def empty_broker_snapshot():
    return BrokerReconciliationSnapshotV1(
        source_session_id=new_id("ib_session"),
        account_id=ACCOUNT,
        captured_at_utc=T3,
        open_orders=(),
        completed_orders=(),
        fills=(),
        requests_complete=True,
    )


class PaperLiquidationCoordinatorTest(unittest.TestCase):
    def test_take_profit_cancel_is_persisted_before_one_cancel_call(self) -> None:
        repository = requested_state(live_exits=True)
        stop = repository.protection.stop_order
        tp = repository.protection.take_profit_order

        broker = BrokerSource(
            lambda: broker_snapshot(
                open_orders=(open_order(stop, captured_at=T3),),
                completed_orders=(
                    completed_order(
                        tp,
                        state="Cancelled",
                        filled=0,
                        remaining=1,
                        captured_at=T3,
                    ),
                ),
            )
        )

        def before_cancel(request):
            self.assertEqual(request.order_ref, tp.order_ref)
            self.assertEqual(
                repository.protection.take_profit_order.state,
                ProtectiveOrderState.CANCEL_REQUESTED,
            )
            self.assertEqual(
                repository.liquidation.operation.next_action,
                LiquidationNextAction.RECONCILE_EXITS,
            )

        cancel_gateway = ScriptedPaperOrderCancellationGateway(
            before_cancel=before_cancel,
            clock=clock(T3),
        )
        order_gateway = ScriptedPaperOrderGateway(
            broker_order_id=8001,
            clock=clock(T3),
        )
        coordinator = PaperLiquidationCoordinator(
            policy=policy(),
            protection_source=repository,
            execution_state_source=repository,
            position_snapshot_source=PositionSource(position_snapshot()),
            repository=repository,
            order_gateway=order_gateway,
            cancellation_gateway=cancel_gateway,
            broker_snapshot_source=broker,
            clock=clock(T3),
        )
        result = asyncio.run(
            coordinator.run_once(
                position_episode_id=repository.episode.position_episode_id
            )
        )
        self.assertTrue(result.broker_mutation_performed)
        self.assertEqual(len(cancel_gateway.requests), 1)
        self.assertEqual(len(order_gateway.requests), 0)
        self.assertEqual(
            repository.protection.take_profit_order.state,
            ProtectiveOrderState.CANCELLED,
        )
        self.assertEqual(
            result.after.operation.next_action,
            LiquidationNextAction.CANCEL_STOP,
        )

    def test_market_close_is_submitting_before_call_and_not_duplicated(self) -> None:
        repository = requested_state()

        def before_submit(request):
            self.assertEqual(request.side, BrokerOrderSide.SELL)
            self.assertEqual(request.quantity, 1)
            self.assertEqual(
                repository.liquidation.operation.state,
                LiquidationOperationState.SUBMITTING,
            )
            self.assertEqual(
                repository.liquidation.attempt.state,
                LiquidationAttemptState.SUBMITTING,
            )
            self.assertEqual(
                repository.liquidation.attempt.broker_order_id,
                request.broker_order_id,
            )

        order_gateway = ScriptedPaperOrderGateway(
            broker_order_id=8001,
            before_submit=before_submit,
            clock=clock(T3),
        )
        broker = BrokerSource(lambda: close_snapshot(repository, filled=True))
        position_source = PositionSource(position_snapshot())
        coordinator = PaperLiquidationCoordinator(
            policy=policy(),
            protection_source=repository,
            execution_state_source=repository,
            position_snapshot_source=position_source,
            repository=repository,
            order_gateway=order_gateway,
            cancellation_gateway=ScriptedPaperOrderCancellationGateway(),
            broker_snapshot_source=broker,
            clock=clock(T3),
        )
        first = asyncio.run(
            coordinator.run_once(
                position_episode_id=repository.episode.position_episode_id
            )
        )
        self.assertTrue(first.broker_mutation_performed)
        self.assertEqual(len(order_gateway.requests), 1)
        self.assertEqual(
            first.after.attempt.state,
            LiquidationAttemptState.FILLED,
        )
        self.assertEqual(
            first.after.operation.next_action,
            LiquidationNextAction.WAIT_FOR_FLAT,
        )

        position_source.snapshot = flat_snapshot()
        second = asyncio.run(
            coordinator.run_once(
                position_episode_id=repository.episode.position_episode_id
            )
        )
        self.assertFalse(second.broker_mutation_performed)
        self.assertTrue(second.completion is not None)
        self.assertEqual(len(order_gateway.requests), 1)
        self.assertEqual(
            second.after.operation.state,
            LiquidationOperationState.SUCCEEDED,
        )
        self.assertEqual(
            repository.readiness.status,
            ExecutionReadinessStatus.READY,
        )

    def test_unknown_close_reconciles_without_second_submission(self) -> None:
        repository = requested_state()
        order_gateway = ScriptedPaperOrderGateway(
            broker_order_id=8001,
            submit_error=RuntimeError("disconnect after placeOrder"),
            clock=clock(T3),
        )
        broker = BrokerSource(empty_broker_snapshot)
        coordinator = PaperLiquidationCoordinator(
            policy=policy(),
            protection_source=repository,
            execution_state_source=repository,
            position_snapshot_source=PositionSource(position_snapshot()),
            repository=repository,
            order_gateway=order_gateway,
            cancellation_gateway=ScriptedPaperOrderCancellationGateway(),
            broker_snapshot_source=broker,
            clock=clock(T3),
        )
        first = asyncio.run(
            coordinator.run_once(
                position_episode_id=repository.episode.position_episode_id
            )
        )
        self.assertEqual(len(order_gateway.requests), 1)
        self.assertEqual(
            first.after.attempt.state,
            LiquidationAttemptState.UNKNOWN_OUTCOME,
        )
        self.assertEqual(
            first.after.operation.next_action,
            LiquidationNextAction.RECONCILE_MARKET_CLOSE,
        )
        second = asyncio.run(
            coordinator.run_once(
                position_episode_id=repository.episode.position_episode_id
            )
        )
        self.assertFalse(second.broker_mutation_performed)
        self.assertEqual(len(order_gateway.requests), 1)
        self.assertEqual(second.after.attempt.attempt_no, 1)

    def test_read_only_recovery_works_when_broker_actions_are_disabled(self) -> None:
        repository = requested_state()
        planned = plan_close_attempt(
            repository.liquidation,
            broker_quantity=1,
            observed_at_utc=T2,
        )
        repository.liquidation = mark_close_submitting(
            planned,
            broker_order_id=8001,
            observed_at_utc=T3,
        )
        repository.readiness = replace(
            repository.readiness,
            status=ExecutionReadinessStatus.BLOCKED,
            command_intake_enabled=False,
            broker_actions_enabled=False,
            blocking_reasons=("liquidation:recovery",),
            updated_at_utc=T3,
        )
        broker = BrokerSource(lambda: close_snapshot(repository, filled=True))
        order_gateway = ScriptedPaperOrderGateway(
            broker_order_id=9001,
            clock=clock(T3),
        )
        coordinator = PaperLiquidationCoordinator(
            policy=policy(),
            protection_source=repository,
            execution_state_source=repository,
            position_snapshot_source=PositionSource(position_snapshot()),
            repository=repository,
            order_gateway=order_gateway,
            cancellation_gateway=ScriptedPaperOrderCancellationGateway(),
            broker_snapshot_source=broker,
            clock=clock(T3),
        )
        result = asyncio.run(
            coordinator.run_once(
                position_episode_id=repository.episode.position_episode_id
            )
        )
        self.assertFalse(result.broker_mutation_performed)
        self.assertEqual(len(order_gateway.requests), 0)
        self.assertEqual(
            result.after.attempt.state,
            LiquidationAttemptState.FILLED,
        )

    def test_live_environment_is_rejected_before_any_mutation(self) -> None:
        repository = requested_state()
        order_gateway = ScriptedPaperOrderGateway(broker_order_id=8001)
        cancel_gateway = ScriptedPaperOrderCancellationGateway()
        coordinator = PaperLiquidationCoordinator(
            policy=policy(environment="live"),
            protection_source=repository,
            execution_state_source=repository,
            position_snapshot_source=PositionSource(position_snapshot()),
            repository=repository,
            order_gateway=order_gateway,
            cancellation_gateway=cancel_gateway,
            broker_snapshot_source=BrokerSource(empty_broker_snapshot),
            clock=clock(T3),
        )
        with self.assertRaisesRegex(PaperLiquidationError, "paper"):
            asyncio.run(
                coordinator.run_once(
                    position_episode_id=repository.episode.position_episode_id
                )
            )
        self.assertEqual(len(order_gateway.requests), 0)
        self.assertEqual(len(cancel_gateway.requests), 0)


if __name__ == "__main__":
    unittest.main()
