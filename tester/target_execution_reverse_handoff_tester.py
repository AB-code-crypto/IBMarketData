from __future__ import annotations

import asyncio
import unittest
from dataclasses import replace
from datetime import datetime

from ibmd.execution.application.protective_lifecycle import (
    ProtectiveLifecycleUpdate,
)
from ibmd.execution.application.reverse_handoff import (
    PaperReverseHandoffCoordinator,
    PaperReverseHandoffError,
    PaperReverseHandoffPolicyV1,
    PersistedReverseSubmitGuard,
)
from ibmd.execution.domain.protection import apply_protective_observation
from ibmd.execution.domain.reverse_handoff import (
    ReverseHandoffAction,
    assess_reverse_handoff,
    mark_reverse_cancel_requested,
    require_reverse_ready_for_submit,
)
from ibmd.execution.domain.protective_uncertainty import readiness_for_protection
from ibmd.ib_gateway.fake_paper_cancellations import (
    ScriptedPaperOrderCancellationGateway,
)
from ibmd.public_contracts.broker_execution import (
    BrokerObservationOutcome,
    BrokerOrderObservationV1,
)
from ibmd.public_contracts.decision import (
    DesiredTargetSide,
    StrategyCommandKind,
    StrategyCommandRequestV1,
)
from ibmd.public_contracts.execution import (
    ExecutionCommandState,
    ExecutionCommandStateV1,
)
from ibmd.public_contracts.protection import (
    ProtectionSetStatus,
    ProtectiveOrderKind,
    ProtectiveOrderState,
)
from tester.target_execution_liquidation_tester import live_protection
from tester.target_execution_protective_submit_tester import (
    ACCOUNT,
    DEPLOYMENT,
    INSTRUMENT,
    STRATEGY,
    T0,
    T2,
    T3,
    blocked_readiness,
    position_snapshot,
    strategy_position,
)

POLICY_HASH = "f" * 64
COMMAND_ID = "strategy_command_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
SIGNAL_ID = "signal_event_bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"


def clock(value: str):
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    return lambda: parsed


def reverse_command() -> ExecutionCommandStateV1:
    return ExecutionCommandStateV1(
        command_id=COMMAND_ID,
        strategy_id=STRATEGY,
        strategy_version=1,
        deployment_id=DEPLOYMENT,
        instrument_id=INSTRUMENT,
        command_kind=StrategyCommandKind.REVERSE,
        desired_target_side=DesiredTargetSide.SHORT,
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


def reverse_request() -> StrategyCommandRequestV1:
    return StrategyCommandRequestV1(
        command_id=COMMAND_ID,
        strategy_id=STRATEGY,
        strategy_version=1,
        deployment_id=DEPLOYMENT,
        instrument_id=INSTRUMENT,
        source_signal_id=SIGNAL_ID,
        desired_target_side=DesiredTargetSide.SHORT,
        desired_target_quantity=1,
        command_kind=StrategyCommandKind.REVERSE,
        reason="reverse_handoff_test",
        created_at_utc=T0,
        expires_at_utc="2026-07-27T11:00:00Z",
        policy_hash=POLICY_HASH,
    )


def observation(order, outcome, *, observed_at=T2):
    return BrokerOrderObservationV1(
        order_ref=order.order_ref,
        outcome=outcome,
        observed_at_utc=observed_at,
        broker_order_id=order.broker_order_id,
        broker_perm_id=9000 + order.planned_sequence,
        broker_status=outcome.value,
        requested_qty=order.quantity,
        filled_qty=(order.quantity if outcome == BrokerObservationOutcome.FILLED else 0),
        remaining_qty=(0 if outcome == BrokerObservationOutcome.FILLED else order.quantity),
        detail=None,
    )


def cancelled(protection, kind, *, observed_at=T2):
    order = (
        protection.stop_order
        if kind == ProtectiveOrderKind.STOP_LOSS
        else protection.take_profit_order
    )
    return apply_protective_observation(
        protection=protection,
        kind=kind,
        observation=observation(
            order,
            BrokerObservationOutcome.CANCELLED,
            observed_at=observed_at,
        ),
        position_open=True,
    )


class MemoryReverseRepository:
    def __init__(self):
        self.episode, self.protection = live_protection()
        self.position = strategy_position(self.episode)
        self.readiness = readiness_for_protection(
            blocked_readiness(),
            protection=self.protection,
            observed_at_utc=T2,
        )
        self.command = reverse_command()
        self.request = reverse_request()
        self.liquidation = None

    def read_command_state(self, command_id):
        return self.command if command_id == self.command.command_id else None

    def read_command(self, command_id):
        return self.request if command_id == self.request.command_id else None

    def read_position(self, **_scope):
        return self.position

    def read_readiness(self, **_scope):
        return self.readiness

    def read_episode(self, position_episode_id):
        return self.episode if position_episode_id == self.episode.position_episode_id else None

    def read_protection_by_episode(self, position_episode_id):
        return self.protection if position_episode_id == self.episode.position_episode_id else None

    def read_latest_complete(self):
        return position_snapshot()

    def read_snapshot_by_episode(self, position_episode_id):
        return self.liquidation if position_episode_id == self.episode.position_episode_id else None

    def publish_state_and_readiness(self, *, current, updated, readiness):
        if current.to_dict() != self.protection.to_dict():
            raise AssertionError("protection changed concurrently")
        self.protection = updated
        self.readiness = readiness
        return updated


class FakeLifecycleService:
    def __init__(
        self,
        repository: MemoryReverseRepository,
        *,
        delay_cancel_confirmation: bool = False,
    ):
        self.repository = repository
        self.calls = 0
        self.cancelled_kind = None
        self.delay_cancel_confirmation = bool(delay_cancel_confirmation)
        self.delayed_kinds = set()

    async def run_once(self, *, position_episode_id, observed_at_utc):
        self.calls += 1
        if position_episode_id != self.repository.episode.position_episode_id:
            raise AssertionError("unexpected episode")
        if self.cancelled_kind is not None:
            kind = self.cancelled_kind
            order = (
                self.repository.protection.stop_order
                if kind == ProtectiveOrderKind.STOP_LOSS
                else self.repository.protection.take_profit_order
            )
            if (
                self.delay_cancel_confirmation
                and kind not in self.delayed_kinds
            ):
                self.repository.protection = apply_protective_observation(
                    protection=self.repository.protection,
                    kind=kind,
                    observation=observation(
                        order,
                        BrokerObservationOutcome.LIVE,
                        observed_at=observed_at_utc,
                    ),
                    position_open=True,
                )
                self.delayed_kinds.add(kind)
            else:
                self.repository.protection = cancelled(
                    self.repository.protection,
                    kind,
                    observed_at=observed_at_utc,
                )
                self.cancelled_kind = None
            self.repository.readiness = readiness_for_protection(
                self.repository.readiness,
                protection=self.repository.protection,
                observed_at_utc=observed_at_utc,
            )
        return ProtectiveLifecycleUpdate(
            episode=self.repository.episode,
            protection=self.repository.protection,
            strategy_position=self.repository.position,
            execution_readiness=self.repository.readiness,
            evidence=(),
            broker_position_state="OPEN",
            episode_closed=False,
            commission_complete=None,
        )


class ReverseHandoffDomainTest(unittest.TestCase):
    def test_live_take_profit_then_stop_are_cancelled_in_order(self) -> None:
        episode, protection = live_protection()
        first = assess_reverse_handoff(protection)
        self.assertEqual(first.action, ReverseHandoffAction.CANCEL_TAKE_PROFIT)
        after_tp = cancelled(protection, ProtectiveOrderKind.TAKE_PROFIT)
        second = assess_reverse_handoff(after_tp)
        self.assertEqual(second.action, ReverseHandoffAction.CANCEL_STOP)
        after_stop = cancelled(after_tp, ProtectiveOrderKind.STOP_LOSS)
        third = assess_reverse_handoff(after_stop)
        self.assertEqual(third.action, ReverseHandoffAction.READY_TO_SUBMIT)
        require_reverse_ready_for_submit(
            command=reverse_command(),
            position=strategy_position(episode),
            episode=episode,
            protection=after_stop,
        )

    def test_unknown_or_filled_exit_never_allows_reverse(self) -> None:
        _episode, protection = live_protection()
        unknown = replace(
            protection,
            orders=tuple(
                replace(
                    item,
                    state=ProtectiveOrderState.UNKNOWN_OUTCOME,
                    updated_at_utc=T3,
                    failure_reason="unknown",
                )
                if item.kind == ProtectiveOrderKind.TAKE_PROFIT
                else item
                for item in protection.orders
            ),
            status=ProtectionSetStatus.STOP_LIVE,
            updated_at_utc=T3,
            blocking_reason="tp_unknown",
        )
        self.assertEqual(
            assess_reverse_handoff(unknown).action,
            ReverseHandoffAction.RECONCILE_EXITS,
        )
        filled = apply_protective_observation(
            protection=protection,
            kind=ProtectiveOrderKind.TAKE_PROFIT,
            observation=observation(
                protection.take_profit_order,
                BrokerObservationOutcome.FILLED,
                observed_at=T3,
            ),
            position_open=True,
        )
        self.assertEqual(
            assess_reverse_handoff(filled).action,
            ReverseHandoffAction.OPERATOR_REQUIRED,
        )

    def test_cancel_request_is_persisted_as_unprotected(self) -> None:
        _episode, protection = live_protection()
        value = mark_reverse_cancel_requested(
            protection,
            kind=ProtectiveOrderKind.TAKE_PROFIT,
            command_id=COMMAND_ID,
            observed_at_utc=T3,
        )
        self.assertEqual(
            value.take_profit_order.state,
            ProtectiveOrderState.CANCEL_REQUESTED,
        )
        self.assertEqual(value.status, ProtectionSetStatus.UNPROTECTED)
        self.assertIn(COMMAND_ID, value.take_profit_order.failure_reason)

    def test_live_broker_fact_preserves_persisted_cancel_intent(self) -> None:
        _episode, protection = live_protection()
        requested = mark_reverse_cancel_requested(
            protection,
            kind=ProtectiveOrderKind.TAKE_PROFIT,
            command_id=COMMAND_ID,
            observed_at_utc=T3,
        )
        reconciled = apply_protective_observation(
            protection=requested,
            kind=ProtectiveOrderKind.TAKE_PROFIT,
            observation=observation(
                requested.take_profit_order,
                BrokerObservationOutcome.LIVE,
                observed_at=T3,
            ),
            position_open=True,
        )
        self.assertEqual(
            reconciled.take_profit_order.state,
            ProtectiveOrderState.CANCEL_REQUESTED,
        )
        self.assertEqual(
            assess_reverse_handoff(reconciled).action,
            ReverseHandoffAction.RECONCILE_EXITS,
        )
        self.assertIn(
            COMMAND_ID,
            reconciled.take_profit_order.failure_reason,
        )


class ReverseHandoffCoordinatorTest(unittest.TestCase):
    def test_two_cancellations_then_ready_without_duplicate_cancel(self) -> None:
        repository = MemoryReverseRepository()
        lifecycle = FakeLifecycleService(repository)

        def before_cancel(request):
            order = next(
                item
                for item in repository.protection.orders
                if item.order_ref == request.order_ref
            )
            lifecycle.cancelled_kind = order.kind

        gateway = ScriptedPaperOrderCancellationGateway(
            before_cancel=before_cancel,
            clock=clock(T3),
        )
        coordinator = PaperReverseHandoffCoordinator(
            policy=PaperReverseHandoffPolicyV1(
                account_id=ACCOUNT,
                environment="paper",
                confirmed_paper_account_id=ACCOUNT,
                strategy_id=STRATEGY,
                strategy_version=1,
                deployment_id=DEPLOYMENT,
                instrument_id=INSTRUMENT,
                policy_hash=POLICY_HASH,
                position_max_age_seconds=10.0,
            ),
            command_state_source=repository,
            command_request_source=repository,
            execution_state_source=repository,
            protection_state_source=repository,
            position_snapshot_source=repository,
            protection_repository=repository,
            liquidation_state_source=repository,
            lifecycle_service=lifecycle,
            cancellation_gateway=gateway,
            clock=clock(T3),
        )
        first = asyncio.run(coordinator.run_once(command_id=COMMAND_ID))
        self.assertTrue(first.broker_mutation_performed)
        self.assertEqual(len(gateway.requests), 1)
        self.assertEqual(
            first.assessment.action,
            ReverseHandoffAction.CANCEL_STOP,
        )
        second = asyncio.run(coordinator.run_once(command_id=COMMAND_ID))
        self.assertTrue(second.broker_mutation_performed)
        self.assertEqual(len(gateway.requests), 2)
        self.assertTrue(second.ready_for_reverse_submit)
        third = asyncio.run(coordinator.run_once(command_id=COMMAND_ID))
        self.assertFalse(third.broker_mutation_performed)
        self.assertEqual(len(gateway.requests), 2)
        self.assertTrue(third.ready_for_reverse_submit)

    def test_delayed_cancel_confirmation_never_repeats_cancel_order(self) -> None:
        repository = MemoryReverseRepository()
        lifecycle = FakeLifecycleService(
            repository,
            delay_cancel_confirmation=True,
        )

        def before_cancel(request):
            order = next(
                item
                for item in repository.protection.orders
                if item.order_ref == request.order_ref
            )
            lifecycle.cancelled_kind = order.kind

        gateway = ScriptedPaperOrderCancellationGateway(
            before_cancel=before_cancel,
            clock=clock(T3),
        )
        coordinator = PaperReverseHandoffCoordinator(
            policy=PaperReverseHandoffPolicyV1(
                account_id=ACCOUNT,
                environment="paper",
                confirmed_paper_account_id=ACCOUNT,
                strategy_id=STRATEGY,
                strategy_version=1,
                deployment_id=DEPLOYMENT,
                instrument_id=INSTRUMENT,
                policy_hash=POLICY_HASH,
                position_max_age_seconds=10.0,
            ),
            command_state_source=repository,
            command_request_source=repository,
            execution_state_source=repository,
            protection_state_source=repository,
            position_snapshot_source=repository,
            protection_repository=repository,
            liquidation_state_source=repository,
            lifecycle_service=lifecycle,
            cancellation_gateway=gateway,
            clock=clock(T3),
        )
        first = asyncio.run(coordinator.run_once(command_id=COMMAND_ID))
        self.assertTrue(first.broker_mutation_performed)
        self.assertEqual(len(gateway.requests), 1)
        self.assertEqual(
            first.assessment.action,
            ReverseHandoffAction.RECONCILE_EXITS,
        )
        self.assertEqual(
            repository.protection.take_profit_order.state,
            ProtectiveOrderState.CANCEL_REQUESTED,
        )

        second = asyncio.run(coordinator.run_once(command_id=COMMAND_ID))
        self.assertTrue(second.broker_mutation_performed)
        self.assertEqual(len(gateway.requests), 2)
        self.assertEqual(
            second.assessment.action,
            ReverseHandoffAction.RECONCILE_EXITS,
        )
        self.assertEqual(
            repository.protection.stop_order.state,
            ProtectiveOrderState.CANCEL_REQUESTED,
        )

        third = asyncio.run(coordinator.run_once(command_id=COMMAND_ID))
        self.assertFalse(third.broker_mutation_performed)
        self.assertEqual(len(gateway.requests), 2)
        self.assertTrue(third.ready_for_reverse_submit)

        fourth = asyncio.run(coordinator.run_once(command_id=COMMAND_ID))
        self.assertFalse(fourth.broker_mutation_performed)
        self.assertEqual(len(gateway.requests), 2)
        self.assertTrue(fourth.ready_for_reverse_submit)

    def test_existing_liquidation_blocks_reverse_handoff(self) -> None:
        repository = MemoryReverseRepository()
        repository.liquidation = object()
        lifecycle = FakeLifecycleService(repository)
        coordinator = PaperReverseHandoffCoordinator(
            policy=PaperReverseHandoffPolicyV1(
                account_id=ACCOUNT,
                environment="paper",
                confirmed_paper_account_id=ACCOUNT,
                strategy_id=STRATEGY,
                strategy_version=1,
                deployment_id=DEPLOYMENT,
                instrument_id=INSTRUMENT,
                policy_hash=POLICY_HASH,
            ),
            command_state_source=repository,
            command_request_source=repository,
            execution_state_source=repository,
            protection_state_source=repository,
            position_snapshot_source=repository,
            protection_repository=repository,
            liquidation_state_source=repository,
            lifecycle_service=lifecycle,
            cancellation_gateway=ScriptedPaperOrderCancellationGateway(),
            clock=clock(T3),
        )
        with self.assertRaisesRegex(
            PaperReverseHandoffError,
            "liquidation operation",
        ):
            asyncio.run(coordinator.run_once(command_id=COMMAND_ID))

    def test_persisted_submit_guard_rejects_live_protection(self) -> None:
        repository = MemoryReverseRepository()
        guard = PersistedReverseSubmitGuard(
            protection_state_source=repository,
            liquidation_state_source=repository,
        )
        with self.assertRaisesRegex(
            PaperReverseHandoffError,
            "protective handoff is incomplete",
        ):
            guard.require_ready(
                command=repository.command,
                position=repository.position,
            )


if __name__ == "__main__":
    unittest.main()
