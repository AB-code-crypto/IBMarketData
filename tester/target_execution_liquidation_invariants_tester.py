from __future__ import annotations

import unittest
from dataclasses import replace

from ibmd.execution.domain.liquidation import (
    apply_close_observation,
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
from ibmd.public_contracts.broker_execution import (
    BrokerObservationOutcome,
    BrokerOrderObservationV1,
)
from ibmd.public_contracts.execution import (
    ExecutionReadinessStatus,
    PositionContractV1,
)
from ibmd.public_contracts.liquidation import (
    LiquidationOperationState,
    LiquidationReason,
)
from ibmd.public_contracts.positions import (
    BrokerPositionRowV1,
    BrokerPositionSnapshotV1,
)
from ibmd.public_contracts.protection import PositionEpisodeV1
from tester.target_execution_liquidation_tester import flat_snapshot
from tester.target_execution_protective_submit_tester import (
    ACCOUNT,
    CON_ID,
    INSTRUMENT,
    LOCAL_SYMBOL,
    T1,
    T2,
    T3,
    blocked_readiness,
    episode_and_protection,
    strategy_position,
)


class LiquidationInvariantTest(unittest.TestCase):
    def test_safety_trigger_is_recorded_when_broker_actions_disabled(self) -> None:
        episode, _ = episode_and_protection()
        readiness = replace(
            blocked_readiness(),
            broker_actions_enabled=False,
        )
        result = request_liquidation(
            episode=episode,
            position=strategy_position(episode),
            readiness=readiness,
            reason=LiquidationReason.MISSING_STOP,
            source_ref="missing-stop",
            observed_at_utc=T1,
        )
        self.assertTrue(result.operation_created)
        self.assertFalse(
            result.execution_readiness.command_intake_enabled
        )
        self.assertFalse(
            result.execution_readiness.broker_actions_enabled
        )
        self.assertEqual(
            result.execution_readiness.status,
            ExecutionReadinessStatus.BLOCKED,
        )

    def test_broker_quantity_above_episode_is_ownership_incident(self) -> None:
        episode, _ = episode_and_protection()
        snapshot = BrokerPositionSnapshotV1.complete(
            snapshot_id="position_snapshot_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            account_id=ACCOUNT,
            captured_at_utc=T2,
            published_at_utc=T2,
            source_session_id="ib_session_bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
            rows=(
                BrokerPositionRowV1(
                    con_id=CON_ID,
                    local_symbol=LOCAL_SYMBOL,
                    symbol=INSTRUMENT,
                    sec_type="FUT",
                    exchange="CME",
                    currency="USD",
                    signed_quantity=2,
                    average_cost=57_200.0,
                ),
            ),
        )
        proof = prove_liquidation_broker_position(
            snapshot=snapshot,
            episode=episode,
            observed_at_utc=T3,
            max_age_seconds=10.0,
        )
        self.assertEqual(proof.state, "INCIDENT")
        self.assertIn("exceeds_owned_episode", proof.reason)

    def test_partial_fill_is_counted_once_across_live_and_terminal(self) -> None:
        original_episode, _ = episode_and_protection()
        episode = replace(original_episode, quantity=2)
        original_position = strategy_position(original_episode)
        position = replace(
            original_position,
            position_episode_id=episode.position_episode_id,
            quantity=2,
            contracts=(
                PositionContractV1(
                    con_id=CON_ID,
                    local_symbol=LOCAL_SYMBOL,
                    signed_quantity=2,
                    contract_is_active=True,
                ),
            ),
        )
        requested = request_liquidation(
            episode=episode,
            position=position,
            readiness=blocked_readiness(),
            reason=LiquidationReason.DAILY_FLAT,
            source_ref="daily-flat",
            observed_at_utc=T1,
        ).snapshot
        planned = plan_close_attempt(
            requested,
            broker_quantity=2,
            observed_at_utc=T1,
        )
        submitting = mark_close_submitting(
            planned,
            broker_order_id=8001,
            observed_at_utc=T1,
        )
        attempt = submitting.attempt
        live = apply_close_observation(
            submitting,
            observation=BrokerOrderObservationV1(
                order_ref=attempt.order_ref,
                outcome=BrokerObservationOutcome.LIVE,
                observed_at_utc=T2,
                broker_order_id=8001,
                broker_perm_id=9001,
                broker_status="Submitted",
                requested_qty=2,
                filled_qty=1,
                remaining_qty=1,
                detail=None,
            ),
        )
        self.assertEqual(live.operation.liquidation_filled_quantity, 1)
        cancelled = apply_close_observation(
            live,
            observation=BrokerOrderObservationV1(
                order_ref=attempt.order_ref,
                outcome=BrokerObservationOutcome.CANCELLED,
                observed_at_utc=T3,
                broker_order_id=8001,
                broker_perm_id=9001,
                broker_status="Cancelled",
                requested_qty=2,
                filled_qty=1,
                remaining_qty=1,
                detail="terminal partial fill",
            ),
        )
        self.assertEqual(cancelled.operation.liquidation_filled_quantity, 1)
        self.assertEqual(cancelled.operation.broker_remaining_quantity, 1)
        self.assertEqual(
            cancelled.operation.state,
            LiquidationOperationState.FAILED_RETRYABLE,
        )

    def test_closed_episode_links_the_liquidation_operation(self) -> None:
        episode, protection = episode_and_protection()
        request = request_liquidation(
            episode=episode,
            position=strategy_position(episode),
            readiness=blocked_readiness(),
            reason=LiquidationReason.MANUAL_EMERGENCY,
            source_ref="operator",
            observed_at_utc=T1,
        ).snapshot
        terminal = mark_broker_flat(request, observed_at_utc=T3)
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
        self.assertEqual(
            completion.episode.closing_operation_id,
            terminal.operation.liquidation_operation_id,
        )
        self.assertEqual(
            PositionEpisodeV1.from_dict(completion.episode.to_dict()),
            completion.episode,
        )


if __name__ == "__main__":
    unittest.main()
