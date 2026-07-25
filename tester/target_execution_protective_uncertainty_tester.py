from __future__ import annotations

import unittest

from ibmd.execution.domain.protection import apply_protective_observation
from ibmd.execution.domain.protective_submission import (
    mark_protective_order_submitting,
)
from ibmd.execution.domain.protective_uncertainty import (
    mark_protective_order_unknown,
    readiness_for_protection,
)
from ibmd.public_contracts.broker_execution import (
    BrokerObservationOutcome,
    BrokerOrderObservationV1,
)
from ibmd.public_contracts.execution import (
    ExecutionReadinessStatus,
    ExecutionReadinessV1,
)
from ibmd.public_contracts.protection import (
    ProtectionSetStatus,
    ProtectiveOrderKind,
    ProtectiveOrderState,
)
from tester.target_execution_protective_submit_tester import (
    ACCOUNT,
    DEPLOYMENT,
    INSTRUMENT,
    STRATEGY,
    T1,
    T2,
    T3,
    episode_and_protection,
)


class ProtectiveUncertaintyTest(unittest.TestCase):
    def test_unknown_tp_keeps_stop_live_and_blocks_command_intake(self) -> None:
        _episode, protection = episode_and_protection()
        stop_submitting = mark_protective_order_submitting(
            protection,
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
                observed_at_utc=T2,
                broker_order_id=7001,
                broker_perm_id=97001,
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
        uncertain = mark_protective_order_unknown(
            tp_submitting,
            kind=ProtectiveOrderKind.TAKE_PROFIT,
            observed_at_utc=T3,
            reason="completed broker query did not prove TP outcome",
        )

        self.assertEqual(uncertain.status, ProtectionSetStatus.STOP_LIVE)
        self.assertEqual(
            uncertain.stop_order.state,
            ProtectiveOrderState.LIVE,
        )
        self.assertEqual(
            uncertain.take_profit_order.state,
            ProtectiveOrderState.UNKNOWN_OUTCOME,
        )

        current = ExecutionReadinessV1(
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
            updated_at_utc=T2,
        )
        readiness = readiness_for_protection(
            current,
            protection=uncertain,
            observed_at_utc=T3,
        )
        self.assertEqual(
            readiness.status,
            ExecutionReadinessStatus.BLOCKED,
        )
        self.assertFalse(readiness.command_intake_enabled)
        self.assertIn(
            "protection:take_profit_outcome_unproven",
            readiness.blocking_reasons,
        )


if __name__ == "__main__":
    unittest.main()
