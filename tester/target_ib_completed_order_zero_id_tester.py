from __future__ import annotations

import unittest
from dataclasses import replace
from types import SimpleNamespace

from ibmd.execution.domain.broker_attempt import apply_broker_observation
from ibmd.execution.domain.ib_reconciliation import (
    reconcile_broker_attempt_snapshot,
)
from ibmd.execution.domain.liquidation import (
    apply_close_observation,
    mark_close_submitting,
    plan_close_attempt,
)
from ibmd.execution.domain.liquidation_reconciliation import (
    reconcile_liquidation_attempt_snapshot,
)
from ibmd.execution.domain.protection import apply_protective_observation
from ibmd.execution.domain.protective_submission import (
    mark_protective_order_submitting,
    reconcile_protective_order_snapshot,
)
from ibmd.execution.domain.liquidation import mark_protective_cancel_requested
from ibmd.foundation.identity import new_id
from ibmd.ib_gateway.broker_reconciliation_mapping import (
    order_fact_from_ib_trade,
)
from ibmd.public_contracts.broker_execution import (
    BrokerObservationOutcome,
    BrokerOrderSide,
)
from ibmd.public_contracts.broker_reconciliation import (
    BrokerOrderFactV1,
    BrokerOrderSource,
    BrokerReconciliationContractError,
    BrokerReconciliationSnapshotV1,
)
from ibmd.public_contracts.protection import (
    ProtectiveOrderKind,
    ProtectiveOrderState,
)
from tester.target_execution_liquidation_tester import request_snapshot
from tester.target_execution_protective_submit_tester import (
    ACCOUNT,
    CON_ID,
    LOCAL_SYMBOL,
    T1,
    T2,
    T3,
    episode_and_protection,
)
from tester.target_ib_broker_reconciliation_tester import (
    ORDER_ID,
    submitting_snapshot,
)


def completed_fact(
    *,
    order_ref: str,
    side: BrokerOrderSide,
    order_type: str,
    broker_order_id: int | None = None,
    broker_perm_id: int | None = 9_001,
    status: str = "Cancelled",
    completed_status: str = "Cancelled",
    filled_qty: int = 0,
    remaining_qty: int = 1,
) -> BrokerOrderFactV1:
    return BrokerOrderFactV1(
        account_id=ACCOUNT,
        order_ref=order_ref,
        broker_order_id=broker_order_id,
        broker_perm_id=broker_perm_id,
        client_id=0,
        con_id=CON_ID,
        local_symbol=LOCAL_SYMBOL,
        side=side,
        order_type=order_type,
        requested_qty=1,
        filled_qty=filled_qty,
        remaining_qty=remaining_qty,
        status=status,
        source=BrokerOrderSource.COMPLETED,
        observed_at_utc=T3,
        completed_status=completed_status,
        warning_text=None,
    )


def snapshot(*facts: BrokerOrderFactV1) -> BrokerReconciliationSnapshotV1:
    return BrokerReconciliationSnapshotV1(
        source_session_id=new_id("ib_session"),
        account_id=ACCOUNT,
        captured_at_utc=T3,
        open_orders=(),
        completed_orders=tuple(facts),
        fills=(),
        requests_complete=True,
    )


def completed_ib_trade(*, order_ref: str, perm_id: int = 9_001):
    return SimpleNamespace(
        contract=SimpleNamespace(conId=CON_ID, localSymbol=LOCAL_SYMBOL),
        order=SimpleNamespace(
            account=ACCOUNT,
            orderId=0,
            permId=perm_id,
            clientId=0,
            totalQuantity=1,
            action="SELL",
            orderType="LMT",
            orderRef=order_ref,
        ),
        orderStatus=SimpleNamespace(
            orderId=0,
            permId=perm_id,
            status="Cancelled",
            filled=0,
            remaining=0,
        ),
        orderState=SimpleNamespace(
            completedStatus="Cancelled",
            warningText="",
        ),
        log=(),
    )


class CompletedOrderZeroIdMappingTest(unittest.TestCase):
    def test_completed_ib_order_with_zero_api_id_maps_by_perm_and_ref(self) -> None:
        order_ref = "IBMD:protection_set_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa:TP"
        fact = order_fact_from_ib_trade(
            completed_ib_trade(order_ref=order_ref),
            expected_account_id=ACCOUNT,
            source=BrokerOrderSource.COMPLETED,
            observed_at_utc=T3,
        )
        self.assertIsNotNone(fact)
        self.assertIsNone(fact.broker_order_id)
        self.assertEqual(fact.broker_perm_id, 9_001)
        self.assertEqual(fact.order_ref, order_ref)
        self.assertEqual(fact.remaining_qty, 1)

    def test_completed_fact_can_fall_back_to_stable_order_ref(self) -> None:
        first = completed_fact(
            order_ref="IBMD:protection_set_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa:SL",
            side=BrokerOrderSide.SELL,
            order_type="STP",
            broker_order_id=None,
            broker_perm_id=None,
        )
        second = completed_fact(
            order_ref="IBMD:protection_set_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa:TP",
            side=BrokerOrderSide.SELL,
            order_type="LMT",
            broker_order_id=None,
            broker_perm_id=None,
        )
        value = snapshot(first, second)
        self.assertEqual(len(value.completed_orders), 2)
        self.assertNotEqual(first.broker_identity, second.broker_identity)

    def test_open_fact_still_requires_positive_api_order_id(self) -> None:
        with self.assertRaisesRegex(
            BrokerReconciliationContractError,
            "open broker order fact requires broker_order_id",
        ):
            BrokerOrderFactV1(
                account_id=ACCOUNT,
                order_ref="IBMD:broker_operation_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa:1",
                broker_order_id=None,
                broker_perm_id=9_001,
                client_id=0,
                con_id=CON_ID,
                local_symbol=LOCAL_SYMBOL,
                side=BrokerOrderSide.BUY,
                order_type="MARKET",
                requested_qty=1,
                filled_qty=0,
                remaining_qty=1,
                status="Submitted",
                source=BrokerOrderSource.OPEN,
                observed_at_utc=T3,
                completed_status=None,
                warning_text=None,
            )


class CompletedOrderZeroIdDomainTest(unittest.TestCase):
    def test_strategic_cancel_preserves_persisted_api_order_id(self) -> None:
        current = submitting_snapshot()
        fact = completed_fact(
            order_ref=current.attempt.order_ref,
            side=current.attempt.side,
            order_type=current.attempt.order_type,
            broker_perm_id=9_101,
        )
        result = reconcile_broker_attempt_snapshot(
            broker_snapshot=snapshot(fact),
            current=current,
        )
        self.assertEqual(
            result.observation.outcome,
            BrokerObservationOutcome.CANCELLED,
        )
        self.assertIsNone(result.observation.broker_order_id)
        updated = apply_broker_observation(
            current,
            observation=result.observation,
        )
        self.assertEqual(updated.attempt.broker_order_id, ORDER_ID)
        self.assertEqual(updated.attempt.broker_perm_id, 9_101)

    def test_protective_cancel_preserves_persisted_api_order_id(self) -> None:
        episode, planned = episode_and_protection()
        submitting = mark_protective_order_submitting(
            planned,
            kind=ProtectiveOrderKind.STOP_LOSS,
            broker_order_id=7_001,
            observed_at_utc=T1,
        )
        live = apply_protective_observation(
            protection=submitting,
            kind=ProtectiveOrderKind.STOP_LOSS,
            observation=SimpleNamespace(),
            position_open=True,
        )
        # Build the LIVE state through the public observation contract.
        from ibmd.public_contracts.broker_execution import BrokerOrderObservationV1

        live = apply_protective_observation(
            protection=submitting,
            kind=ProtectiveOrderKind.STOP_LOSS,
            observation=BrokerOrderObservationV1(
                order_ref=submitting.stop_order.order_ref,
                outcome=BrokerObservationOutcome.LIVE,
                observed_at_utc=T2,
                broker_order_id=7_001,
                broker_perm_id=9_201,
                broker_status="Submitted",
                requested_qty=1,
                filled_qty=0,
                remaining_qty=1,
                detail=None,
            ),
            position_open=True,
        )
        cancel_requested = mark_protective_cancel_requested(
            live,
            kind=ProtectiveOrderKind.STOP_LOSS,
            observed_at_utc=T2,
        )
        fact = completed_fact(
            order_ref=cancel_requested.stop_order.order_ref,
            side=BrokerOrderSide.SELL,
            order_type="STP",
            broker_perm_id=9_201,
        )
        result = reconcile_protective_order_snapshot(
            broker_snapshot=snapshot(fact),
            episode=episode,
            protection=cancel_requested,
            kind=ProtectiveOrderKind.STOP_LOSS,
        )
        self.assertEqual(
            result.observation.outcome,
            BrokerObservationOutcome.CANCELLED,
        )
        updated = apply_protective_observation(
            protection=cancel_requested,
            kind=ProtectiveOrderKind.STOP_LOSS,
            observation=result.observation,
            position_open=True,
        )
        self.assertEqual(updated.stop_order.state, ProtectiveOrderState.CANCELLED)
        self.assertEqual(updated.stop_order.broker_order_id, 7_001)
        self.assertEqual(updated.stop_order.broker_perm_id, 9_201)

    def test_liquidation_cancel_fact_does_not_conflict_with_attempt_id(self) -> None:
        requested = request_snapshot().snapshot
        planned = plan_close_attempt(
            requested,
            broker_quantity=1,
            observed_at_utc=T1,
        )
        submitting = mark_close_submitting(
            planned,
            broker_order_id=8_001,
            observed_at_utc=T2,
        )
        fact = completed_fact(
            order_ref=submitting.attempt.order_ref,
            side=BrokerOrderSide.SELL,
            order_type="MARKET",
            broker_perm_id=9_301,
        )
        result = reconcile_liquidation_attempt_snapshot(
            broker_snapshot=snapshot(fact),
            current=submitting,
        )
        self.assertEqual(
            result.observation.outcome,
            BrokerObservationOutcome.CANCELLED,
        )
        updated = apply_close_observation(
            submitting,
            observation=result.observation,
        )
        self.assertEqual(updated.attempt.broker_order_id, 8_001)
        self.assertEqual(updated.attempt.broker_perm_id, 9_301)


if __name__ == "__main__":
    unittest.main()
