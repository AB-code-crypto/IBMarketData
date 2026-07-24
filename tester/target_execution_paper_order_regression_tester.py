from __future__ import annotations

import unittest
from types import SimpleNamespace

from ibmd.execution.application.new_risk_window import (
    broker_operation_requires_new_risk_gate,
)
from ibmd.ib_gateway.broker_reconciliation import BrokerReconciliationReadError
from ibmd.ib_gateway.broker_reconciliation_mapping import (
    order_fact_from_ib_trade,
)
from ibmd.ib_gateway.ib_async_paper_orders import build_paper_market_order
from ibmd.ib_gateway.paper_orders import (
    PaperMarketOrderRequest,
    PaperOrderRoute,
)
from ibmd.public_contracts.broker_execution import (
    BrokerOperationState,
    BrokerOrderSide,
)
from ibmd.public_contracts.broker_reconciliation import BrokerOrderSource

ACCOUNT = "DU000000"
ORDER_REF = "IBMD:broker_operation_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa:1"
OBSERVED_AT = "2026-07-24T19:26:14Z"


def request() -> PaperMarketOrderRequest:
    return PaperMarketOrderRequest(
        account_id=ACCOUNT,
        broker_order_id=1,
        order_ref=ORDER_REF,
        side=BrokerOrderSide.BUY,
        quantity=1,
        route=PaperOrderRoute(
            instrument_id="MNQ",
            con_id=793_356_225,
            local_symbol="MNQU6",
            last_trade_date="20260918",
            sec_type="FUT",
            exchange="CME",
            currency="USD",
            trading_class="MNQ",
            multiplier=2.0,
        ),
    )


def trade(*, status: str, remaining: float, message: str = ""):
    return SimpleNamespace(
        contract=SimpleNamespace(
            conId=793_356_225,
            localSymbol="MNQU6",
        ),
        order=SimpleNamespace(
            account=ACCOUNT,
            orderId=1,
            permId=0,
            clientId=320,
            totalQuantity=1,
            action="BUY",
            orderType="MKT",
            orderRef=ORDER_REF,
        ),
        orderStatus=SimpleNamespace(
            orderId=1,
            permId=0,
            status=status,
            filled=0.0,
            remaining=remaining,
        ),
        orderState=None,
        log=(
            SimpleNamespace(
                errorCode=10349 if message else 0,
                message=message,
            ),
        ),
    )


class PaperOrderRegressionTest(unittest.TestCase):
    def test_market_order_sets_day_tif_explicitly(self) -> None:
        order = build_paper_market_order(request())
        self.assertEqual(order.tif, "DAY")
        self.assertEqual(order.orderType, "MKT")
        self.assertEqual(order.orderRef, ORDER_REF)
        self.assertEqual(order.account, ACCOUNT)

    def test_cancelled_validation_order_normalizes_unfilled_quantity(self) -> None:
        fact = order_fact_from_ib_trade(
            trade=trade(
                status="Cancelled",
                remaining=0.0,
                message=(
                    "Error 10349, reqId 1: Order TIF was set to DAY based "
                    "on order preset"
                ),
            ),
            expected_account_id=ACCOUNT,
            source=BrokerOrderSource.COMPLETED,
            observed_at_utc=OBSERVED_AT,
        )
        self.assertIsNotNone(fact)
        self.assertEqual(fact.filled_qty, 0)
        self.assertEqual(fact.remaining_qty, 1)
        self.assertEqual(fact.status, "Cancelled")
        self.assertIn("10349", fact.warning_text)

    def test_active_order_quantity_disagreement_remains_rejected(self) -> None:
        with self.assertRaisesRegex(
            BrokerReconciliationReadError,
            "IB order quantities disagree",
        ):
            order_fact_from_ib_trade(
                trade=trade(status="Submitted", remaining=0.0),
                expected_account_id=ACCOUNT,
                source=BrokerOrderSource.OPEN,
                observed_at_utc=OBSERVED_AT,
            )

    def test_only_new_or_preparing_operation_uses_new_risk_gate(self) -> None:
        self.assertTrue(broker_operation_requires_new_risk_gate(None))
        self.assertTrue(
            broker_operation_requires_new_risk_gate(
                BrokerOperationState.PREPARING
            )
        )
        for state in (
            BrokerOperationState.SUBMITTING,
            BrokerOperationState.LIVE,
            BrokerOperationState.RECONCILING,
            BrokerOperationState.SUCCEEDED,
            BrokerOperationState.FAILED_RETRYABLE,
            BrokerOperationState.FAILED_OPERATOR_REQUIRED,
            BrokerOperationState.UNKNOWN_OUTCOME,
        ):
            with self.subTest(state=state):
                self.assertFalse(
                    broker_operation_requires_new_risk_gate(state)
                )


if __name__ == "__main__":
    unittest.main()
