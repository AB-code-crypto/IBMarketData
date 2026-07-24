from __future__ import annotations

import asyncio
import unittest
from datetime import datetime, timezone
from types import SimpleNamespace

from ibmd.execution.application.new_risk_window import (
    broker_operation_requires_new_risk_gate,
)
from ibmd.ib_gateway.broker_reconciliation import BrokerReconciliationReadError
from ibmd.ib_gateway.broker_reconciliation_mapping import (
    order_fact_from_ib_trade,
)
from ibmd.ib_gateway.ib_async_broker_reconciliation import (
    IBAsyncBrokerReconciliationReader,
    IBBrokerReconciliationConnectionSettings,
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
OLD_ORDER_REF = "IBMD:broker_operation_bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb:1"
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


def trade(
    *,
    status: str,
    remaining: float,
    message: str = "",
    order_ref: str = ORDER_REF,
    total_quantity: float = 1.0,
    order_id: int = 1,
):
    return SimpleNamespace(
        contract=SimpleNamespace(
            conId=793_356_225,
            localSymbol="MNQU6",
        ),
        order=SimpleNamespace(
            account=ACCOUNT,
            orderId=order_id,
            permId=0,
            clientId=320,
            totalQuantity=total_quantity,
            action="BUY",
            orderType="MKT",
            orderRef=order_ref,
        ),
        orderStatus=SimpleNamespace(
            orderId=order_id,
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


class FakeIB:
    def __init__(self, *, completed=()) -> None:
        self._connected = False
        self._completed = list(completed)
        self.connect_kwargs = None

    async def connectAsync(self, **kwargs):
        self.connect_kwargs = kwargs
        self._connected = True
        return self

    def isConnected(self):
        return self._connected

    def disconnect(self):
        self._connected = False

    def managedAccounts(self):
        return [ACCOUNT]

    async def reqAllOpenOrdersAsync(self):
        return []

    async def reqCompletedOrdersAsync(self, apiOnly):
        return list(self._completed)

    async def reqExecutionsAsync(self, execution_filter):
        return []

    def fills(self):
        return []


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

    def test_reader_ignores_foreign_and_zero_quantity_history_rows(self) -> None:
        foreign = trade(
            status="Cancelled",
            remaining=0.0,
            order_ref="IBMD_INTENT_OLD_MNQ",
            total_quantity=0.0,
            order_id=2,
        )
        unusable_target_history = trade(
            status="Cancelled",
            remaining=0.0,
            order_ref=OLD_ORDER_REF,
            total_quantity=0.0,
            order_id=3,
        )
        current_target = trade(
            status="Cancelled",
            remaining=0.0,
            order_ref=ORDER_REF,
            total_quantity=1.0,
            order_id=1,
        )
        fake = FakeIB(
            completed=(foreign, unusable_target_history, current_target)
        )
        reader = IBAsyncBrokerReconciliationReader(
            IBBrokerReconciliationConnectionSettings(
                host="127.0.0.1",
                port=7497,
                client_id=300,
                account_id=ACCOUNT,
                commission_wait_seconds=0.0,
            ),
            ib_factory=lambda: fake,
            clock=lambda: datetime(
                2026, 7, 24, 19, 26, 14, tzinfo=timezone.utc
            ),
        )

        snapshot = asyncio.run(reader.read_snapshot(account_id=ACCOUNT))
        asyncio.run(reader.close())

        self.assertTrue(snapshot.requests_complete)
        self.assertEqual(len(snapshot.open_orders), 0)
        self.assertEqual(len(snapshot.completed_orders), 1)
        self.assertEqual(snapshot.completed_orders[0].order_ref, ORDER_REF)
        self.assertEqual(snapshot.completed_orders[0].remaining_qty, 1)

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
