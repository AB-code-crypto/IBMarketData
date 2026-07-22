from __future__ import annotations

import os
import sqlite3
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

# config.py creates Settings at import time. Keep this module runnable directly.
os.environ.setdefault("IB_HOST", "127.0.0.1")
os.environ.setdefault("IB_PORT", "7497")
os.environ.setdefault("IB_CLIENT_ID", "200")
os.environ.setdefault("IB_ACCOUNT_ID", "U0000000")
os.environ.setdefault("TELEGRAM_BOT_TOKEN", "test-token")
os.environ.setdefault("TELEGRAM_CHAT_ID", "-1000000000000")

from ib_execution.execution_models import ExecutionResult, ExecutionStatus, TradeIntent
from ib_execution.execution_store import (
    mark_trade_intent_order_submitted,
    mark_trade_intent_sending,
)
from ib_execution.protective_execution_runner import (
    PROTECTIVE_ORDER_ROLE_STOP_LOSS,
    PROTECTIVE_ORDER_ROLE_TAKE_PROFIT,
    adopt_live_protective_orders_for_intent,
    place_protective_orders_after_entry,
    wait_for_protective_exit_order_working_or_done,
)
from ib_execution.protective_order_store import (
    PROTECTIVE_ORDER_STATUS_CANCELLED,
    initialize_protective_order_db,
    read_active_protective_orders,
)
from ib_execution.uncertain_execution_reconciliation import (
    reconcile_uncertain_trade_intents_once,
)
from ib_trader.trade_intent_repository import write_trade_intent
from ib_trader.trade_models import PositionSide, TradeDecisionAction
from ib_trader.trade_schema import (
    TRADE_INTENTS_TABLE_NAME,
    TradeIntentDraft,
    get_trade_db_connection,
    initialize_trade_db,
)

ACCOUNT_ID = "U0000000"
INSTRUMENT = "MNQ"


def draft(
    *,
    source_id: int = 901,
    ts: int = 1_800_100_120,
    action: TradeDecisionAction = TradeDecisionAction.OPEN_POSITION,
    before_side: PositionSide = PositionSide.FLAT,
    before_qty: float = 0.0,
    target_side: PositionSide = PositionSide.LONG,
    target_qty: float = 1.0,
) -> TradeIntentDraft:
    return TradeIntentDraft(
        source_signal_id=source_id,
        instrument_code=INSTRUMENT,
        signal_bar_ts=ts,
        signal_time_ct="2027-01-15 21:48:40",
        intent_source="SIGNAL",
        action=action,
        reason="recovery_characterization_test",
        signal_direction=(
            target_side.value if target_side != PositionSide.FLAT else "CLOSE"
        ),
        entry_price=20_000.0,
        potential_end_delta_points=45.0,
        order_type="MARKET",
        position_before_side=before_side,
        position_before_qty=before_qty,
        position_after_side=target_side,
        position_after_qty=target_qty,
    )


def execution_intent(
    *,
    trade_intent_id: int = 77,
    action: str = "OPEN_POSITION",
    target_side: str = "LONG",
    target_qty: float = 1.0,
) -> TradeIntent:
    return TradeIntent(
        trade_intent_id=trade_intent_id,
        source_signal_id=901,
        instrument_code=INSTRUMENT,
        order_ref=f"IBMD_INTENT_{trade_intent_id}_{INSTRUMENT}",
        action=action,
        target_side=target_side,
        target_qty=target_qty,
        position_before_side="FLAT",
        position_before_qty=0.0,
        order_type="MARKET",
        limit_price=None,
        limit_offset_points=None,
        ttl_seconds=None,
        status="NEW",
        created_at_ts=1_800_100_120,
    )


def broker_fill(
    *,
    order_id: int,
    exec_id: str,
    shares: float,
    price: float,
    commission: float | None = 0.62,
    realized_pnl: float | None = 0.0,
):
    execution = SimpleNamespace(
        orderId=int(order_id),
        execId=str(exec_id),
        shares=float(shares),
        price=float(price),
        time=datetime(2027, 1, 15, 3, 49, tzinfo=timezone.utc),
    )
    commission_report = SimpleNamespace(
        execId=str(exec_id),
        commission=commission,
        realizedPNL=realized_pnl,
    )
    return SimpleNamespace(
        execution=execution,
        commissionReport=commission_report,
    )


def broker_trade(
    *,
    order_id: int,
    order_ref: str,
    status: str,
    action: str = "BUY",
    quantity: int = 1,
    fills: list | None = None,
):
    order = SimpleNamespace(
        orderId=int(order_id),
        orderRef=str(order_ref),
        action=str(action),
        totalQuantity=int(quantity),
    )
    return SimpleNamespace(
        order=order,
        orderStatus=SimpleNamespace(status=str(status)),
        fills=list(fills or []),
    )


class FakeIB:
    LIVE_STATUSES = {
        "ApiPending",
        "PendingSubmit",
        "PreSubmitted",
        "Submitted",
        "PendingCancel",
    }

    def __init__(self, trades: list):
        self._trades = list(trades)

    def openTrades(self):
        return [
            trade
            for trade in self._trades
            if str(getattr(trade.orderStatus, "status", "")) in self.LIVE_STATUSES
        ]

    def trades(self):
        return list(self._trades)

    def fills(self):
        result = []
        for trade in self._trades:
            result.extend(list(getattr(trade, "fills", []) or []))
        return result


class BrokerRecoveryCharacterizationTest(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.db_path = Path(self.temp_dir.name) / "trade.sqlite3"
        self.path_patch = patch("ib_trader.trade_schema.TRADE_DB_PATH", self.db_path)
        self.path_patch.start()

    def tearDown(self):
        self.path_patch.stop()
        self.temp_dir.cleanup()

    def _create_pending_intent(
        self,
        *,
        target_qty: float = 1.0,
        order_id: int | None = None,
    ) -> tuple[int, str]:
        conn = get_trade_db_connection()
        try:
            initialize_trade_db(conn)
            intent_id = write_trade_intent(
                conn,
                draft(target_qty=target_qty),
            )
            mark_trade_intent_sending(conn, trade_intent_id=intent_id)
            if order_id is not None:
                mark_trade_intent_order_submitted(
                    conn,
                    trade_intent_id=intent_id,
                    order_id=order_id,
                    order_action="BUY",
                    order_quantity=int(target_qty),
                )
            order_ref = conn.execute(
                f"SELECT order_ref FROM {TRADE_INTENTS_TABLE_NAME} "
                "WHERE trade_intent_id=?",
                (intent_id,),
            ).fetchone()[0]
            conn.commit()
            return int(intent_id), str(order_ref)
        finally:
            conn.close()

    async def _reconcile(self, ib: FakeIB):
        service = SimpleNamespace(ib=ib)
        with (
            patch(
                "ib_execution.uncertain_execution_reconciliation."
                "refresh_ib_executions_if_possible",
                new_callable=AsyncMock,
                return_value=True,
            ),
            patch(
                "ib_execution.uncertain_execution_reconciliation."
                "refresh_ib_open_orders_if_possible",
                new_callable=AsyncMock,
                return_value=(True, None),
            ),
        ):
            return await reconcile_uncertain_trade_intents_once(
                order_service=service,
            )

    def _read_execution_row(self, intent_id: int):
        conn = get_trade_db_connection()
        try:
            return conn.execute(
                f"SELECT status, order_id, order_action, order_quantity, "
                "avg_fill_price, total_commission, realized_pnl, finished_at_ts, "
                "error_text "
                f"FROM {TRADE_INTENTS_TABLE_NAME} WHERE trade_intent_id=?",
                (int(intent_id),),
            ).fetchone()
        finally:
            conn.close()

    async def test_order_identity_is_recovered_by_order_ref_before_live_state(self):
        intent_id, order_ref = self._create_pending_intent(order_id=None)
        ib = FakeIB(
            [
                broker_trade(
                    order_id=7101,
                    order_ref=order_ref,
                    status="Submitted",
                )
            ]
        )

        events = await self._reconcile(ib)

        row = self._read_execution_row(intent_id)
        self.assertEqual(row[:4], ("ACCEPTED", 7101, "BUY", 1))
        self.assertIsNone(row[7])
        self.assertEqual(events, [])

    async def test_full_fill_is_recovered_without_duplicate_submission(self):
        intent_id, order_ref = self._create_pending_intent(order_id=7102)
        fill = broker_fill(
            order_id=7102,
            exec_id="EXEC-7102",
            shares=1.0,
            price=20_001.25,
            commission=0.62,
            realized_pnl=0.0,
        )
        ib = FakeIB(
            [
                broker_trade(
                    order_id=7102,
                    order_ref=order_ref,
                    status="Filled",
                    fills=[fill],
                )
            ]
        )

        events = await self._reconcile(ib)

        row = self._read_execution_row(intent_id)
        self.assertEqual(row[0], "EXECUTED")
        self.assertEqual(row[1:7], (7102, "BUY", 1, 20_001.25, 0.62, 0.0))
        self.assertIsNotNone(row[7])
        self.assertEqual(len(events), 1)
        self.assertEqual(events[0].event, "EXECUTED_RECOVERED")
        self.assertTrue(events[0].needs_protection)

    async def test_partial_fill_remains_visible_as_reconciling(self):
        intent_id, order_ref = self._create_pending_intent(
            target_qty=2.0,
            order_id=7103,
        )
        fill = broker_fill(
            order_id=7103,
            exec_id="EXEC-7103-A",
            shares=1.0,
            price=20_001.0,
        )
        ib = FakeIB(
            [
                broker_trade(
                    order_id=7103,
                    order_ref=order_ref,
                    status="Cancelled",
                    quantity=2,
                    fills=[fill],
                )
            ]
        )

        events = await self._reconcile(ib)

        row = self._read_execution_row(intent_id)
        self.assertEqual(row[0], "RECONCILING")
        self.assertIsNone(row[7])
        self.assertIn("partial fill", row[8] or "")
        self.assertEqual(len(events), 1)
        self.assertEqual(events[0].event, "PARTIAL_FILL_RECONCILING")


class RecordingOrderApi:
    def __init__(self, *, fail_on_place_number: int | None = None):
        self.fail_on_place_number = fail_on_place_number
        self.place_attempts = 0
        self.placed: list[dict] = []
        self.trades: list = []

    @staticmethod
    def build_stop(*, action, quantity, stop_price, time_in_force):
        return SimpleNamespace(
            action=str(action),
            totalQuantity=int(quantity),
            orderType="STP",
            auxPrice=float(stop_price),
            lmtPrice=0.0,
            tif=str(time_in_force),
            outsideRth=False,
            ocaGroup="",
        )

    @staticmethod
    def build_limit(*, action, quantity, limit_price, time_in_force):
        return SimpleNamespace(
            action=str(action),
            totalQuantity=int(quantity),
            orderType="LMT",
            auxPrice=0.0,
            lmtPrice=float(limit_price),
            tif=str(time_in_force),
            outsideRth=False,
            ocaGroup="",
        )

    @staticmethod
    def apply_oca_group(orders, *, oca_group, oca_type):
        for order in orders:
            order.ocaGroup = str(oca_group)
            order.ocaType = int(oca_type)

    async def place_order(self, contract, order, *, order_ref):
        _ = contract
        self.place_attempts += 1
        if self.fail_on_place_number == self.place_attempts:
            raise RuntimeError("simulated protective placement failure")

        order_id = 8200 + self.place_attempts
        order.orderId = order_id
        order.orderRef = str(order_ref)
        trade = SimpleNamespace(
            order=order,
            orderStatus=SimpleNamespace(status="Submitted"),
            fills=[],
        )
        self.trades.append(trade)
        self.placed.append(
            {
                "order_id": order_id,
                "order_ref": str(order_ref),
                "order_type": str(order.orderType),
                "oca_group": str(getattr(order, "ocaGroup", "") or ""),
            }
        )
        return SimpleNamespace(order_id=order_id, trade=trade)


class ProtectivePlacementCharacterizationTest(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.conn = sqlite3.connect(":memory:")
        initialize_protective_order_db(self.conn)

    def tearDown(self):
        self.conn.close()

    @staticmethod
    def _result(intent_id: int = 77):
        return ExecutionResult(
            trade_intent_id=intent_id,
            order_id=7201,
            order_action="BUY",
            order_quantity=1,
            status=ExecutionStatus.EXECUTED,
            avg_fill_price=20_000.0,
            total_commission=0.62,
            realized_pnl=0.0,
            error_text=None,
        )

    @staticmethod
    def _service(api: RecordingOrderApi):
        monitor = SimpleNamespace(
            find_error=lambda order_id, code=None: None,
            last_error=lambda order_id: None,
        )

        async def cancel_order(order_id):
            for trade in api.trades:
                if int(getattr(trade.order, "orderId", 0) or 0) == int(order_id):
                    trade.orderStatus.status = "Cancelled"
                    return SimpleNamespace(order_id=int(order_id))
            raise RuntimeError(f"unknown order_id={order_id}")

        ib = SimpleNamespace(
            openTrades=lambda: [
                trade
                for trade in api.trades
                if str(trade.orderStatus.status)
                in {"ApiPending", "PendingSubmit", "PreSubmitted", "Submitted", "PendingCancel"}
            ],
            trades=lambda: list(api.trades),
        )
        return SimpleNamespace(
            api=api,
            monitor=monitor,
            ib=ib,
            qualify=AsyncMock(side_effect=lambda contract: contract),
            cancel_order_id=cancel_order,
            account_id=ACCOUNT_ID,
        )

    async def test_stop_is_placed_before_take_profit_and_both_are_persisted(self):
        api = RecordingOrderApi()
        service = self._service(api)
        intent = execution_intent()
        contract = object()

        with (
            patch(
                "ib_execution.protective_execution_runner.build_execution_contract",
                return_value=contract,
            ),
            patch(
                "ib_execution.protective_execution_runner.asyncio.sleep",
                new_callable=AsyncMock,
            ),
        ):
            await place_protective_orders_after_entry(
                conn=self.conn,
                order_service=service,
                intent=intent,
                result=self._result(),
            )

        self.assertEqual(
            [item["order_ref"] for item in api.placed],
            [f"{intent.order_ref}_SL", f"{intent.order_ref}_TP"],
        )
        rows = read_active_protective_orders(self.conn)
        self.assertEqual(
            [row["role"] for row in rows],
            [PROTECTIVE_ORDER_ROLE_STOP_LOSS, PROTECTIVE_ORDER_ROLE_TAKE_PROFIT],
        )
        self.assertTrue(rows[0]["oca_group"])
        self.assertEqual(rows[0]["oca_group"], rows[1]["oca_group"])
        self.assertTrue(bool(getattr(api.trades[0].order, "outsideRth", False)))

    async def test_held_stop_is_not_accepted_as_working_protection(self):
        error = SimpleNamespace(id=8201, code=399, message="held until RTH")
        trade = SimpleNamespace(
            order=SimpleNamespace(orderId=8201),
            orderStatus=SimpleNamespace(status="PreSubmitted"),
        )
        service = SimpleNamespace(
            monitor=SimpleNamespace(
                find_error=lambda order_id, code=None: error if int(code or 0) == 399 else None,
                last_error=lambda order_id: error,
            )
        )

        with self.assertRaisesRegex(RuntimeError, "held until RTH"):
            await wait_for_protective_exit_order_working_or_done(
                order_service=service,
                trade=trade,
                role="stop-loss",
                order_ref="IBMD_INTENT_77_MNQ_SL",
            )

    async def test_second_protective_failure_cancels_first_and_requests_market_close(self):
        api = RecordingOrderApi(fail_on_place_number=2)
        service = self._service(api)
        intent = execution_intent()

        with (
            patch(
                "ib_execution.protective_execution_runner.build_execution_contract",
                return_value=object(),
            ),
            patch(
                "ib_execution.protective_execution_runner.asyncio.sleep",
                new_callable=AsyncMock,
            ),
            patch(
                "ib_execution.protective_execution_runner."
                "emergency_close_position_after_protective_failure",
                new_callable=AsyncMock,
            ) as emergency_close,
        ):
            await place_protective_orders_after_entry(
                conn=self.conn,
                order_service=service,
                intent=intent,
                result=self._result(),
            )

        self.assertEqual(len(api.placed), 1)
        row = self.conn.execute(
            "SELECT status, error_text FROM protective_orders WHERE order_id=?",
            (api.placed[0]["order_id"],),
        ).fetchone()
        self.assertEqual(row[0], PROTECTIVE_ORDER_STATUS_CANCELLED)
        self.assertIn("failed protective order placement", row[1] or "")
        emergency_close.assert_awaited_once()


class ProtectiveAdoptionCharacterizationTest(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.conn = sqlite3.connect(":memory:")
        initialize_protective_order_db(self.conn)

    def tearDown(self):
        self.conn.close()

    @staticmethod
    def _broker_order(*, order_id, order_ref, order_type, action="SELL", quantity=1, oca_group="IBMD_OCA_77_MNQ"):
        return SimpleNamespace(
            order=SimpleNamespace(
                orderId=int(order_id),
                orderRef=str(order_ref),
                orderType=str(order_type),
                action=str(action),
                totalQuantity=int(quantity),
                lmtPrice=20_075.0 if str(order_type).upper() == "LMT" else 0.0,
                auxPrice=19_850.0 if str(order_type).upper() == "STP" else 0.0,
                ocaGroup=str(oca_group),
            ),
            orderStatus=SimpleNamespace(status="Submitted"),
        )

    async def test_live_stop_and_tp_are_adopted_idempotently(self):
        intent = execution_intent()
        trades = [
            self._broker_order(
                order_id=8301,
                order_ref=f"{intent.order_ref}_SL",
                order_type="STP",
            ),
            self._broker_order(
                order_id=8302,
                order_ref=f"{intent.order_ref}_TP",
                order_type="LMT",
            ),
        ]
        service = SimpleNamespace(
            ib=SimpleNamespace(openTrades=lambda: list(trades)),
            cancel_order_id=AsyncMock(),
            monitor=SimpleNamespace(wait_for_done=AsyncMock()),
        )

        with patch(
            "ib_execution.protective_execution_runner.refresh_ib_open_orders_if_possible",
            new_callable=AsyncMock,
            return_value=(True, None),
        ):
            first = await adopt_live_protective_orders_for_intent(
                conn=self.conn,
                order_service=service,
                intent=intent,
            )
            second = await adopt_live_protective_orders_for_intent(
                conn=self.conn,
                order_service=service,
                intent=intent,
            )

        self.assertTrue(first)
        self.assertTrue(second)
        rows = read_active_protective_orders(self.conn)
        self.assertEqual(len(rows), 2)
        self.assertEqual(
            {row["role"] for row in rows},
            {PROTECTIVE_ORDER_ROLE_STOP_LOSS, PROTECTIVE_ORDER_ROLE_TAKE_PROFIT},
        )
        service.cancel_order_id.assert_not_awaited()

    async def test_tp_only_leftover_is_terminally_cancelled_before_replacement(self):
        intent = execution_intent()
        tp_trade = self._broker_order(
            order_id=8303,
            order_ref=f"{intent.order_ref}_TP",
            order_type="LMT",
            oca_group="",
        )
        service = SimpleNamespace(
            ib=SimpleNamespace(openTrades=lambda: [tp_trade]),
            cancel_order_id=AsyncMock(),
            monitor=SimpleNamespace(
                wait_for_done=AsyncMock(
                    return_value=SimpleNamespace(
                        done=True,
                        status="Cancelled",
                        timed_out=False,
                    )
                )
            ),
        )

        with patch(
            "ib_execution.protective_execution_runner.refresh_ib_open_orders_if_possible",
            new_callable=AsyncMock,
            return_value=(True, None),
        ):
            adopted = await adopt_live_protective_orders_for_intent(
                conn=self.conn,
                order_service=service,
                intent=intent,
            )

        self.assertFalse(adopted)
        service.cancel_order_id.assert_awaited_once_with(8303)
        service.monitor.wait_for_done.assert_awaited_once()
        self.assertEqual(read_active_protective_orders(self.conn), [])


if __name__ == "__main__":
    unittest.main()
