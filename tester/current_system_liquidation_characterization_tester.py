from __future__ import annotations

import os
import tempfile
import unittest
from contextlib import ExitStack
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

from core.daily_trading_guard import (
    DAILY_GUARD_STATUS_CLOSING,
    DAILY_GUARD_STATUS_HALTED,
    get_moscow_day_context,
    read_effective_daily_trading_halt,
    trigger_daily_trading_halt,
    update_daily_guard_runtime_state,
)
from core.time_utils import MSK_TIMEZONE
from ib_execution.daily_take_profit import (
    DAILY_TAKE_PROFIT_INTENT_SOURCE,
    DAILY_TAKE_PROFIT_SOURCE_SIGNAL_ID,
    quarantine_trade_intents_for_daily_halt,
    run_daily_take_profit_cleanup_once,
)
from ib_execution.emergency_close import (
    EmergencyCloseEvent,
    create_and_execute_market_close,
)
from ib_execution.execution_models import ExecutionResult, ExecutionStatus
from ib_execution.execution_store import write_trade_intent_execution_result
from ib_execution.protective_execution_runner import (
    reconcile_and_notify_protective_orders,
    restore_executed_entries_missing_protection,
)
from ib_execution.protective_order_reconciliation import (
    reconcile_protective_orders_once,
)
from ib_execution.protective_order_store import (
    PROTECTIVE_ORDER_STATUS_UNPROTECTED,
    initialize_protective_order_db,
    record_protective_order,
)
from ib_position_sync.position_models import BrokerPositionSnapshot
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
    source_id: int,
    ts: int,
    intent_source: str = "SIGNAL",
    action: TradeDecisionAction = TradeDecisionAction.OPEN_POSITION,
    before_side: PositionSide = PositionSide.FLAT,
    before_qty: float = 0.0,
    target_side: PositionSide = PositionSide.LONG,
    target_qty: float = 1.0,
) -> TradeIntentDraft:
    return TradeIntentDraft(
        source_signal_id=int(source_id),
        instrument_code=INSTRUMENT,
        signal_bar_ts=int(ts),
        signal_time_ct="2026-07-23 05:00:00",
        intent_source=str(intent_source),
        action=action,
        reason="liquidation_characterization_test",
        signal_direction=(
            target_side.value if target_side != PositionSide.FLAT else "CLOSE"
        ),
        entry_price=20_000.0,
        potential_end_delta_points=45.0,
        order_type="MARKET",
        position_before_side=before_side,
        position_before_qty=float(before_qty),
        position_after_side=target_side,
        position_after_qty=float(target_qty),
    )


def snapshot(
    side: str,
    quantity: float,
    *,
    contract: str | None = "MNQU6",
    con_id: int | None = 793356225,
    active: bool | None = True,
) -> BrokerPositionSnapshot:
    return BrokerPositionSnapshot(
        instrument_code=INSTRUMENT,
        side=str(side),
        quantity=float(quantity),
        broker_contract=None if str(side).upper() == "FLAT" else contract,
        broker_account=ACCOUNT_ID,
        broker_con_id=None if str(side).upper() == "FLAT" else con_id,
        contract_is_active=active,
    )


def robot_trade(
    *,
    order_id: int,
    order_ref: str,
    status: str = "Submitted",
):
    return SimpleNamespace(
        order=SimpleNamespace(
            orderId=int(order_id),
            orderRef=str(order_ref),
            account=ACCOUNT_ID,
        ),
        orderStatus=SimpleNamespace(status=str(status)),
    )


class PersistentStateTestCase(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        root = Path(self.temp_dir.name)
        self.trade_db_path = root / "trade.sqlite3"
        self.state_db_path = root / "state.sqlite3"
        self.stack = ExitStack()
        self.stack.enter_context(
            patch("ib_trader.trade_schema.TRADE_DB_PATH", self.trade_db_path)
        )
        self.stack.enter_context(
            patch("core.daily_trading_guard.STATE_DB_PATH", self.state_db_path)
        )

    def tearDown(self):
        self.stack.close()
        self.temp_dir.cleanup()

    @staticmethod
    def moscow_ts(year: int, month: int, day: int, hour: int = 12) -> int:
        return int(
            datetime(year, month, day, hour, 0, 0, tzinfo=MSK_TIMEZONE)
            .astimezone(timezone.utc)
            .timestamp()
        )

    def create_halt(self, *, trigger_ts: int, effective_ts: int | None = None):
        day = get_moscow_day_context(trigger_ts)
        trigger_daily_trading_halt(
            account_id=ACCOUNT_ID,
            moscow_day=day.day_key,
            target_usd=500.0,
            realized_pnl_usd=450.0,
            unrealized_pnl_usd=60.0,
            total_pnl_usd=510.0,
            now_ts=trigger_ts,
        )
        halt = read_effective_daily_trading_halt(
            account_id=ACCOUNT_ID,
            now_ts=trigger_ts if effective_ts is None else effective_ts,
        )
        self.assertIsNotNone(halt)
        return halt

    def create_intent(
        self,
        *,
        source_id: int,
        ts: int,
        intent_source: str = "SIGNAL",
        action: TradeDecisionAction = TradeDecisionAction.OPEN_POSITION,
        before_side: PositionSide = PositionSide.FLAT,
        before_qty: float = 0.0,
        target_side: PositionSide = PositionSide.LONG,
        target_qty: float = 1.0,
        status: str = "NEW",
        order_id: int | None = None,
        avg_fill_price: float | None = None,
    ) -> int:
        conn = get_trade_db_connection()
        try:
            initialize_trade_db(conn)
            intent_id = write_trade_intent(
                conn,
                draft(
                    source_id=source_id,
                    ts=ts,
                    intent_source=intent_source,
                    action=action,
                    before_side=before_side,
                    before_qty=before_qty,
                    target_side=target_side,
                    target_qty=target_qty,
                ),
            )
            conn.execute(
                f"UPDATE {TRADE_INTENTS_TABLE_NAME} SET "
                "status=?, order_id=?, order_action=?, order_quantity=?, "
                "avg_fill_price=?, updated_at_ts=?, sent_at_ts=?, "
                "finished_at_ts=CASE WHEN ? IN "
                "('EXECUTED', 'FAILED', 'CANCELLED', 'EXPIRED') THEN ? ELSE NULL END "
                "WHERE trade_intent_id=?",
                (
                    str(status).upper(),
                    None if order_id is None else int(order_id),
                    None if order_id is None else "BUY",
                    None if order_id is None else int(target_qty or before_qty or 1),
                    avg_fill_price,
                    int(ts),
                    None if status == "NEW" else int(ts),
                    str(status).upper(),
                    int(ts),
                    int(intent_id),
                ),
            )
            conn.commit()
            return int(intent_id)
        finally:
            conn.close()


class DailyHaltCleanupCharacterizationTest(PersistentStateTestCase):
    async def test_quarantine_cancels_new_requests_active_and_preserves_daily_close(self):
        now_ts = self.moscow_ts(2026, 7, 23)
        halt = self.create_halt(trigger_ts=now_ts)

        new_id = self.create_intent(source_id=1001, ts=now_ts + 1, status="NEW")
        sending_id = self.create_intent(
            source_id=1002,
            ts=now_ts + 2,
            status="SENDING",
            order_id=7102,
        )
        accepted_id = self.create_intent(
            source_id=1003,
            ts=now_ts + 3,
            status="ACCEPTED",
            order_id=7103,
        )
        reconciling_id = self.create_intent(
            source_id=1004,
            ts=now_ts + 4,
            status="RECONCILING",
            order_id=7104,
        )
        daily_id = self.create_intent(
            source_id=DAILY_TAKE_PROFIT_SOURCE_SIGNAL_ID,
            ts=now_ts + 5,
            intent_source=DAILY_TAKE_PROFIT_INTENT_SOURCE,
            action=TradeDecisionAction.CLOSE_POSITION,
            before_side=PositionSide.LONG,
            before_qty=1.0,
            target_side=PositionSide.FLAT,
            target_qty=0.0,
            status="NEW",
        )

        with patch("ib_execution.daily_take_profit.time.time", return_value=now_ts + 10):
            cancelled_new, cancel_requested = quarantine_trade_intents_for_daily_halt(
                halt=halt
            )

        self.assertEqual((cancelled_new, cancel_requested), (1, 3))
        conn = get_trade_db_connection()
        try:
            rows = {
                int(row[0]): row[1:]
                for row in conn.execute(
                    f"SELECT trade_intent_id, status, cancel_requested, "
                    "cancel_source_signal_id, finished_at_ts "
                    f"FROM {TRADE_INTENTS_TABLE_NAME}"
                ).fetchall()
            }
        finally:
            conn.close()

        self.assertEqual(
            rows[new_id][0:3],
            ("CANCELLED", 1, DAILY_TAKE_PROFIT_SOURCE_SIGNAL_ID),
        )
        self.assertIsNotNone(rows[new_id][3])
        for intent_id, status in (
            (sending_id, "SENDING"),
            (accepted_id, "ACCEPTED"),
            (reconciling_id, "RECONCILING"),
        ):
            self.assertEqual(
                rows[intent_id][0:3],
                (status, 1, DAILY_TAKE_PROFIT_SOURCE_SIGNAL_ID),
            )
            self.assertIsNone(rows[intent_id][3])
        self.assertEqual(rows[daily_id], ("NEW", 0, None, None))

    async def test_cleanup_reaches_halted_only_after_flat_and_no_robot_orders(self):
        now_ts = self.moscow_ts(2026, 7, 23)
        halt = self.create_halt(trigger_ts=now_ts)
        service = SimpleNamespace(
            account_id=ACCOUNT_ID,
            ib=SimpleNamespace(openTrades=lambda: []),
            broker_state=SimpleNamespace(
                refresh_open_orders=AsyncMock(return_value=(True, None))
            ),
        )

        with (
            patch("ib_execution.daily_take_profit.time.time", return_value=now_ts + 20),
            patch("core.daily_trading_guard.time.time", return_value=now_ts + 20),
            patch(
                "ib_execution.daily_take_profit.get_trading_enabled_instrument_codes",
                return_value=[INSTRUMENT],
            ),
            patch(
                "ib_execution.daily_take_profit.sync_broker_positions_once",
                new_callable=AsyncMock,
                side_effect=[[snapshot("FLAT", 0.0)], [snapshot("FLAT", 0.0)]],
            ),
            patch(
                "ib_execution.daily_take_profit.cancel_exit_orders_for_instrument",
                new_callable=AsyncMock,
                return_value=(True, []),
            ) as cancel_exits,
            patch(
                "ib_execution.daily_take_profit.close_market_safely",
                new_callable=AsyncMock,
            ) as close_market,
        ):
            result = await run_daily_take_profit_cleanup_once(
                service,
                halt=halt,
            )

        self.assertTrue(result.cleanup_completed)
        self.assertEqual(result.halt.state.status, DAILY_GUARD_STATUS_HALTED)
        self.assertIsNotNone(result.halt.state.cleanup_completed_at_ts)
        self.assertTrue(any(event.event == "HALTED" for event in result.events))
        cancel_exits.assert_awaited_once()
        close_market.assert_not_awaited()

    async def test_failed_liquidation_keeps_daily_guard_closing(self):
        now_ts = self.moscow_ts(2026, 7, 23)
        halt = self.create_halt(trigger_ts=now_ts)
        open_position = snapshot("LONG", 1.0)
        service = SimpleNamespace(
            account_id=ACCOUNT_ID,
            ib=SimpleNamespace(openTrades=lambda: []),
            broker_state=SimpleNamespace(
                refresh_open_orders=AsyncMock(return_value=(True, None))
            ),
        )
        close_event = EmergencyCloseEvent(
            instrument_code=INSTRUMENT,
            event="MARKET_CLOSE_SENT",
            message="simulated uncertain liquidation",
            log_level="WARNING",
        )

        with (
            patch("ib_execution.daily_take_profit.time.time", return_value=now_ts + 30),
            patch("core.daily_trading_guard.time.time", return_value=now_ts + 30),
            patch(
                "ib_execution.daily_take_profit.get_trading_enabled_instrument_codes",
                return_value=[INSTRUMENT],
            ),
            patch(
                "ib_execution.daily_take_profit.sync_broker_positions_once",
                new_callable=AsyncMock,
                side_effect=[[open_position], [open_position]],
            ),
            patch(
                "ib_execution.daily_take_profit.close_market_safely",
                new_callable=AsyncMock,
                return_value=[close_event],
            ) as close_market,
        ):
            result = await run_daily_take_profit_cleanup_once(
                service,
                halt=halt,
            )

        self.assertFalse(result.cleanup_completed)
        self.assertEqual(result.halt.state.status, DAILY_GUARD_STATUS_CLOSING)
        self.assertIn("open_positions", result.halt.state.error_text or "")
        close_market.assert_awaited_once()

    async def test_unresolved_daily_close_suppresses_duplicate_liquidation(self):
        now_ts = self.moscow_ts(2026, 7, 23)
        halt = self.create_halt(trigger_ts=now_ts)
        daily_id = self.create_intent(
            source_id=DAILY_TAKE_PROFIT_SOURCE_SIGNAL_ID,
            ts=now_ts + 40,
            intent_source=DAILY_TAKE_PROFIT_INTENT_SOURCE,
            action=TradeDecisionAction.CLOSE_POSITION,
            before_side=PositionSide.LONG,
            before_qty=1.0,
            target_side=PositionSide.FLAT,
            target_qty=0.0,
            status="RECONCILING",
            order_id=7401,
        )
        conn = get_trade_db_connection()
        try:
            order_ref = str(
                conn.execute(
                    f"SELECT order_ref FROM {TRADE_INTENTS_TABLE_NAME} "
                    "WHERE trade_intent_id=?",
                    (daily_id,),
                ).fetchone()[0]
            )
        finally:
            conn.close()

        trade = robot_trade(order_id=7401, order_ref=order_ref)
        service = SimpleNamespace(
            account_id=ACCOUNT_ID,
            ib=SimpleNamespace(openTrades=lambda: [trade]),
            broker_state=SimpleNamespace(
                refresh_open_orders=AsyncMock(return_value=(True, None))
            ),
            cancel_order_id=AsyncMock(),
        )
        open_position = snapshot("LONG", 1.0)

        with (
            patch("ib_execution.daily_take_profit.time.time", return_value=now_ts + 50),
            patch("core.daily_trading_guard.time.time", return_value=now_ts + 50),
            patch(
                "ib_execution.daily_take_profit.get_trading_enabled_instrument_codes",
                return_value=[INSTRUMENT],
            ),
            patch(
                "ib_execution.daily_take_profit.sync_broker_positions_once",
                new_callable=AsyncMock,
                side_effect=[[open_position], [open_position]],
            ),
            patch(
                "ib_execution.daily_take_profit.cancel_exit_orders_for_instrument",
                new_callable=AsyncMock,
                return_value=(True, []),
            ) as cancel_exits,
            patch(
                "ib_execution.daily_take_profit.close_market_safely",
                new_callable=AsyncMock,
            ) as close_market,
        ):
            result = await run_daily_take_profit_cleanup_once(
                service,
                halt=halt,
            )

        self.assertFalse(result.cleanup_completed)
        self.assertEqual(result.halt.state.status, DAILY_GUARD_STATUS_CLOSING)
        close_market.assert_not_awaited()
        cancel_exits.assert_awaited_once()
        service.cancel_order_id.assert_not_awaited()

    async def test_prior_day_cleanup_completion_unlocks_new_moscow_day(self):
        prior_ts = self.moscow_ts(2026, 7, 22)
        current_ts = self.moscow_ts(2026, 7, 23)
        halt = self.create_halt(trigger_ts=prior_ts, effective_ts=current_ts)
        self.assertFalse(halt.is_current_moscow_day)
        update_daily_guard_runtime_state(
            account_id=ACCOUNT_ID,
            moscow_day=halt.state.moscow_day,
            status=DAILY_GUARD_STATUS_CLOSING,
            now_ts=prior_ts + 1,
        )
        halt = read_effective_daily_trading_halt(
            account_id=ACCOUNT_ID,
            now_ts=current_ts,
        )
        self.assertIsNotNone(halt)
        self.assertFalse(halt.is_current_moscow_day)

        service = SimpleNamespace(
            account_id=ACCOUNT_ID,
            ib=SimpleNamespace(openTrades=lambda: []),
            broker_state=SimpleNamespace(
                refresh_open_orders=AsyncMock(return_value=(True, None))
            ),
        )

        with (
            patch("ib_execution.daily_take_profit.time.time", return_value=current_ts),
            patch("core.daily_trading_guard.time.time", return_value=current_ts),
            patch(
                "ib_execution.daily_take_profit.get_trading_enabled_instrument_codes",
                return_value=[INSTRUMENT],
            ),
            patch(
                "ib_execution.daily_take_profit.sync_broker_positions_once",
                new_callable=AsyncMock,
                side_effect=[[snapshot("FLAT", 0.0)], [snapshot("FLAT", 0.0)]],
            ),
            patch(
                "ib_execution.daily_take_profit.cancel_exit_orders_for_instrument",
                new_callable=AsyncMock,
                return_value=(True, []),
            ),
        ):
            result = await run_daily_take_profit_cleanup_once(
                service,
                halt=halt,
            )

        self.assertTrue(result.cleanup_completed)
        self.assertEqual(result.halt.state.status, DAILY_GUARD_STATUS_HALTED)
        self.assertFalse(result.halt.is_current_moscow_day)
        self.assertIsNone(
            read_effective_daily_trading_halt(
                account_id=ACCOUNT_ID,
                now_ts=current_ts,
            )
        )


class MissingProtectionRecoveryCharacterizationTest(PersistentStateTestCase):
    def create_executed_entry(self, *, avg_fill_price: float | None) -> int:
        conn = get_trade_db_connection()
        try:
            initialize_trade_db(conn)
            intent_id = write_trade_intent(
                conn,
                draft(source_id=2001, ts=1_800_200_120),
            )
            write_trade_intent_execution_result(
                conn,
                result=ExecutionResult(
                    trade_intent_id=intent_id,
                    order_id=7501,
                    order_action="BUY",
                    order_quantity=1,
                    status=ExecutionStatus.EXECUTED,
                    avg_fill_price=avg_fill_price,
                    total_commission=0.62,
                    realized_pnl=0.0,
                    error_text=None,
                ),
            )
            conn.commit()
            return int(intent_id)
        finally:
            conn.close()

    async def test_hard_kill_after_entry_restores_missing_protection(self):
        intent_id = self.create_executed_entry(avg_fill_price=20_000.0)
        service = SimpleNamespace(
            account_id=ACCOUNT_ID,
            ib=object(),
        )

        with (
            patch(
                "ib_execution.protective_execution_runner.sync_broker_positions_once",
                new_callable=AsyncMock,
                return_value=[snapshot("LONG", 1.0)],
            ),
            patch(
                "ib_execution.protective_execution_runner."
                "adopt_live_protective_orders_for_intent",
                new_callable=AsyncMock,
                return_value=False,
            ),
            patch(
                "ib_execution.protective_execution_runner."
                "place_protective_orders_after_entry",
                new_callable=AsyncMock,
            ) as place_protection,
            patch(
                "ib_execution.protective_execution_runner."
                "emergency_close_position_after_protective_failure",
                new_callable=AsyncMock,
            ) as emergency_close,
        ):
            await restore_executed_entries_missing_protection(
                order_service=service,
            )

        place_protection.assert_awaited_once()
        self.assertEqual(
            place_protection.await_args.kwargs["intent"].trade_intent_id,
            intent_id,
        )
        emergency_close.assert_not_awaited()

    async def test_missing_stop_and_missing_avg_fill_forces_emergency_close(self):
        intent_id = self.create_executed_entry(avg_fill_price=None)
        service = SimpleNamespace(
            account_id=ACCOUNT_ID,
            ib=object(),
        )

        with (
            patch(
                "ib_execution.protective_execution_runner.sync_broker_positions_once",
                new_callable=AsyncMock,
                return_value=[snapshot("LONG", 1.0)],
            ),
            patch(
                "ib_execution.protective_execution_runner."
                "adopt_live_protective_orders_for_intent",
                new_callable=AsyncMock,
                return_value=False,
            ),
            patch(
                "ib_execution.protective_execution_runner."
                "place_protective_orders_after_entry",
                new_callable=AsyncMock,
            ) as place_protection,
            patch(
                "ib_execution.protective_execution_runner."
                "emergency_close_position_after_protective_failure",
                new_callable=AsyncMock,
            ) as emergency_close,
        ):
            await restore_executed_entries_missing_protection(
                order_service=service,
            )

        place_protection.assert_not_awaited()
        emergency_close.assert_awaited_once()
        self.assertEqual(
            emergency_close.await_args.kwargs["instrument_code"],
            INSTRUMENT,
        )
        self.assertIn(
            f"trade_intent_id={intent_id}",
            emergency_close.await_args.kwargs["reason"],
        )

    async def test_missing_broker_stop_becomes_unprotected_and_requests_market_close(self):
        conn = get_trade_db_connection()
        try:
            initialize_protective_order_db(conn)
            record_protective_order(
                conn,
                instrument_code=INSTRUMENT,
                parent_trade_intent_id=1,
                role="STOP_LOSS",
                order_ref="IBMD_INTENT_1_MNQ_SL",
                order_id=7601,
                order_action="SELL",
                order_quantity=1,
                order_type="STP",
                stop_price=19_850.0,
            )
            conn.commit()
        finally:
            conn.close()

        service = SimpleNamespace(
            account_id=ACCOUNT_ID,
            ib=SimpleNamespace(
                trades=lambda: [],
                openTrades=lambda: [],
                fills=lambda: [],
            ),
            broker_state=SimpleNamespace(
                refresh_executions=AsyncMock(return_value=True),
                refresh_open_orders=AsyncMock(return_value=(True, None)),
            ),
        )

        with patch(
            "ib_execution.protective_order_reconciliation.sync_broker_positions_once",
            new_callable=AsyncMock,
            return_value=[snapshot("LONG", 1.0)],
        ):
            events = await reconcile_protective_orders_once(
                order_service=service,
            )

        self.assertEqual(len(events), 1)
        self.assertEqual(events[0]["event"], "UNPROTECTED")
        self.assertTrue(events[0]["requires_market_close"])
        conn = get_trade_db_connection()
        try:
            row = conn.execute(
                "SELECT status, error_text FROM protective_orders WHERE order_id=7601"
            ).fetchone()
        finally:
            conn.close()
        self.assertEqual(row[0], PROTECTIVE_ORDER_STATUS_UNPROTECTED)
        self.assertIn("absent from refreshed IB open orders", row[1] or "")

        with (
            patch(
                "ib_execution.protective_execution_runner."
                "reconcile_protective_orders_once",
                new_callable=AsyncMock,
                return_value=events,
            ),
            patch(
                "ib_execution.protective_execution_runner."
                "emergency_close_position_after_protective_failure",
                new_callable=AsyncMock,
            ) as emergency_close,
        ):
            await reconcile_and_notify_protective_orders(
                order_service=service,
            )

        emergency_close.assert_awaited_once()
        self.assertEqual(
            emergency_close.await_args.kwargs["instrument_code"],
            INSTRUMENT,
        )


class EmergencyCloseIdempotencyGapCharacterizationTest(PersistentStateTestCase):
    async def test_new_timestamp_creates_second_uncertain_emergency_close_current_gap(self):
        # Characterization only: DEC-009 requires a stable operation key in the
        # target system. The current source-signal + timestamp key permits a
        # second liquidation intent when the previous outcome is unresolved.
        calls = 0

        async def uncertain_execution(*, order_service, intent, order_submitted_callback):
            nonlocal calls
            _ = order_service, order_submitted_callback
            calls += 1
            return ExecutionResult(
                trade_intent_id=intent.trade_intent_id,
                order_id=7700 + calls,
                order_action="SELL",
                order_quantity=1,
                status=ExecutionStatus.RECONCILING,
                avg_fill_price=None,
                total_commission=None,
                realized_pnl=None,
                error_text="simulated broker outcome unknown",
            )

        service = SimpleNamespace()
        open_position = snapshot("LONG", 1.0)
        with patch(
            "ib_execution.emergency_close.execute_trade_intent",
            side_effect=uncertain_execution,
        ):
            first = await create_and_execute_market_close(
                order_service=service,
                snapshot=open_position,
                source_signal_id=-10,
                intent_source="PROTECTIVE_ORDER_SAFETY",
                reason="missing stop",
                now_ts=1_800_300_100,
            )
            second = await create_and_execute_market_close(
                order_service=service,
                snapshot=open_position,
                source_signal_id=-10,
                intent_source="PROTECTIVE_ORDER_SAFETY",
                reason="missing stop",
                now_ts=1_800_300_101,
            )

        self.assertEqual(calls, 2)
        self.assertNotEqual(first.intent.trade_intent_id, second.intent.trade_intent_id)
        self.assertEqual(first.result.status, ExecutionStatus.RECONCILING)
        self.assertEqual(second.result.status, ExecutionStatus.RECONCILING)
        conn = get_trade_db_connection()
        try:
            rows = conn.execute(
                f"SELECT trade_intent_id, signal_bar_ts, status "
                f"FROM {TRADE_INTENTS_TABLE_NAME} "
                "WHERE intent_source='PROTECTIVE_ORDER_SAFETY' "
                "ORDER BY trade_intent_id"
            ).fetchall()
        finally:
            conn.close()
        self.assertEqual(
            [(row[1], row[2]) for row in rows],
            [
                (1_800_300_100, "RECONCILING"),
                (1_800_300_101, "RECONCILING"),
            ],
        )


if __name__ == "__main__":
    unittest.main()
