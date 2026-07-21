from __future__ import annotations

import os
import sqlite3
from types import SimpleNamespace
import unittest
from unittest.mock import AsyncMock, patch

os.environ.setdefault("IB_ACCOUNT_ID", "U0000000")
os.environ.setdefault("TELEGRAM_BOT_TOKEN", "test-token")
os.environ.setdefault("TELEGRAM_CHAT_ID", "-1000000000000")

from core.price_source import mid_close_sql
from ib_execution.execution_store import initialize_execution_db
from ib_execution.emergency_close import (
    cancel_legacy_auxiliary_exit_orders_once,
    collect_live_legacy_auxiliary_exit_orders,
)
from ib_execution.protective_order_store import create_protective_orders_table_sql
from ib_signal.signal_config import SignalConfig
from ib_signal.signal_event_store import (
    SIGNAL_EVENTS_TABLE_NAME,
    create_signal_events_table_sql,
    initialize_signal_events_table,
)
from ib_signal.signal_schedule import get_due_signal_bar_ts
from ib_signal.signal_time import resolve_allowed_hours
from ib_signal.signal_window import build_rolling_signal_window
from ib_trader.trade_decision_service import build_signal_trade_intent_draft
from ib_trader.trade_models import (
    PositionSide,
    PositionSnapshot,
    TradeDecisionAction,
    TraderSignalEvent,
)
from ib_trader.trade_schema import (
    TRADE_INTENT_COLUMN_NAMES,
    TRADE_INTENTS_TABLE_NAME,
    create_positions_latest_table_sql,
    create_trade_intents_table_sql,
    initialize_trade_db,
)


class RollingSignalSmokeTest(unittest.TestCase):
    def test_rolling_window_and_schedule(self) -> None:
        settings = SignalConfig(
            rolling_signal_step_seconds=60,
            rolling_back_minutes=90,
            rolling_trade_minutes=30,
        )
        window = build_rolling_signal_window(
            signal_bar_ts=1_800_000_120,
            settings=settings,
        )
        self.assertEqual(window.pattern_start_ts, 1_800_000_120 - 90 * 60)
        self.assertEqual(window.pattern_end_ts, 1_800_000_120)
        self.assertEqual(window.trade_end_ts, 1_800_000_120 + 30 * 60)
        self.assertEqual(
            get_due_signal_bar_ts(
                current_bar_ts=1_800_000_179,
                settings=settings,
                last_calculated_bar_ts=None,
            ),
            1_800_000_120,
        )
        self.assertIsNone(
            get_due_signal_bar_ts(
                current_bar_ts=1_800_000_179,
                settings=settings,
                last_calculated_bar_ts=1_800_000_120,
            )
        )

    def test_candidate_hour_groups(self) -> None:
        self.assertEqual(resolve_allowed_hours(15, "FUT"), [])
        self.assertEqual(resolve_allowed_hours(16, "FUT"), [])
        self.assertIn(9, resolve_allowed_hours(8, "FUT"))
        self.assertEqual(
            resolve_allowed_hours(8, "CASH"),
            [7, 8, 9, 10],
        )
        self.assertEqual(len(resolve_allowed_hours(3, "CRYPTO")), 24)

    def test_mid_close_is_read_time_expression(self) -> None:
        expression = mid_close_sql("MNQ")
        self.assertIn("bid_close", expression)
        self.assertIn("ask_close", expression)
        self.assertIn("ROUND", expression)

    def test_signal_events_legacy_schema_migration(self) -> None:
        conn = sqlite3.connect(":memory:")
        try:
            conn.execute(create_signal_events_table_sql())
            conn.execute(
                f"ALTER TABLE {SIGNAL_EVENTS_TABLE_NAME} "
                "ADD COLUMN signal_window_mode TEXT"
            )
            conn.execute(
                f"ALTER TABLE {SIGNAL_EVENTS_TABLE_NAME} "
                "ADD COLUMN market_regime_filter_mode TEXT"
            )
            conn.execute(
                f"ALTER TABLE {SIGNAL_EVENTS_TABLE_NAME} "
                "ADD COLUMN signal_allowed INTEGER"
            )
            conn.execute(
                f"""
                INSERT INTO {SIGNAL_EVENTS_TABLE_NAME} (
                    signal_id, instrument_code, signal_bar_ts,
                    signal_time_utc, signal_time_ct, signal_time_msk,
                    created_at_ts, direction, entry_price, best_pearson,
                    candidate_score_best, potential_end_delta_points,
                    potential_max_profit_points, potential_max_drawdown_points,
                    potential_used, settings_json, signal_window_mode,
                    market_regime_filter_mode, signal_allowed
                ) VALUES (
                    17, 'MNQ', 1800000120,
                    '2027-01-15 00:02:00', '2027-01-14 18:02:00',
                    '2027-01-15 03:02:00', 1800000121, 'LONG', 20000.0,
                    0.91, 0.88, 45.0, 80.0, 20.0, 8, '{{}}',
                    'ROLLING', 'OFF', 1
                )
                """
            )
            conn.executemany(
                f"""
                INSERT INTO {SIGNAL_EVENTS_TABLE_NAME} (
                    signal_id, instrument_code, signal_bar_ts,
                    signal_time_utc, signal_time_ct, signal_time_msk,
                    created_at_ts, direction, entry_price, best_pearson,
                    candidate_score_best, potential_end_delta_points,
                    potential_max_profit_points, potential_max_drawdown_points,
                    potential_used, settings_json, signal_window_mode,
                    market_regime_filter_mode, signal_allowed
                ) VALUES (?, 'MNQ', ?, '2027-01-15 00:04:00',
                    '2027-01-14 18:04:00', '2027-01-15 03:04:00',
                    1800000241, 'SHORT', 20002.0, 0.90, 0.87, -45.0,
                    80.0, 20.0, 8, '{{}}', ?, ?, ?)
                """,
                [
                    (18, 1800000240, "SLOT", "OFF", 1),
                    (19, 1800000300, "ROLLING", "HARD", 1),
                    (20, 1800000360, "ROLLING", "OFF", 0),
                ],
            )
            conn.execute(
                "UPDATE sqlite_sequence SET seq = 25 WHERE name = ?",
                (SIGNAL_EVENTS_TABLE_NAME,),
            )
            initialize_signal_events_table(conn)
            columns = tuple(
                row[1]
                for row in conn.execute(
                    f"PRAGMA table_info({SIGNAL_EVENTS_TABLE_NAME})"
                ).fetchall()
            )
            self.assertNotIn("signal_window_mode", columns)
            self.assertNotIn("market_regime_filter_mode", columns)
            row = conn.execute(
                f"SELECT signal_id, instrument_code, direction "
                f"FROM {SIGNAL_EVENTS_TABLE_NAME}"
            ).fetchone()
            self.assertEqual(row, (17, "MNQ", "LONG"))
            conn.execute(
                f"""
                INSERT INTO {SIGNAL_EVENTS_TABLE_NAME} (
                    instrument_code, signal_bar_ts, signal_time_utc,
                    signal_time_ct, signal_time_msk, created_at_ts,
                    direction, entry_price, best_pearson,
                    candidate_score_best, potential_end_delta_points,
                    potential_max_profit_points, potential_max_drawdown_points,
                    potential_used, settings_json
                ) VALUES (
                    'MNQ', 1800000180, '2027-01-15 00:03:00',
                    '2027-01-14 18:03:00', '2027-01-15 03:03:00',
                    1800000181, 'SHORT', 20001.0, 0.92, 0.89,
                    -46.0, 81.0, 21.0, 8, '{{}}'
                )
                """
            )
            next_id = conn.execute(
                f"SELECT MAX(signal_id) FROM {SIGNAL_EVENTS_TABLE_NAME}"
            ).fetchone()[0]
            self.assertEqual(next_id, 26)
        finally:
            conn.close()

    def test_trade_intents_legacy_metadata_is_removed(self) -> None:
        conn = sqlite3.connect(":memory:")
        try:
            conn.execute(create_positions_latest_table_sql())
            conn.execute(create_trade_intents_table_sql())
            conn.execute(
                f"ALTER TABLE {TRADE_INTENTS_TABLE_NAME} "
                "ADD COLUMN entry_regime INTEGER"
            )
            conn.execute(
                f"ALTER TABLE {TRADE_INTENTS_TABLE_NAME} "
                "ADD COLUMN entry_ma_zone INTEGER"
            )
            conn.execute(
                f"""
                INSERT INTO {TRADE_INTENTS_TABLE_NAME} (
                    trade_intent_id, source_signal_id, instrument_code,
                    signal_bar_ts, signal_time_utc, signal_time_ct,
                    signal_time_msk, intent_source, action, action_reason,
                    target_side, target_qty, position_before_side,
                    position_before_qty, order_type, status, created_at_ts,
                    updated_at_ts, entry_regime, entry_ma_zone
                ) VALUES (
                    31, 101, 'MNQ', 1800000120,
                    '2027-01-15 00:02:00', '2027-01-14 18:02:00',
                    '2027-01-15 03:02:00', 'SIGNAL', 'OPEN_POSITION',
                    'legacy row', 'LONG', 1.0, 'FLAT', 0.0, 'MARKET',
                    'EXECUTED', 1800000121, 1800000121, 1, 2
                )
                """
            )
            conn.execute(
                "UPDATE sqlite_sequence SET seq = 40 WHERE name = ?",
                (TRADE_INTENTS_TABLE_NAME,),
            )
            initialize_trade_db(conn)
            columns = tuple(
                row[1]
                for row in conn.execute(
                    f"PRAGMA table_info({TRADE_INTENTS_TABLE_NAME})"
                ).fetchall()
            )
            self.assertEqual(columns, TRADE_INTENT_COLUMN_NAMES)
            self.assertNotIn("entry_regime", columns)
            self.assertNotIn("entry_ma_zone", columns)
            row = conn.execute(
                f"SELECT trade_intent_id, instrument_code, action "
                f"FROM {TRADE_INTENTS_TABLE_NAME}"
            ).fetchone()
            self.assertEqual(row, (31, "MNQ", "OPEN_POSITION"))
            conn.execute(
                f"""
                INSERT INTO {TRADE_INTENTS_TABLE_NAME} (
                    source_signal_id, instrument_code, signal_bar_ts,
                    signal_time_utc, signal_time_ct, signal_time_msk,
                    intent_source, action, action_reason, target_side,
                    target_qty, position_before_side, position_before_qty,
                    order_type, status, created_at_ts, updated_at_ts
                ) VALUES (
                    102, 'MNQ', 1800000180,
                    '2027-01-15 00:03:00', '2027-01-14 18:03:00',
                    '2027-01-15 03:03:00', 'SIGNAL', 'CLOSE_POSITION',
                    'next row', 'FLAT', 0.0, 'LONG', 1.0, 'MARKET',
                    'NEW', 1800000181, 1800000181
                )
                """
            )
            next_id = conn.execute(
                f"SELECT MAX(trade_intent_id) FROM {TRADE_INTENTS_TABLE_NAME}"
            ).fetchone()[0]
            self.assertEqual(next_id, 41)
        finally:
            conn.close()

    def test_legacy_broker_auxiliary_orders_are_detected(self) -> None:
        def trade(order_id: int, order_ref: str, account: str = "U0000000"):
            return SimpleNamespace(
                order=SimpleNamespace(
                    orderId=order_id,
                    orderRef=order_ref,
                    account=account,
                ),
                orderStatus=SimpleNamespace(status="Submitted"),
            )

        ib = SimpleNamespace(
            openTrades=lambda: [
                trade(10, "IBMD_INTENT_1_MNQ_EXT_TP"),
                trade(11, "IBMD_INTENT_1_MNQ_EXT_SL"),
                trade(12, "IBMD_INTENT_1_MNQ_TP"),
                trade(13, "OTHER_EXT_SL"),
                trade(14, "IBMD_INTENT_2_MNQ_EXT_SL", account="OTHER"),
            ]
        )
        service = SimpleNamespace(ib=ib, account_id="U0000000")
        detected = collect_live_legacy_auxiliary_exit_orders(service)
        self.assertEqual(
            [(item["order_id"], item["order_ref"]) for item in detected],
            [
                (10, "IBMD_INTENT_1_MNQ_EXT_TP"),
                (11, "IBMD_INTENT_1_MNQ_EXT_SL"),
            ],
        )

    def test_legacy_auxiliary_exit_state_is_cleaned(self) -> None:
        conn = sqlite3.connect(":memory:")
        try:
            conn.execute(create_positions_latest_table_sql())
            conn.execute(create_trade_intents_table_sql())
            conn.execute(
                "CREATE TABLE slot_loss_extensions (id INTEGER PRIMARY KEY)"
            )
            conn.execute(create_protective_orders_table_sql())
            base_values = (
                "MNQ", 1, "STOP_LOSS", None, "ref", 10, "SELL", 1,
                "STP", None, 19000.0, "CANCELLED", None, None, None,
                None, None, None, None, 1, 1, 1,
            )
            conn.execute(
                """
                INSERT INTO protective_orders (
                    instrument_code, parent_trade_intent_id, role, oca_group,
                    order_ref, order_id, order_action, order_quantity,
                    order_type, limit_price, stop_price, status, error_text,
                    filled_qty, avg_fill_price, total_commission, realized_pnl,
                    filled_at_ts, synthetic_trade_intent_id, created_at_ts,
                    updated_at_ts, finished_at_ts
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                base_values[:4] + ("IBMD_INTENT_1_MNQ_EXT_SL",) + base_values[5:],
            )
            normal_values = list(base_values)
            normal_values[4] = "IBMD_INTENT_1_MNQ_SL"
            normal_values[5] = 11
            conn.execute(
                """
                INSERT INTO protective_orders (
                    instrument_code, parent_trade_intent_id, role, oca_group,
                    order_ref, order_id, order_action, order_quantity,
                    order_type, limit_price, stop_price, status, error_text,
                    filled_qty, avg_fill_price, total_commission, realized_pnl,
                    filled_at_ts, synthetic_trade_intent_id, created_at_ts,
                    updated_at_ts, finished_at_ts
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                tuple(normal_values),
            )
            initialize_execution_db(conn)
            self.assertIsNone(
                conn.execute(
                    "SELECT 1 FROM sqlite_master WHERE type='table' "
                    "AND name='slot_loss_extensions'"
                ).fetchone()
            )
            refs = [
                row[0] for row in conn.execute(
                    "SELECT order_ref FROM protective_orders ORDER BY order_id"
                ).fetchall()
            ]
            self.assertEqual(refs, ["IBMD_INTENT_1_MNQ_SL"])
        finally:
            conn.close()

    def test_market_only_trade_decisions(self) -> None:
        signal = TraderSignalEvent(
            source_signal_id=101,
            instrument_code="MNQ",
            signal_bar_ts=1_800_000_120,
            signal_time_utc="2027-01-15 00:02:00",
            signal_time_ct="2027-01-14 18:02:00",
            signal_time_msk="2027-01-15 03:02:00",
            direction="LONG",
            entry_price=20_000.0,
            potential_end_delta_points=45.0,
        )
        flat = PositionSnapshot("MNQ", PositionSide.FLAT, 0.0)
        open_draft = build_signal_trade_intent_draft(signal=signal, position=flat)
        self.assertIsNotNone(open_draft)
        assert open_draft is not None
        self.assertEqual(open_draft.action, TradeDecisionAction.OPEN_POSITION)
        self.assertEqual(open_draft.order_type, "MARKET")

        short = PositionSnapshot("MNQ", PositionSide.SHORT, 2.0)
        reverse = build_signal_trade_intent_draft(signal=signal, position=short)
        self.assertIsNotNone(reverse)
        assert reverse is not None
        self.assertEqual(reverse.action, TradeDecisionAction.REVERSE_POSITION)
        self.assertEqual(reverse.position_after_side, PositionSide.LONG)
        self.assertEqual(reverse.position_after_qty, 2.0)

        same = PositionSnapshot("MNQ", PositionSide.LONG, 1.0)
        self.assertIsNone(
            build_signal_trade_intent_draft(signal=signal, position=same)
        )


class LegacyAuxiliaryBrokerCleanupTest(unittest.IsolatedAsyncioTestCase):
    async def test_startup_cleanup_cancels_and_rechecks_legacy_orders(self) -> None:
        def trade(order_id: int, order_ref: str):
            return SimpleNamespace(
                order=SimpleNamespace(
                    orderId=order_id,
                    orderRef=order_ref,
                    account="U0000000",
                ),
                orderStatus=SimpleNamespace(status="Submitted"),
            )

        open_trades = [
            trade(10, "IBMD_INTENT_1_MNQ_EXT_TP"),
            trade(11, "IBMD_INTENT_1_MNQ_EXT_SL"),
            trade(12, "IBMD_INTENT_1_MNQ_TP"),
        ]
        ib = SimpleNamespace(openTrades=lambda: list(open_trades))
        broker_state = SimpleNamespace(
            refresh_open_orders=AsyncMock(return_value=(True, None)),
        )
        service = SimpleNamespace(
            ib=ib,
            account_id="U0000000",
            broker_state=broker_state,
        )

        async def cancel_side_effect(_service, *, order_id: int, context: str):
            self.assertIn("rolling-only startup cleanup", context)
            open_trades[:] = [
                item
                for item in open_trades
                if int(item.order.orderId) != int(order_id)
            ]
            return SimpleNamespace(terminal_status="Cancelled")

        with patch(
            "ib_execution.emergency_close.cancel_order_and_require_terminal",
            side_effect=cancel_side_effect,
        ) as cancel_mock:
            cancelled = await cancel_legacy_auxiliary_exit_orders_once(service)

        self.assertEqual(
            cancelled,
            [
                {
                    "order_id": 10,
                    "order_ref": "IBMD_INTENT_1_MNQ_EXT_TP",
                    "terminal_status": "Cancelled",
                },
                {
                    "order_id": 11,
                    "order_ref": "IBMD_INTENT_1_MNQ_EXT_SL",
                    "terminal_status": "Cancelled",
                },
            ],
        )
        self.assertEqual(cancel_mock.call_count, 2)
        self.assertEqual(broker_state.refresh_open_orders.await_count, 2)
        self.assertEqual(
            [item.order.orderRef for item in open_trades],
            ["IBMD_INTENT_1_MNQ_TP"],
        )

    async def test_startup_cleanup_aborts_when_open_orders_cannot_be_verified(self) -> None:
        service = SimpleNamespace(
            ib=SimpleNamespace(openTrades=lambda: []),
            account_id="U0000000",
            broker_state=SimpleNamespace(
                refresh_open_orders=AsyncMock(
                    return_value=(False, "broker refresh failed")
                )
            ),
        )
        with self.assertRaisesRegex(
            RuntimeError,
            "cannot verify legacy auxiliary exit orders",
        ):
            await cancel_legacy_auxiliary_exit_orders_once(service)


if __name__ == "__main__":
    unittest.main(verbosity=2)
