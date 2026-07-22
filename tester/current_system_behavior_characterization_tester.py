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

from core.daily_trading_guard import (
    DAILY_GUARD_STATUS_CLOSING,
    DAILY_GUARD_STATUS_HALTED,
    DAILY_GUARD_STATUS_MONITORING,
    DAILY_GUARD_STATUS_TRIGGERED,
    get_moscow_day_context,
    read_effective_daily_trading_halt,
    trigger_daily_trading_halt,
    update_daily_guard_runtime_state,
    upsert_daily_guard_monitoring,
)
from core.ib_clock import IB_CLOCK_TABLE_NAME, initialize_ib_clock_table, read_ib_clock_health
from core.time_utils import CT_TIMEZONE, MSK_TIMEZONE
from ib_execution.execution_logic import (
    LivePositionStateError,
    classify_limit_terminal_status,
    resolve_live_execution_plan,
)
from ib_execution.execution_models import ExecutionResult, ExecutionStatus, TradeIntent
from ib_execution.execution_store import (
    expire_stale_active_trade_intents,
    expire_stale_new_trade_intents,
    mark_trade_intent_order_submitted,
    mark_trade_intent_sending,
    write_trade_intent_execution_result,
)
from ib_execution.order_cancellation import (
    OrderCancellationUncertainError,
    OrderFilledDuringCancellationError,
    cancel_order_and_require_terminal,
    read_known_order_status,
)
from ib_execution.protective_order_reconciliation import (
    create_protective_close_trade_intent,
    mark_oca_siblings_cancelled_after_fill,
)
from ib_execution.protective_order_store import (
    PROTECTIVE_ORDER_STATUS_ACTIVE,
    PROTECTIVE_ORDER_STATUS_CANCELLED,
    PROTECTIVE_ORDER_STATUS_FILLED,
    initialize_protective_order_db,
    mark_protective_order_status,
    read_active_protective_orders,
    record_protective_order,
)
from ib_position_sync.position_models import BrokerPositionSnapshot
from ib_trader.trade_decision_service import (
    FUTURES_DAILY_FLAT_INTENT_SOURCE,
    FUTURES_ROLLOVER_CLOSE_INTENT_SOURCE,
    build_futures_rollover_close_trade_intent_draft,
    build_signal_trade_intent_draft,
    get_futures_daily_flat_context,
    process_futures_daily_flat_once,
    process_futures_rollover_close_once,
    process_signal_events_once,
)
from ib_trader.trade_intent_repository import (
    has_unresolved_trade_intent_for_instrument,
    write_trade_intent,
)
from ib_trader.trade_models import (
    PositionSide,
    PositionSnapshot,
    TradeDecisionAction,
    TraderSignalEvent,
)
from ib_trader.trade_position_repository import (
    POSITION_SNAPSHOT_MAX_AGE_SECONDS,
    read_position_snapshot_freshness,
)
from ib_trader.trade_schema import (
    POSITIONS_LATEST_TABLE_NAME,
    TRADE_INTENTS_TABLE_NAME,
    TradeIntentDraft,
    get_trade_db_connection,
    initialize_trade_db,
)

ACCOUNT_ID = "U0000000"
INSTRUMENT = "MNQ"


def signal(direction: str = "LONG", *, signal_id: int = 101, ts: int = 1_800_000_120):
    return TraderSignalEvent(
        source_signal_id=signal_id,
        instrument_code=INSTRUMENT,
        signal_bar_ts=ts,
        signal_time_utc="2027-01-15 00:02:00",
        signal_time_ct="2027-01-14 18:02:00",
        signal_time_msk="2027-01-15 03:02:00",
        direction=direction,
        entry_price=20_000.0,
        potential_end_delta_points=45.0 if direction == "LONG" else -45.0,
    )


def draft(
    *,
    source_id: int = 101,
    ts: int = 1_800_000_120,
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
        signal_time_ct="2027-01-14 18:02:00",
        intent_source="SIGNAL",
        action=action,
        reason="characterization_test",
        signal_direction=target_side.value if target_side != PositionSide.FLAT else "CLOSE",
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
    action: str,
    target_side: str,
    target_qty: float,
    before_side: str = "FLAT",
    before_qty: float = 0.0,
    order_type: str = "MARKET",
) -> TradeIntent:
    return TradeIntent(
        trade_intent_id=77,
        source_signal_id=101,
        instrument_code=INSTRUMENT,
        order_ref="IBMD_INTENT_77_MNQ",
        action=action,
        target_side=target_side,
        target_qty=target_qty,
        position_before_side=before_side,
        position_before_qty=before_qty,
        order_type=order_type,
        limit_price=None,
        limit_offset_points=None,
        ttl_seconds=None,
        status="NEW",
        created_at_ts=1_800_000_120,
    )


def put_position(
    conn: sqlite3.Connection,
    *,
    updated_at: int,
    side: str = "FLAT",
    qty: float = 0.0,
    active: bool | None = True,
) -> None:
    initialize_trade_db(conn)
    conn.execute(
        f"""
        INSERT INTO {POSITIONS_LATEST_TABLE_NAME} (
            instrument_code, side, quantity, broker_contract, broker_con_id,
            broker_account, contract_is_active, updated_at_ts, updated_at_utc
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(instrument_code) DO UPDATE SET
            side=excluded.side, quantity=excluded.quantity,
            broker_contract=excluded.broker_contract,
            broker_con_id=excluded.broker_con_id,
            broker_account=excluded.broker_account,
            contract_is_active=excluded.contract_is_active,
            updated_at_ts=excluded.updated_at_ts,
            updated_at_utc=excluded.updated_at_utc
        """,
        (
            INSTRUMENT,
            side,
            float(qty),
            None if side == "FLAT" else "MNQU6",
            None if side == "FLAT" else 793356225,
            ACCOUNT_ID,
            None if active is None else int(active),
            int(updated_at),
            "2027-01-15 00:02:00",
        ),
    )


class TradeIntentStateCharacterizationTest(unittest.TestCase):
    def setUp(self):
        self.conn = sqlite3.connect(":memory:")
        initialize_trade_db(self.conn)

    def tearDown(self):
        self.conn.close()

    def test_creation_dedup_order_ref_and_position_acknowledgement(self):
        first = write_trade_intent(self.conn, draft())
        second = write_trade_intent(self.conn, draft())
        rows = self.conn.execute(
            f"SELECT trade_intent_id, status, order_ref FROM {TRADE_INTENTS_TABLE_NAME}"
        ).fetchall()
        self.assertEqual(first, second)
        self.assertEqual(rows, [(first, "NEW", f"IBMD_INTENT_{first}_{INSTRUMENT}")])

        self.conn.execute(
            f"UPDATE {TRADE_INTENTS_TABLE_NAME} "
            "SET status='EXECUTED', updated_at_ts=100, finished_at_ts=100 "
            "WHERE trade_intent_id=?",
            (first,),
        )
        self.assertTrue(has_unresolved_trade_intent_for_instrument(self.conn, instrument_code=INSTRUMENT))
        put_position(self.conn, updated_at=100)
        self.assertTrue(has_unresolved_trade_intent_for_instrument(self.conn, instrument_code=INSTRUMENT))
        put_position(self.conn, updated_at=101)
        self.assertFalse(has_unresolved_trade_intent_for_instrument(self.conn, instrument_code=INSTRUMENT))

    def test_submission_identity_precedes_terminal_result(self):
        intent_id = write_trade_intent(self.conn, draft())
        with patch("ib_execution.execution_store.time.time", return_value=200):
            mark_trade_intent_sending(self.conn, trade_intent_id=intent_id)
        with patch("ib_execution.execution_store.time.time", return_value=201):
            mark_trade_intent_order_submitted(
                self.conn,
                trade_intent_id=intent_id,
                order_id=7001,
                order_action="BUY",
                order_quantity=1,
            )
        row = self.conn.execute(
            f"SELECT status, order_id, order_action, order_quantity, sent_at_ts, finished_at_ts "
            f"FROM {TRADE_INTENTS_TABLE_NAME} WHERE trade_intent_id=?",
            (intent_id,),
        ).fetchone()
        self.assertEqual(row, ("SENDING", 7001, "BUY", 1, 200, None))

        with patch("ib_execution.execution_store.time.time", return_value=202):
            write_trade_intent_execution_result(
                self.conn,
                result=ExecutionResult(
                    trade_intent_id=intent_id,
                    order_id=7001,
                    order_action="BUY",
                    order_quantity=1,
                    status=ExecutionStatus.EXECUTED,
                    avg_fill_price=20_001.25,
                    total_commission=0.62,
                    realized_pnl=0.0,
                    error_text=None,
                ),
            )
        row = self.conn.execute(
            f"SELECT status, finished_at_ts, avg_fill_price, total_commission, realized_pnl "
            f"FROM {TRADE_INTENTS_TABLE_NAME} WHERE trade_intent_id=?",
            (intent_id,),
        ).fetchone()
        self.assertEqual(row, ("EXECUTED", 202, 20_001.25, 0.62, 0.0))

    def test_reconciling_is_nonterminal(self):
        intent_id = write_trade_intent(self.conn, draft())
        with patch("ib_execution.execution_store.time.time", return_value=300):
            write_trade_intent_execution_result(
                self.conn,
                result=ExecutionResult(
                    trade_intent_id=intent_id,
                    order_id=7002,
                    order_action="BUY",
                    order_quantity=1,
                    status=ExecutionStatus.RECONCILING,
                    avg_fill_price=None,
                    total_commission=None,
                    realized_pnl=None,
                    error_text="broker result unknown",
                ),
            )
        self.assertEqual(
            self.conn.execute(
                f"SELECT status, finished_at_ts FROM {TRADE_INTENTS_TABLE_NAME} "
                "WHERE trade_intent_id=?",
                (intent_id,),
            ).fetchone(),
            ("RECONCILING", None),
        )

    def test_stale_state_transitions_and_known_reconciling_gap(self):
        cases = (
            ("NEW", None, "FAILED", expire_stale_new_trade_intents),
            ("SENDING", 7003, "RECONCILING", expire_stale_active_trade_intents),
            ("ACCEPTED", None, "FAILED", expire_stale_active_trade_intents),
            # Characterization only: this is classified CHANGE, not a target invariant.
            ("RECONCILING", None, "RECONCILING", expire_stale_active_trade_intents),
        )
        for index, (start, order_id, expected, cleanup) in enumerate(cases, start=1):
            with self.subTest(start=start, order_id=order_id):
                intent_id = write_trade_intent(
                    self.conn,
                    draft(source_id=200 + index, ts=100 + index),
                )
                self.conn.execute(
                    f"UPDATE {TRADE_INTENTS_TABLE_NAME} "
                    "SET status=?, order_id=?, sent_at_ts=10, updated_at_ts=10 "
                    "WHERE trade_intent_id=?",
                    (start, order_id, intent_id),
                )
                if cleanup is expire_stale_new_trade_intents:
                    cleanup(self.conn, max_age_seconds=30, now_ts=200)
                else:
                    cleanup(self.conn, now_ts=10_000, market_max_age_seconds=90)
                actual = self.conn.execute(
                    f"SELECT status FROM {TRADE_INTENTS_TABLE_NAME} WHERE trade_intent_id=?",
                    (intent_id,),
                ).fetchone()[0]
                self.assertEqual(actual, expected)

    def test_current_limit_partial_fill_is_terminal_executed(self):
        status, error = classify_limit_terminal_status(
            intent=execution_intent(
                action="OPEN_POSITION",
                target_side="LONG",
                target_qty=2.0,
                order_type="LIMIT",
            ),
            ib_status="Cancelled",
            timed_out=False,
            filled_qty=1.0,
            expected_qty=2,
        )
        self.assertEqual(status, ExecutionStatus.EXECUTED)
        self.assertIn("partial fill", error or "")


class DecisionAndGuardCharacterizationTest(unittest.TestCase):
    def test_signal_position_decision_table(self):
        cases = (
            (PositionSide.FLAT, 0.0, True, "LONG", TradeDecisionAction.OPEN_POSITION, PositionSide.LONG, 1.0),
            (PositionSide.LONG, 2.0, True, "LONG", None, None, None),
            (PositionSide.LONG, 2.0, True, "SHORT", TradeDecisionAction.REVERSE_POSITION, PositionSide.SHORT, 2.0),
            (PositionSide.UNKNOWN, 0.0, None, "LONG", None, None, None),
            (PositionSide.LONG, 1.0, False, "SHORT", None, None, None),
        )
        for side, qty, active, direction, action, target, target_qty in cases:
            with self.subTest(side=side, direction=direction, active=active):
                result = build_signal_trade_intent_draft(
                    signal=signal(direction),
                    position=PositionSnapshot(
                        instrument_code=INSTRUMENT,
                        side=side,
                        quantity=qty,
                        contract_is_active=active,
                    ),
                )
                if action is None:
                    self.assertIsNone(result)
                    continue
                assert result is not None
                self.assertEqual(result.action, action)
                self.assertEqual(result.position_after_side, target)
                self.assertEqual(result.position_after_qty, target_qty)
                self.assertEqual(result.order_type, "MARKET")

    def test_daily_flat_boundaries(self):
        def ct_ts(hour, minute, second):
            return int(
                datetime(2026, 7, 22, hour, minute, second, tzinfo=CT_TIMEZONE)
                .astimezone(timezone.utc)
                .timestamp()
            )

        checks = (
            (14, 59, 49, False),
            (14, 59, 50, True),
            (15, 59, 59, True),
            (16, 0, 0, False),
        )
        for hour, minute, second, expected in checks:
            with self.subTest(time=(hour, minute, second)):
                self.assertEqual(
                    get_futures_daily_flat_context(now_ts=ct_ts(hour, minute, second)) is not None,
                    expected,
                )

    def test_service_liquidation_drafts_and_unresolved_idempotency(self):
        rollover = build_futures_rollover_close_trade_intent_draft(
            position=PositionSnapshot(
                instrument_code=INSTRUMENT,
                side=PositionSide.LONG,
                quantity=1.0,
                broker_contract="MNQM6",
                broker_con_id=770561201,
                contract_is_active=False,
            ),
            now_ts=1_800_000_120,
        )
        self.assertEqual(rollover.intent_source, FUTURES_ROLLOVER_CLOSE_INTENT_SOURCE)
        self.assertIn("MNQM6", rollover.reason)
        self.assertIn("770561201", rollover.reason)

        now_ts = int(
            datetime(2026, 7, 22, 15, 0, 0, tzinfo=CT_TIMEZONE)
            .astimezone(timezone.utc)
            .timestamp()
        )
        for active, processor, expected_source in (
            (False, process_futures_rollover_close_once, FUTURES_ROLLOVER_CLOSE_INTENT_SOURCE),
            (True, process_futures_daily_flat_once, FUTURES_DAILY_FLAT_INTENT_SOURCE),
        ):
            conn = sqlite3.connect(":memory:")
            try:
                initialize_trade_db(conn)
                put_position(conn, updated_at=now_ts, side="LONG", qty=1.0, active=active)
                first = processor(conn, now_ts=now_ts)
                second = processor(conn, now_ts=now_ts + 1)
                self.assertEqual(len(first), 1)
                self.assertEqual(first[0].intent_source, expected_source)
                self.assertEqual(second, [])
            finally:
                conn.close()

    def _run_guard_case(self, *, position_ts, clock_ok, pending):
        with tempfile.TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "trade.sqlite3"
            now_ts = 1_800_000_150
            with patch("ib_trader.trade_schema.TRADE_DB_PATH", path):
                conn = get_trade_db_connection()
                try:
                    initialize_trade_db(conn)
                    put_position(conn, updated_at=position_ts)
                    conn.commit()
                finally:
                    conn.close()

                health = SimpleNamespace(
                    sample=object(),
                    corrected_now_ts=now_ts,
                    is_healthy=clock_ok,
                    reason="ok" if clock_ok else "clock drift",
                )
                with (
                    patch("ib_trader.trade_decision_service.read_effective_daily_trading_halt", return_value=None),
                    patch("ib_trader.trade_decision_service.read_latest_signal_events", return_value=[signal(ts=now_ts - 5)]),
                    patch("ib_trader.trade_decision_service.read_ib_clock_health", return_value=health),
                    patch("ib_trader.trade_decision_service.has_pending_daily_execution_stats", return_value=pending),
                ):
                    result = process_signal_events_once(max_signal_age_seconds=30)

                verify = get_trade_db_connection()
                try:
                    rows = verify.execute(
                        f"SELECT status, action, target_side FROM {TRADE_INTENTS_TABLE_NAME}"
                    ).fetchall()
                finally:
                    verify.close()
                return result, rows

    def test_integrated_risk_guards(self):
        cases = (
            (1_800_000_149, True, False, 1, 0, None),
            (1_800_000_100, True, False, 0, 1, "stale"),
            (1_800_000_149, False, False, 0, 0, "clock guard"),
            (1_800_000_149, True, True, 0, 0, "pending reconciliation"),
        )
        for position_ts, clock_ok, pending, created, rejected, warning in cases:
            with self.subTest(position_ts=position_ts, clock_ok=clock_ok, pending=pending):
                result, rows = self._run_guard_case(
                    position_ts=position_ts,
                    clock_ok=clock_ok,
                    pending=pending,
                )
                self.assertEqual(len(result.created), created)
                self.assertEqual(len(result.rejected), rejected)
                self.assertEqual(len(rows), created)
                if warning == "stale":
                    self.assertEqual(result.rejected[0].reason, "positions_latest_stale_trade_rejected")
                elif warning:
                    self.assertTrue(any(warning in text for text in result.guard_warnings))

    def test_position_freshness_boundary(self):
        conn = sqlite3.connect(":memory:")
        try:
            initialize_trade_db(conn)
            missing = read_position_snapshot_freshness(conn, instrument_code=INSTRUMENT, now_ts=100)
            self.assertTrue(missing.is_stale)
            put_position(conn, updated_at=100)
            exact = read_position_snapshot_freshness(
                conn,
                instrument_code=INSTRUMENT,
                now_ts=100 + POSITION_SNAPSHOT_MAX_AGE_SECONDS,
            )
            late = read_position_snapshot_freshness(
                conn,
                instrument_code=INSTRUMENT,
                now_ts=101 + POSITION_SNAPSHOT_MAX_AGE_SECONDS,
            )
            self.assertFalse(exact.is_stale)
            self.assertTrue(late.is_stale)
        finally:
            conn.close()


class ClockAndDailyGuardCharacterizationTest(unittest.TestCase):
    @staticmethod
    def _clock_sample(path: Path, *, sampled_at: int, offset: float):
        conn = sqlite3.connect(str(path))
        try:
            initialize_ib_clock_table(conn)
            conn.execute(
                f"INSERT INTO {IB_CLOCK_TABLE_NAME} "
                "(singleton_id, sampled_at_ts, server_time_ts, local_midpoint_ts, "
                "offset_seconds, round_trip_seconds, source_client_id) "
                "VALUES (1, ?, ?, ?, ?, ?, ?)",
                (sampled_at, sampled_at + offset, float(sampled_at), offset, 0.05, 240),
            )
            conn.commit()
        finally:
            conn.close()

    def test_clock_guard_missing_fresh_stale_and_drift(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            cases = (
                (root / "missing.sqlite3", None, None, False, "missing"),
                (root / "fresh.sqlite3", 990, 0.25, True, "ok"),
                (root / "stale.sqlite3", 700, 0.10, False, "stale"),
                (root / "drift.sqlite3", 990, 3.01, False, "drift"),
            )
            for path, sampled, offset, expected, reason in cases:
                with self.subTest(reason=reason):
                    if sampled is not None:
                        self._clock_sample(path, sampled_at=sampled, offset=offset)
                    with patch("core.ib_clock.STATE_DB_PATH", path):
                        health = read_ib_clock_health(
                            max_abs_offset_seconds=3.0,
                            max_sample_age_seconds=180,
                            now_ts=1_000,
                        )
                    self.assertEqual(health.is_healthy, expected)
                    self.assertIn(reason, health.reason)

    def test_current_day_halt_is_idempotent_and_unlocks_next_day(self):
        now_ts = int(
            datetime(2026, 7, 23, 12, 0, 0, tzinfo=MSK_TIMEZONE)
            .astimezone(timezone.utc)
            .timestamp()
        )
        day = get_moscow_day_context(now_ts)
        with tempfile.TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "state.sqlite3"
            with patch("core.daily_trading_guard.STATE_DB_PATH", path):
                monitoring = upsert_daily_guard_monitoring(
                    account_id=ACCOUNT_ID,
                    moscow_day=day.day_key,
                    target_usd=500.0,
                    realized_pnl_usd=400.0,
                    unrealized_pnl_usd=50.0,
                    total_pnl_usd=450.0,
                    now_ts=now_ts,
                )
                self.assertEqual(monitoring.status, DAILY_GUARD_STATUS_MONITORING)
                self.assertIsNone(read_effective_daily_trading_halt(account_id=ACCOUNT_ID, now_ts=now_ts))

                triggered, first = trigger_daily_trading_halt(
                    account_id=ACCOUNT_ID,
                    moscow_day=day.day_key,
                    target_usd=500.0,
                    realized_pnl_usd=450.0,
                    unrealized_pnl_usd=60.0,
                    total_pnl_usd=510.0,
                    now_ts=now_ts,
                )
                _, second = trigger_daily_trading_halt(
                    account_id=ACCOUNT_ID,
                    moscow_day=day.day_key,
                    target_usd=500.0,
                    realized_pnl_usd=450.0,
                    unrealized_pnl_usd=60.0,
                    total_pnl_usd=510.0,
                    now_ts=now_ts + 1,
                )
                self.assertEqual(triggered.status, DAILY_GUARD_STATUS_TRIGGERED)
                self.assertTrue(first)
                self.assertFalse(second)

                update_daily_guard_runtime_state(
                    account_id=ACCOUNT_ID,
                    moscow_day=day.day_key,
                    status=DAILY_GUARD_STATUS_CLOSING,
                    now_ts=now_ts + 2,
                )
                halted = update_daily_guard_runtime_state(
                    account_id=ACCOUNT_ID,
                    moscow_day=day.day_key,
                    status=DAILY_GUARD_STATUS_HALTED,
                    cleanup_completed=True,
                    now_ts=now_ts + 3,
                )
                effective = read_effective_daily_trading_halt(account_id=ACCOUNT_ID, now_ts=now_ts + 4)
                self.assertEqual(halted.status, DAILY_GUARD_STATUS_HALTED)
                self.assertIsNotNone(effective)
                self.assertIsNone(read_effective_daily_trading_halt(account_id=ACCOUNT_ID, now_ts=day.end_ts + 1))

    def test_unfinished_prior_day_cleanup_blocks_new_day(self):
        current = int(
            datetime(2026, 7, 23, 12, 0, 0, tzinfo=MSK_TIMEZONE)
            .astimezone(timezone.utc)
            .timestamp()
        )
        prior = current - 24 * 60 * 60
        prior_day = get_moscow_day_context(prior)
        with tempfile.TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "state.sqlite3"
            with patch("core.daily_trading_guard.STATE_DB_PATH", path):
                trigger_daily_trading_halt(
                    account_id=ACCOUNT_ID,
                    moscow_day=prior_day.day_key,
                    target_usd=500.0,
                    realized_pnl_usd=500.0,
                    unrealized_pnl_usd=10.0,
                    total_pnl_usd=510.0,
                    now_ts=prior,
                )
                update_daily_guard_runtime_state(
                    account_id=ACCOUNT_ID,
                    moscow_day=prior_day.day_key,
                    status=DAILY_GUARD_STATUS_CLOSING,
                    now_ts=prior + 1,
                )
                effective = read_effective_daily_trading_halt(account_id=ACCOUNT_ID, now_ts=current)
                self.assertIsNotNone(effective)
                self.assertFalse(effective.is_current_moscow_day)

                update_daily_guard_runtime_state(
                    account_id=ACCOUNT_ID,
                    moscow_day=prior_day.day_key,
                    status=DAILY_GUARD_STATUS_HALTED,
                    cleanup_completed=True,
                    now_ts=current + 1,
                )
                self.assertIsNone(read_effective_daily_trading_halt(account_id=ACCOUNT_ID, now_ts=current + 2))


class ExecutionAndProtectionCharacterizationTest(unittest.IsolatedAsyncioTestCase):
    async def test_live_preflight_open_close_and_nonactive_reverse(self):
        service = SimpleNamespace(ib=object(), account_id=ACCOUNT_ID)
        contract = object()

        with (
            patch(
                "ib_execution.execution_logic.sync_broker_positions_once",
                new_callable=AsyncMock,
                return_value=[BrokerPositionSnapshot(INSTRUMENT, "FLAT", 0.0, None, ACCOUNT_ID)],
            ),
            patch("ib_execution.execution_logic.build_execution_contract", return_value=contract),
        ):
            plan = await resolve_live_execution_plan(
                order_service=service,
                intent=execution_intent(
                    action="OPEN_POSITION",
                    target_side="LONG",
                    target_qty=1.0,
                    before_side="SHORT",
                    before_qty=9.0,
                ),
            )
        self.assertEqual(plan[:2], ("BUY", 1))
        self.assertIs(plan[2], contract)

        with (
            patch(
                "ib_execution.execution_logic.sync_broker_positions_once",
                new_callable=AsyncMock,
                return_value=[
                    BrokerPositionSnapshot(
                        INSTRUMENT,
                        "LONG",
                        2.0,
                        "MNQM6",
                        ACCOUNT_ID,
                        broker_con_id=770561201,
                        contract_is_active=False,
                    )
                ],
            ),
            patch("ib_execution.execution_logic.build_execution_contract", return_value=contract) as resolver,
        ):
            plan = await resolve_live_execution_plan(
                order_service=service,
                intent=execution_intent(
                    action="CLOSE_POSITION",
                    target_side="FLAT",
                    target_qty=0.0,
                    before_side="LONG",
                    before_qty=1.0,
                ),
            )
        self.assertEqual(plan[:2], ("SELL", 2))
        resolver.assert_called_once_with(
            instrument_code=INSTRUMENT,
            broker_con_id=770561201,
            broker_local_symbol="MNQM6",
        )

        with patch(
            "ib_execution.execution_logic.sync_broker_positions_once",
            new_callable=AsyncMock,
            return_value=[
                BrokerPositionSnapshot(
                    INSTRUMENT,
                    "LONG",
                    1.0,
                    "MNQM6",
                    ACCOUNT_ID,
                    broker_con_id=770561201,
                    contract_is_active=False,
                )
            ],
        ):
            with self.assertRaises(LivePositionStateError):
                await resolve_live_execution_plan(
                    order_service=service,
                    intent=execution_intent(
                        action="REVERSE_POSITION",
                        target_side="SHORT",
                        target_qty=1.0,
                        before_side="LONG",
                        before_qty=1.0,
                    ),
                )

    async def test_open_is_rejected_when_live_broker_is_not_flat(self):
        service = SimpleNamespace(ib=object(), account_id=ACCOUNT_ID)
        with patch(
            "ib_execution.execution_logic.sync_broker_positions_once",
            new_callable=AsyncMock,
            return_value=[
                BrokerPositionSnapshot(
                    INSTRUMENT,
                    "LONG",
                    1.0,
                    "MNQU6",
                    ACCOUNT_ID,
                    broker_con_id=793356225,
                    contract_is_active=True,
                )
            ],
        ):
            with self.assertRaises(LivePositionStateError):
                await resolve_live_execution_plan(
                    order_service=service,
                    intent=execution_intent(
                        action="OPEN_POSITION",
                        target_side="LONG",
                        target_qty=1.0,
                    ),
                )

    async def test_cancellation_requires_terminal_proof(self):
        trade = SimpleNamespace(
            order=SimpleNamespace(orderId=9001),
            orderStatus=SimpleNamespace(status="Submitted"),
        )
        ib = SimpleNamespace(openTrades=lambda: [trade], trades=lambda: [trade])

        async def cancel(order_id):
            self.assertEqual(order_id, 9001)
            trade.orderStatus.status = "Cancelled"

        service = SimpleNamespace(
            ib=ib,
            broker_state=SimpleNamespace(refresh_open_orders=AsyncMock(return_value=(True, None))),
            cancel_order_id=cancel,
        )
        result = await cancel_order_and_require_terminal(service, order_id=9001, context="test")
        self.assertEqual(result.terminal_status, "Cancelled")

        absent = SimpleNamespace(
            ib=SimpleNamespace(openTrades=lambda: [], trades=lambda: []),
            broker_state=SimpleNamespace(refresh_open_orders=AsyncMock(return_value=(True, None))),
            cancel_order_id=AsyncMock(),
        )
        with self.assertRaises(OrderCancellationUncertainError):
            await cancel_order_and_require_terminal(absent, order_id=9002, context="test")

        filled_trade = SimpleNamespace(
            order=SimpleNamespace(orderId=9003),
            orderStatus=SimpleNamespace(status="Filled"),
        )
        filled = SimpleNamespace(
            ib=SimpleNamespace(openTrades=lambda: [], trades=lambda: [filled_trade]),
            broker_state=SimpleNamespace(refresh_open_orders=AsyncMock(return_value=(True, None))),
            cancel_order_id=AsyncMock(),
        )
        with self.assertRaises(OrderFilledDuringCancellationError):
            await cancel_order_and_require_terminal(filled, order_id=9003, context="test")

        historical = SimpleNamespace(
            ib=SimpleNamespace(
                openTrades=lambda: [],
                trades=lambda: [
                    SimpleNamespace(
                        order=SimpleNamespace(orderId=9004),
                        orderStatus=SimpleNamespace(status="Submitted"),
                    )
                ],
            )
        )
        self.assertIsNone(read_known_order_status(historical, order_id=9004))


class ProtectiveStoreCharacterizationTest(unittest.TestCase):
    def setUp(self):
        self.conn = sqlite3.connect(":memory:")
        initialize_protective_order_db(self.conn)

    def tearDown(self):
        self.conn.close()

    def _pair(self):
        specs = (
            ("STOP_LOSS", 8001, "SL", "STP", None, 19_850.0),
            ("TAKE_PROFIT", 8002, "TP", "LIMIT", 20_075.0, None),
        )
        for role, order_id, suffix, order_type, limit_price, stop_price in specs:
            record_protective_order(
                self.conn,
                instrument_code=INSTRUMENT,
                parent_trade_intent_id=1,
                role=role,
                order_ref=f"IBMD_INTENT_1_MNQ_{suffix}",
                order_id=order_id,
                order_action="SELL",
                order_quantity=1,
                order_type=order_type,
                limit_price=limit_price,
                stop_price=stop_price,
                oca_group="IBMD_OCA_1_MNQ",
            )
        return read_active_protective_orders(self.conn)

    def test_status_terminality_and_oca_sibling(self):
        orders = self._pair()
        stop = next(row for row in orders if row["role"] == "STOP_LOSS")
        mark_protective_order_status(
            self.conn,
            order_id=stop["order_id"],
            status=PROTECTIVE_ORDER_STATUS_ACTIVE,
            error_text="still live",
        )
        self.assertEqual(
            self.conn.execute(
                "SELECT status, finished_at_ts FROM protective_orders WHERE order_id=8001"
            ).fetchone(),
            (PROTECTIVE_ORDER_STATUS_ACTIVE, None),
        )
        events = mark_oca_siblings_cancelled_after_fill(
            self.conn,
            protective_order=stop,
            filled_order_id=8001,
            filled_at_ts=1_800_000_200,
        )
        self.assertEqual(
            self.conn.execute(
                "SELECT status, finished_at_ts FROM protective_orders WHERE order_id=8002"
            ).fetchone(),
            (PROTECTIVE_ORDER_STATUS_CANCELLED, 1_800_000_200),
        )
        self.assertEqual(events[0]["event"], "CANCELLED")

    def test_protective_fill_creates_idempotent_synthetic_close(self):
        parent = write_trade_intent(
            self.conn,
            draft(source_id=501, ts=1_800_000_100),
        )
        record_protective_order(
            self.conn,
            instrument_code=INSTRUMENT,
            parent_trade_intent_id=parent,
            role="STOP_LOSS",
            order_ref=f"IBMD_INTENT_{parent}_{INSTRUMENT}_SL",
            order_id=8101,
            order_action="SELL",
            order_quantity=1,
            order_type="STP",
            stop_price=19_850.0,
        )
        protective = read_active_protective_orders(self.conn)[0]
        stats = {
            "filled_at_ts": 1_800_000_300,
            "filled_qty": 1.0,
            "avg_fill_price": 19_849.75,
            "total_commission": 0.62,
            "realized_pnl": -151.12,
        }
        first = create_protective_close_trade_intent(
            self.conn,
            protective_order=protective,
            statistics=stats,
        )
        second = create_protective_close_trade_intent(
            self.conn,
            protective_order=protective,
            statistics=stats,
        )
        self.assertEqual(first, second)
        self.assertEqual(
            self.conn.execute(
                f"SELECT action, target_side, status, order_id, realized_pnl "
                f"FROM {TRADE_INTENTS_TABLE_NAME} WHERE trade_intent_id=?",
                (first,),
            ).fetchone(),
            ("CLOSE_POSITION", "FLAT", "EXECUTED", 8101, -151.12),
        )
        self.assertEqual(
            self.conn.execute(
                "SELECT status, synthetic_trade_intent_id FROM protective_orders WHERE order_id=8101"
            ).fetchone(),
            (PROTECTIVE_ORDER_STATUS_FILLED, first),
        )


if __name__ == "__main__":
    unittest.main()
