import json
import time
from datetime import datetime, timezone
from pathlib import Path

from contracts import Instrument
from core.sqlite_utils import open_sqlite_connection
from core.time_utils import CT_TIMEZONE, MSK_TIMEZONE, SQLITE_DATETIME_FORMAT
from core.state_db import STATE_DB_PATH, initialize_state_db
from ib_signal.signal_config import DEFAULT_SIGNAL_CONFIG, SignalWindowMode
from ib_signal.signal_event_store import SIGNAL_EVENTS_TABLE_NAME, initialize_signal_events_table
from ib_signal.signal_schedule import get_grid_slot_start_ts
from ib_trader.trade_models import (
    MarketFeatureSnapshot,
    PositionSide,
    PositionSnapshot,
    TradeDecision,
    TradeDecisionAction,
    TraderSignalEvent,
)

TRADE_DB_PATH = Path(__file__).resolve().parent.parent / "data" / "trade.sqlite3"

POSITIONS_LATEST_TABLE_NAME = "positions_latest"
TRADE_DECISIONS_TABLE_NAME = "trade_decisions"
TRADE_INTENTS_TABLE_NAME = "trade_intents"


def get_trade_db_connection():
    return open_sqlite_connection(
        str(TRADE_DB_PATH),
        create_parent_dir=True,
        use_wal=True,
    )


def create_positions_latest_table_sql() -> str:
    return f"""
    CREATE TABLE IF NOT EXISTS {POSITIONS_LATEST_TABLE_NAME} (
        instrument_code TEXT PRIMARY KEY,

        side TEXT NOT NULL,
        quantity REAL NOT NULL,

        updated_at_ts INTEGER NOT NULL,
        last_decision_id INTEGER,
        last_source_signal_id INTEGER
    );
    """


def create_trade_decisions_table_sql() -> str:
    return f"""
    CREATE TABLE IF NOT EXISTS {TRADE_DECISIONS_TABLE_NAME} (
        decision_id INTEGER PRIMARY KEY AUTOINCREMENT,

        source_signal_id INTEGER NOT NULL,
        instrument_code TEXT NOT NULL,

        signal_bar_ts INTEGER NOT NULL,
        signal_time_utc TEXT NOT NULL,
        signal_time_ct TEXT,
        signal_time_msk TEXT NOT NULL,

        signal_direction TEXT NOT NULL,
        entry_price REAL NOT NULL,

        best_pearson REAL NOT NULL,
        candidate_score_best REAL,

        potential_end_delta_points REAL NOT NULL,
        potential_max_profit_points REAL NOT NULL,
        potential_max_drawdown_points REAL NOT NULL,
        potential_used INTEGER NOT NULL,

        regime INTEGER,
        ma_zone INTEGER,
        signal_strength TEXT NOT NULL,

        order_type TEXT NOT NULL,
        order_policy_reason TEXT NOT NULL,
        limit_offset_points REAL,
        limit_price REAL,
        ttl_seconds INTEGER,
        rules_json TEXT NOT NULL,

        decision_action TEXT NOT NULL,
        decision_reason TEXT NOT NULL,

        position_before_side TEXT NOT NULL,
        position_before_qty REAL NOT NULL,

        position_after_side TEXT NOT NULL,
        position_after_qty REAL NOT NULL,

        created_at_ts INTEGER NOT NULL,

        UNIQUE (
            instrument_code,
            source_signal_id,
            signal_bar_ts
        )
    );
    """


def create_trade_intents_table_sql() -> str:
    return f"""
    CREATE TABLE IF NOT EXISTS {TRADE_INTENTS_TABLE_NAME} (
        trade_intent_id INTEGER PRIMARY KEY AUTOINCREMENT,

        decision_id INTEGER NOT NULL UNIQUE,
        source_signal_id INTEGER NOT NULL,
        instrument_code TEXT NOT NULL,

        action TEXT NOT NULL,
        target_side TEXT NOT NULL,
        target_qty REAL NOT NULL,

        position_before_side TEXT NOT NULL,
        position_before_qty REAL NOT NULL,

        order_type TEXT NOT NULL,
        limit_price REAL,
        limit_offset_points REAL,
        ttl_seconds INTEGER,

        status TEXT NOT NULL,

        order_id INTEGER,
        order_action TEXT,
        order_quantity INTEGER,
        avg_fill_price REAL,
        total_commission REAL,
        realized_pnl REAL,
        error_text TEXT,

        created_at_ts INTEGER NOT NULL,
        updated_at_ts INTEGER NOT NULL,
        sent_at_ts INTEGER,
        finished_at_ts INTEGER
    );
    """


def initialize_trade_db(conn) -> None:
    conn.execute(create_positions_latest_table_sql())
    conn.execute(create_trade_decisions_table_sql())
    conn.execute(create_trade_intents_table_sql())

    conn.execute(
        f"""
        CREATE INDEX IF NOT EXISTS idx_trade_decisions_signal
        ON {TRADE_DECISIONS_TABLE_NAME}(instrument_code, source_signal_id, signal_bar_ts);
        """
    )
    conn.execute(
        f"""
        CREATE INDEX IF NOT EXISTS idx_trade_decisions_action_time
        ON {TRADE_DECISIONS_TABLE_NAME}(decision_action, created_at_ts);
        """
    )
    conn.execute(
        f"""
        CREATE INDEX IF NOT EXISTS idx_trade_intents_status
        ON {TRADE_INTENTS_TABLE_NAME}(status, created_at_ts);
        """
    )
    conn.execute(
        f"""
        CREATE INDEX IF NOT EXISTS idx_trade_intents_instrument_time
        ON {TRADE_INTENTS_TABLE_NAME}(instrument_code, created_at_ts);
        """
    )


def read_latest_signal_events(*, max_signal_age_seconds: int) -> list[TraderSignalEvent]:
    initialize_state_db()

    conn = open_sqlite_connection(
        str(STATE_DB_PATH),
        create_parent_dir=True,
        use_wal=True,
    )

    try:
        initialize_signal_events_table(conn)

        max_signal_age_seconds = int(max_signal_age_seconds)
        if max_signal_age_seconds <= 0:
            return []

        now_ts = int(time.time())
        min_signal_bar_ts = now_ts - max_signal_age_seconds

        rows = conn.execute(
            f"""
            SELECT
                se.signal_id,
                se.instrument_code,

                se.signal_bar_ts,
                se.signal_time_utc,
                se.signal_time_ct,
                se.signal_time_msk,

                se.direction,
                se.entry_price,

                se.best_pearson,
                se.candidate_score_best,

                se.potential_end_delta_points,
                se.potential_max_profit_points,
                se.potential_max_drawdown_points,
                se.potential_used,

                se.feature_bar_ts,
                se.regime,
                se.ma_zone,

                se.signal_allowed,
                se.signal_reject_reason,

                se.signal_strength,

                se.order_type,
                se.order_policy_reason,
                se.limit_offset_points,
                se.limit_price,
                se.ttl_seconds,

                se.signal_rules_json
            FROM {SIGNAL_EVENTS_TABLE_NAME} AS se
            WHERE se.signal_bar_ts >= ?
              AND se.signal_id = (
                  SELECT se2.signal_id
                  FROM {SIGNAL_EVENTS_TABLE_NAME} AS se2
                  WHERE se2.instrument_code = se.instrument_code
                    AND se2.signal_bar_ts >= ?
                  ORDER BY se2.signal_bar_ts DESC, se2.signal_id DESC
                  LIMIT 1
              )
            ORDER BY se.instrument_code ASC
            """,
            (
                min_signal_bar_ts,
                min_signal_bar_ts,
            ),
        ).fetchall()

        return [
            TraderSignalEvent(
                source_signal_id=int(row[0]),
                instrument_code=str(row[1]),
                signal_bar_ts=int(row[2]),
                signal_time_utc=str(row[3]),
                signal_time_ct=None if row[4] is None else str(row[4]),
                signal_time_msk=str(row[5]),
                direction=str(row[6]),
                entry_price=float(row[7]),
                best_pearson=float(row[8]),
                candidate_score_best=None if row[9] is None else float(row[9]),
                potential_end_delta_points=float(row[10]),
                potential_max_profit_points=float(row[11]),
                potential_max_drawdown_points=float(row[12]),
                potential_used=int(row[13]),
                feature_bar_ts=None if row[14] is None else int(row[14]),
                regime=None if row[15] is None else int(row[15]),
                ma_zone=None if row[16] is None else int(row[16]),
                signal_allowed=bool(int(row[17])),
                signal_reject_reason=None if row[18] is None else str(row[18]),
                signal_strength=str(row[19]),
                order_type=str(row[20]).upper(),
                order_policy_reason=str(row[21]),
                limit_offset_points=None if row[22] is None else float(row[22]),
                limit_price=None if row[23] is None else float(row[23]),
                ttl_seconds=None if row[24] is None else int(row[24]),
                signal_rules_json=str(row[25]),
            )
            for row in rows
        ]

    finally:
        conn.close()


def build_market_features_from_signal_event(signal: TraderSignalEvent) -> MarketFeatureSnapshot:
    """Что делает: строит market-features из уже интерпретированного signal_events.
    Зачем нужна: ib_trader больше не должен читать job DB и знать правила интерпретации сигнала."""
    return MarketFeatureSnapshot(
        instrument_code=signal.instrument_code,
        signal_bar_ts=signal.signal_bar_ts,
        feature_bar_ts=signal.feature_bar_ts,
        regime=signal.regime,
        ma_zone=signal.ma_zone,
    )


def build_rule_result_from_signal_event(signal: TraderSignalEvent) -> TraderRuleEvaluation:
    """Что делает: превращает поля signal_events в rule-result для старой decision-логики.
    Зачем нужна: ib_trader принимает stateful-решение, но не интерпретирует сигнал заново."""
    reject_reasons = []
    if not signal.signal_allowed:
        reject_reasons.append(signal.signal_reject_reason or "signal_interpretation_rejected")

    return TraderRuleEvaluation(
        allowed=bool(signal.signal_allowed),
        reject_reasons=reject_reasons,
        signal_strength=signal.signal_strength,
        order_type=signal.order_type,
        order_policy_reason=signal.order_policy_reason,
        limit_offset_points=signal.limit_offset_points,
        ttl_seconds=signal.ttl_seconds,
        rules_json=signal.signal_rules_json,
    )


def has_decision_for_signal(
        conn,
        *,
        instrument_code: str,
        source_signal_id: int,
        signal_bar_ts: int,
) -> bool:
    initialize_trade_db(conn)

    row = conn.execute(
        f"""
        SELECT 1
        FROM {TRADE_DECISIONS_TABLE_NAME}
        WHERE instrument_code = ?
          AND source_signal_id = ?
          AND signal_bar_ts = ?
        LIMIT 1
        """,
        (
            str(instrument_code),
            int(source_signal_id),
            int(signal_bar_ts),
        ),
    ).fetchone()

    return row is not None


def read_position_snapshot(conn, *, instrument_code: str) -> PositionSnapshot:
    initialize_trade_db(conn)

    row = conn.execute(
        f"""
        SELECT side, quantity
        FROM {POSITIONS_LATEST_TABLE_NAME}
        WHERE instrument_code = ?
        """,
        (str(instrument_code),),
    ).fetchone()

    if row is None:
        return PositionSnapshot(
            instrument_code=str(instrument_code),
            side=PositionSide.UNKNOWN,
            quantity=0.0,
        )

    quantity = float(row[1])
    side = PositionSide(str(row[0]).upper())

    if side == PositionSide.UNKNOWN:
        return PositionSnapshot(str(instrument_code), PositionSide.UNKNOWN, 0.0)

    if quantity <= 0.0:
        return PositionSnapshot(str(instrument_code), PositionSide.FLAT, 0.0)

    return PositionSnapshot(str(instrument_code), side, quantity)



TRADER_MAX_NEW_INTENT_AGE_SECONDS = 10
STALE_NEW_INTENT_ERROR_TEXT = "stale trade_intent before trader decision"


def expire_stale_new_trade_intents(
        conn,
        *,
        max_age_seconds: int = TRADER_MAX_NEW_INTENT_AGE_SECONDS,
        now_ts: int | None = None,
) -> int:
    """Что делает: переводит старые NEW trade_intents в FAILED.
    Зачем нужна: если execution не был запущен, trader не должен копить старые NEW intents."""
    max_age_seconds = int(max_age_seconds)

    if max_age_seconds <= 0:
        return 0

    now_ts = int(time.time() if now_ts is None else now_ts)
    min_created_at_ts = now_ts - max_age_seconds

    changes_before = conn.total_changes

    conn.execute(
        f"""
        UPDATE {TRADE_INTENTS_TABLE_NAME}
        SET
            status = 'FAILED',
            error_text = ?,
            updated_at_ts = ?,
            finished_at_ts = ?
        WHERE status = 'NEW'
          AND created_at_ts < ?
        """,
        (
            f"{STALE_NEW_INTENT_ERROR_TEXT}; max_age_seconds={max_age_seconds}",
            now_ts,
            now_ts,
            min_created_at_ts,
        ),
    )

    return int(conn.total_changes - changes_before)


def read_position_updated_at_ts(conn, *, instrument_code: str) -> int | None:
    row = conn.execute(
        f"""
        SELECT updated_at_ts
        FROM {POSITIONS_LATEST_TABLE_NAME}
        WHERE instrument_code = ?
        """,
        (str(instrument_code),),
    ).fetchone()

    if row is None or row[0] is None:
        return None

    return int(row[0])


def read_latest_non_failed_trade_intent(conn, *, instrument_code: str) -> dict | None:
    """Что делает: читает последний не-FAILED intent по инструменту.
    Зачем нужна: trader не должен создавать новый приказ, пока предыдущий активен или позиция не синхронизирована."""
    row = conn.execute(
        f"""
        SELECT
            trade_intent_id,
            action,
            target_side,
            target_qty,
            status,
            created_at_ts,
            updated_at_ts,
            sent_at_ts,
            finished_at_ts
        FROM {TRADE_INTENTS_TABLE_NAME}
        WHERE instrument_code = ?
          AND status != 'FAILED'
        ORDER BY
            COALESCE(finished_at_ts, updated_at_ts, sent_at_ts, created_at_ts) DESC,
            trade_intent_id DESC
        LIMIT 1
        """,
        (str(instrument_code),),
    ).fetchone()

    if row is None:
        return None

    return {
        "trade_intent_id": int(row[0]),
        "action": str(row[1]),
        "target_side": str(row[2]),
        "target_qty": float(row[3]),
        "status": str(row[4]),
        "created_at_ts": int(row[5]),
        "updated_at_ts": int(row[6]),
        "sent_at_ts": None if row[7] is None else int(row[7]),
        "finished_at_ts": None if row[8] is None else int(row[8]),
        "event_ts": int(row[8] or row[6] or row[7] or row[5]),
    }


def build_trader_guard_reject_result(
        *,
        reason: str,
        details: dict,
) -> TraderRuleEvaluation:
    return TraderRuleEvaluation(
        allowed=False,
        reject_reasons=[reason],
        signal_strength="NEUTRAL",
        order_type="MARKET",
        order_policy_reason="trader_intent_guard",
        limit_offset_points=None,
        ttl_seconds=None,
        rules_json=json.dumps(
            [
                {
                    "rule": "trader_intent_guard",
                    "result": "REJECT",
                    "reason": reason,
                    "details": details,
                }
            ],
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        ),
    )


def build_trader_intent_guard_result_if_needed(
        conn,
        *,
        signal: TraderSignalEvent,
        position: PositionSnapshot,
) -> TraderRuleEvaluation | None:
    """Что делает: запрещает новый intent, если предыдущий ещё активен или позиция после execution не синхронизирована.
    Зачем нужна: нельзя открывать второй ордер по новому сигналу, пока старый приказ не обработан корректно."""
    latest_intent = read_latest_non_failed_trade_intent(
        conn,
        instrument_code=signal.instrument_code,
    )

    if latest_intent is None:
        return None

    active_statuses = {"NEW", "SENDING", "ACCEPTED"}

    if latest_intent["status"] in active_statuses:
        return build_trader_guard_reject_result(
            reason="active_trade_intent_exists",
            details={
                "source_signal_id": signal.source_signal_id,
                "latest_trade_intent_id": latest_intent["trade_intent_id"],
                "latest_status": latest_intent["status"],
                "latest_action": latest_intent["action"],
                "latest_target_side": latest_intent["target_side"],
                "position_side": position.side.value,
                "position_qty": position.quantity,
            },
        )

    if latest_intent["status"] == "EXECUTED":
        position_updated_at_ts = read_position_updated_at_ts(
            conn,
            instrument_code=signal.instrument_code,
        )

        if position_updated_at_ts is None or position_updated_at_ts <= latest_intent["event_ts"]:
            return build_trader_guard_reject_result(
                reason="position_sync_stale_after_trade_intent",
                details={
                    "source_signal_id": signal.source_signal_id,
                    "latest_trade_intent_id": latest_intent["trade_intent_id"],
                    "latest_status": latest_intent["status"],
                    "latest_action": latest_intent["action"],
                    "latest_target_side": latest_intent["target_side"],
                    "latest_event_ts": latest_intent["event_ts"],
                    "position_side": position.side.value,
                    "position_qty": position.quantity,
                    "position_updated_at_ts": position_updated_at_ts,
                },
            )

    return None


def calculate_limit_price(
        *,
        signal_direction: str,
        entry_price: float,
        limit_offset_points: float | None,
) -> float | None:
    offset = float(limit_offset_points or 0.0)

    if offset <= 0.0:
        return float(entry_price)

    direction = str(signal_direction).upper()

    if direction == "LONG":
        return float(entry_price) - offset

    if direction == "SHORT":
        return float(entry_price) + offset

    return float(entry_price)


def decide_trade_action(
        *,
        signal: TraderSignalEvent,
        position: PositionSnapshot,
        market_features: MarketFeatureSnapshot,
        rule_result: TraderRuleEvaluation,
) -> TradeDecision:
    signal_direction = str(signal.direction).upper()

    if signal_direction not in {"LONG", "SHORT"}:
        raise ValueError(f"Неизвестное направление сигнала: {signal.direction!r}")

    order_type = str(rule_result.order_type).upper()
    limit_price = (
        calculate_limit_price(
            signal_direction=signal_direction,
            entry_price=float(signal.entry_price),
            limit_offset_points=rule_result.limit_offset_points,
        )
        if order_type == "LIMIT"
        else None
    )

    if not rule_result.allowed:
        action = TradeDecisionAction.NO_ACTION
        reason = ";".join(rule_result.reject_reasons) or "rules_rejected"
        after_side = position.side
        after_qty = float(position.quantity)

    elif position.side == PositionSide.UNKNOWN:
        action = TradeDecisionAction.NO_ACTION
        reason = "position_unknown"
        after_side = PositionSide.UNKNOWN
        after_qty = 0.0

    elif position.side == PositionSide.FLAT or position.quantity <= 0.0:
        action = TradeDecisionAction.OPEN_POSITION
        reason = "flat_position_open_by_signal"
        after_side = PositionSide(signal_direction)
        after_qty = 1.0

    elif position.side.value == signal_direction:
        action = TradeDecisionAction.NO_ACTION
        reason = "same_direction_position_exists"
        after_side = position.side
        after_qty = float(position.quantity)

    else:
        action = TradeDecisionAction.REVERSE_POSITION
        reason = "opposite_signal_reverse_position"
        after_side = PositionSide(signal_direction)
        after_qty = max(1.0, float(position.quantity))

    return TradeDecision(
        source_signal_id=signal.source_signal_id,
        instrument_code=signal.instrument_code,
        signal_bar_ts=signal.signal_bar_ts,
        signal_time_utc=signal.signal_time_utc,
        signal_time_ct=signal.signal_time_ct,
        signal_time_msk=signal.signal_time_msk,
        signal_direction=signal_direction,
        entry_price=float(signal.entry_price),
        best_pearson=float(signal.best_pearson),
        candidate_score_best=signal.candidate_score_best,
        potential_end_delta_points=float(signal.potential_end_delta_points),
        potential_max_profit_points=float(signal.potential_max_profit_points),
        potential_max_drawdown_points=float(signal.potential_max_drawdown_points),
        potential_used=int(signal.potential_used),
        regime=market_features.regime,
        ma_zone=market_features.ma_zone,
        signal_strength=rule_result.signal_strength,
        order_type=order_type,
        order_policy_reason=rule_result.order_policy_reason,
        limit_offset_points=rule_result.limit_offset_points,
        limit_price=limit_price,
        ttl_seconds=rule_result.ttl_seconds,
        rules_json=rule_result.rules_json,
        action=action,
        reason=reason,
        position_before_side=position.side,
        position_before_qty=float(position.quantity),
        position_after_side=after_side,
        position_after_qty=float(after_qty),
    )


def write_trade_decision(conn, decision: TradeDecision) -> int:
    initialize_trade_db(conn)

    created_at_ts = int(time.time())

    conn.execute(
        f"""
        INSERT INTO {TRADE_DECISIONS_TABLE_NAME} (
            source_signal_id,
            instrument_code,

            signal_bar_ts,
            signal_time_utc,
            signal_time_ct,
            signal_time_msk,

            signal_direction,
            entry_price,

            best_pearson,
            candidate_score_best,

            potential_end_delta_points,
            potential_max_profit_points,
            potential_max_drawdown_points,
            potential_used,

            regime,
            ma_zone,
            signal_strength,

            order_type,
            order_policy_reason,
            limit_offset_points,
            limit_price,
            ttl_seconds,
            rules_json,

            decision_action,
            decision_reason,

            position_before_side,
            position_before_qty,

            position_after_side,
            position_after_qty,

            created_at_ts
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)

        ON CONFLICT (
            instrument_code,
            source_signal_id,
            signal_bar_ts
        ) DO NOTHING
        """,
        (
            int(decision.source_signal_id),
            decision.instrument_code,
            int(decision.signal_bar_ts),
            decision.signal_time_utc,
            decision.signal_time_ct,
            decision.signal_time_msk,
            decision.signal_direction,
            float(decision.entry_price),
            float(decision.best_pearson),
            decision.candidate_score_best,
            float(decision.potential_end_delta_points),
            float(decision.potential_max_profit_points),
            float(decision.potential_max_drawdown_points),
            int(decision.potential_used),
            decision.regime,
            decision.ma_zone,
            decision.signal_strength,
            decision.order_type,
            decision.order_policy_reason,
            decision.limit_offset_points,
            decision.limit_price,
            decision.ttl_seconds,
            decision.rules_json,
            decision.action.value,
            decision.reason,
            decision.position_before_side.value,
            float(decision.position_before_qty),
            decision.position_after_side.value,
            float(decision.position_after_qty),
            created_at_ts,
        ),
    )

    row = conn.execute(
        f"""
        SELECT decision_id
        FROM {TRADE_DECISIONS_TABLE_NAME}
        WHERE instrument_code = ?
          AND source_signal_id = ?
          AND signal_bar_ts = ?
        """,
        (
            decision.instrument_code,
            int(decision.source_signal_id),
            int(decision.signal_bar_ts),
        ),
    ).fetchone()

    if row is None or row[0] is None:
        raise RuntimeError("TradeDecision был записан, но decision_id не найден")

    return int(row[0])


def write_trade_intent_if_needed(conn, *, decision_id: int, decision: TradeDecision) -> None:
    if decision.action == TradeDecisionAction.NO_ACTION:
        return

    now_ts = int(time.time())

    conn.execute(
        f"""
        INSERT INTO {TRADE_INTENTS_TABLE_NAME} (
            decision_id,
            source_signal_id,
            instrument_code,

            action,
            target_side,
            target_qty,

            position_before_side,
            position_before_qty,

            order_type,
            limit_price,
            limit_offset_points,
            ttl_seconds,

            status,

            created_at_ts,
            updated_at_ts
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)

        ON CONFLICT(decision_id) DO NOTHING
        """,
        (
            int(decision_id),
            int(decision.source_signal_id),
            decision.instrument_code,
            decision.action.value,
            decision.position_after_side.value,
            float(decision.position_after_qty),
            decision.position_before_side.value,
            float(decision.position_before_qty),
            decision.order_type,
            decision.limit_price,
            decision.limit_offset_points,
            decision.ttl_seconds,
            "NEW",
            now_ts,
            now_ts,
        ),
    )



GRID_CLOSE_SOURCE_SIGNAL_ID = -1
GRID_CLOSE_DECISION_REASON = "grid_close_before_slot_end"


def build_time_text_fields_from_ts(ts: int) -> tuple[str, str, str]:
    dt_utc = datetime.fromtimestamp(int(ts), tz=timezone.utc)
    dt_ct = dt_utc.astimezone(CT_TIMEZONE)
    dt_msk = dt_utc.astimezone(MSK_TIMEZONE)

    return (
        dt_utc.strftime(SQLITE_DATETIME_FORMAT),
        dt_ct.strftime(SQLITE_DATETIME_FORMAT),
        dt_msk.strftime(SQLITE_DATETIME_FORMAT),
    )


def get_grid_close_context(*, now_ts: int) -> dict | None:
    """Что делает: определяет, настало ли окно закрытия GRID-слота.
    Зачем нужна: в GRID позиция должна закрываться за slot_close_before_end_seconds до конца слота."""
    settings = DEFAULT_SIGNAL_CONFIG

    if settings.signal_window_mode != SignalWindowMode.GRID:
        return None

    slot_step_seconds = int(settings.slot_step_minutes) * 60
    close_before_seconds = int(settings.slot_close_before_end_seconds)

    if slot_step_seconds <= 0:
        return None

    if close_before_seconds < 0 or close_before_seconds >= slot_step_seconds:
        return None

    slot_start_ts = get_grid_slot_start_ts(
        current_bar_ts=int(now_ts),
        slot_step_minutes=int(settings.slot_step_minutes),
        slot_start_minute_of_day=int(settings.slot_start_minute_of_day),
    )
    slot_end_ts = int(slot_start_ts) + slot_step_seconds
    close_at_ts = int(slot_end_ts) - close_before_seconds

    if close_at_ts <= int(now_ts) < slot_end_ts:
        return {
            "slot_start_ts": int(slot_start_ts),
            "slot_end_ts": int(slot_end_ts),
            "close_at_ts": int(close_at_ts),
            "slot_step_seconds": int(slot_step_seconds),
            "close_before_seconds": int(close_before_seconds),
        }

    return None


def read_open_position_snapshots(conn) -> list[PositionSnapshot]:
    """Что делает: читает все открытые позиции из positions_latest.
    Зачем нужна: GRID-close работает даже без нового signal_event."""
    initialize_trade_db(conn)

    rows = conn.execute(
        f"""
        SELECT instrument_code, side, quantity
        FROM {POSITIONS_LATEST_TABLE_NAME}
        WHERE side IN ('LONG', 'SHORT')
          AND quantity > 0
        ORDER BY instrument_code
        """
    ).fetchall()

    return [
        PositionSnapshot(
            instrument_code=str(row[0]),
            side=PositionSide(str(row[1]).upper()),
            quantity=float(row[2]),
        )
        for row in rows
    ]


def has_grid_close_decision(
        conn,
        *,
        instrument_code: str,
        close_at_ts: int,
) -> bool:
    row = conn.execute(
        f"""
        SELECT 1
        FROM {TRADE_DECISIONS_TABLE_NAME}
        WHERE instrument_code = ?
          AND source_signal_id = ?
          AND signal_bar_ts = ?
        LIMIT 1
        """,
        (
            str(instrument_code),
            GRID_CLOSE_SOURCE_SIGNAL_ID,
            int(close_at_ts),
        ),
    ).fetchone()

    return row is not None


def has_active_trade_intent_for_instrument(conn, *, instrument_code: str) -> bool:
    row = conn.execute(
        f"""
        SELECT 1
        FROM {TRADE_INTENTS_TABLE_NAME}
        WHERE instrument_code = ?
          AND status IN ('NEW', 'SENDING', 'ACCEPTED')
        LIMIT 1
        """,
        (str(instrument_code),),
    ).fetchone()

    return row is not None


def build_grid_close_trade_decision(
        *,
        position: PositionSnapshot,
        close_context: dict,
) -> TradeDecision:
    decision_ts = int(close_context.get("decision_ts", close_context["close_at_ts"]))
    signal_time_utc, signal_time_ct, signal_time_msk = build_time_text_fields_from_ts(decision_ts)

    rules_json = json.dumps(
        [
            {
                "rule": "grid_time_exit",
                "result": "CLOSE_POSITION",
                "reason": GRID_CLOSE_DECISION_REASON,
                "details": {
                    "slot_start_ts": int(close_context["slot_start_ts"]),
                    "slot_end_ts": int(close_context["slot_end_ts"]),
                    "close_at_ts": int(close_context["close_at_ts"]),
                    "slot_step_seconds": int(close_context["slot_step_seconds"]),
                    "close_before_seconds": int(close_context["close_before_seconds"]),
                    "position_side": position.side.value,
                    "position_qty": float(position.quantity),
                },
            }
        ],
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )

    return TradeDecision(
        source_signal_id=GRID_CLOSE_SOURCE_SIGNAL_ID,
        instrument_code=position.instrument_code,
        signal_bar_ts=decision_ts,
        signal_time_utc=signal_time_utc,
        signal_time_ct=signal_time_ct,
        signal_time_msk=signal_time_msk,
        signal_direction="CLOSE",
        entry_price=0.0,
        best_pearson=0.0,
        candidate_score_best=None,
        potential_end_delta_points=0.0,
        potential_max_profit_points=0.0,
        potential_max_drawdown_points=0.0,
        potential_used=0,
        regime=None,
        ma_zone=None,
        signal_strength="GRID_CLOSE",
        order_type="MARKET",
        order_policy_reason="grid_time_exit",
        limit_offset_points=None,
        limit_price=None,
        ttl_seconds=None,
        rules_json=rules_json,
        action=TradeDecisionAction.CLOSE_POSITION,
        reason=GRID_CLOSE_DECISION_REASON,
        position_before_side=position.side,
        position_before_qty=float(position.quantity),
        position_after_side=PositionSide.FLAT,
        position_after_qty=0.0,
    )



FUTURES_DAILY_FLAT_SOURCE_SIGNAL_ID = -2
FUTURES_DAILY_FLAT_DECISION_REASON = "futures_daily_flat_before_no_trade_hour"
FUTURES_NO_NEW_TRADES_HOUR_CT = 15
FUTURES_CLEARING_HOUR_CT = 16


def get_ct_day_ts(*, now_ts: int, hour: int, minute: int = 0, second: int = 0) -> int:
    now_ct = datetime.fromtimestamp(int(now_ts), tz=timezone.utc).astimezone(CT_TIMEZONE)
    target_ct = now_ct.replace(
        hour=int(hour),
        minute=int(minute),
        second=int(second),
        microsecond=0,
    )
    return int(target_ct.astimezone(timezone.utc).timestamp())


def get_futures_daily_flat_context(*, now_ts: int) -> dict | None:
    """Что делает: определяет окно принудительного закрытия futures перед no-trade hour.
    Зачем нужна: робот никогда не должен переносить futures-позицию через клиринг."""
    settings = DEFAULT_SIGNAL_CONFIG
    close_before_seconds = int(settings.slot_close_before_end_seconds)

    no_new_trades_ts = get_ct_day_ts(
        now_ts=now_ts,
        hour=FUTURES_NO_NEW_TRADES_HOUR_CT,
    )
    clearing_ts = get_ct_day_ts(
        now_ts=now_ts,
        hour=FUTURES_CLEARING_HOUR_CT,
    )
    close_at_ts = no_new_trades_ts - close_before_seconds

    # Если сервис пропустил точку 14:59:50, всё равно пытаемся закрыть до клиринга.
    if close_at_ts <= int(now_ts) < clearing_ts:
        return {
            "no_new_trades_ts": int(no_new_trades_ts),
            "clearing_ts": int(clearing_ts),
            "close_at_ts": int(close_at_ts),
            "close_before_seconds": int(close_before_seconds),
        }

    return None


def is_futures_instrument(instrument_code: str) -> bool:
    instrument_row = Instrument.get(str(instrument_code))

    if instrument_row is None:
        return False

    return str(instrument_row.get("secType", "")).upper() == "FUT"


def has_unresolved_trade_intent_for_instrument(conn, *, instrument_code: str) -> bool:
    """Что делает: проверяет, есть ли активный/ещё не подтверждённый intent по инструменту.
    Зачем нужна: close-manager не должен плодить повторные CLOSE до обновления positions_latest."""
    latest_intent = read_latest_non_failed_trade_intent(
        conn,
        instrument_code=instrument_code,
    )

    if latest_intent is None:
        return False

    if latest_intent["status"] in {"NEW", "SENDING", "ACCEPTED"}:
        return True

    if latest_intent["status"] == "EXECUTED":
        position_updated_at_ts = read_position_updated_at_ts(
            conn,
            instrument_code=instrument_code,
        )
        return position_updated_at_ts is None or position_updated_at_ts <= latest_intent["event_ts"]

    return False


def build_futures_daily_flat_trade_decision(
        *,
        position: PositionSnapshot,
        close_context: dict,
        decision_ts: int,
) -> TradeDecision:
    signal_time_utc, signal_time_ct, signal_time_msk = build_time_text_fields_from_ts(decision_ts)

    rules_json = json.dumps(
        [
            {
                "rule": "futures_daily_flat",
                "result": "CLOSE_POSITION",
                "reason": FUTURES_DAILY_FLAT_DECISION_REASON,
                "details": {
                    "no_new_trades_ts": int(close_context["no_new_trades_ts"]),
                    "clearing_ts": int(close_context["clearing_ts"]),
                    "close_at_ts": int(close_context["close_at_ts"]),
                    "close_before_seconds": int(close_context["close_before_seconds"]),
                    "position_side": position.side.value,
                    "position_qty": float(position.quantity),
                },
            }
        ],
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )

    return TradeDecision(
        source_signal_id=FUTURES_DAILY_FLAT_SOURCE_SIGNAL_ID,
        instrument_code=position.instrument_code,
        signal_bar_ts=int(decision_ts),
        signal_time_utc=signal_time_utc,
        signal_time_ct=signal_time_ct,
        signal_time_msk=signal_time_msk,
        signal_direction="CLOSE",
        entry_price=0.0,
        best_pearson=0.0,
        candidate_score_best=None,
        potential_end_delta_points=0.0,
        potential_max_profit_points=0.0,
        potential_max_drawdown_points=0.0,
        potential_used=0,
        regime=None,
        ma_zone=None,
        signal_strength="FUTURES_DAILY_FLAT",
        order_type="MARKET",
        order_policy_reason="futures_daily_flat",
        limit_offset_points=None,
        limit_price=None,
        ttl_seconds=None,
        rules_json=rules_json,
        action=TradeDecisionAction.CLOSE_POSITION,
        reason=FUTURES_DAILY_FLAT_DECISION_REASON,
        position_before_side=position.side,
        position_before_qty=float(position.quantity),
        position_after_side=PositionSide.FLAT,
        position_after_qty=0.0,
    )


def process_futures_daily_flat_once(conn, *, now_ts: int | None = None) -> list[TradeDecision]:
    """Что делает: создаёт CLOSE_POSITION для открытых futures перед no-trade hour/клирингом.
    Зачем нужна: единое правило для GRID и ROLLING — не переносить futures через ночь."""
    now_ts = int(time.time() if now_ts is None else now_ts)
    close_context = get_futures_daily_flat_context(now_ts=now_ts)

    if close_context is None:
        return []

    decisions: list[TradeDecision] = []

    for position in read_open_position_snapshots(conn):
        if not is_futures_instrument(position.instrument_code):
            continue

        if has_unresolved_trade_intent_for_instrument(
                conn,
                instrument_code=position.instrument_code,
        ):
            continue

        decision = build_futures_daily_flat_trade_decision(
            position=position,
            close_context=close_context,
            decision_ts=now_ts,
        )
        decision_id = write_trade_decision(conn, decision)
        write_trade_intent_if_needed(
            conn,
            decision_id=decision_id,
            decision=decision,
        )
        decisions.append(decision)

    return decisions


def process_grid_close_once(conn, *, now_ts: int | None = None) -> list[TradeDecision]:
    """Что делает: создаёт CLOSE_POSITION intent для GRID-режима в окне закрытия слота.
    Зачем нужна: GRID должен закрывать позицию по времени, даже если нет нового сигнала."""
    now_ts = int(time.time() if now_ts is None else now_ts)
    close_context = get_grid_close_context(now_ts=now_ts)

    if close_context is None:
        return []

    decisions: list[TradeDecision] = []

    for position in read_open_position_snapshots(conn):
        if has_unresolved_trade_intent_for_instrument(
                conn,
                instrument_code=position.instrument_code,
        ):
            continue

        close_context["decision_ts"] = now_ts
        decision = build_grid_close_trade_decision(
            position=position,
            close_context=close_context,
        )
        decision_id = write_trade_decision(conn, decision)
        write_trade_intent_if_needed(
            conn,
            decision_id=decision_id,
            decision=decision,
        )
        decisions.append(decision)

    return decisions


def process_signal_events_once(*, max_signal_age_seconds: int) -> list[TradeDecision]:
    signals = read_latest_signal_events(max_signal_age_seconds=max_signal_age_seconds)

    conn = get_trade_db_connection()

    try:
        initialize_trade_db(conn)

        expire_stale_new_trade_intents(
            conn,
            max_age_seconds=TRADER_MAX_NEW_INTENT_AGE_SECONDS,
        )

        decisions: list[TradeDecision] = []

        decisions.extend(process_futures_daily_flat_once(conn))
        decisions.extend(process_grid_close_once(conn))

        for signal in signals:
            if has_decision_for_signal(
                    conn,
                    instrument_code=signal.instrument_code,
                    source_signal_id=signal.source_signal_id,
                    signal_bar_ts=signal.signal_bar_ts,
            ):
                continue

            market_features = build_market_features_from_signal_event(signal)
            position = read_position_snapshot(conn, instrument_code=signal.instrument_code)

            rule_result = build_trader_intent_guard_result_if_needed(
                conn,
                signal=signal,
                position=position,
            )

            if rule_result is None:
                rule_result = build_rule_result_from_signal_event(signal)

            decision = decide_trade_action(
                signal=signal,
                position=position,
                market_features=market_features,
                rule_result=rule_result,
            )

            decision_id = write_trade_decision(conn, decision)
            write_trade_intent_if_needed(conn, decision_id=decision_id, decision=decision)
            decisions.append(decision)

        conn.commit()
        return decisions

    finally:
        conn.close()
