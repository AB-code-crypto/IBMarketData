import sqlite3
import time
from pathlib import Path

from contracts import Instrument
from core.sqlite_utils import open_sqlite_connection
from core.state_db import STATE_DB_PATH, initialize_state_db
from ib_job_data.feature_db_sql import quote_identifier
from ib_job_data.job_features_config import MA_ZONE_COLUMN_NAME, REGIME_COLUMN_NAME
from ib_job_data.rebuild_mid_price import get_instrument_feature_db_path
from ib_job_data.sma_features import SMA_TABLE_NAME
from ib_signal.signal_event_store import SIGNAL_EVENTS_TABLE_NAME, initialize_signal_events_table
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
    """Что делает: открывает trade.sqlite3.
    Зачем нужна: торговые решения, intents и позиции живут отдельно от state DB."""
    return open_sqlite_connection(
        str(TRADE_DB_PATH),
        create_parent_dir=True,
        use_wal=True,
    )


def create_positions_latest_table_sql() -> str:
    """Что делает: возвращает SQL создания positions_latest.
    Зачем нужна: ib_trader читает оттуда только подтверждённую текущую позицию."""
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
    """Что делает: возвращает SQL создания trade_decisions.
    Зачем нужна: каждый свежий signal_event должен быть обработан один раз и получить решение."""
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
    """Что делает: возвращает SQL создания trade_intents.
    Зачем нужна: trade_intents — это очередь исполнения и одновременно минимальная история исполнений."""
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
    """Что делает: создаёт минимальные таблицы trade.sqlite3.
    Зачем нужна: trader/execution могут стартовать с чистой trade DB."""
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
    """Что делает: читает последние свежие signal_events по инструментам из state DB.
    Зачем нужна: ib_trader теперь работает напрямую с выходом ib_signal без filtered_signal_latest."""
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
                se.potential_used
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
            )
            for row in rows
        ]

    finally:
        conn.close()


def read_market_features_for_signal(signal: TraderSignalEvent) -> MarketFeatureSnapshot:
    """Что делает: читает regime/ma_zone из job DB на момент сигнала.
    Зачем нужна: ib_trader принимает торговое решение с учётом рыночных признаков."""
    if signal.instrument_code not in Instrument:
        return MarketFeatureSnapshot(
            instrument_code=signal.instrument_code,
            signal_bar_ts=signal.signal_bar_ts,
            feature_bar_ts=None,
            regime=None,
            ma_zone=None,
        )

    instrument_row = Instrument[signal.instrument_code]
    feature_db_path = get_instrument_feature_db_path(
        instrument_code=signal.instrument_code,
        instrument_row=instrument_row,
    )

    if not feature_db_path.is_file():
        return MarketFeatureSnapshot(
            instrument_code=signal.instrument_code,
            signal_bar_ts=signal.signal_bar_ts,
            feature_bar_ts=None,
            regime=None,
            ma_zone=None,
        )

    conn = open_sqlite_connection(
        str(feature_db_path),
        require_existing_file=True,
        use_wal=False,
    )

    try:
        row = conn.execute(
            f"""
            SELECT
                bar_time_ts,
                {quote_identifier(REGIME_COLUMN_NAME)},
                {quote_identifier(MA_ZONE_COLUMN_NAME)}
            FROM {quote_identifier(SMA_TABLE_NAME)}
            WHERE bar_time_ts <= ?
            ORDER BY bar_time_ts DESC
            LIMIT 1
            """,
            (int(signal.signal_bar_ts),),
        ).fetchone()

    except sqlite3.Error:
        return MarketFeatureSnapshot(
            instrument_code=signal.instrument_code,
            signal_bar_ts=signal.signal_bar_ts,
            feature_bar_ts=None,
            regime=None,
            ma_zone=None,
        )

    finally:
        conn.close()

    if row is None:
        return MarketFeatureSnapshot(
            instrument_code=signal.instrument_code,
            signal_bar_ts=signal.signal_bar_ts,
            feature_bar_ts=None,
            regime=None,
            ma_zone=None,
        )

    return MarketFeatureSnapshot(
        instrument_code=signal.instrument_code,
        signal_bar_ts=signal.signal_bar_ts,
        feature_bar_ts=int(row[0]),
        regime=None if row[1] is None else int(row[1]),
        ma_zone=None if row[2] is None else int(row[2]),
    )


def has_decision_for_signal(
        conn,
        *,
        instrument_code: str,
        source_signal_id: int,
        signal_bar_ts: int,
) -> bool:
    """Что делает: проверяет, был ли signal_event уже обработан ib_trader.
    Зачем нужна: один source-сигнал нельзя превращать в несколько торговых решений."""
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
    """Что делает: читает текущую подтверждённую позицию инструмента.
    Зачем нужна: отсутствие строки означает UNKNOWN, а не FLAT."""
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
        return PositionSnapshot(
            instrument_code=str(instrument_code),
            side=PositionSide.UNKNOWN,
            quantity=0.0,
        )

    if quantity <= 0.0:
        return PositionSnapshot(
            instrument_code=str(instrument_code),
            side=PositionSide.FLAT,
            quantity=0.0,
        )

    return PositionSnapshot(
        instrument_code=str(instrument_code),
        side=side,
        quantity=quantity,
    )


def is_market_features_available(features: MarketFeatureSnapshot) -> bool:
    """Что делает: проверяет, что regime/ma_zone реально прочитаны.
    Зачем нужна: trader не должен открывать сделку, если рыночное состояние неизвестно."""
    return features.regime is not None and features.ma_zone is not None


def decide_trade_action(
        *,
        signal: TraderSignalEvent,
        position: PositionSnapshot,
        market_features: MarketFeatureSnapshot,
) -> TradeDecision:
    """Что делает: принимает минимальное торговое решение по сигналу, позиции и market-features.
    Зачем нужна: ib_trader теперь является полноценным слоем принятия решения."""
    signal_direction = str(signal.direction).upper()

    if signal_direction not in {"LONG", "SHORT"}:
        raise ValueError(f"Неизвестное направление сигнала: {signal.direction!r}")

    if not is_market_features_available(market_features):
        action = TradeDecisionAction.NO_ACTION
        reason = "market_features_unknown"
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
        action=action,
        reason=reason,
        position_before_side=position.side,
        position_before_qty=float(position.quantity),
        position_after_side=after_side,
        position_after_qty=float(after_qty),
    )


def write_trade_decision(conn, decision: TradeDecision) -> int:
    """Что делает: пишет торговое решение и возвращает decision_id.
    Зачем нужна: downstream-слои должны видеть, почему и что решил ib_trader."""
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

            decision_action,
            decision_reason,

            position_before_side,
            position_before_qty,

            position_after_side,
            position_after_qty,

            created_at_ts
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)

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
    """Что делает: пишет trade_intent для action != NO_ACTION.
    Зачем нужна: execution-слой читает только действия, которые требуют исполнения."""
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

            status,

            created_at_ts,
            updated_at_ts
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)

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
            "NEW",
            now_ts,
            now_ts,
        ),
    )


def process_signal_events_once(*, max_signal_age_seconds: int) -> list[TradeDecision]:
    """Что делает: один раз обрабатывает свежие signal_events.
    Зачем нужна: trader принимает решение напрямую по сигналу, job-features и текущей позиции."""
    signals = read_latest_signal_events(
        max_signal_age_seconds=max_signal_age_seconds,
    )

    if not signals:
        return []

    conn = get_trade_db_connection()

    try:
        initialize_trade_db(conn)

        decisions: list[TradeDecision] = []

        for signal in signals:
            if has_decision_for_signal(
                    conn,
                    instrument_code=signal.instrument_code,
                    source_signal_id=signal.source_signal_id,
                    signal_bar_ts=signal.signal_bar_ts,
            ):
                continue

            market_features = read_market_features_for_signal(signal)
            position = read_position_snapshot(
                conn,
                instrument_code=signal.instrument_code,
            )
            decision = decide_trade_action(
                signal=signal,
                position=position,
                market_features=market_features,
            )

            decision_id = write_trade_decision(conn, decision)
            write_trade_intent_if_needed(
                conn,
                decision_id=decision_id,
                decision=decision,
            )

            decisions.append(decision)

        conn.commit()
        return decisions

    finally:
        conn.close()
