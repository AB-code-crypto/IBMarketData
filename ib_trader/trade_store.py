import time
from pathlib import Path

from core.sqlite_utils import open_sqlite_connection
from core.state_db import STATE_DB_PATH, initialize_state_db
from ib_signal_filters.filter_event_store import (
    FILTERED_SIGNAL_LATEST_TABLE_NAME,
    initialize_filtered_signal_latest_table,
)
from ib_trader.trade_models import (
    FilteredSignalLatest,
    PositionSide,
    PositionSnapshot,
    TradeDecision,
    TradeDecisionAction,
)

TRADE_DB_PATH = Path(__file__).resolve().parent.parent / "data" / "trade.sqlite3"

POSITIONS_LATEST_TABLE_NAME = "positions_latest"
TRADE_DECISIONS_TABLE_NAME = "trade_decisions"
TRADE_INTENTS_TABLE_NAME = "trade_intents"


def get_trade_db_connection():
    """Что делает: открывает trade.sqlite3.
    Зачем нужна: торговые решения и логическая позиция живут отдельно от state DB."""
    return open_sqlite_connection(
        str(TRADE_DB_PATH),
        create_parent_dir=True,
        use_wal=True,
    )


def create_positions_latest_table_sql() -> str:
    """Что делает: возвращает SQL создания positions_latest.
    Зачем нужна: минимальный ib_trader должен читать текущее наличие позиции из trade.sqlite3."""
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
    Зачем нужна: каждый свежий filtered-сигнал должен быть обработан один раз и получить решение."""
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
    Зачем нужна: execution-слой будет читать только реальные торговые действия, а не NO_ACTION."""
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
        created_at_ts INTEGER NOT NULL
    );
    """


def initialize_trade_db(conn) -> None:
    """Что делает: создаёт минимальные таблицы trade.sqlite3.
    Зачем нужна: ib_trader может стартовать с чистой БД без отдельной миграции."""
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
        CREATE INDEX IF NOT EXISTS idx_trade_intents_status
        ON {TRADE_INTENTS_TABLE_NAME}(status, created_at_ts);
        """
    )


def read_latest_filtered_signals(*, max_signal_age_seconds: int) -> list[FilteredSignalLatest]:
    """Что делает: читает свежие разрешённые filtered-сигналы из state DB.
    Зачем нужна: ib_trader работает только с выходом ib_signal_filters."""
    initialize_state_db()

    conn = open_sqlite_connection(
        str(STATE_DB_PATH),
        create_parent_dir=True,
        use_wal=True,
    )

    try:
        initialize_filtered_signal_latest_table(conn)

        max_signal_age_seconds = int(max_signal_age_seconds)
        now_ts = int(time.time())
        min_signal_bar_ts = now_ts - max_signal_age_seconds

        rows = conn.execute(
            f"""
            SELECT
                instrument_code,
                source_signal_id,

                signal_bar_ts,
                signal_time_utc,
                signal_time_ct,
                signal_time_msk,

                direction
            FROM {FILTERED_SIGNAL_LATEST_TABLE_NAME}
            WHERE signal_bar_ts >= ?
            ORDER BY signal_bar_ts ASC, instrument_code ASC
            """,
            (min_signal_bar_ts,),
        ).fetchall()

        return [
            FilteredSignalLatest(
                instrument_code=str(row[0]),
                source_signal_id=int(row[1]),
                signal_bar_ts=int(row[2]),
                signal_time_utc=str(row[3]),
                signal_time_ct=None if row[4] is None else str(row[4]),
                signal_time_msk=str(row[5]),
                direction=str(row[6]),
            )
            for row in rows
        ]

    finally:
        conn.close()


def has_decision_for_signal(
        conn,
        *,
        instrument_code: str,
        source_signal_id: int,
        signal_bar_ts: int,
) -> bool:
    """Что делает: проверяет, был ли signal уже обработан ib_trader.
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
    """Что делает: читает текущую логическую позицию инструмента.
    Зачем нужна: решение OPEN/NO_ACTION/REVERSE зависит от текущей стороны позиции."""
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
            side=PositionSide.FLAT,
            quantity=0.0,
        )

    quantity = float(row[1])
    side = PositionSide(str(row[0]))

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


def decide_trade_action(
        *,
        signal: FilteredSignalLatest,
        position: PositionSnapshot,
) -> TradeDecision:
    """Что делает: принимает минимальное торговое решение по сигналу и текущей позиции.
    Зачем нужна: это первый простой ib_trader без риск-менеджмента и без исполнения."""
    signal_direction = str(signal.direction).upper()

    if signal_direction not in {"LONG", "SHORT"}:
        raise ValueError(f"Неизвестное направление сигнала: {signal.direction!r}")

    if position.side == PositionSide.FLAT or position.quantity <= 0.0:
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

            decision_action,
            decision_reason,

            position_before_side,
            position_before_qty,

            position_after_side,
            position_after_qty,

            created_at_ts
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)

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
    Зачем нужна: execution-слой должен читать только действия, которые требуют исполнения."""
    if decision.action == TradeDecisionAction.NO_ACTION:
        return

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
            created_at_ts
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)

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
            int(time.time()),
        ),
    )

def process_filtered_signals_once(*, max_signal_age_seconds: int) -> list[TradeDecision]:
    """Что делает: один раз обрабатывает свежие filtered_signal_latest.
    Зачем нужна: runner вызывает эту функцию циклом, а сама обработка остаётся синхронной и простой."""
    signals = read_latest_filtered_signals(
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

            position = read_position_snapshot(
                conn,
                instrument_code=signal.instrument_code,
            )
            decision = decide_trade_action(
                signal=signal,
                position=position,
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
