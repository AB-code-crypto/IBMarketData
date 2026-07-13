from __future__ import annotations

import time
from typing import Any

from core.sqlite_schema import require_exact_table_schema


SLOT_LOSS_EXTENSIONS_TABLE_NAME = "slot_loss_extensions"

SLOT_LOSS_EXTENSIONS_SCHEMA = (
    ('slot_loss_extension_id', 'INTEGER', 0, None, 1),
    ('instrument_code', 'TEXT', 1, None, 0),
    ('entry_trade_intent_id', 'INTEGER', 1, None, 0),
    ('entry_order_ref', 'TEXT', 1, None, 0),
    ('position_side', 'TEXT', 1, None, 0),
    ('position_qty', 'REAL', 1, None, 0),
    ('entry_price', 'REAL', 1, None, 0),
    ('slot_start_ts', 'INTEGER', 1, None, 0),
    ('slot_end_ts', 'INTEGER', 1, None, 0),
    ('close_at_ts', 'INTEGER', 1, None, 0),
    ('deadline_ts', 'INTEGER', 1, None, 0),
    ('max_adverse_price', 'REAL', 1, None, 0),
    ('max_drawdown_points', 'REAL', 1, None, 0),
    ('current_exit_price', 'REAL', 1, None, 0),
    ('current_drawdown_points', 'REAL', 1, None, 0),
    ('drawdown_ratio', 'REAL', 1, None, 0),
    ('take_profit_price', 'REAL', 1, None, 0),
    ('stop_loss_price', 'REAL', 1, None, 0),
    ('take_profit_order_id', 'INTEGER', 1, None, 0),
    ('stop_loss_order_id', 'INTEGER', 1, None, 0),
    ('take_profit_order_ref', 'TEXT', 1, None, 0),
    ('stop_loss_order_ref', 'TEXT', 1, None, 0),
    ('oca_group', 'TEXT', 1, None, 0),
    ('status', 'TEXT', 1, None, 0),
    ('finish_reason', 'TEXT', 0, None, 0),
    ('error_text', 'TEXT', 0, None, 0),
    ('created_at_ts', 'INTEGER', 1, None, 0),
    ('updated_at_ts', 'INTEGER', 1, None, 0),
    ('finished_at_ts', 'INTEGER', 0, None, 0),
)


SLOT_LOSS_EXTENSION_STATUS_ACTIVE = "ACTIVE"
SLOT_LOSS_EXTENSION_STATUS_FINISHED = "FINISHED"
SLOT_LOSS_EXTENSION_STATUS_FAILED = "FAILED"


TERMINAL_SLOT_LOSS_EXTENSION_STATUSES = {
    SLOT_LOSS_EXTENSION_STATUS_FINISHED,
    SLOT_LOSS_EXTENSION_STATUS_FAILED,
}


def create_slot_loss_extensions_table_sql() -> str:
    return f"""
    CREATE TABLE IF NOT EXISTS {SLOT_LOSS_EXTENSIONS_TABLE_NAME} (
        slot_loss_extension_id INTEGER PRIMARY KEY AUTOINCREMENT,

        instrument_code TEXT NOT NULL,
        entry_trade_intent_id INTEGER NOT NULL,
        entry_order_ref TEXT NOT NULL,

        position_side TEXT NOT NULL,
        position_qty REAL NOT NULL,
        entry_price REAL NOT NULL,

        slot_start_ts INTEGER NOT NULL,
        slot_end_ts INTEGER NOT NULL,
        close_at_ts INTEGER NOT NULL,
        deadline_ts INTEGER NOT NULL,

        max_adverse_price REAL NOT NULL,
        max_drawdown_points REAL NOT NULL,
        current_exit_price REAL NOT NULL,
        current_drawdown_points REAL NOT NULL,
        drawdown_ratio REAL NOT NULL,

        take_profit_price REAL NOT NULL,
        stop_loss_price REAL NOT NULL,
        take_profit_order_id INTEGER NOT NULL,
        stop_loss_order_id INTEGER NOT NULL,
        take_profit_order_ref TEXT NOT NULL,
        stop_loss_order_ref TEXT NOT NULL,
        oca_group TEXT NOT NULL,

        status TEXT NOT NULL,
        finish_reason TEXT,
        error_text TEXT,

        created_at_ts INTEGER NOT NULL,
        updated_at_ts INTEGER NOT NULL,
        finished_at_ts INTEGER
    );
    """


def initialize_slot_loss_extension_db(conn) -> None:
    conn.execute(create_slot_loss_extensions_table_sql())
    require_exact_table_schema(
        conn,
        table_name=SLOT_LOSS_EXTENSIONS_TABLE_NAME,
        expected_schema=SLOT_LOSS_EXTENSIONS_SCHEMA,
    )
    conn.execute(
        f"""CREATE INDEX IF NOT EXISTS idx_slot_loss_extensions_active_instrument
        ON {SLOT_LOSS_EXTENSIONS_TABLE_NAME}(instrument_code, status, created_at_ts);"""
    )
    conn.execute(
        f"""CREATE UNIQUE INDEX IF NOT EXISTS idx_slot_loss_extensions_one_active
        ON {SLOT_LOSS_EXTENSIONS_TABLE_NAME}(instrument_code)
        WHERE status = '{SLOT_LOSS_EXTENSION_STATUS_ACTIVE}';"""
    )


def has_active_slot_loss_extension_for_instrument(
        conn,
        *,
        instrument_code: str,
) -> bool:
    initialize_slot_loss_extension_db(conn)
    row = conn.execute(
        f"""SELECT 1 FROM {SLOT_LOSS_EXTENSIONS_TABLE_NAME}
        WHERE instrument_code = ? AND status = ? LIMIT 1""",
        (str(instrument_code), SLOT_LOSS_EXTENSION_STATUS_ACTIVE),
    ).fetchone()
    return row is not None


def row_to_slot_loss_extension(row) -> dict[str, Any]:
    return {
        "slot_loss_extension_id": int(row[0]),
        "instrument_code": str(row[1]),
        "entry_trade_intent_id": int(row[2]),
        "entry_order_ref": str(row[3]),
        "position_side": str(row[4]),
        "position_qty": float(row[5]),
        "entry_price": float(row[6]),
        "slot_start_ts": int(row[7]),
        "slot_end_ts": int(row[8]),
        "close_at_ts": int(row[9]),
        "deadline_ts": int(row[10]),
        "max_adverse_price": float(row[11]),
        "max_drawdown_points": float(row[12]),
        "current_exit_price": float(row[13]),
        "current_drawdown_points": float(row[14]),
        "drawdown_ratio": float(row[15]),
        "take_profit_price": float(row[16]),
        "stop_loss_price": float(row[17]),
        "take_profit_order_id": int(row[18]),
        "stop_loss_order_id": int(row[19]),
        "take_profit_order_ref": str(row[20]),
        "stop_loss_order_ref": str(row[21]),
        "oca_group": str(row[22]),
        "status": str(row[23]),
        "finish_reason": None if row[24] is None else str(row[24]),
        "error_text": None if row[25] is None else str(row[25]),
        "created_at_ts": int(row[26]),
        "updated_at_ts": int(row[27]),
        "finished_at_ts": None if row[28] is None else int(row[28]),
    }


def read_active_slot_loss_extensions(conn, *, instrument_code: str | None = None) -> list[dict[str, Any]]:
    initialize_slot_loss_extension_db(conn)

    where_instrument = ""
    params: tuple[Any, ...]

    if instrument_code is None:
        params = (SLOT_LOSS_EXTENSION_STATUS_ACTIVE,)
    else:
        where_instrument = "AND instrument_code = ?"
        params = (SLOT_LOSS_EXTENSION_STATUS_ACTIVE, str(instrument_code))

    rows = conn.execute(
        f"""
        SELECT
            slot_loss_extension_id,
            instrument_code,
            entry_trade_intent_id,
            entry_order_ref,
            position_side,
            position_qty,
            entry_price,
            slot_start_ts,
            slot_end_ts,
            close_at_ts,
            deadline_ts,
            max_adverse_price,
            max_drawdown_points,
            current_exit_price,
            current_drawdown_points,
            drawdown_ratio,
            take_profit_price,
            stop_loss_price,
            take_profit_order_id,
            stop_loss_order_id,
            take_profit_order_ref,
            stop_loss_order_ref,
            oca_group,
            status,
            finish_reason,
            error_text,
            created_at_ts,
            updated_at_ts,
            finished_at_ts
        FROM {SLOT_LOSS_EXTENSIONS_TABLE_NAME}
        WHERE status = ?
          {where_instrument}
        ORDER BY created_at_ts ASC, slot_loss_extension_id ASC
        """,
        params,
    ).fetchall()

    return [row_to_slot_loss_extension(row) for row in rows]


def record_slot_loss_extension_started(
        conn,
        *,
        instrument_code: str,
        entry_trade_intent_id: int,
        entry_order_ref: str,
        position_side: str,
        position_qty: float,
        entry_price: float,
        slot_start_ts: int,
        slot_end_ts: int,
        close_at_ts: int,
        deadline_ts: int,
        max_adverse_price: float,
        max_drawdown_points: float,
        current_exit_price: float,
        current_drawdown_points: float,
        drawdown_ratio: float,
        take_profit_price: float,
        stop_loss_price: float,
        take_profit_order_id: int,
        stop_loss_order_id: int,
        take_profit_order_ref: str,
        stop_loss_order_ref: str,
        oca_group: str,
        now_ts: int | None = None,
) -> int:
    initialize_slot_loss_extension_db(conn)
    now_ts = int(time.time() if now_ts is None else now_ts)

    conn.execute(
        f"""
        INSERT INTO {SLOT_LOSS_EXTENSIONS_TABLE_NAME} (
            instrument_code,
            entry_trade_intent_id,
            entry_order_ref,
            position_side,
            position_qty,
            entry_price,
            slot_start_ts,
            slot_end_ts,
            close_at_ts,
            deadline_ts,
            max_adverse_price,
            max_drawdown_points,
            current_exit_price,
            current_drawdown_points,
            drawdown_ratio,
            take_profit_price,
            stop_loss_price,
            take_profit_order_id,
            stop_loss_order_id,
            take_profit_order_ref,
            stop_loss_order_ref,
            oca_group,
            status,
            finish_reason,
            error_text,
            created_at_ts,
            updated_at_ts,
            finished_at_ts
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, NULL, ?, ?, NULL)
        """,
        (
            str(instrument_code),
            int(entry_trade_intent_id),
            str(entry_order_ref),
            str(position_side).upper(),
            float(position_qty),
            float(entry_price),
            int(slot_start_ts),
            int(slot_end_ts),
            int(close_at_ts),
            int(deadline_ts),
            float(max_adverse_price),
            float(max_drawdown_points),
            float(current_exit_price),
            float(current_drawdown_points),
            float(drawdown_ratio),
            float(take_profit_price),
            float(stop_loss_price),
            int(take_profit_order_id),
            int(stop_loss_order_id),
            str(take_profit_order_ref),
            str(stop_loss_order_ref),
            str(oca_group),
            SLOT_LOSS_EXTENSION_STATUS_ACTIVE,
            now_ts,
            now_ts,
        ),
    )

    row = conn.execute(
        f"""
        SELECT slot_loss_extension_id
        FROM {SLOT_LOSS_EXTENSIONS_TABLE_NAME}
        WHERE instrument_code = ?
          AND status = ?
        ORDER BY slot_loss_extension_id DESC
        LIMIT 1
        """,
        (
            str(instrument_code),
            SLOT_LOSS_EXTENSION_STATUS_ACTIVE,
        ),
    ).fetchone()

    if row is None:
        raise RuntimeError(f"slot_loss_extension was inserted, but id was not found: {instrument_code}")

    return int(row[0])


def mark_slot_loss_extension_finished(
        conn,
        *,
        slot_loss_extension_id: int,
        finish_reason: str,
        error_text: str | None = None,
        now_ts: int | None = None,
) -> None:
    initialize_slot_loss_extension_db(conn)
    now_ts = int(time.time() if now_ts is None else now_ts)

    conn.execute(
        f"""
        UPDATE {SLOT_LOSS_EXTENSIONS_TABLE_NAME}
        SET
            status = ?,
            finish_reason = ?,
            error_text = ?,
            updated_at_ts = ?,
            finished_at_ts = ?
        WHERE slot_loss_extension_id = ?
        """,
        (
            SLOT_LOSS_EXTENSION_STATUS_FINISHED,
            str(finish_reason),
            error_text,
            now_ts,
            now_ts,
            int(slot_loss_extension_id),
        ),
    )


def mark_slot_loss_extension_failed(
        conn,
        *,
        slot_loss_extension_id: int,
        error_text: str,
        now_ts: int | None = None,
) -> None:
    initialize_slot_loss_extension_db(conn)
    now_ts = int(time.time() if now_ts is None else now_ts)

    conn.execute(
        f"""
        UPDATE {SLOT_LOSS_EXTENSIONS_TABLE_NAME}
        SET
            status = ?,
            finish_reason = ?,
            error_text = ?,
            updated_at_ts = ?,
            finished_at_ts = ?
        WHERE slot_loss_extension_id = ?
        """,
        (
            SLOT_LOSS_EXTENSION_STATUS_FAILED,
            "failed",
            str(error_text),
            now_ts,
            now_ts,
            int(slot_loss_extension_id),
        ),
    )
