from __future__ import annotations

from pathlib import Path
from typing import Any

from config import settings_live as settings
from contracts import Instrument
from core.instrument_db import get_instrument_db_path, get_instrument_table_name
from core.sqlite_utils import open_sqlite_connection


def quote_identifier(value: str) -> str:
    return '"' + str(value).replace('"', '""') + '"'


def get_price_db_context(instrument_code: str) -> tuple[Path, str] | None:
    instrument_row = Instrument.get(str(instrument_code))
    if instrument_row is None:
        return None

    db_path = Path(
        get_instrument_db_path(
            settings=settings,
            instrument_code=str(instrument_code),
            instrument_row=instrument_row,
        )
    )
    if not db_path.is_file():
        return None

    table_name = get_instrument_table_name(
        instrument_code=str(instrument_code),
        instrument_row=instrument_row,
    )
    return db_path, table_name


def _row_to_bar(row) -> dict[str, Any]:
    bid_open = float(row[1])
    bid_high = float(row[2])
    bid_low = float(row[3])
    bid_close = float(row[4])
    ask_open = float(row[5])
    ask_high = float(row[6])
    ask_low = float(row[7])
    ask_close = float(row[8])

    return {
        "bar_time_ts": int(row[0]),
        "bid_open": bid_open,
        "bid_high": bid_high,
        "bid_low": bid_low,
        "bid_close": bid_close,
        "ask_open": ask_open,
        "ask_high": ask_high,
        "ask_low": ask_low,
        "ask_close": ask_close,
        # Compatibility fields. Trigger calculations never use them.
        "mid_open": (bid_open + ask_open) / 2.0,
        "mid_high": (bid_high + ask_high) / 2.0,
        "mid_low": (bid_low + ask_low) / 2.0,
        "mid_close": (bid_close + ask_close) / 2.0,
        "spread_open": ask_open - bid_open,
        "spread_high": ask_high - bid_high,
        "spread_low": ask_low - bid_low,
        "spread_close": ask_close - bid_close,
    }


def read_latest_executable_bar(
        *,
        instrument_code: str,
        start_ts: int,
        now_ts: int,
) -> dict[str, Any] | None:
    context = get_price_db_context(instrument_code)
    if context is None:
        return None

    db_path, table_name = context
    conn = open_sqlite_connection(str(db_path), use_wal=True)

    try:
        row = conn.execute(
            f"""
            SELECT
                bar_time_ts,
                bid_open,
                bid_high,
                bid_low,
                bid_close,
                ask_open,
                ask_high,
                ask_low,
                ask_close
            FROM {quote_identifier(table_name)}
            WHERE bar_time_ts >= ?
              AND bar_time_ts <= ?
              AND bid_open IS NOT NULL
              AND bid_high IS NOT NULL
              AND bid_low IS NOT NULL
              AND bid_close IS NOT NULL
              AND ask_open IS NOT NULL
              AND ask_high IS NOT NULL
              AND ask_low IS NOT NULL
              AND ask_close IS NOT NULL
            ORDER BY bar_time_ts DESC
            LIMIT 1
            """,
            (int(start_ts), int(now_ts)),
        ).fetchone()
    finally:
        conn.close()

    if row is None:
        return None

    return _row_to_bar(row)


def read_first_executable_level_touch_row(
        *,
        instrument_code: str,
        side: str,
        level_kind: str,
        level_price: float,
        start_ts: int,
        now_ts: int,
) -> dict[str, Any] | None:
    context = get_price_db_context(instrument_code)
    if context is None:
        return None

    side_value = str(side).upper()
    level_kind_value = str(level_kind).upper()

    if side_value == "LONG" and level_kind_value == "STOP":
        trigger_column = "bid_low"
        operator = "<="
    elif side_value == "LONG" and level_kind_value == "TAKE_PROFIT":
        trigger_column = "bid_high"
        operator = ">="
    elif side_value == "SHORT" and level_kind_value == "STOP":
        trigger_column = "ask_high"
        operator = ">="
    elif side_value == "SHORT" and level_kind_value == "TAKE_PROFIT":
        trigger_column = "ask_low"
        operator = "<="
    else:
        return None

    db_path, table_name = context
    conn = open_sqlite_connection(str(db_path), use_wal=True)

    try:
        row = conn.execute(
            f"""
            SELECT
                bar_time_ts,
                bid_open,
                bid_high,
                bid_low,
                bid_close,
                ask_open,
                ask_high,
                ask_low,
                ask_close,
                {quote_identifier(trigger_column)} AS trigger_price
            FROM {quote_identifier(table_name)}
            WHERE bar_time_ts >= ?
              AND bar_time_ts <= ?
              AND bid_open IS NOT NULL
              AND bid_high IS NOT NULL
              AND bid_low IS NOT NULL
              AND bid_close IS NOT NULL
              AND ask_open IS NOT NULL
              AND ask_high IS NOT NULL
              AND ask_low IS NOT NULL
              AND ask_close IS NOT NULL
              AND {quote_identifier(trigger_column)} {operator} ?
            ORDER BY bar_time_ts ASC
            LIMIT 1
            """,
            (int(start_ts), int(now_ts), float(level_price)),
        ).fetchone()
    finally:
        conn.close()

    if row is None:
        return None

    bar = _row_to_bar(row[:9])
    bar["trigger_price"] = float(row[9])
    return bar


def read_executable_price_path_stats(
        *,
        instrument_code: str,
        position_side: str,
        entry_price: float,
        entry_ts: int,
        now_ts: int,
) -> dict[str, float | int] | None:
    context = get_price_db_context(instrument_code)
    if context is None:
        return None

    side = str(position_side).upper()
    if side == "LONG":
        current_column = "bid_close"
        adverse_aggregate = "MIN(bid_low)"
    elif side == "SHORT":
        current_column = "ask_close"
        adverse_aggregate = "MAX(ask_high)"
    else:
        return None

    db_path, table_name = context
    table_ref = quote_identifier(table_name)
    conn = open_sqlite_connection(str(db_path), use_wal=True)

    try:
        latest = conn.execute(
            f"""
            SELECT bar_time_ts, {quote_identifier(current_column)}
            FROM {table_ref}
            WHERE bar_time_ts >= ?
              AND bar_time_ts <= ?
              AND bid_close IS NOT NULL
              AND ask_close IS NOT NULL
            ORDER BY bar_time_ts DESC
            LIMIT 1
            """,
            (int(entry_ts), int(now_ts)),
        ).fetchone()

        adverse = conn.execute(
            f"""
            SELECT {adverse_aggregate}
            FROM {table_ref}
            WHERE bar_time_ts >= ?
              AND bar_time_ts <= ?
            """,
            (int(entry_ts), int(now_ts)),
        ).fetchone()
    finally:
        conn.close()

    if latest is None or latest[1] is None:
        return None
    if adverse is None or adverse[0] is None:
        return None

    entry = float(entry_price)
    current_exit_price = float(latest[1])
    max_adverse_price = float(adverse[0])

    if side == "LONG":
        max_drawdown_points = max(0.0, entry - max_adverse_price)
        current_drawdown_points = max(0.0, entry - current_exit_price)
    else:
        max_drawdown_points = max(0.0, max_adverse_price - entry)
        current_drawdown_points = max(0.0, current_exit_price - entry)

    drawdown_ratio = (
        0.0
        if max_drawdown_points <= 0.0
        else current_drawdown_points / max_drawdown_points
    )

    return {
        "latest_bar_ts": int(latest[0]),
        "current_exit_price": current_exit_price,
        "max_adverse_price": max_adverse_price,
        "max_drawdown_points": max_drawdown_points,
        "current_drawdown_points": current_drawdown_points,
        "drawdown_ratio": drawdown_ratio,
    }
