from __future__ import annotations

import sqlite3
import time
from dataclasses import dataclass
from pathlib import Path

from config import settings_live as settings
from contracts import Instrument
from core.bar_utils import get_bar_size_seconds
from core.instrument_db import get_instrument_db_path, get_instrument_table_name
from core.sqlite_utils import open_sqlite_connection


@dataclass(frozen=True)
class PriceDbTarget:
    instrument_code: str
    db_path: Path
    table_name: str
    bar_size_seconds: int
    mid_price_digits: int


@dataclass(frozen=True)
class FreshPriceBarStatus:
    instrument_code: str
    is_ready: bool
    reason: str
    db_path: Path
    table_name: str
    last_bar_time_ts: int | None = None
    last_bar_close_ts: int | None = None
    last_bar_time_ct: str | None = None
    last_bar_lag_seconds: int | None = None
    mid_close: float | None = None


def quote_identifier(value: str) -> str:
    return '"' + str(value).replace('"', '""') + '"'


def get_price_db_target(
        instrument_code: str,
        *,
        require_existing_file: bool = True,
) -> PriceDbTarget:
    code = str(instrument_code)
    instrument_row = Instrument.get(code)
    if instrument_row is None:
        raise ValueError(f"Инструмент {code!r} не найден в contracts.py")

    db_path = Path(
        get_instrument_db_path(
            settings=settings,
            instrument_code=code,
            instrument_row=instrument_row,
        )
    )
    if require_existing_file and not db_path.is_file():
        raise FileNotFoundError(f"Price DB не найдена: {db_path}")

    return PriceDbTarget(
        instrument_code=code,
        db_path=db_path,
        table_name=get_instrument_table_name(code, instrument_row),
        bar_size_seconds=get_bar_size_seconds(instrument_row["barSizeSetting"]),
        mid_price_digits=int(instrument_row["mid_price_digits"]),
    )


def _qualified_column(column_name: str, table_alias: str | None) -> str:
    column = quote_identifier(column_name)
    if table_alias is None:
        return column
    return f"{quote_identifier(table_alias)}.{column}"


def mid_close_sql(
        instrument_code: str,
        *,
        table_alias: str | None = None,
) -> str:
    """Returns the canonical read-time midpoint expression.

    ``mid_close`` is deliberately not stored as a second source of truth.  It is
    derived from the two executable quote sides using the same per-instrument
    rounding that the removed feature service used.
    """
    target = get_price_db_target(
        instrument_code,
        require_existing_file=False,
    )
    bid_close = _qualified_column("bid_close", table_alias)
    ask_close = _qualified_column("ask_close", table_alias)
    return (
        f"ROUND(({bid_close} + {ask_close}) / 2.0, "
        f"{target.mid_price_digits})"
    )


def complete_mid_close_predicate(*, table_alias: str | None = None) -> str:
    bid_close = _qualified_column("bid_close", table_alias)
    ask_close = _qualified_column("ask_close", table_alias)
    return f"{bid_close} IS NOT NULL AND {ask_close} IS NOT NULL"


def _open_price_db(target: PriceDbTarget):
    return open_sqlite_connection(
        str(target.db_path),
        require_existing_file=True,
        use_wal=False,
    )


def read_latest_complete_price_bar(
        instrument_code: str,
) -> tuple[int, int, str, float]:
    target = get_price_db_target(instrument_code)
    table_ref = quote_identifier(target.table_name)
    midpoint = mid_close_sql(instrument_code)

    conn = _open_price_db(target)
    try:
        row = conn.execute(
            f"""
            SELECT
                bar_time_ts,
                bar_time_ct,
                {midpoint} AS mid_close
            FROM {table_ref}
            WHERE {complete_mid_close_predicate()}
            ORDER BY bar_time_ts DESC
            LIMIT 1
            """
        ).fetchone()
    finally:
        conn.close()

    if row is None or row[0] is None:
        raise RuntimeError(
            f"Price DB не содержит полного BID/ASK close-бара: "
            f"instrument={instrument_code}, db={target.db_path}"
        )

    bar_time_ts = int(row[0])
    return (
        bar_time_ts,
        bar_time_ts + target.bar_size_seconds,
        str(row[1]),
        float(row[2]),
    )


def read_price_bar_time_ct(
        instrument_code: str,
        bar_time_ts: int,
) -> str | None:
    target = get_price_db_target(instrument_code)
    conn = _open_price_db(target)
    try:
        row = conn.execute(
            f"""
            SELECT bar_time_ct
            FROM {quote_identifier(target.table_name)}
            WHERE bar_time_ts = ?
              AND {complete_mid_close_predicate()}
            """,
            (int(bar_time_ts),),
        ).fetchone()
    finally:
        conn.close()

    if row is None:
        return None
    return str(row[0])


def get_fresh_price_bar_status(
        instrument_code: str,
        max_price_bar_lag_seconds: int,
) -> FreshPriceBarStatus:
    code = str(instrument_code)
    try:
        target = get_price_db_target(code, require_existing_file=False)
    except Exception as exc:
        return FreshPriceBarStatus(
            instrument_code=code,
            is_ready=False,
            reason=f"price target error: {type(exc).__name__}: {exc}",
            db_path=Path("."),
            table_name="",
        )

    if not target.db_path.is_file():
        return FreshPriceBarStatus(
            instrument_code=code,
            is_ready=False,
            reason="price DB file not found",
            db_path=target.db_path,
            table_name=target.table_name,
        )

    try:
        start_ts, close_ts, time_ct, midpoint = read_latest_complete_price_bar(code)
    except (RuntimeError, sqlite3.OperationalError) as exc:
        return FreshPriceBarStatus(
            instrument_code=code,
            is_ready=False,
            reason=f"price DB not ready: {exc}",
            db_path=target.db_path,
            table_name=target.table_name,
        )

    lag_seconds = max(0, int(time.time()) - close_ts)
    max_lag = int(max_price_bar_lag_seconds)
    if max_lag <= 0:
        return FreshPriceBarStatus(
            instrument_code=code,
            is_ready=False,
            reason=f"max_price_bar_lag_seconds must be positive: {max_lag}",
            db_path=target.db_path,
            table_name=target.table_name,
            last_bar_time_ts=start_ts,
            last_bar_close_ts=close_ts,
            last_bar_time_ct=time_ct,
            last_bar_lag_seconds=lag_seconds,
            mid_close=midpoint,
        )

    if lag_seconds > max_lag:
        return FreshPriceBarStatus(
            instrument_code=code,
            is_ready=False,
            reason=f"last complete bar is stale: {lag_seconds}s > {max_lag}s",
            db_path=target.db_path,
            table_name=target.table_name,
            last_bar_time_ts=start_ts,
            last_bar_close_ts=close_ts,
            last_bar_time_ct=time_ct,
            last_bar_lag_seconds=lag_seconds,
            mid_close=midpoint,
        )

    return FreshPriceBarStatus(
        instrument_code=code,
        is_ready=True,
        reason="ready",
        db_path=target.db_path,
        table_name=target.table_name,
        last_bar_time_ts=start_ts,
        last_bar_close_ts=close_ts,
        last_bar_time_ct=time_ct,
        last_bar_lag_seconds=lag_seconds,
        mid_close=midpoint,
    )


__all__ = [
    "PriceDbTarget",
    "FreshPriceBarStatus",
    "quote_identifier",
    "get_price_db_target",
    "mid_close_sql",
    "complete_mid_close_predicate",
    "read_latest_complete_price_bar",
    "read_price_bar_time_ct",
    "get_fresh_price_bar_status",
]
