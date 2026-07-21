from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from datetime import datetime, timedelta

from contracts import Instrument
from core.bar_utils import get_bar_size_seconds
from core.price_source import (
    complete_mid_close_predicate,
    get_price_db_target,
    quote_identifier,
)
from core.sqlite_utils import open_sqlite_connection
from core.time_utils import SQLITE_DATETIME_FORMAT
from ib_signal.signal_config import SignalConfig
from ib_signal.signal_errors import SignalDataNotReadyError
from ib_signal.signal_time import resolve_allowed_hours
from ib_signal.signal_window import SignalWindow

SECONDS_PER_DAY = 24 * 60 * 60


@dataclass(frozen=True)
class CandidateWindow:
    signal_bar_ts: int
    signal_bar_time_ct: str
    hour_ct: int
    pattern_start_ts: int
    pattern_end_ts: int
    trade_start_ts: int
    trade_end_ts: int


@dataclass(frozen=True)
class CandidateSearchResult:
    current_signal_bar_time_ct: str
    current_hour_ct: int
    allowed_hours_ct: list[int]
    candidates: list[CandidateWindow]
    raw_candidate_rows_count: int = 0
    min_candidate_signal_ts: int | None = None
    max_candidate_signal_ts: int | None = None
    signal_phase_seconds: int | None = None


def shift_ct_time_text(time_text_ct: str, seconds: int) -> str:
    dt = datetime.strptime(str(time_text_ct), SQLITE_DATETIME_FORMAT)
    return (dt + timedelta(seconds=int(seconds))).strftime(SQLITE_DATETIME_FORMAT)


def get_hour_ct(time_text_ct: str) -> int:
    return int(str(time_text_ct)[11:13])


def read_signal_bar_time_ct(
        *,
        instrument_code: str,
        signal_bar_ts: int,
        bar_size_seconds: int,
) -> str:
    target = get_price_db_target(instrument_code)
    source_bar_ts = int(signal_bar_ts) - int(bar_size_seconds)
    conn = open_sqlite_connection(
        str(target.db_path),
        require_existing_file=True,
        use_wal=False,
    )
    try:
        row = conn.execute(
            f"""
            SELECT bar_time_ct
            FROM {quote_identifier(target.table_name)}
            WHERE bar_time_ts = ?
              AND {complete_mid_close_predicate()}
            """,
            (source_bar_ts,),
        ).fetchone()
    finally:
        conn.close()

    if row is None:
        raise SignalDataNotReadyError(
            f"Не найден полный price-бар для signal_bar_ts={signal_bar_ts}: "
            f"instrument={instrument_code}, source_bar_ts={source_bar_ts}"
        )
    return shift_ct_time_text(str(row[0]), bar_size_seconds)


def get_min_candidate_signal_ts(
        *,
        current_signal_bar_ts: int,
        history_lookback_days: int | None,
) -> int | None:
    if history_lookback_days is None:
        return None
    days = int(history_lookback_days)
    if days <= 0:
        return None
    return int(current_signal_bar_ts) - days * SECONDS_PER_DAY


def get_max_candidate_signal_ts(window: SignalWindow) -> int:
    return int(window.signal_bar_ts) - int(window.trade_seconds)


def build_candidate_window(
        *,
        signal_bar_ts: int,
        signal_bar_time_ct: str,
        hour_ct: int,
        current_window: SignalWindow,
) -> CandidateWindow:
    signal_ts = int(signal_bar_ts)
    return CandidateWindow(
        signal_bar_ts=signal_ts,
        signal_bar_time_ct=str(signal_bar_time_ct),
        hour_ct=int(hour_ct),
        pattern_start_ts=signal_ts - int(current_window.pattern_seconds),
        pattern_end_ts=signal_ts,
        trade_start_ts=signal_ts,
        trade_end_ts=signal_ts + int(current_window.trade_seconds),
    )


def build_candidate_signal_rows_query(
        *,
        table_name: str,
        min_signal_bar_ts: int | None,
        max_signal_bar_ts: int,
        current_signal_bar_ts: int,
        allowed_hours_ct: list[int],
        bar_size_seconds: int,
) -> tuple[str, list[object]]:
    """Build an index-friendly candidate query over the canonical price table."""
    bar_size = int(bar_size_seconds)
    modifier = f"+{bar_size} seconds"
    phase = int(current_signal_bar_ts) % 3600
    hour_placeholders = ", ".join("?" for _ in allowed_hours_ct)

    where_parts = [
        "bar_time_ts <= ?",
        "((bar_time_ts + ?) % 3600) = ?",
        (
            "CAST(substr(datetime(bar_time_ct, ?), 12, 2) AS INTEGER) "
            f"IN ({hour_placeholders})"
        ),
        complete_mid_close_predicate(),
    ]
    where_params: list[object] = [
        int(max_signal_bar_ts) - bar_size,
        bar_size,
        phase,
        modifier,
        *[int(value) for value in allowed_hours_ct],
    ]
    if min_signal_bar_ts is not None:
        where_parts.insert(0, "bar_time_ts >= ?")
        where_params.insert(0, int(min_signal_bar_ts) - bar_size)

    sql = f"""
    SELECT
        bar_time_ts + ? AS signal_bar_ts,
        datetime(bar_time_ct, ?) AS signal_bar_time_ct,
        CAST(substr(datetime(bar_time_ct, ?), 12, 2) AS INTEGER) AS hour_ct
    FROM {quote_identifier(table_name)}
    WHERE {' AND '.join(where_parts)}
    ORDER BY bar_time_ts
    """
    return sql, [bar_size, modifier, modifier, *where_params]


def read_candidate_signal_rows(
        *,
        instrument_code: str,
        min_signal_bar_ts: int | None,
        max_signal_bar_ts: int,
        current_signal_bar_ts: int,
        allowed_hours_ct: list[int],
        bar_size_seconds: int,
) -> list[tuple[int, str, int]]:
    if not allowed_hours_ct:
        return []

    target = get_price_db_target(instrument_code)
    sql, params = build_candidate_signal_rows_query(
        table_name=target.table_name,
        min_signal_bar_ts=min_signal_bar_ts,
        max_signal_bar_ts=max_signal_bar_ts,
        current_signal_bar_ts=current_signal_bar_ts,
        allowed_hours_ct=allowed_hours_ct,
        bar_size_seconds=bar_size_seconds,
    )

    conn = open_sqlite_connection(
        str(target.db_path),
        require_existing_file=True,
        use_wal=False,
    )
    try:
        return [
            (int(row[0]), str(row[1]), int(row[2]))
            for row in conn.execute(sql, params).fetchall()
        ]
    except sqlite3.OperationalError as exc:
        if "locked" in str(exc).lower():
            raise SignalDataNotReadyError(
                f"price DB locked during candidate search: instrument={instrument_code}, "
                f"db={target.db_path}, error={exc}"
            ) from exc
        raise RuntimeError(
            f"Ошибка чтения candidate signal rows: instrument={instrument_code}, "
            f"db={target.db_path}, error={exc}"
        ) from exc
    finally:
        conn.close()


def find_candidate_windows(
        *,
        instrument_code: str,
        current_window: SignalWindow,
        settings: SignalConfig,
) -> CandidateSearchResult:
    instrument_row = Instrument[instrument_code]
    bar_size_seconds = get_bar_size_seconds(instrument_row["barSizeSetting"])
    current_time_ct = read_signal_bar_time_ct(
        instrument_code=instrument_code,
        signal_bar_ts=current_window.signal_bar_ts,
        bar_size_seconds=bar_size_seconds,
    )
    current_hour_ct = get_hour_ct(current_time_ct)
    allowed_hours = resolve_allowed_hours(
        current_hour_ct=current_hour_ct,
        sec_type=str(instrument_row["secType"]),
    )
    min_signal_ts = get_min_candidate_signal_ts(
        current_signal_bar_ts=current_window.signal_bar_ts,
        history_lookback_days=settings.history_lookback_days,
    )
    max_signal_ts = get_max_candidate_signal_ts(current_window)
    phase = int(current_window.signal_bar_ts) % 3600

    if max_signal_ts <= 0:
        return CandidateSearchResult(
            current_signal_bar_time_ct=current_time_ct,
            current_hour_ct=current_hour_ct,
            allowed_hours_ct=allowed_hours,
            candidates=[],
            raw_candidate_rows_count=0,
            min_candidate_signal_ts=min_signal_ts,
            max_candidate_signal_ts=max_signal_ts,
            signal_phase_seconds=phase,
        )

    rows = read_candidate_signal_rows(
        instrument_code=instrument_code,
        min_signal_bar_ts=min_signal_ts,
        max_signal_bar_ts=max_signal_ts,
        current_signal_bar_ts=current_window.signal_bar_ts,
        allowed_hours_ct=allowed_hours,
        bar_size_seconds=bar_size_seconds,
    )
    candidates = [
        build_candidate_window(
            signal_bar_ts=signal_ts,
            signal_bar_time_ct=time_ct,
            hour_ct=hour_ct,
            current_window=current_window,
        )
        for signal_ts, time_ct, hour_ct in rows
    ]
    return CandidateSearchResult(
        current_signal_bar_time_ct=current_time_ct,
        current_hour_ct=current_hour_ct,
        allowed_hours_ct=allowed_hours,
        candidates=candidates,
        raw_candidate_rows_count=len(rows),
        min_candidate_signal_ts=min_signal_ts,
        max_candidate_signal_ts=max_signal_ts,
        signal_phase_seconds=phase,
    )


def format_candidate_search_result(result: CandidateSearchResult) -> str:
    prefix = (
        f"current_hour_ct={result.current_hour_ct}, "
        f"allowed_hours_ct={result.allowed_hours_ct}, "
        f"raw={result.raw_candidate_rows_count}, "
        f"phase={result.signal_phase_seconds}, "
    )
    if not result.candidates:
        return prefix + "candidates=0"
    return (
        prefix
        + f"candidates={len(result.candidates)}, "
        + f"first={result.candidates[0].signal_bar_time_ct} CT, "
        + f"last={result.candidates[-1].signal_bar_time_ct} CT"
    )
