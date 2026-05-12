import sqlite3
from dataclasses import dataclass
from datetime import datetime, timedelta

from contracts import Instrument
from core.bar_utils import get_bar_size_seconds
from core.sqlite_utils import open_sqlite_connection
from core.time_utils import SQLITE_DATETIME_FORMAT
from ib_job_data.feature_db_sql import MID_PRICE_TABLE_NAME, quote_identifier
from ib_job_data.rebuild_mid_price import get_instrument_feature_db_path
from ib_signal.signal_config import SignalConfig, SignalWindowMode
from ib_signal.signal_schedule import get_grid_slot_start_ts
from ib_signal.signal_time import resolve_allowed_hour_slots
from ib_signal.signal_window import SignalWindow

SECONDS_PER_DAY = 24 * 60 * 60


@dataclass(frozen=True)
class CandidateWindow:
    signal_bar_ts: int
    signal_bar_time_ct: str
    hour_slot_ct: int

    pattern_start_ts: int
    pattern_end_ts: int

    trade_start_ts: int
    trade_end_ts: int


@dataclass(frozen=True)
class CandidateSearchResult:
    current_hour_slot_ct: int
    allowed_hour_slots_ct: list[int]
    candidates: list[CandidateWindow]


def shift_ct_time_text(time_text_ct: str, seconds: int) -> str:
    """Что делает: сдвигает CT-время из job DB на заданное число секунд.
    Зачем нужна: в job DB bar_time_ct хранит начало 5-секундного бара, а signal_bar живёт на границе его закрытия."""
    dt = datetime.strptime(time_text_ct, SQLITE_DATETIME_FORMAT)
    return (dt + timedelta(seconds=seconds)).strftime(SQLITE_DATETIME_FORMAT)


def get_hour_slot_ct(time_text_ct: str) -> int:
    """Что делает: возвращает час из строки CT-времени формата YYYY-MM-DD HH:MM:SS.
    Зачем нужна: signal_time.py фильтрует кандидатов по CT-часу точки принятия решения."""
    return int(time_text_ct[11:13])


def read_signal_bar_time_ct(
    *,
    instrument_code: str,
    signal_bar_ts: int,
    bar_size_seconds: int,
) -> str:
    """Что делает: читает CT-время точки signal_bar по строке job DB, которая закрылась в этот момент.
    Зачем нужна: signal_bar_ts обычно равен bar_time_ts + bar_size_seconds, а не bar_time_ts строки."""
    instrument_row = Instrument[instrument_code]
    job_db_path = get_instrument_feature_db_path(
        instrument_code=instrument_code,
        instrument_row=instrument_row,
    )

    source_bar_ts = signal_bar_ts - bar_size_seconds

    conn = open_sqlite_connection(
        str(job_db_path),
        require_existing_file=True,
        use_wal=False,
    )

    try:
        row = conn.execute(
            f"""
            SELECT bar_time_ct
            FROM {quote_identifier(MID_PRICE_TABLE_NAME)}
            WHERE bar_time_ts = ?
            """,
            (source_bar_ts,),
        ).fetchone()

        if row is None:
            raise RuntimeError(
                f"Не найден job-бар для signal_bar_ts={signal_bar_ts}: "
                f"instrument={instrument_code}, source_bar_ts={source_bar_ts}"
            )

        return shift_ct_time_text(str(row[0]), bar_size_seconds)

    finally:
        conn.close()


def get_min_candidate_signal_ts(
    *,
    current_signal_bar_ts: int,
    history_lookback_days: int | None,
) -> int | None:
    """Что делает: рассчитывает левую границу поиска candidate signal-bar по history_lookback_days.
    Зачем нужна: ограничивает глубину исторического поиска кандидатов."""
    if history_lookback_days is None:
        return None

    return current_signal_bar_ts - history_lookback_days * SECONDS_PER_DAY


def get_max_candidate_signal_ts(window: SignalWindow) -> int:
    """Что делает: рассчитывает последнюю допустимую историческую точку кандидата.
    Зачем нужна: future/trade-окно кандидата должно полностью завершиться до текущего signal_bar."""
    return window.signal_bar_ts - window.trade_seconds


def build_candidate_window(
    *,
    signal_bar_ts: int,
    signal_bar_time_ct: str,
    hour_slot_ct: int,
    current_window: SignalWindow,
) -> CandidateWindow:
    """Что делает: строит candidate window той же длины, что и текущий SignalWindow.
    Зачем нужна: Pearson можно считать только по паттернам одинаковой длины."""
    return CandidateWindow(
        signal_bar_ts=signal_bar_ts,
        signal_bar_time_ct=signal_bar_time_ct,
        hour_slot_ct=hour_slot_ct,
        pattern_start_ts=signal_bar_ts - current_window.pattern_seconds,
        pattern_end_ts=signal_bar_ts,
        trade_start_ts=signal_bar_ts,
        trade_end_ts=signal_bar_ts + current_window.trade_seconds,
    )


def is_same_grid_offset(
    *,
    candidate_signal_bar_ts: int,
    current_window: SignalWindow,
    settings: SignalConfig,
) -> bool:
    """Что делает: проверяет, что GRID-кандидат имеет тот же offset внутри своего слота.
    Зачем нужна: GRID-сигнал с offset 45 минут нельзя сравнивать с историческим offset 30 минут."""
    if settings.signal_window_mode != SignalWindowMode.GRID:
        return True

    if current_window.slot_offset_seconds is None:
        raise ValueError("Для GRID-режима current_window.slot_offset_seconds не должен быть None")

    candidate_slot_start_ts = get_grid_slot_start_ts(
        current_bar_ts=candidate_signal_bar_ts,
        slot_step_minutes=settings.slot_step_minutes,
        slot_start_minute_of_day=settings.slot_start_minute_of_day,
    )

    return (
        candidate_signal_bar_ts - candidate_slot_start_ts
        == current_window.slot_offset_seconds
    )


def read_candidate_signal_rows(
    *,
    instrument_code: str,
    min_signal_bar_ts: int | None,
    max_signal_bar_ts: int,
    candidate_search_step_seconds: int,
    allowed_hour_slots_ct: list[int],
    bar_size_seconds: int,
) -> list[tuple[int, str, int]]:
    """Что делает: читает из job DB исторические candidate signal-bar точки по времени и CT-часам.
    Зачем нужна: перед сборкой NumPy-матрицы нужен список допустимых исторических окон."""
    if not allowed_hour_slots_ct:
        return []

    if candidate_search_step_seconds <= 0:
        raise ValueError(
            f"candidate_search_step_seconds должен быть > 0, "
            f"получено: {candidate_search_step_seconds}"
        )

    instrument_row = Instrument[instrument_code]
    job_db_path = get_instrument_feature_db_path(
        instrument_code=instrument_code,
        instrument_row=instrument_row,
    )

    hour_placeholders = ", ".join("?" for _ in allowed_hour_slots_ct)
    bar_size_modifier = f"+{bar_size_seconds} seconds"

    where_parts = [
        "signal_bar_ts <= ?",
        "(signal_bar_ts % ?) = 0",
        f"hour_slot_ct IN ({hour_placeholders})",
    ]
    params: list[object] = [
        int(max_signal_bar_ts),
        int(candidate_search_step_seconds),
        *allowed_hour_slots_ct,
    ]

    if min_signal_bar_ts is not None:
        where_parts.insert(0, "signal_bar_ts >= ?")
        params.insert(0, int(min_signal_bar_ts))

    sql = f"""
    SELECT
        signal_bar_ts,
        signal_bar_time_ct,
        hour_slot_ct
    FROM (
        SELECT
            bar_time_ts + ? AS signal_bar_ts,
            datetime(bar_time_ct, ?) AS signal_bar_time_ct,
            CAST(substr(datetime(bar_time_ct, ?), 12, 2) AS INTEGER) AS hour_slot_ct
        FROM {quote_identifier(MID_PRICE_TABLE_NAME)}
    )
    WHERE {" AND ".join(where_parts)}
    ORDER BY signal_bar_ts
    """

    final_params: list[object] = [
        int(bar_size_seconds),
        bar_size_modifier,
        bar_size_modifier,
        *params,
    ]

    conn = open_sqlite_connection(
        str(job_db_path),
        require_existing_file=True,
        use_wal=False,
    )

    try:
        return [
            (int(row[0]), str(row[1]), int(row[2]))
            for row in conn.execute(sql, final_params).fetchall()
        ]

    except sqlite3.OperationalError as exc:
        raise RuntimeError(
            f"Ошибка чтения candidate signal rows: instrument={instrument_code}, "
            f"db={job_db_path}, error={exc}"
        ) from exc

    finally:
        conn.close()


def find_candidate_windows(
    *,
    instrument_code: str,
    current_window: SignalWindow,
    settings: SignalConfig,
) -> CandidateSearchResult:
    """Что делает: строит список исторических candidate windows для текущего сигнального окна.
    Зачем нужна: это первый слой отбора перед сборкой NumPy-матрицы и расчётом Pearson."""
    instrument_row = Instrument[instrument_code]
    sec_type = instrument_row["secType"]
    bar_size_seconds = get_bar_size_seconds(instrument_row["barSizeSetting"])

    current_signal_bar_time_ct = read_signal_bar_time_ct(
        instrument_code=instrument_code,
        signal_bar_ts=current_window.signal_bar_ts,
        bar_size_seconds=bar_size_seconds,
    )
    current_hour_slot_ct = get_hour_slot_ct(current_signal_bar_time_ct)
    allowed_hour_slots_ct = resolve_allowed_hour_slots(
        current_hour_slot_ct=current_hour_slot_ct,
        sec_type=sec_type,
    )

    min_candidate_signal_ts = get_min_candidate_signal_ts(
        current_signal_bar_ts=current_window.signal_bar_ts,
        history_lookback_days=settings.history_lookback_days,
    )
    max_candidate_signal_ts = get_max_candidate_signal_ts(current_window)

    if max_candidate_signal_ts <= 0:
        return CandidateSearchResult(
            current_hour_slot_ct=current_hour_slot_ct,
            allowed_hour_slots_ct=allowed_hour_slots_ct,
            candidates=[],
        )

    candidate_rows = read_candidate_signal_rows(
        instrument_code=instrument_code,
        min_signal_bar_ts=min_candidate_signal_ts,
        max_signal_bar_ts=max_candidate_signal_ts,
        candidate_search_step_seconds=settings.candidate_search_step_seconds,
        allowed_hour_slots_ct=allowed_hour_slots_ct,
        bar_size_seconds=bar_size_seconds,
    )

    candidates: list[CandidateWindow] = []

    for signal_bar_ts, signal_bar_time_ct, hour_slot_ct in candidate_rows:
        if not is_same_grid_offset(
            candidate_signal_bar_ts=signal_bar_ts,
            current_window=current_window,
            settings=settings,
        ):
            continue

        candidates.append(
            build_candidate_window(
                signal_bar_ts=signal_bar_ts,
                signal_bar_time_ct=signal_bar_time_ct,
                hour_slot_ct=hour_slot_ct,
                current_window=current_window,
            )
        )

    return CandidateSearchResult(
        current_hour_slot_ct=current_hour_slot_ct,
        allowed_hour_slots_ct=allowed_hour_slots_ct,
        candidates=candidates,
    )


def format_candidate_search_result(result: CandidateSearchResult) -> str:
    """Что делает: форматирует результат первичного поиска кандидатов для runtime-лога.
    Зачем нужна: в логах видно, какие CT-часы разрешены и сколько candidate windows найдено."""
    if not result.candidates:
        return (
            f"current_hour_ct={result.current_hour_slot_ct}, "
            f"allowed_hours_ct={result.allowed_hour_slots_ct}, "
            f"candidates=0"
        )

    first_candidate = result.candidates[0]
    last_candidate = result.candidates[-1]

    return (
        f"current_hour_ct={result.current_hour_slot_ct}, "
        f"allowed_hours_ct={result.allowed_hour_slots_ct}, "
        f"candidates={len(result.candidates)}, "
        f"first={first_candidate.signal_bar_time_ct} CT, "
        f"last={last_candidate.signal_bar_time_ct} CT"
    )
