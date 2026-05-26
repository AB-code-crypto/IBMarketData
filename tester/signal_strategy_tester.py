"""
Быстрый промежуточный тестер pattern/potential-сигналов.

Что делает
----------
1. Прогоняет заданные UTC-интервалы по локальной job DB.
2. Берёт тестируемые параметры из TESTER_CONFIG.
3. Собирает все комбинации TESTER_CONFIG через Cartesian product.
4. Отсутствующие параметры SignalConfig берёт из BASE_SETTINGS / DEFAULT_SIGNAL_CONFIG.
5. Комбинации с одинаковыми настройками, кроме market_regime_filter_mode, считает в одном проходе.
6. Читает job DB в in-memory cache один раз на группу price_source, чтобы не делать тысячи SQLite range join.
7. PNG/картинки не строит намеренно.

Торговая логика
---------------
ROLLING:
- первый LONG/SHORT открывает позицию;
- такой же сигнал игнорируется;
- противоположный сигнал делает reverse;
- отсутствие сигнала не закрывает позицию;
- в конце интервала позиция закрывается принудительно, если FORCE_CLOSE_AT_INTERVAL_END=True.

SLOT:
- signal calculation выполняется только внутри entry/reverse окна слота;
- позиция закрывается на trade_end_ts слота, даже если вне entry/reverse окна расчеты пропускаются;
- вне entry/reverse окна время идет дальше только для закрытия позиции и статистики.
"""

import bisect
import csv
import json
import sqlite3
import sys
import time
from dataclasses import asdict, dataclass, fields, replace
from datetime import datetime, timezone
from itertools import product
from pathlib import Path
from typing import Any, Literal

import numpy as np

# Чтобы скрипт можно было запускать как:
# python tester/signal_strategy_tester.py
PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from contracts import Instrument
from core.bar_utils import get_bar_size_seconds
from core.sqlite_utils import open_sqlite_connection
from core.time_utils import build_bar_time_fields_from_utc_dt
from ib_job_data.feature_db_sql import MID_PRICE_TABLE_NAME, quote_identifier
from ib_job_data.rebuild_mid_price import get_instrument_feature_db_path
from ib_job_data.sma_features import SMA_TABLE_NAME, get_sma_column_name
from ib_signal.pearson import calculate_centered_pearson_batch
from ib_signal.signal_candidate_potential import (
    CandidatePotentialResult,
    build_empty_potential_result,
    calculate_direction_stats,
    calculate_future_delta_bps,
    get_ct_time_text,
    get_direction,
    normalize_candidate_weights,
)
from ib_signal.signal_candidate_rank_features import (
    filter_candidates_by_minmax_ratio,
    rank_candidates_by_score,
)
from ib_signal.signal_candidates import (
    CandidateSearchResult,
    CandidateWindow,
    get_hour_slot_ct,
    get_max_candidate_signal_ts,
    get_min_candidate_signal_ts,
)
from ib_signal.signal_config import (
    DEFAULT_SIGNAL_CONFIG,
    MarketRegimeFilterMode,
    SignalConfig,
    SignalWindowMode,
)
from ib_signal.signal_errors import SignalDataNotReadyError
from ib_signal.signal_regression import build_linear_regression, classify_regression_direction
from ib_signal.signal_regression_relation import build_regression_relation
from ib_signal.signal_regression_threshold import get_regression_flat_delta_threshold_bps
from ib_signal.signal_schedule import get_due_signal_bar_ts, get_slot_start_ts
from ib_signal.signal_time import resolve_allowed_hour_slots
from ib_signal.signal_window import SignalWindow, build_current_signal_window

# ============================================================
# НАСТРОЙКИ ПРОМЕЖУТОЧНОГО ТЕСТА
# ============================================================

TEST_INSTRUMENT_CODE = "MNQ"

# Интервалы задаются в UTC. End включается по последнему закрытому бару <= end.
TEST_INTERVALS_UTC: list[tuple[str, str]] = [
    ("2026-05-01 00:00:00", "2026-05-15 12:00:00"),
]

# Базовые настройки берём из проекта. Всё, чего нет в TESTER_CONFIG,
# остаётся в этом значении.
BASE_SETTINGS: SignalConfig = DEFAULT_SIGNAL_CONFIG

# Здесь задаётся только то, что реально надо перебирать.
# Ключи можно писать как имена полей SignalConfig:
#   "signal_window_mode", "market_regime_filter_mode", "pearson_min",
#   "rolling_back_minutes", "slot_step_minutes" и т.д.
# Также поддержаны удобные алиасы:
#   "SignalWindowMode" -> "signal_window_mode"
#   "MarketRegimeFilterMode" -> "market_regime_filter_mode"
#
# Примеры:
# TESTER_CONFIG = {"SignalWindowMode": ["ROLLING"]}
# TESTER_CONFIG = {
#     "SignalWindowMode": ["ROLLING", "SLOT"],
#     "MarketRegimeFilterMode": ["OFF", "SOFT", "HARD"],
#     "pearson_min": [0.65, 0.70, 0.75],
# }
TESTER_CONFIG: dict[str, list[Any]] = {
    "SignalWindowMode": ["ROLLING"],
    "candidate_potential_min_abs_end_delta_points": [0.0],
}

# Для smoke-test можно поставить 20/50/100. None = без лимита.
MAX_DUE_SIGNALS_PER_WINDOW_MODE: int | None = None

# Для ROLLING нужно закрыть последнюю открытую позицию в конце интервала,
# иначе нельзя получить конечный realized PnL по тестовому отрезку.
FORCE_CLOSE_AT_INTERVAL_END = True

# Если True, SLOT считает сигналы только в entry/reverse окне:
# slot_start + slot_back_minutes .. + slot_entry_minutes.
# Позиция при этом держится до штатного trade_end_ts слота.
SLOT_USE_SLOT_ENTRY_WINDOW = True

# Большие интервалы могут создать крупный signals.csv. Для ускорения можно поставить False.
WRITE_SIGNALS_CSV = True

# После каждого window-mode перезаписывает CSV/JSON, чтобы уже готовые результаты были видны.
WRITE_PARTIAL_OUTPUTS_AFTER_EACH_WINDOW_MODE = True

# PNG/plot выключены жёстко. Флага для включения нет намеренно.
GENERATE_PLOTS = False

# Видимый прогресс.
VERBOSE_PROGRESS = True
PRINT_EACH_CALCULATION_START = False
PROGRESS_EVERY_CALCULATIONS = 10
PROGRESS_EVERY_SECONDS = 5.0

OUTPUT_DIR = PROJECT_ROOT / "out" / "test" / "signal_strategy_tester"

Direction = Literal["LONG", "SHORT"]
SignalStatus = Literal[
    "SIGNAL",
    "NO_SIGNAL",
    "REGIME_SKIP",
    "DATA_NOT_READY",
    "NO_PRICE",
    "ERROR",
]
SelectionStatus = Literal["OK", "REGIME_SKIP"]


# ============================================================
# DATA STRUCTURES
# ============================================================


@dataclass(frozen=True)
class PricePoint:
    closed_bar_ts: int
    price: float


@dataclass(frozen=True)
class PhaseSignalRows:
    signal_ts: np.ndarray
    hour_slot_ct: np.ndarray


@dataclass(frozen=True)
class PatternMatrixResultFast:
    current_values: np.ndarray
    candidate_matrix: np.ndarray
    valid_candidates: list[CandidateWindow]
    skipped_candidates_count: int
    expected_points: int


@dataclass(frozen=True)
class CandidateSelection:
    status: SelectionStatus
    reason: str | None
    candidates: list[CandidateWindow]
    candidate_matrix: np.ndarray
    pearson_scores: np.ndarray


@dataclass(frozen=True)
class PatternSignalResult:
    instrument_code: str
    signal_bar_ts: int
    signal_time_ct: str | None
    status: SignalStatus
    direction: Direction | None
    entry_price: float | None
    reason: str | None

    candidates_found: int
    valid_candidates: int
    pearson_passed: int
    selected_candidates: int
    best_pearson: float

    potential_available: bool
    potential_used: int
    potential_source: int
    potential_end_delta_points: float
    potential_end_delta_bps: float
    potential_max_profit_points: float
    potential_max_drawdown_points: float

    trade_end_ts: int | None


@dataclass(frozen=True)
class OpenPosition:
    direction: Direction
    entry_ts: int
    entry_price: float
    entry_signal_time_ct: str | None
    close_due_ts: int | None
    entry_best_pearson: float
    entry_potential_end_delta_points: float
    entry_potential_used: int


@dataclass(frozen=True)
class Trade:
    interval_start_utc: str
    interval_end_utc: str
    config_id: int
    config_label: str
    signal_window_mode: str
    market_regime_filter_mode: str
    instrument_code: str
    direction: Direction
    entry_ts: int
    entry_utc: str
    entry_signal_time_ct: str | None
    entry_price: float
    exit_ts: int
    exit_utc: str
    exit_price: float
    exit_reason: str
    holding_seconds: int
    profit_points: float
    entry_best_pearson: float
    entry_potential_end_delta_points: float
    entry_potential_used: int


@dataclass
class RunCounters:
    closed_bars_seen: int = 0
    due_signals: int = 0
    calculations: int = 0
    signals_long: int = 0
    signals_short: int = 0
    no_signal: int = 0
    regime_skips: int = 0
    data_not_ready: int = 0
    no_price: int = 0
    errors: int = 0
    entries: int = 0
    reversals: int = 0
    same_side_ignored: int = 0
    slot_entry_window_skips: int = 0
    slot_window_closes: int = 0
    interval_forced_closes: int = 0


@dataclass
class BacktestState:
    interval_start_utc: str
    interval_end_utc: str
    config_id: int
    config_label: str
    settings: SignalConfig
    signal_window_mode: SignalWindowMode
    market_regime_filter_mode: MarketRegimeFilterMode
    instrument_code: str
    position: OpenPosition | None
    trades: list[Trade]
    signal_rows: list[dict[str, object]]
    counters: RunCounters


@dataclass(frozen=True)
class RunPlan:
    due_signal_bar_timestamps: list[int]
    slot_entry_window_skips: int
    max_due_limit_reached: bool


@dataclass(frozen=True)
class TestRunSpec:
    config_id: int
    config_label: str
    settings: SignalConfig


@dataclass
class JobDataCache:
    instrument_code: str
    price_source: str
    bar_size_seconds: int
    bar_time_ts: np.ndarray
    price_values: np.ndarray
    sma_600_values: np.ndarray | None
    ts_to_index: dict[int, int]
    phase_rows: dict[int, PhaseSignalRows]

    @property
    def row_count(self) -> int:
        return int(self.bar_time_ts.size)

    def read_value_window(self, *, start_ts: int, expected_points: int) -> np.ndarray | None:
        start_index = self.ts_to_index.get(int(start_ts))
        if start_index is None:
            return None

        end_index = start_index + int(expected_points)
        if end_index > self.price_values.size:
            return None

        expected_last_ts = int(start_ts) + (int(expected_points) - 1) * self.bar_size_seconds
        if int(self.bar_time_ts[end_index - 1]) != expected_last_ts:
            return None

        values = self.price_values[start_index:end_index]
        if values.size != int(expected_points) or not np.all(np.isfinite(values)):
            return None

        return values.astype(float, copy=True)

    def read_sma_600_window(self, *, start_ts: int, expected_points: int) -> np.ndarray | None:
        if self.sma_600_values is None:
            return None

        start_index = self.ts_to_index.get(int(start_ts))
        if start_index is None:
            return None

        end_index = start_index + int(expected_points)
        if end_index > self.sma_600_values.size:
            return None

        expected_last_ts = int(start_ts) + (int(expected_points) - 1) * self.bar_size_seconds
        if int(self.bar_time_ts[end_index - 1]) != expected_last_ts:
            return None

        values = self.sma_600_values[start_index:end_index]
        if values.size != int(expected_points) or not np.all(np.isfinite(values)):
            return None

        return values.astype(float, copy=True)

    def read_price_at_closed_ts(self, closed_bar_ts: int) -> PricePoint | None:
        source_bar_ts = int(closed_bar_ts) - self.bar_size_seconds
        source_index = self.ts_to_index.get(source_bar_ts)
        if source_index is None:
            return None

        value = float(self.price_values[source_index])
        if not np.isfinite(value):
            return None

        return PricePoint(closed_bar_ts=int(closed_bar_ts), price=value)

    def read_last_price_at_or_before_closed_ts(self, closed_bar_ts: int) -> PricePoint | None:
        source_bar_ts = int(closed_bar_ts) - self.bar_size_seconds
        index = bisect.bisect_right(self.bar_time_ts.tolist(), source_bar_ts) - 1
        while index >= 0:
            value = float(self.price_values[index])
            if np.isfinite(value):
                return PricePoint(
                    closed_bar_ts=int(self.bar_time_ts[index]) + self.bar_size_seconds,
                    price=value,
                )
            index -= 1
        return None

    def get_closed_bar_timestamps(self, *, start_ts: int, end_ts: int) -> list[int]:
        closed = self.bar_time_ts + self.bar_size_seconds
        start_index = int(np.searchsorted(closed, int(start_ts), side="left"))
        end_index = int(np.searchsorted(closed, int(end_ts), side="right"))
        return [int(value) for value in closed[start_index:end_index]]


# ============================================================
# ОБЩИЕ УТИЛИТЫ
# ============================================================


def parse_utc_text(value: str) -> int:
    text = str(value).strip()
    if not text:
        raise ValueError("UTC-время не должно быть пустым")

    if text.endswith("Z"):
        text = text[:-1]

    if len(text) == 10:
        dt = datetime.strptime(text, "%Y-%m-%d").replace(tzinfo=timezone.utc)
        return int(dt.timestamp())

    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S"):
        try:
            dt = datetime.strptime(text, fmt).replace(tzinfo=timezone.utc)
            return int(dt.timestamp())
        except ValueError:
            pass

    raise ValueError(
        "Неподдерживаемый формат UTC-времени. "
        "Используй YYYY-MM-DD, YYYY-MM-DD HH:MM:SS или YYYY-MM-DDTHH:MM:SSZ"
    )


def format_utc_ts(ts: int) -> str:
    return datetime.fromtimestamp(int(ts), tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def format_ct_for_signal_ts(ts: int) -> str:
    dt_utc = datetime.fromtimestamp(int(ts), tz=timezone.utc)
    return str(build_bar_time_fields_from_utc_dt(dt_utc)["bar_time_ct"])


def format_duration(seconds: float | None) -> str:
    if seconds is None:
        return "n/a"

    total = max(0, int(seconds))
    minutes, sec = divmod(total, 60)
    hours, minutes = divmod(minutes, 60)

    if hours > 0:
        return f"{hours}h {minutes:02d}m {sec:02d}s"

    if minutes > 0:
        return f"{minutes}m {sec:02d}s"

    return f"{sec}s"


def progress_print(message: str) -> None:
    if not VERBOSE_PROGRESS:
        return

    now_text = datetime.now().strftime("%H:%M:%S")
    print(f"[{now_text}] {message}", flush=True)


def format_position_for_progress(position: OpenPosition | None) -> str:
    if position is None:
        return "FLAT"

    close_due_text = format_utc_ts(position.close_due_ts) if position.close_due_ts else "-"
    return (
        f"{position.direction}@{position.entry_price:.2f} "
        f"entry={format_utc_ts(position.entry_ts)} close_due={close_due_text}"
    )


def get_instrument_bar_size_seconds(instrument_code: str) -> int:
    return get_bar_size_seconds(Instrument[instrument_code]["barSizeSetting"])


def get_job_db_path(instrument_code: str) -> Path:
    if instrument_code not in Instrument:
        raise ValueError(f"Инструмент {instrument_code!r} не найден в contracts.py")

    return get_instrument_feature_db_path(
        instrument_code=instrument_code,
        instrument_row=Instrument[instrument_code],
    )


def calculate_shifted_ct_hour(time_text_ct: str, shift_seconds: int) -> int:
    """Быстро получает CT-hour после сдвига bar_time_ct на bar_size_seconds."""
    hour = int(time_text_ct[11:13])
    minute = int(time_text_ct[14:16])
    second = int(time_text_ct[17:19])
    shifted = hour * 3600 + minute * 60 + second + int(shift_seconds)
    return int((shifted // 3600) % 24)


def get_active_pattern_seconds(settings: SignalConfig) -> int:
    if settings.signal_window_mode == SignalWindowMode.ROLLING:
        return int(settings.rolling_back_minutes) * 60

    if settings.signal_window_mode == SignalWindowMode.SLOT:
        return int(settings.slot_step_minutes) * 60

    rolling_pattern = int(settings.rolling_back_minutes) * 60
    grid_pattern = int(settings.slot_step_minutes) * 60
    return max(rolling_pattern, grid_pattern)


def calculate_cache_source_bounds(
        *,
        intervals_utc: list[tuple[str, str]],
        settings_list: list[SignalConfig],
        bar_size_seconds: int,
) -> tuple[int, int]:
    if not settings_list:
        raise ValueError("settings_list не должен быть пустым")

    start_values = [parse_utc_text(start) for start, _ in intervals_utc]
    end_values = [parse_utc_text(end) for _, end in intervals_utc]
    min_start_ts = min(start_values)
    max_end_ts = max(end_values)

    has_unlimited_history = any(settings.history_lookback_days is None for settings in settings_list)
    max_lookback_days = max(
        int(settings.history_lookback_days or 0)
        for settings in settings_list
    )
    max_pattern_seconds = max(get_active_pattern_seconds(settings) for settings in settings_list)

    if has_unlimited_history:
        source_start_ts = 0
    else:
        source_start_ts = min_start_ts - max_lookback_days * 24 * 60 * 60 - max_pattern_seconds

    source_end_ts = max_end_ts - int(bar_size_seconds)
    return int(source_start_ts), int(source_end_ts)


# ============================================================
# IN-MEMORY JOB DB CACHE
# ============================================================


def load_job_data_cache(
        *,
        instrument_code: str,
        price_source: str,
        intervals_utc: list[tuple[str, str]],
        settings_list: list[SignalConfig],
) -> JobDataCache:
    bar_size_seconds = get_instrument_bar_size_seconds(instrument_code)
    source_start_ts, source_end_ts = calculate_cache_source_bounds(
        intervals_utc=intervals_utc,
        settings_list=settings_list,
        bar_size_seconds=bar_size_seconds,
    )
    job_db_path = get_job_db_path(instrument_code)
    sma_column_name = get_sma_column_name(600)

    progress_print(
        "CACHE_LOAD_START "
        f"instrument={instrument_code} "
        f"db={job_db_path} "
        f"source_utc={format_utc_ts(source_start_ts)}->{format_utc_ts(source_end_ts)} "
        f"price={price_source} "
        f"configs={len(settings_list)} "
        "sma600=try"
    )

    bar_ts_values: list[int] = []
    price_values: list[float] = []
    sma_values: list[float] = []
    signal_hours: list[int] = []
    loaded_sma = True

    conn = open_sqlite_connection(
        str(job_db_path),
        require_existing_file=True,
        use_wal=False,
    )

    try:
        sql_with_sma = f"""
            SELECT
                mp.bar_time_ts,
                mp.bar_time_ct,
                mp.{quote_identifier(price_source)},
                sma.{quote_identifier(sma_column_name)}
            FROM {quote_identifier(MID_PRICE_TABLE_NAME)} AS mp
            LEFT JOIN {quote_identifier(SMA_TABLE_NAME)} AS sma
              ON sma.bar_time_ts = mp.bar_time_ts
            WHERE mp.bar_time_ts >= ?
              AND mp.bar_time_ts <= ?
            ORDER BY mp.bar_time_ts
        """
        try:
            cursor = conn.execute(sql_with_sma, (source_start_ts, source_end_ts))
            for row in cursor:
                bar_ts_values.append(int(row[0]))
                signal_hours.append(calculate_shifted_ct_hour(str(row[1]), bar_size_seconds))
                price_values.append(float(row[2]) if row[2] is not None else float("nan"))
                sma_values.append(float(row[3]) if row[3] is not None else float("nan"))

        except sqlite3.OperationalError as exc:
            loaded_sma = False
            bar_ts_values.clear()
            price_values.clear()
            sma_values.clear()
            signal_hours.clear()

            progress_print(f"CACHE_LOAD_SMA_DISABLED reason={exc}")
            sql_price_only = f"""
                SELECT
                    bar_time_ts,
                    bar_time_ct,
                    {quote_identifier(price_source)}
                FROM {quote_identifier(MID_PRICE_TABLE_NAME)}
                WHERE bar_time_ts >= ?
                  AND bar_time_ts <= ?
                ORDER BY bar_time_ts
            """
            cursor = conn.execute(sql_price_only, (source_start_ts, source_end_ts))
            for row in cursor:
                bar_ts_values.append(int(row[0]))
                signal_hours.append(calculate_shifted_ct_hour(str(row[1]), bar_size_seconds))
                price_values.append(float(row[2]) if row[2] is not None else float("nan"))

    finally:
        conn.close()

    if not bar_ts_values:
        raise RuntimeError(
            f"Job DB cache пустой: instrument={instrument_code}, "
            f"source_start={source_start_ts}, source_end={source_end_ts}, db={job_db_path}"
        )

    bar_time_ts = np.asarray(bar_ts_values, dtype=np.int64)
    prices = np.asarray(price_values, dtype=float)
    sma_array = np.asarray(sma_values, dtype=float) if loaded_sma else None

    ts_to_index = {int(ts): int(index) for index, ts in enumerate(bar_time_ts)}

    signal_ts = bar_time_ts + int(bar_size_seconds)
    signal_hours_array = np.asarray(signal_hours, dtype=np.int16)
    signal_phase = signal_ts % 3600
    phase_rows: dict[int, PhaseSignalRows] = {}

    for phase in np.unique(signal_phase):
        indices = np.flatnonzero(signal_phase == phase)
        phase_rows[int(phase)] = PhaseSignalRows(
            signal_ts=signal_ts[indices].astype(np.int64, copy=True),
            hour_slot_ct=signal_hours_array[indices].astype(np.int16, copy=True),
        )

    cache = JobDataCache(
        instrument_code=instrument_code,
        price_source=price_source,
        bar_size_seconds=int(bar_size_seconds),
        bar_time_ts=bar_time_ts,
        price_values=prices,
        sma_600_values=sma_array,
        ts_to_index=ts_to_index,
        phase_rows=phase_rows,
    )

    progress_print(
        "CACHE_LOAD_DONE "
        f"rows={cache.row_count} "
        f"first_source_utc={format_utc_ts(int(cache.bar_time_ts[0]))} "
        f"last_source_utc={format_utc_ts(int(cache.bar_time_ts[-1]))} "
        f"sma600={'yes' if cache.sma_600_values is not None else 'no'} "
        f"phases={len(cache.phase_rows)}"
    )
    return cache


# ============================================================
# FAST CANDIDATE / MATRIX / POTENTIAL
# ============================================================


def is_same_slot_offset_cached(
        *,
        candidate_signal_bar_ts: int,
        current_window: SignalWindow,
        settings: SignalConfig,
) -> bool:
    if settings.signal_window_mode != SignalWindowMode.SLOT:
        return True

    if current_window.slot_offset_seconds is None:
        raise ValueError("Для SLOT-режима current_window.slot_offset_seconds не должен быть None")

    candidate_slot_start_ts = get_slot_start_ts(
        current_bar_ts=candidate_signal_bar_ts,
        slot_step_minutes=settings.slot_step_minutes,
        slot_start_minute_of_day=settings.slot_start_minute_of_day,
    )

    return candidate_signal_bar_ts - candidate_slot_start_ts == current_window.slot_offset_seconds


def find_candidate_windows_cached(
        *,
        cache: JobDataCache,
        current_window: SignalWindow,
        settings: SignalConfig,
) -> CandidateSearchResult:
    instrument_row = Instrument[cache.instrument_code]
    sec_type = instrument_row["secType"]

    current_signal_bar_time_ct = format_ct_for_signal_ts(current_window.signal_bar_ts)
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

    if max_candidate_signal_ts <= 0 or not allowed_hour_slots_ct:
        return CandidateSearchResult(
            current_signal_bar_time_ct=current_signal_bar_time_ct,
            current_hour_slot_ct=current_hour_slot_ct,
            allowed_hour_slots_ct=allowed_hour_slots_ct,
            candidates=[],
        )

    phase = int(current_window.signal_bar_ts % 3600)
    rows = cache.phase_rows.get(phase)
    if rows is None:
        return CandidateSearchResult(
            current_signal_bar_time_ct=current_signal_bar_time_ct,
            current_hour_slot_ct=current_hour_slot_ct,
            allowed_hour_slots_ct=allowed_hour_slots_ct,
            candidates=[],
        )

    left_bound = int(min_candidate_signal_ts) if min_candidate_signal_ts is not None else -10 ** 18
    right_bound = int(max_candidate_signal_ts)
    left_index = int(np.searchsorted(rows.signal_ts, left_bound, side="left"))
    right_index = int(np.searchsorted(rows.signal_ts, right_bound, side="right"))

    candidate_signal_ts = rows.signal_ts[left_index:right_index]
    candidate_hours = rows.hour_slot_ct[left_index:right_index]

    if candidate_signal_ts.size == 0:
        candidates: list[CandidateWindow] = []
    else:
        allowed_array = np.asarray(allowed_hour_slots_ct, dtype=np.int16)
        allowed_mask = np.isin(candidate_hours, allowed_array)
        candidates = []

        for signal_bar_ts_raw, hour_raw in zip(candidate_signal_ts[allowed_mask], candidate_hours[allowed_mask]):
            signal_bar_ts = int(signal_bar_ts_raw)
            if not is_same_slot_offset_cached(
                    candidate_signal_bar_ts=signal_bar_ts,
                    current_window=current_window,
                    settings=settings,
            ):
                continue

            # candidate.signal_bar_time_ct в tester-расчетах не используется; оставляем пустым,
            # чтобы не делать тысячи timezone-format операций на каждой due-точке.
            candidates.append(
                CandidateWindow(
                    signal_bar_ts=signal_bar_ts,
                    signal_bar_time_ct="",
                    hour_slot_ct=int(hour_raw),
                    pattern_start_ts=signal_bar_ts - current_window.pattern_seconds,
                    pattern_end_ts=signal_bar_ts,
                    trade_start_ts=signal_bar_ts,
                    trade_end_ts=signal_bar_ts + current_window.trade_seconds,
                )
            )

    return CandidateSearchResult(
        current_signal_bar_time_ct=current_signal_bar_time_ct,
        current_hour_slot_ct=current_hour_slot_ct,
        allowed_hour_slots_ct=allowed_hour_slots_ct,
        candidates=candidates,
    )


def get_expected_points_fast(window: SignalWindow, bar_size_seconds: int) -> int:
    if window.pattern_seconds % bar_size_seconds != 0:
        raise ValueError(
            "Длина pattern window должна быть кратна размеру бара: "
            f"pattern_seconds={window.pattern_seconds}, bar_size_seconds={bar_size_seconds}"
        )

    expected_points = window.pattern_seconds // bar_size_seconds
    if expected_points < 2:
        raise ValueError(f"Для Pearson нужно минимум две точки, получено: {expected_points}")

    return int(expected_points)


def build_pattern_matrix_cached(
        *,
        cache: JobDataCache,
        window: SignalWindow,
        candidates: list[CandidateWindow],
) -> PatternMatrixResultFast:
    expected_points = get_expected_points_fast(window, cache.bar_size_seconds)
    current_values = cache.read_value_window(
        start_ts=window.pattern_start_ts,
        expected_points=expected_points,
    )
    if current_values is None:
        raise SignalDataNotReadyError(
            "current pattern: нет полного окна или есть NaN: "
            f"start={window.pattern_start_ts}, expected_points={expected_points}"
        )

    valid_candidates: list[CandidateWindow] = []
    matrix_rows: list[np.ndarray] = []

    for candidate in candidates:
        values = cache.read_value_window(
            start_ts=candidate.pattern_start_ts,
            expected_points=expected_points,
        )
        if values is None:
            continue
        valid_candidates.append(candidate)
        matrix_rows.append(values)

    candidate_matrix = (
        np.vstack(matrix_rows).astype(float)
        if matrix_rows
        else np.empty((0, expected_points), dtype=float)
    )

    return PatternMatrixResultFast(
        current_values=current_values,
        candidate_matrix=candidate_matrix,
        valid_candidates=valid_candidates,
        skipped_candidates_count=len(candidates) - len(valid_candidates),
        expected_points=expected_points,
    )


def apply_pearson_only_selection(
        *,
        candidates: list[CandidateWindow],
        candidate_matrix: np.ndarray,
        pearson_scores: np.ndarray,
        pearson_min: float,
) -> CandidateSelection:
    passed_indices = np.flatnonzero(pearson_scores >= pearson_min)
    selected_candidates = [candidates[int(index)] for index in passed_indices]
    selected_matrix = candidate_matrix[passed_indices, :]
    selected_scores = pearson_scores[passed_indices]
    return CandidateSelection(
        status="OK",
        reason=None,
        candidates=selected_candidates,
        candidate_matrix=selected_matrix,
        pearson_scores=selected_scores,
    )


def build_regime_selections_once(
        *,
        cache: JobDataCache,
        pattern_matrix_result: PatternMatrixResultFast,
        pearson_scores: np.ndarray,
        pearson_min: float,
        requested_modes: tuple[MarketRegimeFilterMode, ...],
        current_relation: str | None,
        current_price_direction: str | None,
        current_sma600_direction: str | None,
        near_threshold_bps: float,
        flat_delta_threshold_bps: float,
) -> dict[MarketRegimeFilterMode, CandidateSelection]:
    requested_set = set(requested_modes)
    needs_regime = any(
        mode in (MarketRegimeFilterMode.SOFT, MarketRegimeFilterMode.HARD)
        for mode in requested_set
    )

    off_selection = apply_pearson_only_selection(
        candidates=pattern_matrix_result.valid_candidates,
        candidate_matrix=pattern_matrix_result.candidate_matrix,
        pearson_scores=pearson_scores,
        pearson_min=pearson_min,
    )

    selections: dict[MarketRegimeFilterMode, CandidateSelection] = {}
    if MarketRegimeFilterMode.OFF in requested_set:
        selections[MarketRegimeFilterMode.OFF] = off_selection

    if not needs_regime:
        return selections

    expected_points = int(pattern_matrix_result.expected_points)
    empty_matrix = np.empty((0, expected_points), dtype=float)
    empty_scores = np.empty((0,), dtype=float)

    def add_regime_skip(reason: str) -> dict[MarketRegimeFilterMode, CandidateSelection]:
        skip = CandidateSelection(
            status="REGIME_SKIP",
            reason=reason,
            candidates=[],
            candidate_matrix=empty_matrix,
            pearson_scores=empty_scores,
        )
        if MarketRegimeFilterMode.SOFT in requested_set:
            selections[MarketRegimeFilterMode.SOFT] = skip
        if MarketRegimeFilterMode.HARD in requested_set:
            selections[MarketRegimeFilterMode.HARD] = skip
        return selections

    if current_relation is None:
        return add_regime_skip("sma600_relation_not_available")

    if current_relation == "mixed_sma":
        return add_regime_skip("current_relation=mixed_sma")

    if current_price_direction is None or current_sma600_direction is None:
        return add_regime_skip("current_direction_not_available")

    soft_candidates: list[CandidateWindow] = []
    soft_matrix_rows: list[np.ndarray] = []
    soft_scores: list[float] = []

    hard_candidates: list[CandidateWindow] = []
    hard_matrix_rows: list[np.ndarray] = []
    hard_scores: list[float] = []

    for index, candidate in enumerate(off_selection.candidates):
        sma_values = cache.read_sma_600_window(
            start_ts=candidate.pattern_start_ts,
            expected_points=expected_points,
        )
        if sma_values is None:
            continue

        price_values = off_selection.candidate_matrix[index]
        candidate_price_regression = build_linear_regression(price_values)
        candidate_sma_regression = build_linear_regression(sma_values)
        candidate_relation = build_regression_relation(
            base_regression=candidate_price_regression,
            reference_regression=candidate_sma_regression,
            near_threshold_bps=near_threshold_bps,
        )

        if candidate_relation.relation != current_relation:
            continue

        soft_candidates.append(candidate)
        soft_matrix_rows.append(price_values.copy())
        soft_scores.append(float(off_selection.pearson_scores[index]))

        if MarketRegimeFilterMode.HARD not in requested_set:
            continue

        candidate_price_direction = classify_regression_direction(
            candidate_price_regression,
            flat_delta_threshold_bps=flat_delta_threshold_bps,
        )
        candidate_sma600_direction = classify_regression_direction(
            candidate_sma_regression,
            flat_delta_threshold_bps=flat_delta_threshold_bps,
        )

        if (
                candidate_price_direction == current_price_direction
                and candidate_sma600_direction == current_sma600_direction
        ):
            hard_candidates.append(candidate)
            hard_matrix_rows.append(price_values.copy())
            hard_scores.append(float(off_selection.pearson_scores[index]))

    if MarketRegimeFilterMode.SOFT in requested_set:
        soft_matrix = (
            np.vstack(soft_matrix_rows).astype(float)
            if soft_matrix_rows
            else np.empty((0, expected_points), dtype=float)
        )
        selections[MarketRegimeFilterMode.SOFT] = CandidateSelection(
            status="OK",
            reason=None,
            candidates=soft_candidates,
            candidate_matrix=soft_matrix,
            pearson_scores=np.asarray(soft_scores, dtype=float),
        )

    if MarketRegimeFilterMode.HARD in requested_set:
        hard_matrix = (
            np.vstack(hard_matrix_rows).astype(float)
            if hard_matrix_rows
            else np.empty((0, expected_points), dtype=float)
        )
        selections[MarketRegimeFilterMode.HARD] = CandidateSelection(
            status="OK",
            reason=None,
            candidates=hard_candidates,
            candidate_matrix=hard_matrix,
            pearson_scores=np.asarray(hard_scores, dtype=float),
        )

    return selections


def build_candidate_potential_result_cached(
        *,
        cache: JobDataCache,
        signal_window: SignalWindow,
        current_values: np.ndarray,
        candidates: list[CandidateWindow],
        candidate_scores: np.ndarray,
        min_count: int,
        max_count: int,
) -> CandidatePotentialResult:
    min_count = int(min_count)
    max_count = int(max_count)

    if min_count <= 0:
        raise ValueError(f"min_count должен быть > 0, получено: {min_count}")

    if max_count <= 0:
        raise ValueError(f"max_count должен быть > 0, получено: {max_count}")

    if max_count < min_count:
        raise ValueError(f"max_count не может быть меньше min_count: min={min_count}, max={max_count}")

    if current_values.size == 0:
        return build_empty_potential_result(
            reason="current_values_empty",
            min_count=min_count,
            max_count=max_count,
            source_candidates_count=len(candidates),
        )

    scores = np.asarray(candidate_scores, dtype=float)
    if scores.ndim != 1:
        raise ValueError(f"candidate_scores должен быть одномерным, shape={scores.shape}")

    if scores.shape[0] != len(candidates):
        raise ValueError(
            f"candidate_scores и candidates не совпадают по длине: "
            f"scores={scores.shape[0]}, candidates={len(candidates)}"
        )

    if not candidates:
        return build_empty_potential_result(
            reason="no_candidates",
            min_count=min_count,
            max_count=max_count,
            source_candidates_count=0,
        )

    pattern_points = int(current_values.size)
    trade_points = int(signal_window.trade_seconds // cache.bar_size_seconds)
    expected_points = pattern_points + trade_points

    if trade_points <= 0:
        return build_empty_potential_result(
            reason="trade_window_empty",
            min_count=min_count,
            max_count=max_count,
            source_candidates_count=len(candidates),
        )

    selected_count = min(max_count, len(candidates))
    selected_candidates = candidates[:selected_count]
    selected_scores = scores[:selected_count]

    future_series: list[np.ndarray] = []
    valid_scores: list[float] = []

    for candidate, candidate_score in zip(selected_candidates, selected_scores):
        full_values = cache.read_value_window(
            start_ts=candidate.pattern_start_ts,
            expected_points=expected_points,
        )
        if full_values is None:
            continue

        future_delta_bps = calculate_future_delta_bps(
            full_values=full_values,
            pattern_points=pattern_points,
            trade_points=trade_points,
        )
        if future_delta_bps is None:
            continue

        future_series.append(future_delta_bps)
        valid_scores.append(float(candidate_score))

    used_count = len(future_series)
    if used_count < min_count:
        return build_empty_potential_result(
            reason=f"not_enough_valid_candidates:{used_count}<{min_count}",
            min_count=min_count,
            max_count=max_count,
            source_candidates_count=len(candidates),
            used_candidates_count=used_count,
        )

    future_matrix = np.vstack(future_series).astype(float)
    score_array = np.asarray(valid_scores, dtype=float)
    weights = normalize_candidate_weights(score_array)

    weighted_future_bps = np.average(future_matrix, axis=0, weights=weights)
    current_entry_price = float(current_values[-1])
    weighted_future_points = weighted_future_bps / 10000.0 * abs(current_entry_price)
    x_minutes = np.arange(weighted_future_points.size, dtype=float) * cache.bar_size_seconds / 60.0

    end_delta_bps = float(weighted_future_bps[-1])
    end_delta_points = float(weighted_future_points[-1])
    direction = get_direction(end_delta_points)

    max_up_index = int(np.argmax(weighted_future_points))
    max_down_index = int(np.argmin(weighted_future_points))

    max_up_points = float(weighted_future_points[max_up_index])
    max_down_points = float(weighted_future_points[max_down_index])
    max_up_bps = float(weighted_future_bps[max_up_index])
    max_down_bps = float(weighted_future_bps[max_down_index])

    if direction == "LONG":
        max_profit_points = max(0.0, max_up_points)
        max_profit_bps = max(0.0, max_up_bps)
        max_profit_index = max_up_index
        max_drawdown_points = min(0.0, max_down_points)
        max_drawdown_bps = min(0.0, max_down_bps)
        max_drawdown_index = max_down_index

    elif direction == "SHORT":
        max_profit_points = max(0.0, -max_down_points)
        max_profit_bps = max(0.0, -max_down_bps)
        max_profit_index = max_down_index
        max_drawdown_points = -max(0.0, max_up_points)
        max_drawdown_bps = -max(0.0, max_up_bps)
        max_drawdown_index = max_up_index

    else:
        max_profit_points = 0.0
        max_profit_bps = 0.0
        max_profit_index = 0
        max_drawdown_points = 0.0
        max_drawdown_bps = 0.0
        max_drawdown_index = 0

    final_candidate_delta_bps = future_matrix[:, -1]
    (
        same_count,
        opposite_count,
        flat_count,
        same_weight,
        opposite_weight,
        flat_weight,
    ) = calculate_direction_stats(
        direction=direction,
        final_delta_bps=final_candidate_delta_bps,
        weights=weights,
    )

    max_profit_ts = signal_window.signal_bar_ts + max_profit_index * cache.bar_size_seconds
    max_drawdown_ts = signal_window.signal_bar_ts + max_drawdown_index * cache.bar_size_seconds

    return CandidatePotentialResult(
        is_available=True,
        unavailable_reason=None,
        direction=direction,
        min_count=min_count,
        max_count=max_count,
        source_candidates_count=len(candidates),
        used_candidates_count=used_count,
        x_minutes=x_minutes,
        weighted_future_delta_bps=weighted_future_bps.astype(float),
        weighted_future_delta_points=weighted_future_points.astype(float),
        end_delta_bps=end_delta_bps,
        end_delta_points=end_delta_points,
        max_profit_bps=float(max_profit_bps),
        max_profit_points=float(max_profit_points),
        max_profit_time_ct=get_ct_time_text(max_profit_ts),
        max_drawdown_bps=float(max_drawdown_bps),
        max_drawdown_points=float(max_drawdown_points),
        max_drawdown_time_ct=get_ct_time_text(max_drawdown_ts),
        same_direction_count=same_count,
        opposite_direction_count=opposite_count,
        flat_count=flat_count,
        same_direction_weight_share=same_weight,
        opposite_direction_weight_share=opposite_weight,
        flat_weight_share=flat_weight,
    )


# ============================================================
# SIGNAL PIPELINE
# ============================================================


def build_empty_signal(
        *,
        instrument_code: str,
        signal_bar_ts: int,
        status: SignalStatus,
        reason: str,
        trade_end_ts: int | None = None,
        entry_price: float | None = None,
        signal_time_ct: str | None = None,
        candidates_found: int = 0,
        valid_candidates: int = 0,
        pearson_passed: int = 0,
        best_pearson: float = 0.0,
) -> PatternSignalResult:
    return PatternSignalResult(
        instrument_code=instrument_code,
        signal_bar_ts=signal_bar_ts,
        signal_time_ct=signal_time_ct,
        status=status,
        direction=None,
        entry_price=entry_price,
        reason=reason,
        candidates_found=candidates_found,
        valid_candidates=valid_candidates,
        pearson_passed=pearson_passed,
        selected_candidates=0,
        best_pearson=float(best_pearson),
        potential_available=False,
        potential_used=0,
        potential_source=0,
        potential_end_delta_points=0.0,
        potential_end_delta_bps=0.0,
        potential_max_profit_points=0.0,
        potential_max_drawdown_points=0.0,
        trade_end_ts=trade_end_ts,
    )


def build_signal_from_selection(
        *,
        cache: JobDataCache,
        settings: SignalConfig,
        signal_window: SignalWindow,
        signal_bar_ts: int,
        signal_time_ct: str | None,
        entry_price: float,
        candidates_found: int,
        valid_candidates_count: int,
        pearson_passed_count: int,
        best_pearson: float,
        current_values: np.ndarray,
        selection: CandidateSelection,
) -> PatternSignalResult:
    if selection.status == "REGIME_SKIP":
        return build_empty_signal(
            instrument_code=cache.instrument_code,
            signal_bar_ts=signal_bar_ts,
            status="REGIME_SKIP",
            reason=selection.reason or "regime_skip",
            trade_end_ts=signal_window.trade_end_ts,
            entry_price=entry_price,
            signal_time_ct=signal_time_ct,
            candidates_found=candidates_found,
            valid_candidates=valid_candidates_count,
            pearson_passed=pearson_passed_count,
            best_pearson=best_pearson,
        )

    candidate_minmax_filter_result = filter_candidates_by_minmax_ratio(
        current_values=current_values,
        candidates=selection.candidates,
        candidate_matrix=selection.candidate_matrix,
        pearson_scores=selection.pearson_scores,
        max_ratio=settings.candidate_minmax_hard_filter_max_ratio,
    )

    candidate_score_result = rank_candidates_by_score(
        current_values=current_values,
        candidates=candidate_minmax_filter_result.valid_candidates,
        candidate_matrix=candidate_minmax_filter_result.candidate_matrix,
        pearson_scores=candidate_minmax_filter_result.pearson_scores,
        pearson_weight=settings.candidate_score_pearson_weight,
        end_delta_weight=settings.candidate_score_end_delta_weight,
        minmax_weight=settings.candidate_score_minmax_weight,
    )

    candidate_potential_result = build_candidate_potential_result_cached(
        cache=cache,
        signal_window=signal_window,
        current_values=current_values,
        candidates=candidate_score_result.valid_candidates,
        candidate_scores=candidate_score_result.candidate_scores,
        min_count=settings.candidate_potential_min_count,
        max_count=settings.candidate_potential_max_count,
    )

    potential_min_abs_end_delta_points = abs(
        float(settings.candidate_potential_min_abs_end_delta_points),
    )

    direction: Direction | None = None
    potential_is_strong_enough = (
            potential_min_abs_end_delta_points <= 0.0
            or abs(candidate_potential_result.end_delta_points) > potential_min_abs_end_delta_points
    )

    if (
            candidate_potential_result.is_available
            and candidate_potential_result.direction in ("LONG", "SHORT")
            and potential_is_strong_enough
    ):
        direction = candidate_potential_result.direction  # type: ignore[assignment]

    status: SignalStatus = "SIGNAL" if direction is not None else "NO_SIGNAL"
    reason = None
    if not candidate_potential_result.is_available:
        reason = candidate_potential_result.unavailable_reason
    elif not potential_is_strong_enough:
        reason = (
            f"potential_abs_end_delta_below_min:"
            f"{abs(candidate_potential_result.end_delta_points):.2f}<={potential_min_abs_end_delta_points:.2f}"
        )
    elif candidate_potential_result.direction == "NONE":
        reason = "potential_direction_none"

    return PatternSignalResult(
        instrument_code=cache.instrument_code,
        signal_bar_ts=signal_bar_ts,
        signal_time_ct=signal_time_ct,
        status=status,
        direction=direction,
        entry_price=entry_price,
        reason=reason,
        candidates_found=candidates_found,
        valid_candidates=valid_candidates_count,
        pearson_passed=pearson_passed_count,
        selected_candidates=len(candidate_score_result.valid_candidates),
        best_pearson=best_pearson,
        potential_available=bool(candidate_potential_result.is_available),
        potential_used=int(candidate_potential_result.used_candidates_count),
        potential_source=int(candidate_potential_result.source_candidates_count),
        potential_end_delta_points=float(candidate_potential_result.end_delta_points),
        potential_end_delta_bps=float(candidate_potential_result.end_delta_bps),
        potential_max_profit_points=float(candidate_potential_result.max_profit_points),
        potential_max_drawdown_points=float(candidate_potential_result.max_drawdown_points),
        trade_end_ts=signal_window.trade_end_ts,
    )


def calculate_pattern_signals_all_filters(
        *,
        cache: JobDataCache,
        signal_bar_ts: int,
        settings: SignalConfig,
        filter_modes: tuple[MarketRegimeFilterMode, ...],
) -> dict[MarketRegimeFilterMode, PatternSignalResult]:
    signal_window: SignalWindow | None = None
    signal_time_ct: str | None = None

    def empty_for_all(status: SignalStatus, reason: str) -> dict[MarketRegimeFilterMode, PatternSignalResult]:
        return {
            mode: build_empty_signal(
                instrument_code=cache.instrument_code,
                signal_bar_ts=signal_bar_ts,
                status=status,
                reason=reason,
                trade_end_ts=signal_window.trade_end_ts if signal_window is not None else None,
                signal_time_ct=signal_time_ct,
            )
            for mode in filter_modes
        }

    try:
        signal_window = build_current_signal_window(signal_bar_ts=signal_bar_ts, settings=settings)
        signal_time_ct = format_ct_for_signal_ts(signal_bar_ts)

        candidate_search_result = find_candidate_windows_cached(
            cache=cache,
            current_window=signal_window,
            settings=settings,
        )
        signal_time_ct = candidate_search_result.current_signal_bar_time_ct

        pattern_matrix_result = build_pattern_matrix_cached(
            cache=cache,
            window=signal_window,
            candidates=candidate_search_result.candidates,
        )

        if pattern_matrix_result.current_values.size == 0:
            return empty_for_all("NO_PRICE", "current_values_empty")

        entry_price = float(pattern_matrix_result.current_values[-1])

        pearson_scores = calculate_centered_pearson_batch(
            pattern_matrix_result.current_values,
            pattern_matrix_result.candidate_matrix,
        )
        pearson_passed_count = int((pearson_scores >= settings.pearson_min).sum())
        best_pearson = float(pearson_scores.max()) if pearson_scores.size else 0.0

        needs_regime = any(
            mode in (MarketRegimeFilterMode.SOFT, MarketRegimeFilterMode.HARD)
            for mode in filter_modes
        )

        regression_flat_delta_threshold_bps = 0.0
        current_price_direction = None
        current_sma600_direction = None
        current_relation = None

        if needs_regime:
            regression_flat_delta_threshold_bps = get_regression_flat_delta_threshold_bps(cache.instrument_code)
            price_regression = build_linear_regression(pattern_matrix_result.current_values)
            sma_600_values = cache.read_sma_600_window(
                start_ts=signal_window.pattern_start_ts,
                expected_points=pattern_matrix_result.expected_points,
            )
            sma_600_regression = build_linear_regression(sma_600_values) if sma_600_values is not None else None
            price_sma_600_relation = (
                build_regression_relation(
                    base_regression=price_regression,
                    reference_regression=sma_600_regression,
                    near_threshold_bps=regression_flat_delta_threshold_bps,
                )
                if sma_600_regression is not None
                else None
            )

            if price_sma_600_relation is not None:
                current_relation = price_sma_600_relation.relation
                current_price_direction = classify_regression_direction(
                    price_regression,
                    flat_delta_threshold_bps=regression_flat_delta_threshold_bps,
                )
                if sma_600_regression is not None:
                    current_sma600_direction = classify_regression_direction(
                        sma_600_regression,
                        flat_delta_threshold_bps=regression_flat_delta_threshold_bps,
                    )

        selections = build_regime_selections_once(
            cache=cache,
            pattern_matrix_result=pattern_matrix_result,
            pearson_scores=pearson_scores,
            pearson_min=settings.pearson_min,
            requested_modes=filter_modes,
            current_relation=current_relation,
            current_price_direction=current_price_direction,
            current_sma600_direction=current_sma600_direction,
            near_threshold_bps=regression_flat_delta_threshold_bps,
            flat_delta_threshold_bps=regression_flat_delta_threshold_bps,
        )

        signals: dict[MarketRegimeFilterMode, PatternSignalResult] = {}
        for mode in filter_modes:
            mode_settings = replace(settings, market_regime_filter_mode=mode)
            signals[mode] = build_signal_from_selection(
                cache=cache,
                settings=mode_settings,
                signal_window=signal_window,
                signal_bar_ts=signal_bar_ts,
                signal_time_ct=signal_time_ct,
                entry_price=entry_price,
                candidates_found=len(candidate_search_result.candidates),
                valid_candidates_count=len(pattern_matrix_result.valid_candidates),
                pearson_passed_count=pearson_passed_count,
                best_pearson=best_pearson,
                current_values=pattern_matrix_result.current_values,
                selection=selections[mode],
            )

        return signals

    except SignalDataNotReadyError as exc:
        return empty_for_all("DATA_NOT_READY", str(exc))

    except Exception as exc:  # noqa: BLE001 - tester не должен падать на всём интервале из-за одной точки.
        return empty_for_all("ERROR", f"{type(exc).__name__}: {exc}")


# ============================================================
# RUN PLAN / SLOT ENTRY WINDOW
# ============================================================


def is_slot_signal_bar_inside_entry_window(*, signal_bar_ts: int, settings: SignalConfig) -> bool:
    if settings.signal_window_mode != SignalWindowMode.SLOT:
        return True

    if not SLOT_USE_SLOT_ENTRY_WINDOW:
        return True

    slot_start_ts = get_slot_start_ts(
        current_bar_ts=signal_bar_ts,
        slot_step_minutes=settings.slot_step_minutes,
        slot_start_minute_of_day=settings.slot_start_minute_of_day,
    )
    signal_start_ts = slot_start_ts + settings.slot_back_minutes * 60
    slot_end_ts = slot_start_ts + settings.slot_step_minutes * 60
    hard_signal_end_ts = slot_end_ts - settings.slot_close_before_end_seconds
    entry_end_ts = signal_start_ts + settings.slot_entry_minutes * 60
    signal_end_ts = min(entry_end_ts, hard_signal_end_ts)

    return signal_start_ts <= int(signal_bar_ts) < signal_end_ts


def build_run_plan(
        *,
        closed_bar_timestamps: list[int],
        start_ts: int,
        end_ts: int,
        signal_window_mode: SignalWindowMode,
        settings: SignalConfig,
) -> RunPlan:
    due_signal_bar_timestamps: list[int] = []
    slot_entry_window_skips = 0
    max_due_limit_reached = False
    last_calculated_bar_ts: int | None = None

    for current_bar_ts in closed_bar_timestamps:
        due_signal_bar_ts = get_due_signal_bar_ts(
            current_bar_ts=current_bar_ts,
            settings=settings,
            last_calculated_bar_ts=last_calculated_bar_ts,
        )
        if due_signal_bar_ts is None:
            continue

        # Повтор live-логики: due-точка считается обработанной сразу после определения,
        # даже если дальше отсекается интервалом или SLOT entry-window.
        last_calculated_bar_ts = due_signal_bar_ts

        if due_signal_bar_ts < start_ts or due_signal_bar_ts > end_ts:
            continue

        if signal_window_mode == SignalWindowMode.SLOT and not is_slot_signal_bar_inside_entry_window(
                signal_bar_ts=due_signal_bar_ts,
                settings=settings,
        ):
            slot_entry_window_skips += 1
            continue

        if (
                MAX_DUE_SIGNALS_PER_WINDOW_MODE is not None
                and len(due_signal_bar_timestamps) >= MAX_DUE_SIGNALS_PER_WINDOW_MODE
        ):
            max_due_limit_reached = True
            break

        due_signal_bar_timestamps.append(due_signal_bar_ts)

    return RunPlan(
        due_signal_bar_timestamps=due_signal_bar_timestamps,
        slot_entry_window_skips=slot_entry_window_skips,
        max_due_limit_reached=max_due_limit_reached,
    )


# ============================================================
# ТОРГОВАЯ ЭМУЛЯЦИЯ
# ============================================================


def read_exit_price(
        *,
        cache: JobDataCache,
        preferred_closed_ts: int,
        fallback_closed_ts: int,
) -> PricePoint | None:
    exact = cache.read_price_at_closed_ts(preferred_closed_ts)
    if exact is not None:
        return exact

    return cache.read_last_price_at_or_before_closed_ts(fallback_closed_ts)


def close_position(*, state: BacktestState, exit_price_point: PricePoint, exit_reason: str) -> None:
    position = state.position
    if position is None:
        return

    direction_multiplier = 1.0 if position.direction == "LONG" else -1.0
    profit_points = (exit_price_point.price - position.entry_price) * direction_multiplier
    holding_seconds = max(0, int(exit_price_point.closed_bar_ts) - int(position.entry_ts))

    state.trades.append(
        Trade(
            interval_start_utc=state.interval_start_utc,
            interval_end_utc=state.interval_end_utc,
            config_id=state.config_id,
            config_label=state.config_label,
            signal_window_mode=state.signal_window_mode.value,
            market_regime_filter_mode=state.market_regime_filter_mode.value,
            instrument_code=state.instrument_code,
            direction=position.direction,
            entry_ts=position.entry_ts,
            entry_utc=format_utc_ts(position.entry_ts),
            entry_signal_time_ct=position.entry_signal_time_ct,
            entry_price=float(position.entry_price),
            exit_ts=exit_price_point.closed_bar_ts,
            exit_utc=format_utc_ts(exit_price_point.closed_bar_ts),
            exit_price=float(exit_price_point.price),
            exit_reason=exit_reason,
            holding_seconds=holding_seconds,
            profit_points=float(profit_points),
            entry_best_pearson=float(position.entry_best_pearson),
            entry_potential_end_delta_points=float(position.entry_potential_end_delta_points),
            entry_potential_used=int(position.entry_potential_used),
        )
    )
    state.position = None


def open_position(*, state: BacktestState, signal: PatternSignalResult) -> None:
    if signal.direction is None or signal.entry_price is None:
        raise ValueError("Нельзя открыть позицию без direction и entry_price")

    close_due_ts = signal.trade_end_ts if state.signal_window_mode == SignalWindowMode.SLOT else None

    state.position = OpenPosition(
        direction=signal.direction,
        entry_ts=signal.signal_bar_ts,
        entry_price=float(signal.entry_price),
        entry_signal_time_ct=signal.signal_time_ct,
        close_due_ts=close_due_ts,
        entry_best_pearson=float(signal.best_pearson),
        entry_potential_end_delta_points=float(signal.potential_end_delta_points),
        entry_potential_used=int(signal.potential_used),
    )
    state.counters.entries += 1


def process_signal_action(
        *,
        state: BacktestState,
        signal: PatternSignalResult,
        cache: JobDataCache,
) -> str:
    if signal.status == "DATA_NOT_READY":
        state.counters.data_not_ready += 1
        return "SKIP_DATA_NOT_READY"

    if signal.status == "NO_PRICE":
        state.counters.no_price += 1
        return "SKIP_NO_PRICE"

    if signal.status == "ERROR":
        state.counters.errors += 1
        return "SKIP_ERROR"

    if signal.status == "REGIME_SKIP":
        state.counters.regime_skips += 1
        return "SKIP_REGIME"

    if signal.direction is None:
        state.counters.no_signal += 1
        return "NO_SIGNAL_HOLD"

    if signal.direction == "LONG":
        state.counters.signals_long += 1
    elif signal.direction == "SHORT":
        state.counters.signals_short += 1

    if state.position is None:
        open_position(state=state, signal=signal)
        return f"OPEN_{signal.direction}"

    if state.position.direction == signal.direction:
        state.counters.same_side_ignored += 1
        return f"IGNORE_SAME_{signal.direction}"

    exit_price_point = cache.read_price_at_closed_ts(signal.signal_bar_ts)
    if exit_price_point is None:
        state.counters.no_price += 1
        return "SKIP_REVERSE_NO_PRICE"

    previous_direction = state.position.direction
    close_position(
        state=state,
        exit_price_point=exit_price_point,
        exit_reason=f"reverse_{previous_direction}_to_{signal.direction}",
    )
    state.counters.reversals += 1
    open_position(state=state, signal=signal)
    return f"REVERSE_{previous_direction}_TO_{signal.direction}"


def maybe_close_slot_position(*, state: BacktestState, current_bar_ts: int, cache: JobDataCache) -> None:
    if state.signal_window_mode != SignalWindowMode.SLOT:
        return

    if state.position is None or state.position.close_due_ts is None:
        return

    if current_bar_ts < state.position.close_due_ts:
        return

    exit_price_point = read_exit_price(
        cache=cache,
        preferred_closed_ts=state.position.close_due_ts,
        fallback_closed_ts=current_bar_ts,
    )
    if exit_price_point is None:
        state.counters.no_price += 1
        return

    close_position(state=state, exit_price_point=exit_price_point, exit_reason="slot_window_end")
    state.counters.slot_window_closes += 1


def append_signal_row(*, state: BacktestState, signal: PatternSignalResult, action: str) -> None:
    if not WRITE_SIGNALS_CSV:
        return

    state.signal_rows.append(
        {
            "interval_start_utc": state.interval_start_utc,
            "interval_end_utc": state.interval_end_utc,
            "config_id": state.config_id,
            "config_label": state.config_label,
            "signal_window_mode": state.signal_window_mode.value,
            "market_regime_filter_mode": state.market_regime_filter_mode.value,
            "instrument_code": state.instrument_code,
            "signal_bar_ts": signal.signal_bar_ts,
            "signal_utc": format_utc_ts(signal.signal_bar_ts),
            "signal_time_ct": signal.signal_time_ct,
            "status": signal.status,
            "direction": signal.direction,
            "action": action,
            "position_after": state.position.direction if state.position is not None else None,
            "entry_price": signal.entry_price,
            "reason": signal.reason,
            "candidates_found": signal.candidates_found,
            "valid_candidates": signal.valid_candidates,
            "pearson_passed": signal.pearson_passed,
            "selected_candidates": signal.selected_candidates,
            "best_pearson": signal.best_pearson,
            "potential_available": signal.potential_available,
            "potential_used": signal.potential_used,
            "potential_source": signal.potential_source,
            "potential_end_delta_points": signal.potential_end_delta_points,
            "potential_end_delta_bps": signal.potential_end_delta_bps,
            "potential_max_profit_points": signal.potential_max_profit_points,
            "potential_max_drawdown_points": signal.potential_max_drawdown_points,
            "trade_end_ts": signal.trade_end_ts,
            "trade_end_utc": format_utc_ts(signal.trade_end_ts) if signal.trade_end_ts else None,
        }
    )


# ============================================================
# СТАТИСТИКА И ВЫВОД
# ============================================================


def calculate_equity_max_drawdown(trades: list[Trade]) -> float:
    equity = 0.0
    peak = 0.0
    max_drawdown = 0.0

    for trade in trades:
        equity += trade.profit_points
        peak = max(peak, equity)
        max_drawdown = max(max_drawdown, peak - equity)

    return float(max_drawdown)


def build_summary_row(*, state: BacktestState, start_ts: int, end_ts: int, settings: SignalConfig) -> dict[str, object]:
    trades = state.trades
    profits = [float(trade.profit_points) for trade in trades]
    wins = [value for value in profits if value > 0.0]
    losses = [value for value in profits if value < 0.0]
    flats = [value for value in profits if value == 0.0]

    total_profit = float(sum(profits))
    gross_profit = float(sum(wins))
    gross_loss = float(sum(losses))
    profit_factor = gross_profit / abs(gross_loss) if gross_loss < 0.0 else None

    total_trades = len(trades)
    win_rate = (len(wins) / total_trades * 100.0) if total_trades else 0.0

    return {
        "interval_start_utc": state.interval_start_utc,
        "interval_end_utc": state.interval_end_utc,
        "config_id": state.config_id,
        "config_label": state.config_label,
        "interval_start_ts": start_ts,
        "interval_end_ts": end_ts,
        "signal_window_mode": state.signal_window_mode.value,
        "market_regime_filter_mode": state.market_regime_filter_mode.value,
        "instrument_code": state.instrument_code,
        "price_source": settings.price_source,
        "pearson_min": settings.pearson_min,
        "history_lookback_days": settings.history_lookback_days,
        "rolling_signal_step_seconds": settings.rolling_signal_step_seconds,
        "rolling_back_minutes": settings.rolling_back_minutes,
        "rolling_trade_minutes": settings.rolling_trade_minutes,
        "slot_signal_step_seconds": settings.slot_signal_step_seconds,
        "slot_step_minutes": settings.slot_step_minutes,
        "slot_back_minutes": settings.slot_back_minutes,
        "slot_entry_minutes": settings.slot_entry_minutes,
        "slot_close_before_end_seconds": settings.slot_close_before_end_seconds,
        "candidate_minmax_hard_filter_max_ratio": settings.candidate_minmax_hard_filter_max_ratio,
        "candidate_potential_min_count": settings.candidate_potential_min_count,
        "candidate_potential_max_count": settings.candidate_potential_max_count,
        "candidate_potential_min_abs_end_delta_points": settings.candidate_potential_min_abs_end_delta_points,
        "closed_bars_seen": state.counters.closed_bars_seen,
        "due_signals": state.counters.due_signals,
        "calculations": state.counters.calculations,
        "signals_long": state.counters.signals_long,
        "signals_short": state.counters.signals_short,
        "no_signal": state.counters.no_signal,
        "regime_skips": state.counters.regime_skips,
        "data_not_ready": state.counters.data_not_ready,
        "no_price": state.counters.no_price,
        "errors": state.counters.errors,
        "entries": state.counters.entries,
        "reversals": state.counters.reversals,
        "same_side_ignored": state.counters.same_side_ignored,
        "slot_entry_window_skips": state.counters.slot_entry_window_skips,
        "slot_window_closes": state.counters.slot_window_closes,
        "interval_forced_closes": state.counters.interval_forced_closes,
        "trades": total_trades,
        "wins": len(wins),
        "losses": len(losses),
        "flats": len(flats),
        "win_rate_pct": float(win_rate),
        "net_profit_points": total_profit,
        "gross_profit_points": gross_profit,
        "gross_loss_points": gross_loss,
        "profit_factor": profit_factor,
        "avg_trade_points": total_profit / total_trades if total_trades else 0.0,
        "max_trade_profit_points": max(profits) if profits else 0.0,
        "max_trade_loss_points": min(profits) if profits else 0.0,
        "max_equity_drawdown_points": calculate_equity_max_drawdown(trades),
    }


def should_print_aggregate_progress(*, index: int, total: int, last_progress_at: float) -> bool:
    if not VERBOSE_PROGRESS:
        return False

    if index <= 1 or index >= total:
        return True

    if PROGRESS_EVERY_CALCULATIONS > 0 and index % PROGRESS_EVERY_CALCULATIONS == 0:
        return True

    if PROGRESS_EVERY_SECONDS > 0 and (time.monotonic() - last_progress_at) >= PROGRESS_EVERY_SECONDS:
        return True

    return False


def print_multi_progress(
        *,
        prefix: str,
        index: int,
        total: int,
        signal_bar_ts: int,
        states: dict[MarketRegimeFilterMode, BacktestState],
        started_at: float,
        statuses: dict[MarketRegimeFilterMode, str] | None = None,
        actions: dict[MarketRegimeFilterMode, str] | None = None,
) -> None:
    elapsed = max(0.0, time.monotonic() - started_at)
    speed = index / elapsed if elapsed > 0.0 else 0.0
    remaining = max(0, total - index)
    eta = remaining / speed if speed > 0.0 else None
    pct = index / total * 100.0 if total else 100.0

    first_state = next(iter(states.values()))
    parts = [
        prefix,
        f"window={first_state.signal_window_mode.value}",
        f"filters={','.join(mode.value for mode in states.keys())}",
        f"calc={index}/{total} ({pct:.1f}%)",
        f"signal_utc={format_utc_ts(signal_bar_ts)}",
        f"elapsed={format_duration(elapsed)}",
        f"eta={format_duration(eta)}",
    ]

    for mode, state in states.items():
        net_points = sum(float(trade.profit_points) for trade in state.trades)
        mode_parts = [
            f"{mode.value}:tr={len(state.trades)}",
            f"net={net_points:+.2f}",
            f"L/S/N={state.counters.signals_long}/{state.counters.signals_short}/{state.counters.no_signal}",
            f"pos={format_position_for_progress(state.position)}",
        ]
        if statuses is not None:
            mode_parts.append(f"st={statuses.get(mode)}")
        if actions is not None:
            mode_parts.append(f"act={actions.get(mode)}")
        parts.append(" ".join(mode_parts))

    progress_print(" | ".join(parts))


SIGNAL_CONFIG_FIELD_NAMES = {field.name for field in fields(SignalConfig)}
CONFIG_KEY_ALIASES = {
    "SignalWindowMode": "signal_window_mode",
    "signal_window_mode": "signal_window_mode",
    "MarketRegimeFilterMode": "market_regime_filter_mode",
    "market_regime_filter_mode": "market_regime_filter_mode",
}


def normalize_tester_config_key(raw_key: str) -> str:
    key = str(raw_key).strip()
    field_name = CONFIG_KEY_ALIASES.get(key, key)

    if field_name not in SIGNAL_CONFIG_FIELD_NAMES:
        raise ValueError(
            f"TESTER_CONFIG содержит неизвестный параметр {raw_key!r}. "
            f"Допустимые поля SignalConfig: {sorted(SIGNAL_CONFIG_FIELD_NAMES)}"
        )

    return field_name


def coerce_enum_value(enum_class, raw_value: object):
    if isinstance(raw_value, enum_class):
        return raw_value

    if isinstance(raw_value, str):
        text = raw_value.strip()
        if not text:
            raise ValueError(f"Пустое значение enum {enum_class.__name__}")

        upper_text = text.upper()
        if upper_text in enum_class.__members__:
            return enum_class[upper_text]

        for item in enum_class:
            if str(item.value).upper() == upper_text:
                return item

    return enum_class(raw_value)


def coerce_config_value(field_name: str, raw_value: object) -> object:
    if field_name == "signal_window_mode":
        return coerce_enum_value(SignalWindowMode, raw_value)

    if field_name == "market_regime_filter_mode":
        return coerce_enum_value(MarketRegimeFilterMode, raw_value)

    if isinstance(raw_value, str) and raw_value.strip().lower() in {"none", "null"}:
        return None

    base_value = getattr(BASE_SETTINGS, field_name)

    if isinstance(base_value, bool):
        if isinstance(raw_value, str):
            text = raw_value.strip().lower()
            if text in {"1", "true", "yes", "y", "on"}:
                return True
            if text in {"0", "false", "no", "n", "off"}:
                return False
        return bool(raw_value)

    if isinstance(base_value, int) and not isinstance(base_value, bool):
        if raw_value is None:
            return None
        return int(raw_value)

    if isinstance(base_value, float):
        if raw_value is None:
            return None
        return float(raw_value)

    if base_value is None:
        return raw_value

    if isinstance(base_value, str):
        return str(raw_value)

    return raw_value


def format_config_value(value: object) -> str:
    if isinstance(value, (SignalWindowMode, MarketRegimeFilterMode)):
        return value.value
    if value is None:
        return "None"
    return str(value)


def build_config_label(*, settings: SignalConfig, config_keys: list[str]) -> str:
    if not config_keys:
        return "DEFAULT_SIGNAL_CONFIG"

    return ",".join(
        f"{key}={format_config_value(getattr(settings, key))}"
        for key in config_keys
    )


def build_test_specs(
        *,
        tester_config: dict[str, list[Any]],
        base_settings: SignalConfig,
) -> list[TestRunSpec]:
    normalized_items: list[tuple[str, list[object]]] = []
    seen_keys: set[str] = set()

    for raw_key, raw_values in tester_config.items():
        field_name = normalize_tester_config_key(raw_key)
        if field_name in seen_keys:
            raise ValueError(
                f"TESTER_CONFIG дублирует параметр {field_name!r}. "
                "Оставь только один ключ/алиас."
            )
        seen_keys.add(field_name)

        if not isinstance(raw_values, list | tuple):
            raise TypeError(
                f"TESTER_CONFIG[{raw_key!r}] должен быть list/tuple, получено {type(raw_values).__name__}"
            )
        if len(raw_values) == 0:
            raise ValueError(f"TESTER_CONFIG[{raw_key!r}] не должен быть пустым")

        normalized_values = [coerce_config_value(field_name, value) for value in raw_values]
        normalized_items.append((field_name, normalized_values))

    if normalized_items:
        config_keys = [key for key, _ in normalized_items]
        value_lists = [values for _, values in normalized_items]
        combinations = product(*value_lists)
    else:
        config_keys = []
        combinations = [()]

    specs: list[TestRunSpec] = []
    seen_settings: set[tuple[tuple[str, object], ...]] = set()

    for combination in combinations:
        overrides = dict(zip(config_keys, combination, strict=True)) if config_keys else {}
        settings = replace(base_settings, **overrides)
        settings_key = tuple((field.name, getattr(settings, field.name)) for field in fields(SignalConfig))
        if settings_key in seen_settings:
            continue
        seen_settings.add(settings_key)

        specs.append(
            TestRunSpec(
                config_id=len(specs) + 1,
                config_label=build_config_label(settings=settings, config_keys=config_keys),
                settings=settings,
            )
        )

    if not specs:
        raise RuntimeError("TESTER_CONFIG не дал ни одной конфигурации")

    return specs


def get_fast_group_key(settings: SignalConfig) -> tuple[tuple[str, object], ...]:
    return tuple(
        (field.name, getattr(settings, field.name))
        for field in fields(SignalConfig)
        if field.name != "market_regime_filter_mode"
    )


def group_specs_for_fast_runs(specs: list[TestRunSpec]) -> list[tuple[TestRunSpec, ...]]:
    groups: dict[tuple[tuple[str, object], ...], list[TestRunSpec]] = {}
    order: list[tuple[tuple[str, object], ...]] = []

    for spec in specs:
        key = get_fast_group_key(spec.settings)
        if key not in groups:
            groups[key] = []
            order.append(key)
        groups[key].append(spec)

    return [tuple(groups[key]) for key in order]


def group_specs_by_price_source(specs: list[TestRunSpec]) -> dict[str, list[TestRunSpec]]:
    grouped: dict[str, list[TestRunSpec]] = {}
    for spec in specs:
        grouped.setdefault(spec.settings.price_source, []).append(spec)
    return grouped


def settings_to_output_dict(settings: SignalConfig) -> dict[str, object]:
    result: dict[str, object] = {}
    for field in fields(SignalConfig):
        value = getattr(settings, field.name)
        if isinstance(value, (SignalWindowMode, MarketRegimeFilterMode)):
            result[field.name] = value.value
        else:
            result[field.name] = value
    return result


def spec_to_config_row(spec: TestRunSpec) -> dict[str, object]:
    row: dict[str, object] = {
        "config_id": spec.config_id,
        "config_label": spec.config_label,
    }
    row.update(settings_to_output_dict(spec.settings))
    return row


def run_backtest_interval_group(
        *,
        cache: JobDataCache,
        interval_start_utc: str,
        interval_end_utc: str,
        specs: tuple[TestRunSpec, ...],
) -> tuple[list[dict[str, object]], list[Trade], list[dict[str, object]]]:
    if not specs:
        raise ValueError("specs не должен быть пустым")

    start_ts = parse_utc_text(interval_start_utc)
    end_ts = parse_utc_text(interval_end_utc)
    if end_ts <= start_ts:
        raise ValueError(
            f"interval_end_utc должен быть больше interval_start_utc: "
            f"{interval_start_utc} -> {interval_end_utc}"
        )

    run_started_at = time.monotonic()
    base_settings = specs[0].settings
    signal_window_mode = base_settings.signal_window_mode
    filter_modes = tuple(spec.settings.market_regime_filter_mode for spec in specs)

    for spec in specs[1:]:
        if spec.settings.signal_window_mode != signal_window_mode:
            raise ValueError("В одной fast-группе не могут быть разные signal_window_mode")
        if get_fast_group_key(spec.settings) != get_fast_group_key(base_settings):
            raise ValueError("В одной fast-группе настройки должны отличаться только market_regime_filter_mode")

    closed_bar_timestamps = cache.get_closed_bar_timestamps(start_ts=start_ts, end_ts=end_ts)
    plan = build_run_plan(
        closed_bar_timestamps=closed_bar_timestamps,
        start_ts=start_ts,
        end_ts=end_ts,
        signal_window_mode=signal_window_mode,
        settings=base_settings,
    )
    due_set = set(plan.due_signal_bar_timestamps)

    states: dict[MarketRegimeFilterMode, BacktestState] = {}
    for spec in specs:
        mode = spec.settings.market_regime_filter_mode
        states[mode] = BacktestState(
            interval_start_utc=interval_start_utc,
            interval_end_utc=interval_end_utc,
            config_id=spec.config_id,
            config_label=spec.config_label,
            settings=spec.settings,
            signal_window_mode=signal_window_mode,
            market_regime_filter_mode=mode,
            instrument_code=cache.instrument_code,
            position=None,
            trades=[],
            signal_rows=[],
            counters=RunCounters(closed_bars_seen=len(closed_bar_timestamps)),
        )
        states[mode].counters.slot_entry_window_skips = plan.slot_entry_window_skips

    progress_print(
        "PLAN "
        f"configs={','.join(str(spec.config_id) for spec in specs)} "
        f"window={signal_window_mode.value} "
        f"filters={','.join(mode.value for mode in filter_modes)} "
        f"closed_bars={len(closed_bar_timestamps)} "
        f"due_to_calculate={len(plan.due_signal_bar_timestamps)} "
        f"slot_entry_window_skips={plan.slot_entry_window_skips} "
        f"max_due={MAX_DUE_SIGNALS_PER_WINDOW_MODE if MAX_DUE_SIGNALS_PER_WINDOW_MODE is not None else 'none'} "
        f"limit_reached={plan.max_due_limit_reached}"
    )

    last_progress_at = run_started_at
    due_index = 0
    total_due = len(plan.due_signal_bar_timestamps)

    for current_bar_ts in closed_bar_timestamps:
        for state in states.values():
            maybe_close_slot_position(state=state, current_bar_ts=current_bar_ts, cache=cache)

        if current_bar_ts not in due_set:
            continue

        due_index += 1
        for state in states.values():
            state.counters.due_signals += 1

        if PRINT_EACH_CALCULATION_START or should_print_aggregate_progress(
                index=due_index,
                total=total_due,
                last_progress_at=last_progress_at,
        ):
            print_multi_progress(
                prefix="CALC_START",
                index=due_index,
                total=total_due,
                signal_bar_ts=current_bar_ts,
                states=states,
                started_at=run_started_at,
            )
            last_progress_at = time.monotonic()

        signals_by_mode = calculate_pattern_signals_all_filters(
            cache=cache,
            signal_bar_ts=current_bar_ts,
            settings=base_settings,
            filter_modes=filter_modes,
        )

        statuses: dict[MarketRegimeFilterMode, str] = {}
        actions: dict[MarketRegimeFilterMode, str] = {}
        for mode, state in states.items():
            signal = signals_by_mode[mode]
            state.counters.calculations += 1
            action = process_signal_action(state=state, signal=signal, cache=cache)
            append_signal_row(state=state, signal=signal, action=action)
            statuses[mode] = signal.status
            actions[mode] = action

        if should_print_aggregate_progress(index=due_index, total=total_due, last_progress_at=last_progress_at):
            print_multi_progress(
                prefix="CALC_DONE",
                index=due_index,
                total=total_due,
                signal_bar_ts=current_bar_ts,
                states=states,
                started_at=run_started_at,
                statuses=statuses,
                actions=actions,
            )
            last_progress_at = time.monotonic()

    for state in states.values():
        if state.position is None:
            continue

        if (
                state.signal_window_mode == SignalWindowMode.SLOT
                and state.position.close_due_ts is not None
                and state.position.close_due_ts <= end_ts
        ):
            exit_price_point = read_exit_price(
                cache=cache,
                preferred_closed_ts=state.position.close_due_ts,
                fallback_closed_ts=end_ts,
            )
            if exit_price_point is not None:
                close_position(state=state, exit_price_point=exit_price_point, exit_reason="slot_window_end")
                state.counters.slot_window_closes += 1
            else:
                state.counters.no_price += 1

        elif FORCE_CLOSE_AT_INTERVAL_END:
            exit_price_point = cache.read_last_price_at_or_before_closed_ts(end_ts)
            if exit_price_point is not None:
                close_position(state=state, exit_price_point=exit_price_point, exit_reason="interval_end")
                state.counters.interval_forced_closes += 1
            else:
                state.counters.no_price += 1

    summary_rows: list[dict[str, object]] = []
    trades: list[Trade] = []
    signal_rows: list[dict[str, object]] = []

    for state in states.values():
        summary = build_summary_row(state=state, start_ts=start_ts, end_ts=end_ts, settings=state.settings)
        summary_rows.append(summary)
        trades.extend(state.trades)
        signal_rows.extend(state.signal_rows)

    elapsed = time.monotonic() - run_started_at
    compact = "; ".join(
        f"cfg={row['config_id']} {row['market_regime_filter_mode']}:calc={row['calculations']} "
        f"trades={row['trades']} net={float(row['net_profit_points']):+.2f}pt err={row['errors']}"
        for row in summary_rows
    )
    progress_print(
        f"DONE configs={','.join(str(spec.config_id) for spec in specs)} "
        f"window={signal_window_mode.value} elapsed={format_duration(elapsed)} | {compact}"
    )
    return summary_rows, trades, signal_rows


# ============================================================
# FILE OUTPUT
# ============================================================


def write_csv(path: Path, rows: list[dict[str, object]]) -> None:
    if not rows:
        return

    path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames: list[str] = []
    seen: set[str] = set()
    for row in rows:
        for key in row.keys():
            if key not in seen:
                seen.add(key)
                fieldnames.append(key)

    with path.open("w", encoding="utf-8", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def write_json(path: Path, rows: list[dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as file:
        json.dump(rows, file, ensure_ascii=False, indent=2)


def trade_to_row(trade: Trade) -> dict[str, object]:
    return asdict(trade)


def write_outputs(
        *,
        summary_rows: list[dict[str, object]],
        trade_rows: list[dict[str, object]],
        signal_rows: list[dict[str, object]],
        config_rows: list[dict[str, object]],
) -> None:
    write_csv(OUTPUT_DIR / "configs.csv", config_rows)
    write_json(OUTPUT_DIR / "configs.json", config_rows)
    write_csv(OUTPUT_DIR / "summary.csv", summary_rows)
    write_json(OUTPUT_DIR / "summary.json", summary_rows)
    write_csv(OUTPUT_DIR / "trades.csv", trade_rows)
    if WRITE_SIGNALS_CSV:
        write_csv(OUTPUT_DIR / "signals.csv", signal_rows)


def print_summary_table(summary_rows: list[dict[str, object]]) -> None:
    if not summary_rows:
        print("Нет summary rows", flush=True)
        return

    columns = [
        "config_id",
        "interval_start_utc",
        "interval_end_utc",
        "signal_window_mode",
        "market_regime_filter_mode",
        "calculations",
        "signals_long",
        "signals_short",
        "trades",
        "win_rate_pct",
        "net_profit_points",
        "max_equity_drawdown_points",
        "errors",
    ]

    def format_cell(value: object) -> str:
        if isinstance(value, float):
            return f"{value:.2f}"
        return "" if value is None else str(value)

    widths = {
        column: max(len(column), *(len(format_cell(row.get(column))) for row in summary_rows))
        for column in columns
    }

    header = " | ".join(column.ljust(widths[column]) for column in columns)
    separator = "-+-".join("-" * widths[column] for column in columns)
    print(header, flush=True)
    print(separator, flush=True)
    for row in summary_rows:
        print(" | ".join(format_cell(row.get(column)).ljust(widths[column]) for column in columns), flush=True)


# ============================================================
# MAIN
# ============================================================


def main() -> None:
    try:
        sys.stdout.reconfigure(line_buffering=True)
    except Exception:
        pass

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    all_summary_rows: list[dict[str, object]] = []
    all_trade_rows: list[dict[str, object]] = []
    all_signal_rows: list[dict[str, object]] = []

    print("signal_strategy_tester: plots=disabled, mode=fast_in_memory_configurable", flush=True)

    specs = build_test_specs(tester_config=TESTER_CONFIG, base_settings=BASE_SETTINGS)
    config_rows = [spec_to_config_row(spec) for spec in specs]

    print(
        "TESTER_CONFIG_PLAN "
        f"configs={len(specs)} "
        f"keys={','.join(normalize_tester_config_key(key) for key in TESTER_CONFIG.keys()) if TESTER_CONFIG else 'DEFAULT'}",
        flush=True,
    )
    for spec in specs:
        print(
            "CONFIG "
            f"id={spec.config_id} "
            f"label={spec.config_label} "
            f"window={spec.settings.signal_window_mode.value} "
            f"filter={spec.settings.market_regime_filter_mode.value} "
            f"price_source={spec.settings.price_source} "
            f"pearson_min={spec.settings.pearson_min} "
            f"history_lookback_days={spec.settings.history_lookback_days}",
            flush=True,
        )

    write_outputs(
        summary_rows=all_summary_rows,
        trade_rows=all_trade_rows,
        signal_rows=all_signal_rows,
        config_rows=config_rows,
    )

    specs_by_price_source = group_specs_by_price_source(specs)

    for price_source, price_specs in specs_by_price_source.items():
        cache = load_job_data_cache(
            instrument_code=TEST_INSTRUMENT_CODE,
            price_source=price_source,
            intervals_utc=TEST_INTERVALS_UTC,
            settings_list=[spec.settings for spec in price_specs],
        )
        fast_groups = group_specs_for_fast_runs(price_specs)

        for interval_start_utc, interval_end_utc in TEST_INTERVALS_UTC:
            for group in fast_groups:
                first = group[0]
                filters_text = ",".join(spec.settings.market_regime_filter_mode.value for spec in group)
                print(
                    "RUN "
                    f"instrument={TEST_INSTRUMENT_CODE} "
                    f"interval={interval_start_utc} -> {interval_end_utc} UTC "
                    f"configs={','.join(str(spec.config_id) for spec in group)} "
                    f"window={first.settings.signal_window_mode.value} "
                    f"filters={filters_text} "
                    f"price_source={price_source}",
                    flush=True,
                )

                summary_rows, trades, signal_rows = run_backtest_interval_group(
                    cache=cache,
                    interval_start_utc=interval_start_utc,
                    interval_end_utc=interval_end_utc,
                    specs=group,
                )

                all_summary_rows.extend(summary_rows)
                all_trade_rows.extend(trade_to_row(trade) for trade in trades)
                all_signal_rows.extend(signal_rows)

                if WRITE_PARTIAL_OUTPUTS_AFTER_EACH_WINDOW_MODE:
                    write_outputs(
                        summary_rows=all_summary_rows,
                        trade_rows=all_trade_rows,
                        signal_rows=all_signal_rows,
                        config_rows=config_rows,
                    )
                    print(f"partial_outputs_written: {OUTPUT_DIR}", flush=True)

    write_outputs(
        summary_rows=all_summary_rows,
        trade_rows=all_trade_rows,
        signal_rows=all_signal_rows,
        config_rows=config_rows,
    )

    print()
    print_summary_table(all_summary_rows)
    print()
    print(f"configs: {OUTPUT_DIR / 'configs.csv'}", flush=True)
    print(f"summary: {OUTPUT_DIR / 'summary.csv'}", flush=True)
    print(f"trades:  {OUTPUT_DIR / 'trades.csv'}", flush=True)
    if WRITE_SIGNALS_CSV:
        print(f"signals: {OUTPUT_DIR / 'signals.csv'}", flush=True)


if __name__ == "__main__":
    main()
