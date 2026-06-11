"""
Мультипроцессорный offline-тестер полного торгового контура робота.

Что учитывает
-----------
1. Текущую signal-логику из tester.signal_strategy_tester:
   - Pearson;
   - potential;
   - market_regime_filter_mode OFF/SOFT/HARD;
   - ROLLING/SLOT окна;
   - SLOT entry-window.

2. Live-интерпретацию сигнала из ib_signal.signal_interpretation:
   - regime/ma_zone фильтры;
   - signal_allowed / reject_reason;
   - MARKET/LIMIT policy;
   - limit_offset_points / ttl_seconds.

3. Execution-модель:
   - MARKET вход;
   - LIMIT вход с TTL;
   - отмена pending LIMIT при противоположном сигнале;
   - same-side signal ignore;
   - opposite LIMIT signal закрывает позицию без reverse;
   - opposite MARKET signal делает reverse;
   - take-profit;
   - stop-loss;
   - SLOT close before slot end;
   - futures daily flat перед no-trade hour;
   - extreme MA-zone close.

4. Мультипроцессинг:
   - каждая комбинация signal-config group + execution params + interval считается отдельной задачей.

Как использовать
----------------
Никакой командной строки не нужно. Меняй настройки в блоке НАСТРОЙКИ и запускай:

    python tester/robot_capabilities_multiprocess_tester.py

Вывод:
    out/test/robot_capabilities_multiprocess_tester/
        configs.csv/json
        summary.csv/json
        trades.csv
        events.csv

Ограничения
-----------
- Это tester по 5-секундным OHLC mid-price барам. Если в одном баре задеты и TP, и SL,
  порядок внутри бара неизвестен. По умолчанию используется консервативная логика:
  stop-loss считается раньше take-profit.
- Реальный IB fill, queue position, spread/commission моделируются упрощённо настройками.
"""

from __future__ import annotations

import csv
import json
import math
import os
import sqlite3
import sys
import time
from concurrent.futures import ProcessPoolExecutor, as_completed
from dataclasses import asdict, dataclass, fields
from datetime import datetime, timezone
from itertools import product
from pathlib import Path
from typing import Any, Literal

import numpy as np

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from contracts import Instrument
from core.time_utils import CT_TIMEZONE
from ib_job_data.feature_db_sql import MID_PRICE_TABLE_NAME, quote_identifier
from ib_job_data.job_features_config import MA_ZONE_COLUMN_NAME
from ib_job_data.sma_features import SMA_TABLE_NAME
from ib_signal.signal_config import (
    DEFAULT_SIGNAL_CONFIG,
    MarketRegimeFilterMode,
    SignalConfig,
    SignalWindowMode,
)
from ib_signal.signal_interpretation import (
    calculate_limit_price,
    interpret_signal_event,
)
from ib_signal.signal_rules_config import SIGNAL_RULES, SIGNAL_RULE_SETTINGS
from tester import signal_strategy_tester as sigtest


# ============================================================
# НАСТРОЙКИ
# ============================================================

TEST_INSTRUMENT_CODE = "MNQ"

# Интервалы задаются в UTC.
TEST_INTERVALS_UTC: list[tuple[str, str]] = [
    ("2026-06-10 00:00:00", "2026-06-11 00:00:00"),
]

# SignalConfig grid. Ключи такие же, как в tester/signal_strategy_tester.py.
SIGNAL_TESTER_CONFIG: dict[str, list[Any]] = {
    "SignalWindowMode": ["SLOT"],
    "MarketRegimeFilterMode": ["HARD"],
    "candidate_potential_min_abs_end_delta_points": [10.0],
    # Примеры:
    # "pearson_min": [0.65, 0.70, 0.75],
    # "slot_back_minutes": [20, 30, 40],
    # "slot_entry_minutes": [10, 15, 20],
}

BASE_SIGNAL_SETTINGS: SignalConfig = DEFAULT_SIGNAL_CONFIG

# Execution grid. Здесь перебираем уже торговые возможности робота.
EXECUTION_TESTER_CONFIG: dict[str, list[Any]] = {
    # 0 или None = без stop-loss.
    "stop_loss_points": [0.0],

    # 0 или None = без take-profit.
    # "LIVE" = взять take_profit_points из contracts.py для инструмента.
    "take_profit_points": ["LIVE"],

    # "LIVE" = как решит ib_signal.signal_interpretation.
    # "MARKET" = все entry-сигналы принудительно MARKET.
    # "LIMIT" = все entry-сигналы принудительно LIMIT с instrument limit_offset_points.
    "entry_order_policy": ["LIVE"],

    "simulate_limit_orders": [True],

    # Если TP и SL задеты внутри одного 5s бара:
    # "STOP_FIRST" — консервативно;
    # "TAKE_PROFIT_FIRST" — оптимистично.
    "same_bar_exit_priority": ["STOP_FIRST"],

    # Упрощённая модель издержек в пунктах.
    # Для market entry/exit ухудшаем цену на slippage_points_per_side.
    # commission_points_per_trade вычитается из итогового profit_points.
    "slippage_points_per_side": [0.0],
    "commission_points_per_trade": [0.0],

    "enable_slot_close": [True],
    "enable_futures_daily_flat": [True],
    "enable_extreme_ma_zone_close": [True],
}

# Мультипроцессинг.
# None = max(1, os.cpu_count() - 1). Для тяжёлых сеток можно поставить конкретно, например 6.
MAX_WORKERS: int | None = 12

# Чтобы быстро проверить, можно ограничить число задач.
MAX_TASKS: int | None = None

# Если True, пишет events.csv. Для больших прогонов может быть много строк.
WRITE_EVENTS_CSV = False

# Если True, пишет partial outputs после каждой завершённой задачи.
WRITE_PARTIAL_OUTPUTS = False

# Видимый прогресс.
VERBOSE_PROGRESS = True

OUTPUT_DIR = PROJECT_ROOT / "out" / "test" / "robot_capabilities_multiprocess_tester"


Direction = Literal["LONG", "SHORT"]
OrderPolicy = Literal["LIVE", "MARKET", "LIMIT"]
SameBarExitPriority = Literal["STOP_FIRST", "TAKE_PROFIT_FIRST"]


# ============================================================
# DATA STRUCTURES
# ============================================================

@dataclass(frozen=True)
class ExecutionParams:
    execution_id: int
    execution_label: str
    stop_loss_points: float | None
    take_profit_points: float | str | None
    entry_order_policy: OrderPolicy
    simulate_limit_orders: bool
    same_bar_exit_priority: SameBarExitPriority
    slippage_points_per_side: float
    commission_points_per_trade: float
    enable_slot_close: bool
    enable_futures_daily_flat: bool
    enable_extreme_ma_zone_close: bool


@dataclass(frozen=True)
class WorkItem:
    task_id: int
    instrument_code: str
    interval_start_utc: str
    interval_end_utc: str
    price_source: str
    specs: tuple[sigtest.TestRunSpec, ...]
    execution_params: ExecutionParams


@dataclass(frozen=True)
class ExecBar:
    bar_time_ts: int
    closed_ts: int
    open: float
    high: float
    low: float
    close: float
    ma_zone: int | None


@dataclass
class ExecutionBarCache:
    instrument_code: str
    bar_size_seconds: int
    bar_time_ts: np.ndarray
    closed_ts: np.ndarray
    open: np.ndarray
    high: np.ndarray
    low: np.ndarray
    close: np.ndarray
    ma_zone: np.ndarray
    ts_to_index: dict[int, int]

    def bar_for_closed_ts(self, closed_ts: int) -> ExecBar | None:
        source_ts = int(closed_ts) - int(self.bar_size_seconds)
        index = self.ts_to_index.get(source_ts)
        if index is None:
            return None
        return self._bar_at_index(index)

    def close_price_at_closed_ts(self, closed_ts: int) -> float | None:
        bar = self.bar_for_closed_ts(closed_ts)
        return None if bar is None else float(bar.close)

    def last_bar_at_or_before_closed_ts(self, closed_ts: int) -> ExecBar | None:
        index = int(np.searchsorted(self.closed_ts, int(closed_ts), side="right")) - 1
        if index < 0:
            return None
        return self._bar_at_index(index)

    def bars_between_closed_ts(self, *, start_exclusive: int, end_inclusive: int) -> list[ExecBar]:
        left = int(np.searchsorted(self.closed_ts, int(start_exclusive), side="right"))
        right = int(np.searchsorted(self.closed_ts, int(end_inclusive), side="right"))
        return [self._bar_at_index(i) for i in range(left, right)]

    def ma_zone_at_or_before_closed_ts(self, closed_ts: int) -> int | None:
        index = int(np.searchsorted(self.closed_ts, int(closed_ts), side="right")) - 1
        while index >= 0:
            value = int(self.ma_zone[index])
            if value != MISSING_INT:
                return value
            index -= 1
        return None

    def _bar_at_index(self, index: int) -> ExecBar:
        ma_zone_raw = int(self.ma_zone[index])
        return ExecBar(
            bar_time_ts=int(self.bar_time_ts[index]),
            closed_ts=int(self.closed_ts[index]),
            open=float(self.open[index]),
            high=float(self.high[index]),
            low=float(self.low[index]),
            close=float(self.close[index]),
            ma_zone=None if ma_zone_raw == MISSING_INT else ma_zone_raw,
        )


@dataclass
class PendingLimitOrder:
    direction: Direction
    created_ts: int
    expires_ts: int
    limit_price: float
    signal_bar_ts: int
    signal_time_ct: str | None
    close_due_ts: int | None
    signal_best_pearson: float
    signal_potential_end_delta_points: float
    signal_potential_used: int
    order_policy_reason: str


@dataclass
class SimPosition:
    direction: Direction
    entry_ts: int
    entry_price: float
    entry_signal_time_ct: str | None
    close_due_ts: int | None
    take_profit_price: float | None
    stop_loss_price: float | None
    entry_order_type: str
    entry_reason: str
    entry_best_pearson: float
    entry_potential_end_delta_points: float
    entry_potential_used: int


@dataclass
class RobotCounters:
    closed_bars_seen: int = 0
    due_signals: int = 0
    calculations: int = 0
    signals_long: int = 0
    signals_short: int = 0
    no_signal: int = 0
    signal_rejected: int = 0
    regime_skips: int = 0
    data_not_ready: int = 0
    no_price: int = 0
    errors: int = 0

    market_entries: int = 0
    limit_orders_submitted: int = 0
    limit_orders_filled: int = 0
    limit_orders_cancelled: int = 0
    limit_orders_expired: int = 0

    same_side_ignored: int = 0
    reversals: int = 0
    opposite_limit_closes: int = 0
    take_profit_hits: int = 0
    stop_loss_hits: int = 0
    slot_closes: int = 0
    futures_daily_flat_closes: int = 0
    extreme_ma_zone_closes: int = 0
    interval_forced_closes: int = 0


@dataclass
class RobotTrade:
    task_id: int
    interval_start_utc: str
    interval_end_utc: str
    config_id: int
    config_label: str
    execution_id: int
    execution_label: str
    signal_window_mode: str
    market_regime_filter_mode: str
    instrument_code: str

    direction: Direction
    entry_ts: int
    entry_utc: str
    entry_signal_time_ct: str | None
    entry_price: float
    entry_order_type: str
    entry_reason: str
    take_profit_price: float | None
    stop_loss_price: float | None

    exit_ts: int
    exit_utc: str
    exit_price: float
    exit_reason: str
    holding_seconds: int
    gross_profit_points: float
    commission_points: float
    net_profit_points: float

    entry_best_pearson: float
    entry_potential_end_delta_points: float
    entry_potential_used: int


@dataclass
class RobotState:
    task_id: int
    interval_start_utc: str
    interval_end_utc: str
    spec: sigtest.TestRunSpec
    execution_params: ExecutionParams
    instrument_code: str
    signal_window_mode: SignalWindowMode
    market_regime_filter_mode: MarketRegimeFilterMode

    position: SimPosition | None
    pending_limit: PendingLimitOrder | None
    trades: list[RobotTrade]
    events: list[dict[str, object]]
    counters: RobotCounters


MISSING_INT = -999999999


# ============================================================
# UTILS
# ============================================================

def progress_print(message: str) -> None:
    if not VERBOSE_PROGRESS:
        return
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {message}", flush=True)


def format_utc_ts(ts: int) -> str:
    return datetime.fromtimestamp(int(ts), tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def parse_utc_text(value: str) -> int:
    return sigtest.parse_utc_text(value)


def safe_float_or_none(value: object) -> float | None:
    if value is None:
        return None
    if isinstance(value, str) and value.strip().upper() in {"LIVE", "NONE", "NULL", ""}:
        return value.strip().upper() if value.strip().upper() == "LIVE" else None  # type: ignore[return-value]
    return float(value)


def side_multiplier(direction: str) -> float:
    return 1.0 if str(direction).upper() == "LONG" else -1.0


def normalize_price_floor(price: float, tick: float) -> float:
    if tick <= 0.0:
        return float(price)
    return math.floor(float(price) / float(tick)) * float(tick)


def market_entry_price(raw_price: float, direction: Direction, slippage_points: float) -> float:
    if direction == "LONG":
        return float(raw_price) + float(slippage_points)
    return float(raw_price) - float(slippage_points)


def market_exit_price(raw_price: float, direction: Direction, slippage_points: float) -> float:
    if direction == "LONG":
        return float(raw_price) - float(slippage_points)
    return float(raw_price) + float(slippage_points)


def get_live_take_profit_points(instrument_code: str) -> float | None:
    row = Instrument.get(str(instrument_code))
    if row is None:
        return None
    value = float(row.get("take_profit_points", 0.0) or 0.0)
    return value if value > 0.0 else None


def resolve_points_setting(value: float | str | None, *, instrument_code: str) -> float | None:
    if value is None:
        return None
    if isinstance(value, str):
        text = value.strip().upper()
        if text in {"", "NONE", "NULL", "OFF"}:
            return None
        if text == "LIVE":
            return get_live_take_profit_points(instrument_code)
        return float(text)
    numeric = float(value)
    return numeric if numeric > 0.0 else None


def build_execution_label(params: ExecutionParams) -> str:
    return (
        f"sl={params.stop_loss_points},"
        f"tp={params.take_profit_points},"
        f"order={params.entry_order_policy},"
        f"limit={params.simulate_limit_orders},"
        f"prio={params.same_bar_exit_priority},"
        f"slip={params.slippage_points_per_side},"
        f"comm={params.commission_points_per_trade}"
    )


def build_execution_params_grid(config: dict[str, list[Any]]) -> list[ExecutionParams]:
    keys = list(config.keys())
    if not keys:
        combinations = [()]
    else:
        combinations = product(*(config[key] for key in keys))

    params: list[ExecutionParams] = []
    for values in combinations:
        raw = dict(zip(keys, values, strict=True)) if keys else {}
        entry_policy = str(raw.get("entry_order_policy", "LIVE")).upper()
        if entry_policy not in {"LIVE", "MARKET", "LIMIT"}:
            raise ValueError(f"entry_order_policy должен быть LIVE/MARKET/LIMIT, получено {entry_policy!r}")

        priority = str(raw.get("same_bar_exit_priority", "STOP_FIRST")).upper()
        if priority not in {"STOP_FIRST", "TAKE_PROFIT_FIRST"}:
            raise ValueError(f"same_bar_exit_priority должен быть STOP_FIRST/TAKE_PROFIT_FIRST, получено {priority!r}")

        p = ExecutionParams(
            execution_id=len(params) + 1,
            execution_label="",
            stop_loss_points=(
                None
                if raw.get("stop_loss_points") is None or float(raw.get("stop_loss_points") or 0.0) <= 0.0
                else float(raw.get("stop_loss_points"))
            ),
            take_profit_points=raw.get("take_profit_points", "LIVE"),
            entry_order_policy=entry_policy,  # type: ignore[arg-type]
            simulate_limit_orders=bool(raw.get("simulate_limit_orders", True)),
            same_bar_exit_priority=priority,  # type: ignore[arg-type]
            slippage_points_per_side=float(raw.get("slippage_points_per_side", 0.0) or 0.0),
            commission_points_per_trade=float(raw.get("commission_points_per_trade", 0.0) or 0.0),
            enable_slot_close=bool(raw.get("enable_slot_close", True)),
            enable_futures_daily_flat=bool(raw.get("enable_futures_daily_flat", True)),
            enable_extreme_ma_zone_close=bool(raw.get("enable_extreme_ma_zone_close", True)),
        )
        p = ExecutionParams(**{**asdict(p), "execution_label": build_execution_label(p)})
        params.append(p)

    if not params:
        raise RuntimeError("EXECUTION_TESTER_CONFIG не дал ни одной execution-конфигурации")

    return params


# ============================================================
# EXECUTION BAR CACHE
# ============================================================

def load_execution_bar_cache(
        *,
        instrument_code: str,
        intervals_utc: list[tuple[str, str]],
) -> ExecutionBarCache:
    bar_size_seconds = sigtest.get_instrument_bar_size_seconds(instrument_code)
    start_ts = min(parse_utc_text(start) for start, _ in intervals_utc)
    end_ts = max(parse_utc_text(end) for _, end in intervals_utc)

    # Нужен источник на один бар раньше, потому что closed_ts = bar_time_ts + bar_size_seconds.
    source_start_ts = start_ts - int(bar_size_seconds)
    source_end_ts = end_ts - int(bar_size_seconds)

    job_db_path = sigtest.get_job_db_path(instrument_code)
    if not job_db_path.is_file():
        raise FileNotFoundError(f"Job DB не найдена: {job_db_path}")

    bar_time_values: list[int] = []
    open_values: list[float] = []
    high_values: list[float] = []
    low_values: list[float] = []
    close_values: list[float] = []
    ma_zone_values: list[int] = []

    conn = sqlite3.connect(str(job_db_path))
    try:
        sql_with_ma = f"""
            SELECT
                mp.bar_time_ts,
                mp.mid_open,
                mp.mid_high,
                mp.mid_low,
                mp.mid_close,
                sma.{quote_identifier(MA_ZONE_COLUMN_NAME)}
            FROM {quote_identifier(MID_PRICE_TABLE_NAME)} AS mp
            LEFT JOIN {quote_identifier(SMA_TABLE_NAME)} AS sma
              ON sma.bar_time_ts = mp.bar_time_ts
            WHERE mp.bar_time_ts >= ?
              AND mp.bar_time_ts <= ?
            ORDER BY mp.bar_time_ts
        """

        try:
            rows = conn.execute(sql_with_ma, (source_start_ts, source_end_ts)).fetchall()
        except sqlite3.OperationalError:
            sql_without_ma = f"""
                SELECT
                    bar_time_ts,
                    mid_open,
                    mid_high,
                    mid_low,
                    mid_close,
                    NULL
                FROM {quote_identifier(MID_PRICE_TABLE_NAME)}
                WHERE bar_time_ts >= ?
                  AND bar_time_ts <= ?
                ORDER BY bar_time_ts
            """
            rows = conn.execute(sql_without_ma, (source_start_ts, source_end_ts)).fetchall()

        for row in rows:
            if row[1] is None or row[2] is None or row[3] is None or row[4] is None:
                continue
            bar_time_values.append(int(row[0]))
            open_values.append(float(row[1]))
            high_values.append(float(row[2]))
            low_values.append(float(row[3]))
            close_values.append(float(row[4]))
            ma_zone_values.append(MISSING_INT if row[5] is None else int(row[5]))

    finally:
        conn.close()

    if not bar_time_values:
        raise RuntimeError(
            f"Execution OHLC cache пустой: instrument={instrument_code}, "
            f"{format_utc_ts(source_start_ts)}->{format_utc_ts(source_end_ts)}, db={job_db_path}"
        )

    bar_time_ts = np.asarray(bar_time_values, dtype=np.int64)
    ts_to_index = {int(ts): int(index) for index, ts in enumerate(bar_time_ts)}

    return ExecutionBarCache(
        instrument_code=instrument_code,
        bar_size_seconds=int(bar_size_seconds),
        bar_time_ts=bar_time_ts,
        closed_ts=bar_time_ts + int(bar_size_seconds),
        open=np.asarray(open_values, dtype=float),
        high=np.asarray(high_values, dtype=float),
        low=np.asarray(low_values, dtype=float),
        close=np.asarray(close_values, dtype=float),
        ma_zone=np.asarray(ma_zone_values, dtype=np.int32),
        ts_to_index=ts_to_index,
    )


# ============================================================
# ROBOT EXECUTION MODEL
# ============================================================

def get_stop_loss_price(position: SimPosition) -> float | None:
    return position.stop_loss_price


def get_take_profit_price(position: SimPosition) -> float | None:
    return position.take_profit_price


def check_position_bar_exit(
        *,
        position: SimPosition,
        bar: ExecBar,
        priority: SameBarExitPriority,
) -> tuple[str, float] | None:
    tp = get_take_profit_price(position)
    sl = get_stop_loss_price(position)

    if position.direction == "LONG":
        tp_hit = tp is not None and bar.high >= tp
        sl_hit = sl is not None and bar.low <= sl
    else:
        tp_hit = tp is not None and bar.low <= tp
        sl_hit = sl is not None and bar.high >= sl

    if tp_hit and sl_hit:
        if priority == "TAKE_PROFIT_FIRST":
            return "take_profit", float(tp)
        return "stop_loss", float(sl)

    if sl_hit:
        return "stop_loss", float(sl)

    if tp_hit:
        return "take_profit", float(tp)

    return None


def check_pending_limit_fill(
        *,
        pending: PendingLimitOrder,
        bar: ExecBar,
) -> bool:
    if pending.direction == "LONG":
        return bar.low <= pending.limit_price
    return bar.high >= pending.limit_price


def calculate_tp_sl_prices(
        *,
        instrument_code: str,
        direction: Direction,
        entry_price: float,
        params: ExecutionParams,
) -> tuple[float | None, float | None]:
    instrument_row = Instrument[str(instrument_code)]
    tick = float(instrument_row.get("price_tick", 0.0) or 0.0)

    tp_points = resolve_points_setting(params.take_profit_points, instrument_code=instrument_code)
    sl_points = params.stop_loss_points if params.stop_loss_points is not None and params.stop_loss_points > 0 else None

    take_profit_price = None
    stop_loss_price = None

    if tp_points is not None and tp_points > 0.0:
        if direction == "LONG":
            take_profit_price = normalize_price_floor(float(entry_price) + float(tp_points), tick)
        else:
            take_profit_price = normalize_price_floor(float(entry_price) - float(tp_points), tick)

    if sl_points is not None and sl_points > 0.0:
        if direction == "LONG":
            stop_loss_price = normalize_price_floor(float(entry_price) - float(sl_points), tick)
        else:
            stop_loss_price = normalize_price_floor(float(entry_price) + float(sl_points), tick)

    return take_profit_price, stop_loss_price


def open_market_position(
        *,
        state: RobotState,
        direction: Direction,
        signal_bar_ts: int,
        raw_entry_price: float,
        signal_time_ct: str | None,
        close_due_ts: int | None,
        entry_reason: str,
        best_pearson: float,
        potential_end_delta_points: float,
        potential_used: int,
) -> None:
    entry_price = market_entry_price(
        raw_price=float(raw_entry_price),
        direction=direction,
        slippage_points=float(state.execution_params.slippage_points_per_side),
    )
    tp, sl = calculate_tp_sl_prices(
        instrument_code=state.instrument_code,
        direction=direction,
        entry_price=entry_price,
        params=state.execution_params,
    )
    state.position = SimPosition(
        direction=direction,
        entry_ts=int(signal_bar_ts),
        entry_price=float(entry_price),
        entry_signal_time_ct=signal_time_ct,
        close_due_ts=close_due_ts,
        take_profit_price=tp,
        stop_loss_price=sl,
        entry_order_type="MARKET",
        entry_reason=entry_reason,
        entry_best_pearson=float(best_pearson),
        entry_potential_end_delta_points=float(potential_end_delta_points),
        entry_potential_used=int(potential_used),
    )
    state.counters.market_entries += 1


def open_limit_filled_position(
        *,
        state: RobotState,
        pending: PendingLimitOrder,
        fill_ts: int,
        fill_price: float,
        close_due_ts: int | None,
) -> None:
    tp, sl = calculate_tp_sl_prices(
        instrument_code=state.instrument_code,
        direction=pending.direction,
        entry_price=float(fill_price),
        params=state.execution_params,
    )
    state.position = SimPosition(
        direction=pending.direction,
        entry_ts=int(fill_ts),
        entry_price=float(fill_price),
        entry_signal_time_ct=pending.signal_time_ct,
        close_due_ts=close_due_ts,
        take_profit_price=tp,
        stop_loss_price=sl,
        entry_order_type="LIMIT",
        entry_reason=pending.order_policy_reason,
        entry_best_pearson=float(pending.signal_best_pearson),
        entry_potential_end_delta_points=float(pending.signal_potential_end_delta_points),
        entry_potential_used=int(pending.signal_potential_used),
    )
    state.counters.limit_orders_filled += 1
    state.pending_limit = None


def close_position(
        *,
        state: RobotState,
        exit_ts: int,
        exit_price: float,
        exit_reason: str,
) -> None:
    position = state.position
    if position is None:
        return

    gross = (float(exit_price) - float(position.entry_price)) * side_multiplier(position.direction)
    commission = float(state.execution_params.commission_points_per_trade)
    net = gross - commission

    state.trades.append(
        RobotTrade(
            task_id=state.task_id,
            interval_start_utc=state.interval_start_utc,
            interval_end_utc=state.interval_end_utc,
            config_id=state.spec.config_id,
            config_label=state.spec.config_label,
            execution_id=state.execution_params.execution_id,
            execution_label=state.execution_params.execution_label,
            signal_window_mode=state.signal_window_mode.value,
            market_regime_filter_mode=state.market_regime_filter_mode.value,
            instrument_code=state.instrument_code,
            direction=position.direction,
            entry_ts=int(position.entry_ts),
            entry_utc=format_utc_ts(position.entry_ts),
            entry_signal_time_ct=position.entry_signal_time_ct,
            entry_price=float(position.entry_price),
            entry_order_type=position.entry_order_type,
            entry_reason=position.entry_reason,
            take_profit_price=position.take_profit_price,
            stop_loss_price=position.stop_loss_price,
            exit_ts=int(exit_ts),
            exit_utc=format_utc_ts(exit_ts),
            exit_price=float(exit_price),
            exit_reason=exit_reason,
            holding_seconds=max(0, int(exit_ts) - int(position.entry_ts)),
            gross_profit_points=float(gross),
            commission_points=float(commission),
            net_profit_points=float(net),
            entry_best_pearson=float(position.entry_best_pearson),
            entry_potential_end_delta_points=float(position.entry_potential_end_delta_points),
            entry_potential_used=int(position.entry_potential_used),
        )
    )

    if exit_reason == "take_profit":
        state.counters.take_profit_hits += 1
    elif exit_reason == "stop_loss":
        state.counters.stop_loss_hits += 1
    elif exit_reason == "slot_close":
        state.counters.slot_closes += 1
    elif exit_reason == "futures_daily_flat":
        state.counters.futures_daily_flat_closes += 1
    elif exit_reason == "extreme_ma_zone_close":
        state.counters.extreme_ma_zone_closes += 1
    elif exit_reason == "interval_end":
        state.counters.interval_forced_closes += 1

    state.position = None


def cancel_pending_limit(*, state: RobotState, reason: str, closed_ts: int) -> None:
    if state.pending_limit is None:
        return
    state.events.append({
        "task_id": state.task_id,
        "config_id": state.spec.config_id,
        "execution_id": state.execution_params.execution_id,
        "instrument_code": state.instrument_code,
        "event": "LIMIT_CANCELLED",
        "event_ts": int(closed_ts),
        "event_utc": format_utc_ts(closed_ts),
        "direction": state.pending_limit.direction,
        "limit_price": state.pending_limit.limit_price,
        "reason": reason,
    })
    state.pending_limit = None
    state.counters.limit_orders_cancelled += 1


def maybe_fill_or_expire_pending_limit(
        *,
        state: RobotState,
        bar: ExecBar,
) -> None:
    pending = state.pending_limit
    if pending is None:
        return

    if bar.closed_ts <= pending.expires_ts and check_pending_limit_fill(pending=pending, bar=bar):
        open_limit_filled_position(
            state=state,
            pending=pending,
            fill_ts=bar.closed_ts,
            fill_price=pending.limit_price,
            close_due_ts=pending.close_due_ts,
        )
        state.events.append({
            "task_id": state.task_id,
            "config_id": state.spec.config_id,
            "execution_id": state.execution_params.execution_id,
            "instrument_code": state.instrument_code,
            "event": "LIMIT_FILLED",
            "event_ts": int(bar.closed_ts),
            "event_utc": format_utc_ts(bar.closed_ts),
            "direction": pending.direction,
            "limit_price": pending.limit_price,
        })
        return

    if bar.closed_ts >= pending.expires_ts:
        state.events.append({
            "task_id": state.task_id,
            "config_id": state.spec.config_id,
            "execution_id": state.execution_params.execution_id,
            "instrument_code": state.instrument_code,
            "event": "LIMIT_EXPIRED",
            "event_ts": int(bar.closed_ts),
            "event_utc": format_utc_ts(bar.closed_ts),
            "direction": pending.direction,
            "limit_price": pending.limit_price,
            "expires_ts": pending.expires_ts,
        })
        state.pending_limit = None
        state.counters.limit_orders_expired += 1


def maybe_exit_by_tp_sl(
        *,
        state: RobotState,
        bar: ExecBar,
) -> None:
    if state.position is None:
        return

    hit = check_position_bar_exit(
        position=state.position,
        bar=bar,
        priority=state.execution_params.same_bar_exit_priority,
    )
    if hit is None:
        return

    reason, price = hit
    close_position(state=state, exit_ts=bar.closed_ts, exit_price=price, exit_reason=reason)


def get_ct_day_ts(*, now_ts: int, hour: int, minute: int = 0, second: int = 0) -> int:
    now_ct = datetime.fromtimestamp(int(now_ts), tz=timezone.utc).astimezone(CT_TIMEZONE)
    target_ct = now_ct.replace(
        hour=int(hour),
        minute=int(minute),
        second=int(second),
        microsecond=0,
    )
    return int(target_ct.astimezone(timezone.utc).timestamp())


def maybe_close_by_slot(
        *,
        state: RobotState,
        bar: ExecBar,
) -> None:
    if not state.execution_params.enable_slot_close:
        return
    if state.signal_window_mode != SignalWindowMode.SLOT:
        return
    if state.position is None or state.position.close_due_ts is None:
        return
    if bar.closed_ts < int(state.position.close_due_ts):
        return

    exit_price = market_exit_price(
        raw_price=bar.close,
        direction=state.position.direction,
        slippage_points=state.execution_params.slippage_points_per_side,
    )
    close_position(state=state, exit_ts=bar.closed_ts, exit_price=exit_price, exit_reason="slot_close")


def maybe_close_by_futures_daily_flat(
        *,
        state: RobotState,
        bar: ExecBar,
) -> None:
    if not state.execution_params.enable_futures_daily_flat:
        return
    if state.position is None:
        return
    instrument_row = Instrument[str(state.instrument_code)]
    if str(instrument_row.get("secType", "")).upper() != "FUT":
        return

    close_before_seconds = int(state.spec.settings.slot_close_before_end_seconds)
    no_new_trades_ts = get_ct_day_ts(now_ts=bar.closed_ts, hour=15)
    clearing_ts = get_ct_day_ts(now_ts=bar.closed_ts, hour=16)
    close_at_ts = no_new_trades_ts - close_before_seconds

    if not (close_at_ts <= bar.closed_ts < clearing_ts):
        return

    exit_price = market_exit_price(
        raw_price=bar.close,
        direction=state.position.direction,
        slippage_points=state.execution_params.slippage_points_per_side,
    )
    close_position(state=state, exit_ts=bar.closed_ts, exit_price=exit_price, exit_reason="futures_daily_flat")


def maybe_close_by_extreme_ma_zone(
        *,
        state: RobotState,
        bar: ExecBar,
) -> None:
    if not state.execution_params.enable_extreme_ma_zone_close:
        return
    if not bool(SIGNAL_RULE_SETTINGS.get("close_position_on_extreme_ma_zone_enabled", True)):
        return
    if state.position is None:
        return
    if bar.ma_zone is None:
        return

    ma_zone = int(bar.ma_zone)
    should_close = False
    if state.position.direction == "LONG":
        should_close = ma_zone >= int(SIGNAL_RULES["long_position_extreme_close_ma_zone"])
    elif state.position.direction == "SHORT":
        should_close = ma_zone <= int(SIGNAL_RULES["short_position_extreme_close_ma_zone"])

    if not should_close:
        return

    exit_price = market_exit_price(
        raw_price=bar.close,
        direction=state.position.direction,
        slippage_points=state.execution_params.slippage_points_per_side,
    )
    close_position(state=state, exit_ts=bar.closed_ts, exit_price=exit_price, exit_reason="extreme_ma_zone_close")


def choose_effective_order(
        *,
        state: RobotState,
        signal: sigtest.PatternSignalResult,
) -> tuple[str, str, float | None, int | None, str, bool]:
    """Возвращает order_type, reason, limit_price, ttl_seconds, signal_strength, allowed."""

    if signal.direction is None or signal.entry_price is None:
        return "NONE", "no_direction", None, None, "NONE", False

    try:
        interpretation = interpret_signal_event(
            instrument_code=state.instrument_code,
            signal_bar_ts=int(signal.signal_bar_ts),
            signal_time_ct=signal.signal_time_ct,
            direction=signal.direction,
            entry_price=float(signal.entry_price),
        )
    except Exception as exc:
        state.events.append({
            "task_id": state.task_id,
            "config_id": state.spec.config_id,
            "execution_id": state.execution_params.execution_id,
            "instrument_code": state.instrument_code,
            "event": "INTERPRET_SIGNAL_FAILED",
            "event_ts": int(signal.signal_bar_ts),
            "event_utc": format_utc_ts(signal.signal_bar_ts),
            "error": f"{type(exc).__name__}: {exc}",
        })
        return "NONE", "interpret_error", None, None, "ERROR", False

    if not interpretation.signal_allowed:
        return (
            str(interpretation.order_type).upper(),
            interpretation.signal_reject_reason or "signal_rejected",
            interpretation.limit_price,
            interpretation.ttl_seconds,
            interpretation.signal_strength,
            False,
        )

    policy = state.execution_params.entry_order_policy
    if policy == "LIVE":
        return (
            str(interpretation.order_type).upper(),
            interpretation.order_policy_reason,
            interpretation.limit_price,
            interpretation.ttl_seconds,
            interpretation.signal_strength,
            True,
        )

    if policy == "MARKET":
        return "MARKET", "forced_market", None, None, interpretation.signal_strength, True

    instrument_row = Instrument[state.instrument_code]
    limit_offset = float(instrument_row.get("limit_offset_points", 0.0) or 0.0)
    ttl_seconds = int(instrument_row.get("limit_ttl_seconds", 600) or 600)
    limit_price = calculate_limit_price(
        direction=signal.direction,
        entry_price=float(signal.entry_price),
        order_type="LIMIT",
        limit_offset_points=limit_offset,
        price_tick=float(instrument_row.get("price_tick", 0.25) or 0.25),
    )
    return "LIMIT", "forced_limit", limit_price, ttl_seconds, interpretation.signal_strength, True


def process_signal(
        *,
        state: RobotState,
        signal: sigtest.PatternSignalResult,
        bar: ExecBar,
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
        return "NO_SIGNAL"

    direction: Direction = signal.direction  # type: ignore[assignment]
    if direction == "LONG":
        state.counters.signals_long += 1
    else:
        state.counters.signals_short += 1

    order_type, order_reason, limit_price, ttl_seconds, signal_strength, allowed = choose_effective_order(
        state=state,
        signal=signal,
    )

    if not allowed:
        state.counters.signal_rejected += 1
        return f"REJECTED_{order_reason}"

    if state.pending_limit is not None and state.position is None:
        if state.pending_limit.direction != direction:
            cancel_pending_limit(state=state, reason=f"opposite_signal_{direction}", closed_ts=signal.signal_bar_ts)
            return f"CANCEL_PENDING_LIMIT_BY_OPPOSITE_{direction}"
        return f"IGNORE_SIGNAL_PENDING_LIMIT_{direction}"

    if state.position is None:
        close_due_ts = signal.trade_end_ts if state.signal_window_mode == SignalWindowMode.SLOT else None

        if order_type == "LIMIT" and state.execution_params.simulate_limit_orders:
            if limit_price is None:
                state.counters.errors += 1
                return "LIMIT_SIGNAL_WITHOUT_LIMIT_PRICE"

            state.pending_limit = PendingLimitOrder(
                direction=direction,
                created_ts=int(signal.signal_bar_ts),
                expires_ts=int(signal.signal_bar_ts) + int(ttl_seconds or 600),
                limit_price=float(limit_price),
                signal_bar_ts=int(signal.signal_bar_ts),
                signal_time_ct=signal.signal_time_ct,
                close_due_ts=close_due_ts,
                signal_best_pearson=float(signal.best_pearson),
                signal_potential_end_delta_points=float(signal.potential_end_delta_points),
                signal_potential_used=int(signal.potential_used),
                order_policy_reason=order_reason,
            )
            state.counters.limit_orders_submitted += 1
            return f"SUBMIT_LIMIT_{direction}"

        open_market_position(
            state=state,
            direction=direction,
            signal_bar_ts=signal.signal_bar_ts,
            raw_entry_price=float(signal.entry_price),
            signal_time_ct=signal.signal_time_ct,
            close_due_ts=close_due_ts,
            entry_reason=order_reason,
            best_pearson=signal.best_pearson,
            potential_end_delta_points=signal.potential_end_delta_points,
            potential_used=signal.potential_used,
        )
        return f"OPEN_MARKET_{direction}"

    # Есть открытая позиция.
    if state.position.direction == direction:
        state.counters.same_side_ignored += 1
        return f"IGNORE_SAME_{direction}"

    # Opposite LIMIT signal in live trader = close-only.
    raw_exit_price = float(signal.entry_price if signal.entry_price is not None else bar.close)
    exit_price = market_exit_price(
        raw_price=raw_exit_price,
        direction=state.position.direction,
        slippage_points=state.execution_params.slippage_points_per_side,
    )

    previous_direction = state.position.direction
    if order_type == "LIMIT":
        close_position(
            state=state,
            exit_ts=signal.signal_bar_ts,
            exit_price=exit_price,
            exit_reason=f"opposite_limit_signal_close_only_{previous_direction}_to_flat",
        )
        state.counters.opposite_limit_closes += 1
        return f"OPPOSITE_LIMIT_CLOSE_ONLY_{previous_direction}_TO_FLAT"

    # Opposite MARKET signal = reverse.
    close_position(
        state=state,
        exit_ts=signal.signal_bar_ts,
        exit_price=exit_price,
        exit_reason=f"reverse_{previous_direction}_to_{direction}",
    )
    state.counters.reversals += 1
    open_market_position(
        state=state,
        direction=direction,
        signal_bar_ts=signal.signal_bar_ts,
        raw_entry_price=float(signal.entry_price),
        signal_time_ct=signal.signal_time_ct,
        close_due_ts=signal.trade_end_ts if state.signal_window_mode == SignalWindowMode.SLOT else None,
        entry_reason=f"reverse_market_{order_reason}",
        best_pearson=signal.best_pearson,
        potential_end_delta_points=signal.potential_end_delta_points,
        potential_used=signal.potential_used,
    )
    return f"REVERSE_MARKET_{previous_direction}_TO_{direction}"


# ============================================================
# SUMMARY / OUTPUT
# ============================================================

def calculate_equity_drawdown(trades: list[RobotTrade]) -> float:
    equity = 0.0
    peak = 0.0
    max_dd = 0.0
    for trade in trades:
        equity += float(trade.net_profit_points)
        peak = max(peak, equity)
        max_dd = max(max_dd, peak - equity)
    return float(max_dd)


def build_summary_row(state: RobotState, *, start_ts: int, end_ts: int) -> dict[str, object]:
    trades = state.trades
    profits = [float(t.net_profit_points) for t in trades]
    wins = [x for x in profits if x > 0]
    losses = [x for x in profits if x < 0]
    gross_profit = float(sum(wins))
    gross_loss = float(sum(losses))
    total = float(sum(profits))
    trade_count = len(trades)

    row: dict[str, object] = {
        "task_id": state.task_id,
        "interval_start_utc": state.interval_start_utc,
        "interval_end_utc": state.interval_end_utc,
        "interval_start_ts": int(start_ts),
        "interval_end_ts": int(end_ts),
        "instrument_code": state.instrument_code,
        "config_id": state.spec.config_id,
        "config_label": state.spec.config_label,
        "execution_id": state.execution_params.execution_id,
        "execution_label": state.execution_params.execution_label,
        "signal_window_mode": state.signal_window_mode.value,
        "market_regime_filter_mode": state.market_regime_filter_mode.value,
        "price_source": state.spec.settings.price_source,
        "pearson_min": state.spec.settings.pearson_min,
        "history_lookback_days": state.spec.settings.history_lookback_days,
        "slot_step_minutes": state.spec.settings.slot_step_minutes,
        "slot_back_minutes": state.spec.settings.slot_back_minutes,
        "slot_entry_minutes": state.spec.settings.slot_entry_minutes,
        "slot_close_before_end_seconds": state.spec.settings.slot_close_before_end_seconds,
        "stop_loss_points": state.execution_params.stop_loss_points,
        "take_profit_points": state.execution_params.take_profit_points,
        "entry_order_policy": state.execution_params.entry_order_policy,
        "simulate_limit_orders": state.execution_params.simulate_limit_orders,
        "same_bar_exit_priority": state.execution_params.same_bar_exit_priority,
        "slippage_points_per_side": state.execution_params.slippage_points_per_side,
        "commission_points_per_trade": state.execution_params.commission_points_per_trade,
    }

    row.update(asdict(state.counters))
    row.update({
        "trades": trade_count,
        "wins": len(wins),
        "losses": len(losses),
        "flats": len([x for x in profits if x == 0]),
        "win_rate_pct": len(wins) / trade_count * 100.0 if trade_count else 0.0,
        "net_profit_points": total,
        "gross_profit_points": gross_profit,
        "gross_loss_points": gross_loss,
        "profit_factor": gross_profit / abs(gross_loss) if gross_loss < 0 else None,
        "avg_trade_points": total / trade_count if trade_count else 0.0,
        "max_trade_profit_points": max(profits) if profits else 0.0,
        "max_trade_loss_points": min(profits) if profits else 0.0,
        "max_equity_drawdown_points": calculate_equity_drawdown(trades),
        "open_position_left": state.position.direction if state.position is not None else None,
        "pending_limit_left": state.pending_limit.direction if state.pending_limit is not None else None,
    })
    return row


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


def print_summary(rows: list[dict[str, object]]) -> None:
    if not rows:
        print("Нет результатов.", flush=True)
        return

    sorted_rows = sorted(rows, key=lambda r: float(r.get("net_profit_points") or 0.0), reverse=True)
    columns = [
        "task_id",
        "config_id",
        "execution_id",
        "instrument_code",
        "signal_window_mode",
        "market_regime_filter_mode",
        "stop_loss_points",
        "take_profit_points",
        "entry_order_policy",
        "trades",
        "win_rate_pct",
        "net_profit_points",
        "profit_factor",
        "max_equity_drawdown_points",
        "stop_loss_hits",
        "take_profit_hits",
        "limit_orders_filled",
        "limit_orders_expired",
    ]

    top = sorted_rows[:30]

    def fmt(value: object) -> str:
        if isinstance(value, float):
            return f"{value:.2f}"
        return "" if value is None else str(value)

    widths = {col: max(len(col), *(len(fmt(row.get(col))) for row in top)) for col in columns}
    print()
    print("TOP RESULTS", flush=True)
    print(" | ".join(col.ljust(widths[col]) for col in columns), flush=True)
    print("-+-".join("-" * widths[col] for col in columns), flush=True)
    for row in top:
        print(" | ".join(fmt(row.get(col)).ljust(widths[col]) for col in columns), flush=True)


def write_outputs(
        *,
        summary_rows: list[dict[str, object]],
        trade_rows: list[dict[str, object]],
        event_rows: list[dict[str, object]],
        config_rows: list[dict[str, object]],
) -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    write_csv(OUTPUT_DIR / "configs.csv", config_rows)
    write_json(OUTPUT_DIR / "configs.json", config_rows)
    write_csv(OUTPUT_DIR / "summary.csv", summary_rows)
    write_json(OUTPUT_DIR / "summary.json", summary_rows)
    write_csv(OUTPUT_DIR / "trades.csv", trade_rows)
    if WRITE_EVENTS_CSV:
        write_csv(OUTPUT_DIR / "events.csv", event_rows)


# ============================================================
# WORKER
# ============================================================

def run_work_item(item: WorkItem) -> tuple[list[dict[str, object]], list[dict[str, object]], list[dict[str, object]]]:
    # Отключаем шум старого tester внутри worker.
    sigtest.VERBOSE_PROGRESS = False
    sigtest.WRITE_SIGNALS_CSV = False
    sigtest.PRINT_EACH_CALCULATION_START = False

    started = time.monotonic()
    start_ts = parse_utc_text(item.interval_start_utc)
    end_ts = parse_utc_text(item.interval_end_utc)

    signal_cache = sigtest.load_job_data_cache(
        instrument_code=item.instrument_code,
        price_source=item.price_source,
        intervals_utc=[(item.interval_start_utc, item.interval_end_utc)],
        settings_list=[spec.settings for spec in item.specs],
    )
    exec_cache = load_execution_bar_cache(
        instrument_code=item.instrument_code,
        intervals_utc=[(item.interval_start_utc, item.interval_end_utc)],
    )

    base_settings = item.specs[0].settings
    signal_window_mode = base_settings.signal_window_mode
    filter_modes = tuple(spec.settings.market_regime_filter_mode for spec in item.specs)

    closed_bar_timestamps = signal_cache.get_closed_bar_timestamps(start_ts=start_ts, end_ts=end_ts)
    plan = sigtest.build_run_plan(
        closed_bar_timestamps=closed_bar_timestamps,
        start_ts=start_ts,
        end_ts=end_ts,
        signal_window_mode=signal_window_mode,
        settings=base_settings,
    )
    due_set = set(plan.due_signal_bar_timestamps)

    states: dict[MarketRegimeFilterMode, RobotState] = {
        spec.settings.market_regime_filter_mode: RobotState(
            task_id=item.task_id,
            interval_start_utc=item.interval_start_utc,
            interval_end_utc=item.interval_end_utc,
            spec=spec,
            execution_params=item.execution_params,
            instrument_code=item.instrument_code,
            signal_window_mode=signal_window_mode,
            market_regime_filter_mode=spec.settings.market_regime_filter_mode,
            position=None,
            pending_limit=None,
            trades=[],
            events=[],
            counters=RobotCounters(slot_entry_window_skips=plan.slot_entry_window_skips)  # type: ignore[call-arg]
            if False else RobotCounters(),
        )
        for spec in item.specs
    }

    # slot_entry_window_skips общий для fast group.
    for state in states.values():
        state.counters.closed_bars_seen = len(closed_bar_timestamps)

    for current_ts in closed_bar_timestamps:
        bar = exec_cache.bar_for_closed_ts(current_ts)
        if bar is None:
            for state in states.values():
                state.counters.no_price += 1
            continue

        for state in states.values():
            maybe_fill_or_expire_pending_limit(state=state, bar=bar)
            maybe_exit_by_tp_sl(state=state, bar=bar)

            # Если TP/SL уже закрыл позицию, остальные close-правила не нужны.
            if state.position is not None:
                maybe_close_by_slot(state=state, bar=bar)
            if state.position is not None:
                maybe_close_by_futures_daily_flat(state=state, bar=bar)
            if state.position is not None:
                maybe_close_by_extreme_ma_zone(state=state, bar=bar)

        if current_ts not in due_set:
            continue

        signals_by_mode = sigtest.calculate_pattern_signals_all_filters(
            cache=signal_cache,
            signal_bar_ts=current_ts,
            settings=base_settings,
            filter_modes=filter_modes,
        )

        for mode, state in states.items():
            state.counters.due_signals += 1
            state.counters.calculations += 1
            signal = signals_by_mode[mode]
            action = process_signal(state=state, signal=signal, bar=bar)
            if WRITE_EVENTS_CSV:
                state.events.append({
                    "task_id": item.task_id,
                    "config_id": state.spec.config_id,
                    "execution_id": item.execution_params.execution_id,
                    "instrument_code": item.instrument_code,
                    "event": "SIGNAL_ACTION",
                    "event_ts": int(current_ts),
                    "event_utc": format_utc_ts(current_ts),
                    "signal_status": signal.status,
                    "signal_direction": signal.direction,
                    "action": action,
                    "position_after": state.position.direction if state.position else None,
                    "pending_limit_after": state.pending_limit.direction if state.pending_limit else None,
                })

    # Закрытие хвостов в конце интервала.
    last_bar = exec_cache.last_bar_at_or_before_closed_ts(end_ts)
    for state in states.values():
        if state.pending_limit is not None:
            cancel_pending_limit(state=state, reason="interval_end", closed_ts=end_ts)

        if state.position is not None and last_bar is not None:
            exit_price = market_exit_price(
                raw_price=last_bar.close,
                direction=state.position.direction,
                slippage_points=state.execution_params.slippage_points_per_side,
            )
            close_position(state=state, exit_ts=last_bar.closed_ts, exit_price=exit_price, exit_reason="interval_end")

    summary_rows = [build_summary_row(state, start_ts=start_ts, end_ts=end_ts) for state in states.values()]
    trade_rows = [asdict(trade) for state in states.values() for trade in state.trades]
    event_rows = [event for state in states.values() for event in state.events]

    elapsed = time.monotonic() - started
    compact = "; ".join(
        f"cfg={row['config_id']} exec={row['execution_id']} tr={row['trades']} net={float(row['net_profit_points']):+.2f}"
        for row in summary_rows
    )
    progress_print(f"TASK_DONE id={item.task_id} elapsed={elapsed:.1f}s {compact}")

    return summary_rows, trade_rows, event_rows


# ============================================================
# MAIN
# ============================================================

def build_work_items(
        *,
        specs: list[sigtest.TestRunSpec],
        execution_params_list: list[ExecutionParams],
) -> list[WorkItem]:
    items: list[WorkItem] = []
    specs_by_price_source = sigtest.group_specs_by_price_source(specs)

    for price_source, price_specs in specs_by_price_source.items():
        fast_groups = sigtest.group_specs_for_fast_runs(price_specs)
        for interval_start_utc, interval_end_utc in TEST_INTERVALS_UTC:
            for group in fast_groups:
                for execution_params in execution_params_list:
                    items.append(
                        WorkItem(
                            task_id=len(items) + 1,
                            instrument_code=TEST_INSTRUMENT_CODE,
                            interval_start_utc=interval_start_utc,
                            interval_end_utc=interval_end_utc,
                            price_source=price_source,
                            specs=tuple(group),
                            execution_params=execution_params,
                        )
                    )

    if MAX_TASKS is not None:
        return items[:int(MAX_TASKS)]

    return items


def main() -> None:
    try:
        sys.stdout.reconfigure(line_buffering=True)
    except Exception:
        pass

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    specs = sigtest.build_test_specs(
        tester_config=SIGNAL_TESTER_CONFIG,
        base_settings=BASE_SIGNAL_SETTINGS,
    )
    execution_params_list = build_execution_params_grid(EXECUTION_TESTER_CONFIG)
    work_items = build_work_items(specs=specs, execution_params_list=execution_params_list)

    config_rows: list[dict[str, object]] = []
    for spec in specs:
        row = sigtest.spec_to_config_row(spec)
        row["config_kind"] = "signal"
        config_rows.append(row)
    for params in execution_params_list:
        row = asdict(params)
        row["config_kind"] = "execution"
        config_rows.append(row)

    all_summary_rows: list[dict[str, object]] = []
    all_trade_rows: list[dict[str, object]] = []
    all_event_rows: list[dict[str, object]] = []

    write_outputs(
        summary_rows=all_summary_rows,
        trade_rows=all_trade_rows,
        event_rows=all_event_rows,
        config_rows=config_rows,
    )

    worker_count = MAX_WORKERS
    if worker_count is None:
        worker_count = max(1, (os.cpu_count() or 2) - 1)
    worker_count = max(1, int(worker_count))

    print(
        "robot_capabilities_multiprocess_tester "
        f"instrument={TEST_INSTRUMENT_CODE} "
        f"tasks={len(work_items)} "
        f"workers={worker_count} "
        f"signal_configs={len(specs)} "
        f"execution_configs={len(execution_params_list)} "
        f"output={OUTPUT_DIR}",
        flush=True,
    )

    started = time.monotonic()

    if worker_count <= 1 or len(work_items) <= 1:
        for item in work_items:
            summary_rows, trade_rows, event_rows = run_work_item(item)
            all_summary_rows.extend(summary_rows)
            all_trade_rows.extend(trade_rows)
            all_event_rows.extend(event_rows)

            if WRITE_PARTIAL_OUTPUTS:
                write_outputs(
                    summary_rows=all_summary_rows,
                    trade_rows=all_trade_rows,
                    event_rows=all_event_rows,
                    config_rows=config_rows,
                )
    else:
        with ProcessPoolExecutor(max_workers=worker_count) as executor:
            future_to_item = {
                executor.submit(run_work_item, item): item
                for item in work_items
            }

            completed = 0
            for future in as_completed(future_to_item):
                item = future_to_item[future]
                completed += 1
                try:
                    summary_rows, trade_rows, event_rows = future.result()
                except Exception as exc:
                    all_event_rows.append({
                        "task_id": item.task_id,
                        "instrument_code": item.instrument_code,
                        "event": "TASK_FAILED",
                        "error": f"{type(exc).__name__}: {exc}",
                    })
                    print(f"TASK_FAILED id={item.task_id}: {type(exc).__name__}: {exc}", flush=True)
                    continue

                all_summary_rows.extend(summary_rows)
                all_trade_rows.extend(trade_rows)
                all_event_rows.extend(event_rows)

                elapsed = time.monotonic() - started
                print(
                    f"PROGRESS {completed}/{len(work_items)} "
                    f"elapsed={elapsed:.1f}s "
                    f"summary_rows={len(all_summary_rows)} trades={len(all_trade_rows)}",
                    flush=True,
                )

                if WRITE_PARTIAL_OUTPUTS:
                    write_outputs(
                        summary_rows=all_summary_rows,
                        trade_rows=all_trade_rows,
                        event_rows=all_event_rows,
                        config_rows=config_rows,
                    )

    write_outputs(
        summary_rows=all_summary_rows,
        trade_rows=all_trade_rows,
        event_rows=all_event_rows,
        config_rows=config_rows,
    )

    print_summary(all_summary_rows)
    print()
    print(f"configs: {OUTPUT_DIR / 'configs.csv'}", flush=True)
    print(f"summary: {OUTPUT_DIR / 'summary.csv'}", flush=True)
    print(f"trades:  {OUTPUT_DIR / 'trades.csv'}", flush=True)
    if WRITE_EVENTS_CSV:
        print(f"events:  {OUTPUT_DIR / 'events.csv'}", flush=True)


if __name__ == "__main__":
    main()
