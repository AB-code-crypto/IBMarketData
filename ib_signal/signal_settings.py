from dataclasses import dataclass
from enum import Enum


class SignalWindowMode(Enum):
    ROLLING = "ROLLING"
    GRID = "GRID"


@dataclass(frozen=True)
class SignalSettings:
    # Режим построения сигнальных окон.
    signal_window_mode: SignalWindowMode

    # Максимально допустимое отставание последнего job-бара от текущего времени.
    max_job_bar_lag_seconds: int

    # ROLLING-режим.
    rolling_signal_step_seconds: int
    rolling_back_minutes: int
    rolling_trade_minutes: int

    # GRID-режим.
    slot_signal_step_seconds: int
    slot_step_minutes: int
    slot_start_minute_of_day: int
    slot_back_minutes: int
    slot_entry_minutes: int
    slot_close_before_end_seconds: int

    # Поиск кандидатов по Pearson.
    price_source: str
    pearson_min: float
    min_candidates: int
    max_candidates: int
    candidate_search_step_seconds: int
    history_lookback_days: int | None


DEFAULT_SIGNAL_SETTINGS = SignalSettings(
    # ============================================================
    # РЕЖИМ ПОСТРОЕНИЯ СИГНАЛЬНЫХ ОКОН
    # ============================================================
    signal_window_mode=SignalWindowMode.ROLLING,

    # Если job DB отстала сильнее этого значения, signal-сервис считает её неготовой.
    max_job_bar_lag_seconds=10,

    # ============================================================
    # ROLLING-РЕЖИМ
    # ============================================================
    # ROLLING работает от последнего доступного закрытого job-бара:
    #   signal_bar = последняя расчётная точка ROLLING, покрытая job DB
    #   back_start = signal_bar - rolling_back_minutes
    #   back_end   = signal_bar
    #   trade_from = signal_bar
    #   trade_to   = signal_bar + rolling_trade_minutes
    rolling_signal_step_seconds=60,
    rolling_back_minutes=30,
    rolling_trade_minutes=30,

    # ============================================================
    # GRID-РЕЖИМ
    # ============================================================
    # GRID строит сигналы по сетке слотов:
    #   1. первые slot_back_minutes только копим анализируемый участок;
    #   2. после slot_back_minutes signal-сервис может пересчитывать сигнал;
    #   3. slot_entry_minutes описывает окно первичного входа для будущего execution;
    #   4. сопровождение позиции решает отдельный контур контроля позиции;
    #   5. за slot_close_before_end_seconds секунд до конца слота расчёты прекращаются.
    slot_signal_step_seconds=5,
    slot_step_minutes=60,
    slot_start_minute_of_day=0,
    slot_back_minutes=30,
    slot_entry_minutes=20,
    slot_close_before_end_seconds=10,

    # ============================================================
    # ПОИСК КАНДИДАТОВ ПО PEARSON
    # ============================================================
    price_source="mid_close",
    pearson_min=0.7,
    min_candidates=10,
    max_candidates=100,
    candidate_search_step_seconds=60,
    history_lookback_days=120,  # None = вся доступная история
)
