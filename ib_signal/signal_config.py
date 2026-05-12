from dataclasses import dataclass
from enum import Enum


class SignalWindowMode(Enum):
    ROLLING = "ROLLING"
    GRID = "GRID"


@dataclass(frozen=True)
class SignalConfig:
    # Режим построения сигнальных окон.
    signal_window_mode: SignalWindowMode = SignalWindowMode.ROLLING

    # Максимально допустимый возраст последнего job-бара относительно текущего времени.
    # Важно: bar_time_ts — это время начала 5-секундного бара, а не время его закрытия.
    # Поэтому значение должно включать размер бара + запас на приход BID/ASK,
    # запись price DB, обновление job DB и polling signal-сервиса.
    # Другими словами минимальный max_job_bar_lag_seconds = 5 секунд это размер бара + пару секунд на обработку сигнала

    max_job_bar_lag_seconds: int = 15

    # ROLLING-режим.
    rolling_signal_step_seconds: int = 60
    rolling_back_minutes: int = 30
    rolling_trade_minutes: int = 30

    # GRID-режим.
    slot_signal_step_seconds: int = 5
    slot_step_minutes: int = 60
    slot_start_minute_of_day: int = 0
    slot_back_minutes: int = 30
    slot_entry_minutes: int = 20
    slot_close_before_end_seconds: int = 10

    # Поиск кандидатов по Pearson.
    price_source: str = "mid_close"
    pearson_min: float = 0.7
    min_candidates: int = 10
    max_candidates: int = 100
    candidate_search_step_seconds: int = 60
    history_lookback_days: int | None = 120  # None = вся доступная история


DEFAULT_SIGNAL_CONFIG = SignalConfig()
