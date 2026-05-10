from dataclasses import dataclass

from ib_signal import signal_config


@dataclass(frozen=True)
class SignalSettings:
    # Режим построения сигнальных окон.
    signal_window_mode = signal_config.SIGNAL_WINDOW_MODE

    # Контроль свежести job DB.
    last_bar_safety_seconds: int

    # Задержка после времени бара перед расчётом сигнала.
    signal_calculation_delay_seconds: int

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
    history_lookback_days: int

    @classmethod
    def from_config(cls) -> "SignalSettings":
        """Создаёт настройки из боевого signal_config.py."""
        return cls(
            signal_window_mode=signal_config.SIGNAL_WINDOW_MODE,
            last_bar_safety_seconds=signal_config.LAST_BAR_SAFETY_SECONDS,
            signal_calculation_delay_seconds=signal_config.SIGNAL_CALCULATION_DELAY_SECONDS,

            rolling_signal_step_seconds=signal_config.ROLLING_SIGNAL_STEP_SECONDS,
            rolling_back_minutes=signal_config.ROLLING_BACK_MINUTES,
            rolling_trade_minutes=signal_config.ROLLING_TRADE_MINUTES,

            slot_signal_step_seconds=signal_config.SLOT_SIGNAL_STEP_SECONDS,
            slot_step_minutes=signal_config.SLOT_STEP_MINUTES,
            slot_start_minute_of_day=signal_config.SLOT_START_MINUTE_OF_DAY,
            slot_back_minutes=signal_config.SLOT_BACK_MINUTES,
            slot_entry_minutes=signal_config.SLOT_ENTRY_MINUTES,
            slot_close_before_end_seconds=signal_config.SLOT_CLOSE_BEFORE_END_SECONDS,

            price_source=signal_config.PRICE_SOURCE,
            pearson_min=signal_config.PEARSON_MIN,
            min_candidates=signal_config.MIN_CANDIDATES,
            max_candidates=signal_config.MAX_CANDIDATES,
            candidate_search_step_seconds=signal_config.CANDIDATE_SEARCH_STEP_SECONDS,
            history_lookback_days=signal_config.HISTORY_LOOKBACK_DAYS,
        )
