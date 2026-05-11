from ib_signal.signal_modes import SignalWindowMode
from ib_signal.signal_settings import SignalSettings


def format_signal_settings(settings: SignalSettings) -> str:
    """Что делает: собирает текст активных signal-настроек.
    Зачем нужна: при старте сервиса Telegram получает только реально используемый режим и параметры Pearson."""
    history_lookback_days_text = (
        "all"
        if settings.history_lookback_days is None
        else str(settings.history_lookback_days)
    )

    lines = [
        "Активные настройки signal-сервиса:",
        f"mode={settings.signal_window_mode.value}",
        f"max_job_bar_lag_seconds={settings.max_job_bar_lag_seconds}",
        "",
    ]

    if settings.signal_window_mode == SignalWindowMode.ROLLING:
        lines.extend(
            [
                "ROLLING:",
                f"rolling_signal_step_seconds={settings.rolling_signal_step_seconds}",
                f"rolling_back_minutes={settings.rolling_back_minutes}",
                f"rolling_trade_minutes={settings.rolling_trade_minutes}",
                "",
            ]
        )

    elif settings.signal_window_mode == SignalWindowMode.GRID:
        lines.extend(
            [
                "GRID:",
                f"slot_signal_step_seconds={settings.slot_signal_step_seconds}",
                f"slot_step_minutes={settings.slot_step_minutes}",
                f"slot_start_minute_of_day={settings.slot_start_minute_of_day}",
                f"slot_back_minutes={settings.slot_back_minutes}",
                f"slot_entry_minutes={settings.slot_entry_minutes}",
                f"slot_close_before_end_seconds={settings.slot_close_before_end_seconds}",
                "",
            ]
        )

    else:
        raise ValueError(
            f"Неизвестный режим signal_window_mode: {settings.signal_window_mode!r}"
        )

    lines.extend(
        [
            "PEARSON:",
            f"price_source={settings.price_source}",
            f"pearson_min={settings.pearson_min}",
            f"min_candidates={settings.min_candidates}",
            f"max_candidates={settings.max_candidates}",
            f"candidate_search_step_seconds={settings.candidate_search_step_seconds}",
            f"history_lookback_days={history_lookback_days_text}",
        ]
    )

    return "\n".join(lines)
