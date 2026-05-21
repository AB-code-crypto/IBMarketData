from ib_signal.signal_config import SignalConfig, SignalWindowMode


def format_signal_config(settings: SignalConfig) -> str:
    """Что делает: собирает текст активных signal-настроек.
    Зачем нужна: при старте сервиса Telegram получает реальные параметры текущего signal-пайплайна."""
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
            f"history_lookback_days={history_lookback_days_text}",
            "",
            "REGIME:",
            f"market_regime_filter_mode={settings.market_regime_filter_mode.value}",
            "",
            "MINMAX HARD FILTER:",
            f"candidate_minmax_hard_filter_max_ratio={settings.candidate_minmax_hard_filter_max_ratio}",
            "",
            "SCORING:",
            f"candidate_score_pearson_weight={settings.candidate_score_pearson_weight}",
            f"candidate_score_end_delta_weight={settings.candidate_score_end_delta_weight}",
            f"candidate_score_minmax_weight={settings.candidate_score_minmax_weight}",
            "",
            "POTENTIAL:",
            f"candidate_potential_min_count={settings.candidate_potential_min_count}",
            f"candidate_potential_max_count={settings.candidate_potential_max_count}",
            f"candidate_potential_min_abs_end_delta_points={settings.candidate_potential_min_abs_end_delta_points}",
            "",
            "SIGNAL EVENTS:",
            f"signal_event_retention_days={settings.signal_event_retention_days}",
        ]
    )

    return "\n".join(lines)
