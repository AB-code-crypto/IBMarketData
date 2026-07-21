from ib_signal.signal_config import SignalConfig


def format_signal_config(settings: SignalConfig) -> str:
    history_text = (
        "all"
        if settings.history_lookback_days is None
        else str(settings.history_lookback_days)
    )
    return "\n".join(
        [
            "Активные настройки signal-сервиса:",
            "mode=ROLLING_ONLY",
            "price_source=read-time mid_close from canonical BID/ASK DB",
            f"max_price_bar_lag_seconds={settings.max_price_bar_lag_seconds}",
            f"decision_pipeline_max_age_seconds={settings.decision_pipeline_max_age_seconds}",
            "",
            "ROLLING:",
            f"rolling_signal_step_seconds={settings.rolling_signal_step_seconds}",
            f"rolling_back_minutes={settings.rolling_back_minutes}",
            f"rolling_trade_minutes={settings.rolling_trade_minutes}",
            "",
            "PEARSON / RANKING:",
            f"pearson_min={settings.pearson_min}",
            f"history_lookback_days={history_text}",
            f"candidate_minmax_hard_filter_max_ratio={settings.candidate_minmax_hard_filter_max_ratio}",
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
