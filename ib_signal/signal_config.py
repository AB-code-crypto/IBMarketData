from dataclasses import dataclass


PLOT_TOP_CANDIDATES = 5


@dataclass(frozen=True)
class SignalConfig:
    # A complete BID/ASK close bar older than this is not safe for live signals.
    max_price_bar_lag_seconds: int = 60

    # Absolute budget from signal-bar close to broker execution pickup.
    decision_pipeline_max_age_seconds: int = 30

    # The only supported signal window is rolling.
    rolling_signal_step_seconds: int = 60
    rolling_back_minutes: int = 90
    rolling_trade_minutes: int = 30

    # Ordinary protective TP/SL safety remains independent from signal features.
    protective_order_accept_timeout_seconds: float = 5.0
    protective_order_price_watchdog_enabled: bool = True
    protective_order_price_watchdog_stale_close_enabled: bool = True
    protective_order_price_stale_max_seconds: int = 600

    pearson_min: float = 0.7
    history_lookback_days: int | None = 365

    candidate_minmax_hard_filter_max_ratio: float = 1.5
    candidate_score_pearson_weight: float = 1.0
    candidate_score_end_delta_weight: float = 1.0
    candidate_score_minmax_weight: float = 1.0

    candidate_potential_min_count: int = 7
    candidate_potential_max_count: int = 9
    candidate_potential_min_abs_end_delta_points: float = 30.0

    signal_event_retention_days: int = 7


DEFAULT_SIGNAL_CONFIG = SignalConfig()
