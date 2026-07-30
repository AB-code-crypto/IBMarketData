import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv

BASE_DIR = Path(__file__).resolve().parents[1]
load_dotenv(BASE_DIR / ".env", encoding="utf-8-sig")


def _read_required_env(name: str) -> str:
    value = os.getenv(name)
    if value is None or not value.strip():
        raise RuntimeError(
            f"Обязательная переменная окружения не задана: {name}"
        )
    return value.strip()


def _read_required_int_env(name: str) -> int:
    value = _read_required_env(name)
    try:
        return int(value)
    except ValueError as exc:
        raise ValueError(
            f"{name} должен быть целым числом; получено: {value!r}"
        ) from exc


def _read_required_float_env(name: str) -> float:
    value = _read_required_env(name)
    try:
        return float(value)
    except ValueError as exc:
        raise ValueError(
            f"{name} должен быть числом; получено: {value!r}"
        ) from exc


@dataclass(frozen=True)
class SignalConfig:
    # A complete BID/ASK close bar older than this is not safe for live signals.
    max_price_bar_lag_seconds: int = 60

    # Absolute budget from signal-bar close to broker execution pickup.
    decision_pipeline_max_age_seconds: int = 30

    # The only supported signal window is rolling.
    rolling_signal_step_seconds: int = _read_required_int_env(
        "ROLLING_SIGNAL_STEP_SECONDS"
    )
    rolling_back_minutes: int = _read_required_int_env(
        "ROLLING_BACK_MINUTES"
    )
    rolling_trade_minutes: int = _read_required_int_env(
        "ROLLING_TRADE_MINUTES"
    )

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

    candidate_potential_min_count: int = _read_required_int_env(
        "CANDIDATE_POTENTIAL_MIN_COUNT"
    )
    candidate_potential_max_count: int = _read_required_int_env(
        "CANDIDATE_POTENTIAL_MAX_COUNT"
    )
    candidate_potential_min_abs_end_delta_points: float = (
        _read_required_float_env(
            "CANDIDATE_POTENTIAL_MIN_ABS_END_DELTA_POINTS"
        )
    )

    signal_event_retention_days: int = 7

    def __post_init__(self) -> None:
        if self.rolling_signal_step_seconds <= 0:
            raise ValueError(
                "ROLLING_SIGNAL_STEP_SECONDS должен быть > 0"
            )
        if self.rolling_back_minutes <= 0:
            raise ValueError("ROLLING_BACK_MINUTES должен быть > 0")
        if self.rolling_trade_minutes <= 0:
            raise ValueError("ROLLING_TRADE_MINUTES должен быть > 0")
        if self.candidate_potential_min_count <= 0:
            raise ValueError(
                "CANDIDATE_POTENTIAL_MIN_COUNT должен быть > 0"
            )
        if (
            self.candidate_potential_max_count
            < self.candidate_potential_min_count
        ):
            raise ValueError(
                "CANDIDATE_POTENTIAL_MAX_COUNT должен быть >= "
                "CANDIDATE_POTENTIAL_MIN_COUNT"
            )
        if self.candidate_potential_min_abs_end_delta_points < 0.0:
            raise ValueError(
                "CANDIDATE_POTENTIAL_MIN_ABS_END_DELTA_POINTS "
                "должен быть >= 0"
            )


DEFAULT_SIGNAL_CONFIG = SignalConfig()
