from dataclasses import dataclass
from datetime import datetime, timezone

from core.time_utils import build_bar_time_fields_from_utc_dt
from ib_signal.signal_config import SignalConfig

SECONDS_PER_MINUTE = 60


@dataclass(frozen=True)
class SignalWindow:
    signal_bar_ts: int
    pattern_start_ts: int
    pattern_end_ts: int
    pattern_seconds: int
    trade_start_ts: int
    trade_end_ts: int
    trade_seconds: int


def validate_signal_window(window: SignalWindow) -> None:
    if window.pattern_seconds <= 0:
        raise ValueError(
            f"Длина pattern window должна быть > 0, получено: {window.pattern_seconds}"
        )
    if window.trade_seconds <= 0:
        raise ValueError(
            f"Длина trade window должна быть > 0, получено: {window.trade_seconds}"
        )
    if window.pattern_end_ts <= window.pattern_start_ts:
        raise ValueError(
            "pattern_end_ts должен быть больше pattern_start_ts: "
            f"{window.pattern_start_ts} -> {window.pattern_end_ts}"
        )
    if window.trade_end_ts <= window.trade_start_ts:
        raise ValueError(
            "trade_end_ts должен быть больше trade_start_ts: "
            f"{window.trade_start_ts} -> {window.trade_end_ts}"
        )


def build_rolling_signal_window(
        *,
        signal_bar_ts: int,
        settings: SignalConfig,
) -> SignalWindow:
    pattern_seconds = int(settings.rolling_back_minutes) * SECONDS_PER_MINUTE
    trade_seconds = int(settings.rolling_trade_minutes) * SECONDS_PER_MINUTE
    signal_ts = int(signal_bar_ts)
    window = SignalWindow(
        signal_bar_ts=signal_ts,
        pattern_start_ts=signal_ts - pattern_seconds,
        pattern_end_ts=signal_ts,
        pattern_seconds=pattern_seconds,
        trade_start_ts=signal_ts,
        trade_end_ts=signal_ts + trade_seconds,
        trade_seconds=trade_seconds,
    )
    validate_signal_window(window)
    return window


def build_current_signal_window(
        *,
        signal_bar_ts: int,
        settings: SignalConfig,
) -> SignalWindow:
    return build_rolling_signal_window(
        signal_bar_ts=signal_bar_ts,
        settings=settings,
    )


def _format_minutes(seconds: int) -> str:
    minutes = seconds / SECONDS_PER_MINUTE
    return str(int(minutes)) if minutes.is_integer() else f"{minutes:.2f}"


def format_ts_ct_for_log(ts: int, get_time_text) -> str:
    time_text = get_time_text(int(ts))
    if time_text is not None:
        return f"{time_text} CT"
    dt_utc = datetime.fromtimestamp(int(ts), tz=timezone.utc)
    return f"{build_bar_time_fields_from_utc_dt(dt_utc)['bar_time_ct']} CT"


def format_signal_window_for_log(window: SignalWindow, get_time_text) -> str:
    return ", ".join(
        [
            f"signal_bar={format_ts_ct_for_log(window.signal_bar_ts, get_time_text)}",
            "pattern="
            f"{format_ts_ct_for_log(window.pattern_start_ts, get_time_text)} -> "
            f"{format_ts_ct_for_log(window.pattern_end_ts, get_time_text)}",
            f"pattern_minutes={_format_minutes(window.pattern_seconds)}",
            "trade="
            f"{format_ts_ct_for_log(window.trade_start_ts, get_time_text)} -> "
            f"{format_ts_ct_for_log(window.trade_end_ts, get_time_text)}",
            f"trade_minutes={_format_minutes(window.trade_seconds)}",
        ]
    )
