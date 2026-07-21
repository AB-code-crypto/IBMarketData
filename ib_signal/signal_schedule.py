from ib_signal.signal_config import SignalConfig


def get_rolling_due_bar_ts(*, current_bar_ts: int, step_seconds: int) -> int:
    if int(step_seconds) <= 0:
        raise ValueError(f"step_seconds должен быть > 0, получено: {step_seconds}")
    current = int(current_bar_ts)
    step = int(step_seconds)
    return current - current % step


def get_due_signal_bar_ts(
        *,
        current_bar_ts: int,
        settings: SignalConfig,
        last_calculated_bar_ts: int | None,
) -> int | None:
    due_bar_ts = get_rolling_due_bar_ts(
        current_bar_ts=current_bar_ts,
        step_seconds=settings.rolling_signal_step_seconds,
    )
    if due_bar_ts == last_calculated_bar_ts:
        return None
    return due_bar_ts
