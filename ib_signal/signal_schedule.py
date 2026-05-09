SECONDS_PER_MINUTE = 60
SECONDS_PER_DAY = 24 * 60 * 60


def is_calculation_delay_passed(
    *,
    current_bar_ts: int,
    now_ts: int,
    delay_seconds: int,
) -> bool:
    # Не считаем сигнал ровно в момент времени бара.
    # Даём job-data сервису небольшую паузу, чтобы последний бар точно попал в job DB.
    return now_ts >= current_bar_ts + delay_seconds


def get_grid_slot_start_ts(
    *,
    current_bar_ts: int,
    slot_step_minutes: int,
    slot_start_minute_of_day: int,
) -> int:
    # Сетка слотов привязана к Unix-дню и bar_time_ts.
    # При SLOT_START_MINUTE_OF_DAY=0 якорь находится в 00:00:00.
    slot_step_seconds = slot_step_minutes * SECONDS_PER_MINUTE
    anchor_offset_seconds = slot_start_minute_of_day * SECONDS_PER_MINUTE

    seconds_from_anchor = (current_bar_ts - anchor_offset_seconds) % slot_step_seconds

    return current_bar_ts - seconds_from_anchor


def should_calculate_rolling_signal(
    *,
    current_bar_ts: int,
    now_ts: int,
    step_seconds: int,
    delay_seconds: int,
    last_calculated_bar_ts: int | None,
) -> bool:
    if current_bar_ts == last_calculated_bar_ts:
        return False

    if current_bar_ts % step_seconds != 0:
        return False

    return is_calculation_delay_passed(
        current_bar_ts=current_bar_ts,
        now_ts=now_ts,
        delay_seconds=delay_seconds,
    )


def should_calculate_grid_signal(
    *,
    current_bar_ts: int,
    now_ts: int,
    slot_signal_step_seconds: int,
    slot_step_minutes: int,
    slot_start_minute_of_day: int,
    slot_back_minutes: int,
    slot_entry_minutes: int,
    delay_seconds: int,
    last_calculated_bar_ts: int | None,
) -> bool:
    if current_bar_ts == last_calculated_bar_ts:
        return False

    slot_start_ts = get_grid_slot_start_ts(
        current_bar_ts=current_bar_ts,
        slot_step_minutes=slot_step_minutes,
        slot_start_minute_of_day=slot_start_minute_of_day,
    )

    seconds_from_slot_start = current_bar_ts - slot_start_ts

    entry_start_seconds = slot_back_minutes * SECONDS_PER_MINUTE
    entry_end_seconds = (slot_back_minutes + slot_entry_minutes) * SECONDS_PER_MINUTE

    if seconds_from_slot_start < entry_start_seconds:
        return False

    if seconds_from_slot_start >= entry_end_seconds:
        return False

    if seconds_from_slot_start % slot_signal_step_seconds != 0:
        return False

    return is_calculation_delay_passed(
        current_bar_ts=current_bar_ts,
        now_ts=now_ts,
        delay_seconds=delay_seconds,
    )


def should_calculate_signal(
    *,
    current_bar_ts: int,
    now_ts: int,
    settings,
    last_calculated_bar_ts: int | None,
) -> bool:
    if settings.signal_window_mode == "ROLLING":
        return should_calculate_rolling_signal(
            current_bar_ts=current_bar_ts,
            now_ts=now_ts,
            step_seconds=settings.rolling_signal_step_seconds,
            delay_seconds=settings.signal_calculation_delay_seconds,
            last_calculated_bar_ts=last_calculated_bar_ts,
        )

    if settings.signal_window_mode == "GRID":
        return should_calculate_grid_signal(
            current_bar_ts=current_bar_ts,
            now_ts=now_ts,
            slot_signal_step_seconds=settings.slot_signal_step_seconds,
            slot_step_minutes=settings.slot_step_minutes,
            slot_start_minute_of_day=settings.slot_start_minute_of_day,
            slot_back_minutes=settings.slot_back_minutes,
            slot_entry_minutes=settings.slot_entry_minutes,
            delay_seconds=settings.signal_calculation_delay_seconds,
            last_calculated_bar_ts=last_calculated_bar_ts,
        )

    raise ValueError(f"Неизвестный режим signal_window_mode: {settings.signal_window_mode!r}")