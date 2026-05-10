from ib_signal.signal_modes import SignalWindowMode
from ib_signal.signal_settings import SignalSettings

SECONDS_PER_MINUTE = 60
SECONDS_PER_DAY = 24 * 60 * 60


def get_rolling_due_bar_ts(
    *,
    current_bar_ts: int,
    step_seconds: int,
) -> int:
    # Возвращает последнюю сеточную точку ROLLING, которая уже покрыта job DB.
    if step_seconds <= 0:
        raise ValueError(f"step_seconds должен быть > 0, получено: {step_seconds}")

    return current_bar_ts - (current_bar_ts % step_seconds)


def get_unix_day_start_ts(timestamp: int) -> int:
    # Возвращает начало Unix-дня для timestamp.
    #
    # Важно: это UTC/Unix-день, не MSK и не биржевая сессия.
    # Сейчас bar_time_ts хранится как Unix timestamp, поэтому сетку считаем от него.
    return timestamp - (timestamp % SECONDS_PER_DAY)


def get_grid_day_anchor_ts(
    *,
    current_bar_ts: int,
    slot_start_minute_of_day: int,
) -> int:
    # Возвращает дневной якорь GRID.
    if slot_start_minute_of_day < 0 or slot_start_minute_of_day >= 24 * 60:
        raise ValueError(
            "slot_start_minute_of_day должен быть в диапазоне [0, 1440), "
            f"получено: {slot_start_minute_of_day}"
        )

    day_start_ts = get_unix_day_start_ts(current_bar_ts)
    anchor_ts = day_start_ts + slot_start_minute_of_day * SECONDS_PER_MINUTE

    if current_bar_ts < anchor_ts:
        anchor_ts -= SECONDS_PER_DAY

    return anchor_ts


def get_grid_slot_start_ts(
    *,
    current_bar_ts: int,
    slot_step_minutes: int,
    slot_start_minute_of_day: int,
) -> int:
    # Возвращает старт текущего GRID-слота.
    #
    # Сетка строится от дневного якоря, а не от момента запуска сервиса.
    if slot_step_minutes <= 0:
        raise ValueError(
            f"slot_step_minutes должен быть > 0, получено: {slot_step_minutes}"
        )

    slot_step_seconds = slot_step_minutes * SECONDS_PER_MINUTE
    anchor_ts = get_grid_day_anchor_ts(
        current_bar_ts=current_bar_ts,
        slot_start_minute_of_day=slot_start_minute_of_day,
    )

    seconds_from_anchor = current_bar_ts - anchor_ts
    slot_index = seconds_from_anchor // slot_step_seconds

    return anchor_ts + slot_index * slot_step_seconds


def get_grid_due_bar_ts(
    *,
    current_bar_ts: int,
    slot_signal_step_seconds: int,
    slot_step_minutes: int,
    slot_start_minute_of_day: int,
    slot_back_minutes: int,
    slot_entry_minutes: int,
) -> int | None:
    # Возвращает последнюю допустимую точку расчёта GRID-сигнала,
    # которая уже покрыта job DB.
    if slot_signal_step_seconds <= 0:
        raise ValueError(
            "slot_signal_step_seconds должен быть > 0, "
            f"получено: {slot_signal_step_seconds}"
        )

    if slot_back_minutes < 0:
        raise ValueError(
            f"slot_back_minutes должен быть >= 0, получено: {slot_back_minutes}"
        )

    if slot_entry_minutes <= 0:
        raise ValueError(
            f"slot_entry_minutes должен быть > 0, получено: {slot_entry_minutes}"
        )

    slot_start_ts = get_grid_slot_start_ts(
        current_bar_ts=current_bar_ts,
        slot_step_minutes=slot_step_minutes,
        slot_start_minute_of_day=slot_start_minute_of_day,
    )

    entry_start_ts = slot_start_ts + slot_back_minutes * SECONDS_PER_MINUTE
    entry_end_ts = entry_start_ts + slot_entry_minutes * SECONDS_PER_MINUTE

    if current_bar_ts < entry_start_ts:
        return None

    if current_bar_ts >= entry_end_ts:
        return None

    seconds_from_entry_start = current_bar_ts - entry_start_ts
    due_shift = seconds_from_entry_start - (
        seconds_from_entry_start % slot_signal_step_seconds
    )
    due_bar_ts = entry_start_ts + due_shift

    if due_bar_ts >= entry_end_ts:
        return None

    return due_bar_ts


def get_due_signal_bar_ts(
    *,
    current_bar_ts: int,
    settings: SignalSettings,
    last_calculated_bar_ts: int | None,
) -> int | None:
    # Возвращает bar_time_ts, для которого пора считать сигнал.
    #
    # Сигнал строится только по барам, которые уже есть в job DB.
    # Поэтому отдельная задержка после времени бара здесь не нужна.
    if settings.signal_window_mode == SignalWindowMode.ROLLING:
        due_bar_ts = get_rolling_due_bar_ts(
            current_bar_ts=current_bar_ts,
            step_seconds=settings.rolling_signal_step_seconds,
        )

    elif settings.signal_window_mode == SignalWindowMode.GRID:
        due_bar_ts = get_grid_due_bar_ts(
            current_bar_ts=current_bar_ts,
            slot_signal_step_seconds=settings.slot_signal_step_seconds,
            slot_step_minutes=settings.slot_step_minutes,
            slot_start_minute_of_day=settings.slot_start_minute_of_day,
            slot_back_minutes=settings.slot_back_minutes,
            slot_entry_minutes=settings.slot_entry_minutes,
        )

        if due_bar_ts is None:
            return None

    else:
        raise ValueError(
            f"Неизвестный режим signal_window_mode: {settings.signal_window_mode!r}"
        )

    if due_bar_ts == last_calculated_bar_ts:
        return None

    return due_bar_ts
