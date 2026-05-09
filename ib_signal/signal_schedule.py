from ib_signal.signal_settings import SignalSettings

SECONDS_PER_MINUTE = 60
SECONDS_PER_DAY = 24 * 60 * 60


def is_calculation_delay_passed(
    *,
    signal_bar_ts: int,
    now_ts: int,
    delay_seconds: int,
) -> bool:
    # Не считаем сигнал ровно в момент времени бара.
    # Даём job-data сервису небольшую паузу, чтобы последний бар точно попал в job DB.
    return now_ts >= signal_bar_ts + delay_seconds


def get_rolling_due_bar_ts(
    *,
    current_bar_ts: int,
    step_seconds: int,
) -> int:
    # Возвращает последнюю сеточную точку ROLLING, которая уже покрыта job DB.
    #
    # Пример:
    #   current_bar_ts = 12:00:05
    #   step_seconds   = 60
    #   due_bar_ts     = 12:00:00
    #
    # Это защищает от пропуска минутного сигнала, если job-data или сам цикл
    # увидели latest bar не ровно в 12:00:00, а чуть позже.
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
    #
    # slot_start_minute_of_day=0    -> 00:00:00 текущего Unix-дня
    # slot_start_minute_of_day=570  -> 09:30:00 текущего Unix-дня
    #
    # Если текущий timestamp раньше сегодняшнего якоря, значит он относится
    # к сетке, начатой от вчерашнего якоря.
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
    # SECONDS_PER_DAY здесь нужен именно для ежедневной переякорки сетки.
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
    #
    # В первые slot_back_minutes минут слота сигнал не считаем.
    # В следующие slot_entry_minutes минут считаем по сетке slot_signal_step_seconds.
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
    now_ts: int,
    settings: SignalSettings,
    last_calculated_bar_ts: int | None,
) -> int | None:
    # Возвращает bar_time_ts, для которого пора считать сигнал.
    #
    # Если сейчас считать ничего не надо, возвращает None.
    # Важно: возвращаем именно due bar, а не просто True/False.
    # Это нужно, чтобы при latest_job_bar_time_ts=12:00:05 ROLLING-сигнал
    # считался для 12:00:00, а не для 12:00:05.
    if settings.signal_window_mode == "ROLLING":
        due_bar_ts = get_rolling_due_bar_ts(
            current_bar_ts=current_bar_ts,
            step_seconds=settings.rolling_signal_step_seconds,
        )

    elif settings.signal_window_mode == "GRID":
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

    if not is_calculation_delay_passed(
        signal_bar_ts=due_bar_ts,
        now_ts=now_ts,
        delay_seconds=settings.signal_calculation_delay_seconds,
    ):
        return None

    return due_bar_ts


def should_calculate_signal(
    *,
    current_bar_ts: int,
    now_ts: int,
    settings: SignalSettings,
    last_calculated_bar_ts: int | None,
) -> bool:
    # Совместимый boolean-helper.
    # Основной runtime-код лучше строить через get_due_signal_bar_ts(),
    # потому что ему нужен конкретный bar_time_ts расчёта.
    return get_due_signal_bar_ts(
        current_bar_ts=current_bar_ts,
        now_ts=now_ts,
        settings=settings,
        last_calculated_bar_ts=last_calculated_bar_ts,
    ) is not None
