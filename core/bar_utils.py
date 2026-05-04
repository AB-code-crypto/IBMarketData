from datetime import timezone

# Размер куска для 5-секундной истории фьючерсов.
# Для IB historical data по фьючерсам работаем часовыми кусками,
# чтобы не упираться в pacing limits и не получать слишком тяжёлые ответы.
FUTURES_5_SECS_CHUNK_SECONDS = 3600


def build_duration_str(start_dt, end_dt):
    # IB historical request работает не со связкой start+end,
    # а с парой endDateTime + durationStr.
    total_seconds = int((end_dt - start_dt).total_seconds())

    if total_seconds <= 0:
        raise ValueError("Конец интервала должен быть строго больше начала")

    return f"{total_seconds} S"


def get_bar_size_seconds(bar_size_setting):
    # Возвращаем размер бара в секундах по строке IB barSizeSetting.
    if bar_size_setting == "5 secs":
        return 5

    raise ValueError(f"Неподдерживаемый barSizeSetting: {bar_size_setting}")


def get_chunk_seconds(sec_type, bar_size_setting):
    # Возвращаем размер одного historical chunk для заданного типа инструмента.
    if sec_type == "FUT" and bar_size_setting == "5 secs":
        return FUTURES_5_SECS_CHUNK_SECONDS

    raise ValueError(
        f"Не задан размер куска для secType={sec_type}, barSizeSetting={bar_size_setting}"
    )


def align_timestamp_down(ts, step_seconds):
    # Выравниваем timestamp вниз до ближайшей границы бара.
    #
    # Примеры:
    # - ts=10:03:27 и step=5 -> 10:03:25
    # - ts=10:03:27 и step=3600 -> 10:00:00
    return ts - (ts % step_seconds)


def iter_chunks(start_ts, end_ts, chunk_seconds):
    # Разбиваем полуоткрытый интервал [start_ts, end_ts)
    # на последовательность кусков фиксированного размера.
    current_start_ts = start_ts

    while current_start_ts < end_ts:
        current_end_ts = min(current_start_ts + chunk_seconds, end_ts)
        yield current_start_ts, current_end_ts
        current_start_ts = current_end_ts


def get_current_aligned_ts(server_dt, bar_size_seconds):
    # Получаем текущее серверное время IB и сразу выравниваем вниз до границы бара.
    #
    # Это нужно, чтобы не пытаться докачивать ещё не закрытый текущий бар.
    raw_ts = int(server_dt.astimezone(timezone.utc).timestamp())
    return align_timestamp_down(raw_ts, bar_size_seconds)
