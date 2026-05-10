from datetime import timezone

# Размер куска для 5-секундной истории.
# Для IB historical data работаем часовыми кусками,
# чтобы не упираться в pacing limits и не получать слишком тяжёлые ответы.
DEFAULT_5_SECS_CHUNK_SECONDS = 3600

# Безопасный lookback по умолчанию для новых single-contract инструментов,
# если в contracts.py не задан history_lookback_days.
DEFAULT_HISTORY_LOOKBACK_DAYS = 14


def build_duration_str(start_dt, end_dt):
    """Что делает: считает durationStr для IB historical request по границам datetime. Зачем нужна: IB принимает historical-запрос как endDateTime + durationStr, а не как start/end."""
    total_seconds = int((end_dt - start_dt).total_seconds())

    if total_seconds <= 0:
        raise ValueError("Конец интервала должен быть строго больше начала")

    return f"{total_seconds} S"


def get_bar_size_seconds(bar_size_setting):
    """Что делает: переводит поддерживаемый barSizeSetting в размер бара в секундах. Зачем нужна: вся логика выравнивания и покрытия истории работает в Unix timestamp."""
    if bar_size_setting == "5 secs":
        return 5

    raise ValueError(f"Неподдерживаемый barSizeSetting: {bar_size_setting}")


def get_chunk_seconds(sec_type, bar_size_setting):
    """Что делает: выбирает размер одного historical chunk для типа инструмента и размера бара. Зачем нужна: ограничивает запросы к IB безопасными кусками и защищает от неподдерживаемых комбинаций."""
    if sec_type in ("FUT", "CASH", "CRYPTO") and bar_size_setting == "5 secs":
        return DEFAULT_5_SECS_CHUNK_SECONDS

    raise ValueError(
        f"Не задан размер куска для secType={sec_type}, barSizeSetting={bar_size_setting}"
    )


def align_timestamp_down(ts, step_seconds):
    """Что делает: выравнивает timestamp вниз до ближайшей границы шага. Зачем нужна: не даёт запрашивать или считать ещё не закрытый бар."""
    return ts - (ts % step_seconds)


def iter_chunks(start_ts, end_ts, chunk_seconds):
    """Что делает: разбивает полуоткрытый интервал на куски фиксированного размера. Зачем нужна: historical loader качает большие интервалы порциями, а не одним тяжёлым запросом."""
    current_start_ts = start_ts

    while current_start_ts < end_ts:
        current_end_ts = min(current_start_ts + chunk_seconds, end_ts)
        yield current_start_ts, current_end_ts
        current_start_ts = current_end_ts


def get_current_aligned_ts(server_dt, bar_size_seconds):
    """Что делает: берёт server time IB и выравнивает его вниз по размеру бара. Зачем нужна: задаёт правую границу истории только по закрытым барам."""
    raw_ts = int(server_dt.astimezone(timezone.utc).timestamp())
    return align_timestamp_down(raw_ts, bar_size_seconds)


def get_history_lookback_start_ts(current_aligned_ts, lookback_days):
    """Что делает: считает левую границу истории по количеству дней lookback. Зачем нужна: ограничивает начальную загрузку новых single-contract инструментов."""
    return current_aligned_ts - int(lookback_days) * 86400
