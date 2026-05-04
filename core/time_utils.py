from datetime import datetime, timezone
from zoneinfo import ZoneInfo

CT_TIMEZONE = ZoneInfo("America/Chicago")
MSK_TIMEZONE = ZoneInfo("Europe/Moscow")

SQLITE_DATETIME_FORMAT = "%Y-%m-%d %H:%M:%S"


def format_utc(dt, for_ib=False):
    """Форматирует datetime как UTC-текст для IB-запросов, SQLite-строк и логов."""
    dt = dt.astimezone(timezone.utc)

    if for_ib:
        return dt.strftime("%Y%m%d %H:%M:%S UTC")

    return dt.strftime(SQLITE_DATETIME_FORMAT)


def format_utc_ts(ts):
    """Форматирует Unix timestamp как UTC-текст для логов."""
    return format_utc(datetime.fromtimestamp(ts, tz=timezone.utc))


def parse_utc_iso_to_ts(utc_text):
    """Преобразует ISO-текст вида 2024-03-13T22:00:00Z в Unix timestamp."""
    dt = datetime.strptime(utc_text, "%Y-%m-%dT%H:%M:%SZ")
    dt = dt.replace(tzinfo=timezone.utc)
    return int(dt.timestamp())


def build_bar_time_fields_from_utc_dt(dt_utc):
    """
    Собирает все поля времени, которые используются в ценовой БД.
    bar_time_ts — канонический UTC Unix timestamp бара.
    bar_time, bar_time_ct и bar_time_msk — человекочитаемые datetime-проекции.
    """
    dt_utc = dt_utc.astimezone(timezone.utc)
    dt_ct = dt_utc.astimezone(CT_TIMEZONE)
    dt_msk = dt_utc.astimezone(MSK_TIMEZONE)

    return {
        "bar_time_ts": int(dt_utc.timestamp()),
        "bar_time": dt_utc.strftime(SQLITE_DATETIME_FORMAT),
        "bar_time_ct": dt_ct.strftime(SQLITE_DATETIME_FORMAT),
        "bar_time_msk": dt_msk.strftime(SQLITE_DATETIME_FORMAT),
    }
