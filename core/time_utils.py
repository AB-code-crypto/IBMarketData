from datetime import datetime, timezone
from zoneinfo import ZoneInfo

CT_TIMEZONE = ZoneInfo("America/Chicago")
MSK_TIMEZONE = ZoneInfo("Europe/Moscow")

SQLITE_DATETIME_FORMAT = "%Y-%m-%d %H:%M:%S"


def format_utc(dt, for_ib=False):
    """Что делает: форматирует datetime в UTC-строку для IB-запросов, SQLite и логов. Зачем нужна: все сервисы используют единый формат времени."""
    dt = dt.astimezone(timezone.utc)

    if for_ib:
        return dt.strftime("%Y%m%d %H:%M:%S UTC")

    return dt.strftime(SQLITE_DATETIME_FORMAT)


def format_utc_ts(ts):
    """Что делает: форматирует Unix timestamp как UTC-строку. Зачем нужна: логи и отчёты показывают timestamp в человекочитаемом виде."""
    return format_utc(datetime.fromtimestamp(ts, tz=timezone.utc))


def parse_utc_iso_to_ts(utc_text):
    """Что делает: переводит ISO UTC-текст из contracts.py в Unix timestamp. Зачем нужна: active windows контрактов сравниваются в timestamp-логике."""
    dt = datetime.strptime(utc_text, "%Y-%m-%dT%H:%M:%SZ")
    dt = dt.replace(tzinfo=timezone.utc)
    return int(dt.timestamp())


def build_bar_time_fields_from_utc_dt(dt_utc):
    """Что делает: собирает timestamp и три текстовых представления времени бара. Зачем нужна: price DB хранит канонический UTC timestamp и удобные human-readable поля."""
    dt_utc = dt_utc.astimezone(timezone.utc)
    dt_ct = dt_utc.astimezone(CT_TIMEZONE)
    dt_msk = dt_utc.astimezone(MSK_TIMEZONE)

    return {
        "bar_time_ts": int(dt_utc.timestamp()),
        "bar_time": dt_utc.strftime(SQLITE_DATETIME_FORMAT),
        "bar_time_ct": dt_ct.strftime(SQLITE_DATETIME_FORMAT),
        "bar_time_msk": dt_msk.strftime(SQLITE_DATETIME_FORMAT),
    }
