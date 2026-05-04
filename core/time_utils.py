from datetime import datetime, timezone
from zoneinfo import ZoneInfo

CT_TIMEZONE = ZoneInfo("America/Chicago")
MSK_TIMEZONE = ZoneInfo("Europe/Moscow")

SQLITE_DATETIME_FORMAT = "%Y-%m-%d %H:%M:%S"


def format_utc(dt, for_ib=False):
    """Format datetime as UTC text for IB requests, SQLite rows and logs."""
    dt = dt.astimezone(timezone.utc)

    if for_ib:
        return dt.strftime("%Y%m%d %H:%M:%S UTC")

    return dt.strftime(SQLITE_DATETIME_FORMAT)


def format_utc_ts(ts):
    """Format Unix timestamp as UTC text for logs."""
    return format_utc(datetime.fromtimestamp(ts, tz=timezone.utc))


def parse_utc_iso_to_ts(utc_text):
    """Convert ISO text like 2024-03-13T22:00:00Z to Unix timestamp."""
    dt = datetime.strptime(utc_text, "%Y-%m-%dT%H:%M:%SZ")
    dt = dt.replace(tzinfo=timezone.utc)
    return int(dt.timestamp())


def build_bar_time_fields_from_utc_dt(dt_utc):
    """
    Build all time fields used by the price database.

    bar_time_ts is the canonical UTC Unix timestamp.
    bar_time, bar_time_ct and bar_time_msk are human-readable datetime projections.
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
