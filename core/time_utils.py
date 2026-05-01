from datetime import datetime, timezone
from zoneinfo import ZoneInfo

CT_TIMEZONE = ZoneInfo("America/Chicago")


def format_utc(dt, for_ib=False):
    """Format datetime as UTC text for IB requests, SQLite rows and logs."""
    dt = dt.astimezone(timezone.utc)

    if for_ib:
        return dt.strftime("%Y%m%d %H:%M:%S UTC")

    return dt.strftime("%Y-%m-%d %H:%M:%S")


def format_utc_ts(ts):
    """Format Unix timestamp as UTC text for logs."""
    return format_utc(datetime.fromtimestamp(ts, tz=timezone.utc))


def parse_utc_iso_to_ts(utc_text):
    """Convert ISO text like 2024-03-13T22:00:00Z to Unix timestamp."""
    dt = datetime.strptime(utc_text, "%Y-%m-%dT%H:%M:%SZ")
    dt = dt.replace(tzinfo=timezone.utc)
    return int(dt.timestamp())


def build_ct_time_fields_from_utc_dt(dt_utc):
    """Build Chicago-time fields used by the price database."""
    dt_utc = dt_utc.astimezone(timezone.utc)
    dt_ct = dt_utc.astimezone(CT_TIMEZONE)

    utc_ts = int(dt_utc.timestamp())
    ct_offset = dt_ct.utcoffset()

    if ct_offset is None:
        raise ValueError(f"Cannot determine UTC offset for CT timezone. dt_utc={dt_utc}")

    bar_time_ts_ct = utc_ts + int(ct_offset.total_seconds())
    bar_time_ct = dt_ct.strftime("%Y-%m-%d %H:%M:%S")

    return bar_time_ts_ct, bar_time_ct
