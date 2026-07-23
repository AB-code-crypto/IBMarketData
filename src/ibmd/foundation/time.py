from __future__ import annotations

from datetime import datetime, timezone

UTC = timezone.utc


class TimestampError(ValueError):
    pass


def utc_now() -> datetime:
    return datetime.now(tz=UTC)


def ensure_utc(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise TimestampError("timestamp must be timezone-aware")
    return value.astimezone(UTC)


def format_utc(value: datetime) -> str:
    normalized = ensure_utc(value)
    return normalized.isoformat(timespec="microseconds").replace("+00:00", "Z")


def utc_now_text() -> str:
    return format_utc(utc_now())


def parse_utc(value: str) -> datetime:
    text = str(value or "").strip()
    if not text:
        raise TimestampError("timestamp text is required")
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError as exc:
        raise TimestampError(f"invalid ISO-8601 timestamp: {value!r}") from exc
    return ensure_utc(parsed)
