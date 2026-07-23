from __future__ import annotations

import re
from dataclasses import dataclass
from enum import Enum
from typing import Any, ClassVar, Mapping
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from .common import (
    CatalogError,
    WEEKDAYS,
    boolean,
    exact_keys,
    identifier,
    parse_date,
    required_text,
    seconds_of_day,
    validate_content_hash,
    validate_hash,
)


@dataclass(frozen=True)
class LocalIntervalV1:
    start_local: str
    end_local: str

    KEYS: ClassVar[set[str]] = {"start_local", "end_local"}

    def __post_init__(self) -> None:
        if self.start_seconds >= self.end_seconds:
            raise CatalogError(
                "local interval must have positive same-day duration: "
                f"{self.start_local}-{self.end_local}"
            )

    @property
    def start_seconds(self) -> int:
        return seconds_of_day(self.start_local, field_name="start_local")

    @property
    def end_seconds(self) -> int:
        return seconds_of_day(self.end_local, field_name="end_local")

    def contains_seconds(self, value: int) -> bool:
        return self.start_seconds <= int(value) < self.end_seconds

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> "LocalIntervalV1":
        exact_keys(value, cls.KEYS, context="local interval")
        return cls(
            start_local=str(value["start_local"]),
            end_local=str(value["end_local"]),
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "start_local": self.start_local,
            "end_local": self.end_local,
        }


@dataclass(frozen=True)
class WeeklySessionDayV1:
    weekday: str
    trading_intervals: tuple[LocalIntervalV1, ...]
    maintenance_intervals: tuple[LocalIntervalV1, ...]

    KEYS: ClassVar[set[str]] = {
        "weekday",
        "trading_intervals",
        "maintenance_intervals",
    }

    def __post_init__(self) -> None:
        weekday = str(self.weekday or "").strip().upper()
        if weekday not in WEEKDAYS:
            raise CatalogError(f"unsupported weekday: {self.weekday!r}")
        object.__setattr__(self, "weekday", weekday)
        self.validate_non_overlapping(
            self.trading_intervals,
            context=f"{weekday} trading",
        )
        self.validate_non_overlapping(
            self.maintenance_intervals,
            context=f"{weekday} maintenance",
        )
        for trading in self.trading_intervals:
            for maintenance in self.maintenance_intervals:
                if max(
                    trading.start_seconds,
                    maintenance.start_seconds,
                ) < min(trading.end_seconds, maintenance.end_seconds):
                    raise CatalogError(
                        f"trading and maintenance intervals overlap on {weekday}"
                    )

    @staticmethod
    def validate_non_overlapping(
        intervals: tuple[LocalIntervalV1, ...],
        *,
        context: str,
    ) -> None:
        ordered = tuple(sorted(intervals, key=lambda item: item.start_seconds))
        if ordered != intervals:
            raise CatalogError(f"{context} intervals must be ordered")
        for previous, current in zip(intervals, intervals[1:]):
            if current.start_seconds < previous.end_seconds:
                raise CatalogError(f"{context} intervals overlap")

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> "WeeklySessionDayV1":
        exact_keys(value, cls.KEYS, context="weekly session day")
        trading = value["trading_intervals"]
        maintenance = value["maintenance_intervals"]
        if not isinstance(trading, list) or not isinstance(maintenance, list):
            raise CatalogError("session intervals must be lists")
        return cls(
            weekday=str(value["weekday"]),
            trading_intervals=tuple(
                LocalIntervalV1.from_dict(item) for item in trading
            ),
            maintenance_intervals=tuple(
                LocalIntervalV1.from_dict(item) for item in maintenance
            ),
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "weekday": self.weekday,
            "trading_intervals": [
                item.to_dict() for item in self.trading_intervals
            ],
            "maintenance_intervals": [
                item.to_dict() for item in self.maintenance_intervals
            ],
        }


class SessionExceptionStatus(str, Enum):
    CLOSED = "CLOSED"
    CUSTOM = "CUSTOM"


@dataclass(frozen=True)
class SessionExceptionV1:
    local_date: str
    status: SessionExceptionStatus
    trading_intervals: tuple[LocalIntervalV1, ...]
    maintenance_intervals: tuple[LocalIntervalV1, ...]
    reason: str

    KEYS: ClassVar[set[str]] = {
        "local_date",
        "status",
        "trading_intervals",
        "maintenance_intervals",
        "reason",
    }

    def __post_init__(self) -> None:
        parse_date(self.local_date, field_name="local_date")
        if not isinstance(self.status, SessionExceptionStatus):
            raise CatalogError(
                f"invalid session exception status: {self.status!r}"
            )
        if self.status == SessionExceptionStatus.CLOSED and (
            self.trading_intervals or self.maintenance_intervals
        ):
            raise CatalogError(
                "CLOSED session exception cannot define intervals"
            )
        WeeklySessionDayV1.validate_non_overlapping(
            self.trading_intervals,
            context=f"{self.local_date} trading",
        )
        WeeklySessionDayV1.validate_non_overlapping(
            self.maintenance_intervals,
            context=f"{self.local_date} maintenance",
        )
        object.__setattr__(
            self,
            "reason",
            required_text(self.reason, field_name="reason"),
        )

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> "SessionExceptionV1":
        exact_keys(value, cls.KEYS, context="session exception")
        trading = value["trading_intervals"]
        maintenance = value["maintenance_intervals"]
        if not isinstance(trading, list) or not isinstance(maintenance, list):
            raise CatalogError("session exception intervals must be lists")
        try:
            status = SessionExceptionStatus(str(value["status"]))
        except ValueError as exc:
            raise CatalogError(
                f"invalid session exception status: {value['status']!r}"
            ) from exc
        return cls(
            local_date=str(value["local_date"]),
            status=status,
            trading_intervals=tuple(
                LocalIntervalV1.from_dict(item) for item in trading
            ),
            maintenance_intervals=tuple(
                LocalIntervalV1.from_dict(item) for item in maintenance
            ),
            reason=str(value["reason"]),
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "local_date": self.local_date,
            "status": self.status.value,
            "trading_intervals": [
                item.to_dict() for item in self.trading_intervals
            ],
            "maintenance_intervals": [
                item.to_dict() for item in self.maintenance_intervals
            ],
            "reason": self.reason,
        }


@dataclass(frozen=True)
class SessionDefinitionV1:
    session_id: str
    timezone: str
    weekly_days: tuple[WeeklySessionDayV1, ...]
    exceptions: tuple[SessionExceptionV1, ...]
    production_qualified: bool
    exception_coverage_end_date: str | None
    qualification_note: str

    KEYS: ClassVar[set[str]] = {
        "session_id",
        "timezone",
        "weekly_days",
        "exceptions",
        "production_qualified",
        "exception_coverage_end_date",
        "qualification_note",
    }

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "session_id",
            identifier(self.session_id, field_name="session_id"),
        )
        timezone_name = required_text(self.timezone, field_name="timezone")
        try:
            ZoneInfo(timezone_name)
        except ZoneInfoNotFoundError as exc:
            raise CatalogError(f"unknown timezone: {timezone_name!r}") from exc
        object.__setattr__(self, "timezone", timezone_name)

        weekdays = [item.weekday for item in self.weekly_days]
        if tuple(weekdays) != WEEKDAYS:
            raise CatalogError(
                "weekly_days must define Monday..Sunday exactly once in order: "
                f"{weekdays}"
            )
        dates = [item.local_date for item in self.exceptions]
        if len(dates) != len(set(dates)):
            raise CatalogError("duplicate session exception dates")
        if tuple(sorted(dates)) != tuple(dates):
            raise CatalogError("session exceptions must be ordered by local_date")
        if self.exception_coverage_end_date is not None:
            parse_date(
                self.exception_coverage_end_date,
                field_name="exception_coverage_end_date",
            )
        object.__setattr__(
            self,
            "production_qualified",
            boolean(
                self.production_qualified,
                field_name="production_qualified",
            ),
        )
        if self.production_qualified and self.exception_coverage_end_date is None:
            raise CatalogError(
                "production-qualified session requires exception_coverage_end_date"
            )
        object.__setattr__(
            self,
            "qualification_note",
            required_text(
                self.qualification_note,
                field_name="qualification_note",
            ),
        )

    @property
    def zone(self) -> ZoneInfo:
        return ZoneInfo(self.timezone)

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> "SessionDefinitionV1":
        exact_keys(value, cls.KEYS, context="session definition")
        weekly = value["weekly_days"]
        exceptions = value["exceptions"]
        if not isinstance(weekly, list) or not isinstance(exceptions, list):
            raise CatalogError("weekly_days and exceptions must be lists")
        coverage = value["exception_coverage_end_date"]
        return cls(
            session_id=str(value["session_id"]),
            timezone=str(value["timezone"]),
            weekly_days=tuple(
                WeeklySessionDayV1.from_dict(item) for item in weekly
            ),
            exceptions=tuple(
                SessionExceptionV1.from_dict(item) for item in exceptions
            ),
            production_qualified=value["production_qualified"],
            exception_coverage_end_date=(
                None if coverage is None else str(coverage)
            ),
            qualification_note=str(value["qualification_note"]),
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "session_id": self.session_id,
            "timezone": self.timezone,
            "weekly_days": [item.to_dict() for item in self.weekly_days],
            "exceptions": [item.to_dict() for item in self.exceptions],
            "production_qualified": self.production_qualified,
            "exception_coverage_end_date": self.exception_coverage_end_date,
            "qualification_note": self.qualification_note,
        }


@dataclass(frozen=True)
class SessionCalendarV1:
    calendar_version: str
    source_runtime_commit: str
    sessions: tuple[SessionDefinitionV1, ...]
    content_hash: str

    SCHEMA_NAME: ClassVar[str] = "SessionCalendar"
    SCHEMA_VERSION: ClassVar[int] = 1
    KEYS: ClassVar[set[str]] = {
        "schema_name",
        "schema_version",
        "calendar_version",
        "source_runtime_commit",
        "sessions",
        "content_hash",
    }

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "calendar_version",
            identifier(self.calendar_version, field_name="calendar_version"),
        )
        source = str(self.source_runtime_commit or "").strip()
        if not re.fullmatch(r"[0-9a-f]{40}", source):
            raise CatalogError("source_runtime_commit must be a 40-char git SHA")
        object.__setattr__(self, "source_runtime_commit", source)
        if not self.sessions:
            raise CatalogError("session calendar cannot be empty")
        ids = [item.session_id for item in self.sessions]
        if len(ids) != len(set(ids)):
            raise CatalogError(f"duplicate session_id values: {ids}")
        object.__setattr__(self, "content_hash", validate_hash(self.content_hash))

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> "SessionCalendarV1":
        exact_keys(value, cls.KEYS, context="session calendar")
        if (
            value["schema_name"] != cls.SCHEMA_NAME
            or value["schema_version"] != cls.SCHEMA_VERSION
        ):
            raise CatalogError("unsupported session-calendar schema")
        validate_content_hash(value)
        raw = value["sessions"]
        if not isinstance(raw, list):
            raise CatalogError("sessions must be a list")
        return cls(
            calendar_version=str(value["calendar_version"]),
            source_runtime_commit=str(value["source_runtime_commit"]),
            sessions=tuple(
                SessionDefinitionV1.from_dict(item) for item in raw
            ),
            content_hash=str(value["content_hash"]),
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_name": self.SCHEMA_NAME,
            "schema_version": self.SCHEMA_VERSION,
            "calendar_version": self.calendar_version,
            "source_runtime_commit": self.source_runtime_commit,
            "sessions": [item.to_dict() for item in self.sessions],
            "content_hash": self.content_hash,
        }

    def require(self, session_id: str) -> SessionDefinitionV1:
        expected = str(session_id)
        for item in self.sessions:
            if item.session_id == expected:
                return item
        raise CatalogError(f"session is not registered: {session_id}")
