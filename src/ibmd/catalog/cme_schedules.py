from __future__ import annotations

import hashlib
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
from enum import Enum
from pathlib import Path
from typing import Any, Iterable, Mapping
from zoneinfo import ZoneInfo

from ibmd.foundation.atomic_json import canonical_json_text, read_json_object

from .common import CatalogError, compute_content_hash, parse_date
from .sessions import (
    LocalIntervalV1,
    SessionCalendarV1,
    SessionDefinitionV1,
    SessionExceptionStatus,
    SessionExceptionV1,
)


class CmeScheduleError(CatalogError):
    pass


class CmeMarketPhase(str, Enum):
    CLOSED = "CLOSED"
    TRADING = "TRADING"
    MAINTENANCE = "MAINTENANCE"


@dataclass(frozen=True)
class CmeMarketEventV1:
    occurred_at_utc: datetime
    event_type: str
    phase: CmeMarketPhase
    source_sequence: int

    def __post_init__(self) -> None:
        value = self.occurred_at_utc
        if value.tzinfo is None or value.utcoffset() is None:
            raise CmeScheduleError("CME market event must be timezone-aware")
        object.__setattr__(self, "occurred_at_utc", value.astimezone(timezone.utc))
        event_type = str(self.event_type or "").strip().lower()
        if not event_type:
            raise CmeScheduleError("CME market event type is required")
        object.__setattr__(self, "event_type", event_type)
        if not isinstance(self.phase, CmeMarketPhase):
            raise CmeScheduleError(f"invalid CME market phase: {self.phase!r}")
        sequence = int(self.source_sequence)
        if sequence < 0:
            raise CmeScheduleError("CME source_sequence must be non-negative")
        object.__setattr__(self, "source_sequence", sequence)


@dataclass(frozen=True)
class CmeTradingScheduleV1:
    trading_schedule_id: str
    applicable_globex_group_codes: tuple[str, ...]
    schedule_names: tuple[str, ...]
    events: tuple[CmeMarketEventV1, ...]

    def __post_init__(self) -> None:
        schedule_id = str(self.trading_schedule_id or "").strip()
        if not schedule_id:
            raise CmeScheduleError("trading_schedule_id is required")
        object.__setattr__(self, "trading_schedule_id", schedule_id)
        groups = tuple(dict.fromkeys(_text_values(self.applicable_globex_group_codes)))
        names = tuple(dict.fromkeys(_text_values(self.schedule_names)))
        if not groups:
            raise CmeScheduleError(
                f"CME schedule {schedule_id} has no applicable Globex group codes"
            )
        if not names:
            raise CmeScheduleError(f"CME schedule {schedule_id} has no names")
        object.__setattr__(self, "applicable_globex_group_codes", groups)
        object.__setattr__(self, "schedule_names", names)
        events = tuple(
            sorted(
                self.events,
                key=lambda item: (item.occurred_at_utc, item.source_sequence),
            )
        )
        if not events:
            raise CmeScheduleError(f"CME schedule {schedule_id} has no events")
        object.__setattr__(self, "events", events)


@dataclass(frozen=True)
class CmeSessionCalendarBuildV1:
    calendar: SessionCalendarV1
    source_sha256: str
    trading_schedule_id: str
    globex_group_code: str
    coverage_start_date: str
    coverage_end_date: str
    generated_exception_count: int

    def to_dict(self) -> dict[str, Any]:
        return {
            "calendar": self.calendar.to_dict(),
            "source_sha256": self.source_sha256,
            "trading_schedule_id": self.trading_schedule_id,
            "globex_group_code": self.globex_group_code,
            "coverage_start_date": self.coverage_start_date,
            "coverage_end_date": self.coverage_end_date,
            "generated_exception_count": self.generated_exception_count,
            "production_qualified": True,
        }


def _text_values(value: object) -> tuple[str, ...]:
    if value is None:
        return ()
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return ()
        return tuple(
            item.strip()
            for item in text.replace(";", ",").split(",")
            if item.strip()
        )
    if isinstance(value, Mapping):
        candidates = []
        for key in (
            "code",
            "name",
            "value",
            "groupCode",
            "globexGroupCode",
            "applicableGlobexGroupCode",
            "scheduleName",
        ):
            if key in value:
                candidates.extend(_text_values(value[key]))
        return tuple(candidates)
    if isinstance(value, Iterable):
        values: list[str] = []
        for item in value:
            values.extend(_text_values(item))
        return tuple(values)
    text = str(value).strip()
    return (text,) if text else ()


def _embedded_schedules(payload: Mapping[str, Any]) -> list[Mapping[str, Any]]:
    direct = payload.get("tradingSchedules")
    if isinstance(direct, list):
        return [item for item in direct if isinstance(item, Mapping)]
    embedded = payload.get("_embedded")
    if isinstance(embedded, Mapping):
        for key in ("tradingSchedules", "tradingSchedule", "schedules"):
            values = embedded.get(key)
            if isinstance(values, list):
                return [item for item in values if isinstance(item, Mapping)]
    if all(key in payload for key in ("tradingScheduleId", "marketEventsByDate")):
        return [payload]
    raise CmeScheduleError(
        "CME export does not contain a tradingSchedules collection"
    )


def _parse_event_time(value: object) -> datetime:
    text = str(value or "").strip()
    formats = (
        "%d%m%Y-%H:%M:%S.%fZ",
        "%d%m%Y-%H:%M:%SZ",
        "%Y-%m-%dT%H:%M:%S.%fZ",
        "%Y-%m-%dT%H:%M:%SZ",
    )
    for pattern in formats:
        try:
            return datetime.strptime(text, pattern).replace(tzinfo=timezone.utc)
        except ValueError:
            continue
    raise CmeScheduleError(f"unsupported CME marketEventTime: {value!r}")


def _phase(event_type: object) -> tuple[str, CmeMarketPhase]:
    normalized = (
        str(event_type or "")
        .strip()
        .lower()
        .replace("_", "")
        .replace("-", "")
        .replace(" ", "")
    )
    if normalized == "open":
        return "open", CmeMarketPhase.TRADING
    if normalized in {
        "paused",
        "pause",
        "preopen",
        "preopenhalt",
        "pcp",
        "postclosepreopen",
    }:
        return normalized, CmeMarketPhase.MAINTENANCE
    if normalized in {"closed", "close"}:
        return normalized, CmeMarketPhase.CLOSED
    raise CmeScheduleError(f"unsupported CME marketEventType: {event_type!r}")


def parse_cme_trading_schedules(
    payload: Mapping[str, Any],
) -> tuple[CmeTradingScheduleV1, ...]:
    schedules = []
    global_sequence = 0
    for raw_schedule in _embedded_schedules(payload):
        schedule_id = str(raw_schedule.get("tradingScheduleId") or "").strip()
        groups = _text_values(raw_schedule.get("applicableGlobexGroupCodes"))
        names = _text_values(raw_schedule.get("scheduleNames"))
        by_date = raw_schedule.get("marketEventsByDate")
        if not isinstance(by_date, list):
            raise CmeScheduleError(
                f"CME schedule {schedule_id!r} marketEventsByDate must be a list"
            )
        events = []
        for raw_day in by_date:
            if not isinstance(raw_day, Mapping):
                raise CmeScheduleError("CME marketEventsByDate row must be an object")
            raw_events = raw_day.get("marketEvents")
            if not isinstance(raw_events, list):
                raise CmeScheduleError("CME marketEvents must be a list")
            for raw_event in raw_events:
                if not isinstance(raw_event, Mapping):
                    raise CmeScheduleError("CME market event must be an object")
                event_type, phase = _phase(raw_event.get("marketEventType"))
                events.append(
                    CmeMarketEventV1(
                        occurred_at_utc=_parse_event_time(
                            raw_event.get("marketEventTime")
                        ),
                        event_type=event_type,
                        phase=phase,
                        source_sequence=global_sequence,
                    )
                )
                global_sequence += 1
        schedules.append(
            CmeTradingScheduleV1(
                trading_schedule_id=schedule_id,
                applicable_globex_group_codes=groups,
                schedule_names=names,
                events=tuple(events),
            )
        )
    if not schedules:
        raise CmeScheduleError("CME export contains no trading schedules")
    return tuple(schedules)


def select_cme_trading_schedule(
    schedules: tuple[CmeTradingScheduleV1, ...],
    *,
    globex_group_code: str,
    trading_schedule_id: str | None = None,
) -> CmeTradingScheduleV1:
    group = str(globex_group_code or "").strip()
    if not group:
        raise CmeScheduleError("globex_group_code is required")
    schedule_id = str(trading_schedule_id or "").strip()
    matches = [
        item
        for item in schedules
        if group in item.applicable_globex_group_codes
        and (not schedule_id or item.trading_schedule_id == schedule_id)
    ]
    if len(matches) != 1:
        candidates = [
            {
                "trading_schedule_id": item.trading_schedule_id,
                "groups": item.applicable_globex_group_codes,
                "names": item.schedule_names,
            }
            for item in schedules
            if group in item.applicable_globex_group_codes
        ]
        raise CmeScheduleError(
            "CME schedule selection is not unique: "
            f"group={group!r}, schedule_id={schedule_id or None!r}, "
            f"matches={len(matches)}, candidates={candidates}"
        )
    return matches[0]


def _local_time_text(value: datetime, *, end_of_day: bool = False) -> str:
    if end_of_day:
        return "24:00:00"
    return value.time().replace(microsecond=0).isoformat()


def _append_interval(
    values: list[LocalIntervalV1],
    *,
    start: datetime,
    end: datetime,
    local_midnight: datetime,
    next_midnight: datetime,
) -> None:
    if end <= start:
        return
    start_text = _local_time_text(start)
    end_text = _local_time_text(
        end,
        end_of_day=end == next_midnight,
    )
    if start == local_midnight:
        start_text = "00:00:00"
    values.append(LocalIntervalV1(start_local=start_text, end_local=end_text))


def project_cme_schedule_day(
    schedule: CmeTradingScheduleV1,
    *,
    local_date: date,
    timezone_name: str,
) -> tuple[tuple[LocalIntervalV1, ...], tuple[LocalIntervalV1, ...]]:
    zone = ZoneInfo(timezone_name)
    local_midnight = datetime.combine(local_date, time.min, tzinfo=zone)
    next_midnight = datetime.combine(
        local_date + timedelta(days=1),
        time.min,
        tzinfo=zone,
    )
    start_utc = local_midnight.astimezone(timezone.utc)
    end_utc = next_midnight.astimezone(timezone.utc)
    before = [item for item in schedule.events if item.occurred_at_utc <= start_utc]
    if not before:
        raise CmeScheduleError(
            "CME schedule has no state at coverage-day start: "
            f"local_date={local_date.isoformat()}, timezone={timezone_name}"
        )
    state = before[-1].phase
    cursor = local_midnight
    trading: list[LocalIntervalV1] = []
    maintenance: list[LocalIntervalV1] = []
    events = [
        item
        for item in schedule.events
        if start_utc < item.occurred_at_utc < end_utc
    ]
    for event in events:
        local_event = event.occurred_at_utc.astimezone(zone)
        if event.phase == state:
            continue
        if state == CmeMarketPhase.TRADING:
            _append_interval(
                trading,
                start=cursor,
                end=local_event,
                local_midnight=local_midnight,
                next_midnight=next_midnight,
            )
        elif state == CmeMarketPhase.MAINTENANCE:
            _append_interval(
                maintenance,
                start=cursor,
                end=local_event,
                local_midnight=local_midnight,
                next_midnight=next_midnight,
            )
        cursor = local_event
        state = event.phase
    if state == CmeMarketPhase.TRADING:
        _append_interval(
            trading,
            start=cursor,
            end=next_midnight,
            local_midnight=local_midnight,
            next_midnight=next_midnight,
        )
    elif state == CmeMarketPhase.MAINTENANCE:
        _append_interval(
            maintenance,
            start=cursor,
            end=next_midnight,
            local_midnight=local_midnight,
            next_midnight=next_midnight,
        )
    return tuple(trading), tuple(maintenance)


def _source_hash(payload: Mapping[str, Any]) -> str:
    return hashlib.sha256(
        canonical_json_text(payload).encode("utf-8")
    ).hexdigest()


def build_qualified_cme_session_calendar(
    *,
    base_calendar: SessionCalendarV1,
    source_payload: Mapping[str, Any],
    session_id: str,
    globex_group_code: str,
    coverage_start_date: str,
    coverage_end_date: str,
    calendar_version: str,
    trading_schedule_id: str | None = None,
) -> CmeSessionCalendarBuildV1:
    coverage_start = parse_date(
        coverage_start_date,
        field_name="coverage_start_date",
    )
    coverage_end = parse_date(
        coverage_end_date,
        field_name="coverage_end_date",
    )
    if coverage_end < coverage_start:
        raise CmeScheduleError("coverage_end_date precedes coverage_start_date")
    base_session = base_calendar.require(session_id)
    schedules = parse_cme_trading_schedules(source_payload)
    schedule = select_cme_trading_schedule(
        schedules,
        globex_group_code=globex_group_code,
        trading_schedule_id=trading_schedule_id,
    )
    zone = base_session.zone
    start_utc = datetime.combine(coverage_start, time.min, tzinfo=zone).astimezone(
        timezone.utc
    )
    end_utc = datetime.combine(
        coverage_end + timedelta(days=1),
        time.min,
        tzinfo=zone,
    ).astimezone(timezone.utc)
    if not any(item.occurred_at_utc <= start_utc for item in schedule.events):
        raise CmeScheduleError(
            "CME source does not establish state before coverage start"
        )
    if not any(item.occurred_at_utc >= end_utc for item in schedule.events):
        raise CmeScheduleError(
            "CME source does not extend through coverage end"
        )

    exceptions: list[SessionExceptionV1] = []
    current = coverage_start
    while current <= coverage_end:
        trading, maintenance = project_cme_schedule_day(
            schedule,
            local_date=current,
            timezone_name=base_session.timezone,
        )
        weekly = base_session.weekly_days[current.weekday()]
        if (
            trading != weekly.trading_intervals
            or maintenance != weekly.maintenance_intervals
        ):
            status = (
                SessionExceptionStatus.CLOSED
                if not trading and not maintenance
                else SessionExceptionStatus.CUSTOM
            )
            exceptions.append(
                SessionExceptionV1(
                    local_date=current.isoformat(),
                    status=status,
                    trading_intervals=trading,
                    maintenance_intervals=maintenance,
                    reason=(
                        "CME_REFERENCE_DATA_API:"
                        f"schedule={schedule.trading_schedule_id}:"
                        f"group={globex_group_code}"
                    ),
                )
            )
        current += timedelta(days=1)

    source_sha256 = _source_hash(source_payload)
    qualified = SessionDefinitionV1(
        session_id=base_session.session_id,
        timezone=base_session.timezone,
        weekly_days=base_session.weekly_days,
        exceptions=tuple(exceptions),
        production_qualified=True,
        exception_coverage_start_date=coverage_start.isoformat(),
        exception_coverage_end_date=coverage_end.isoformat(),
        qualification_note=(
            "Official CME Reference Data API Trading Schedules export; "
            f"schedule_id={schedule.trading_schedule_id}; "
            f"globex_group_code={globex_group_code}; "
            f"source_sha256={source_sha256}; "
            f"coverage={coverage_start.isoformat()}..{coverage_end.isoformat()}"
        ),
    )
    sessions = tuple(
        qualified if item.session_id == session_id else item
        for item in base_calendar.sessions
    )
    raw = {
        "schema_name": SessionCalendarV1.SCHEMA_NAME,
        "schema_version": SessionCalendarV1.SCHEMA_VERSION,
        "calendar_version": str(calendar_version),
        "source_runtime_commit": base_calendar.source_runtime_commit,
        "sessions": [item.to_dict() for item in sessions],
        "content_hash": "",
    }
    raw["content_hash"] = compute_content_hash(raw)
    calendar = SessionCalendarV1.from_dict(raw)
    return CmeSessionCalendarBuildV1(
        calendar=calendar,
        source_sha256=source_sha256,
        trading_schedule_id=schedule.trading_schedule_id,
        globex_group_code=str(globex_group_code),
        coverage_start_date=coverage_start.isoformat(),
        coverage_end_date=coverage_end.isoformat(),
        generated_exception_count=len(exceptions),
    )


def build_qualified_cme_session_calendar_from_files(
    *,
    base_calendar_path: str | Path,
    source_export_path: str | Path,
    session_id: str,
    globex_group_code: str,
    coverage_start_date: str,
    coverage_end_date: str,
    calendar_version: str,
    trading_schedule_id: str | None = None,
) -> CmeSessionCalendarBuildV1:
    base = SessionCalendarV1.from_dict(read_json_object(base_calendar_path))
    source = read_json_object(source_export_path)
    return build_qualified_cme_session_calendar(
        base_calendar=base,
        source_payload=source,
        session_id=session_id,
        globex_group_code=globex_group_code,
        coverage_start_date=coverage_start_date,
        coverage_end_date=coverage_end_date,
        calendar_version=calendar_version,
        trading_schedule_id=trading_schedule_id,
    )
