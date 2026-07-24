from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from enum import Enum

from ibmd.foundation.time import ensure_utc, format_utc, parse_utc

from .models import (
    CatalogError,
    DeclaredActivationGapV1,
    FuturesContractCalendarV1,
    FuturesContractSpecV1,
    LocalIntervalV1,
    SessionCalendarV1,
    SessionExceptionStatus,
    SessionDefinitionV1,
)


class ActiveContractStatus(str, Enum):
    ACTIVE = "ACTIVE"
    DECLARED_GAP = "DECLARED_GAP"
    OUTSIDE_CALENDAR = "OUTSIDE_CALENDAR"


@dataclass(frozen=True)
class ActiveContractResolutionV1:
    status: ActiveContractStatus
    instrument_id: str
    observed_at_utc: str
    contract: FuturesContractSpecV1 | None = None
    gap: DeclaredActivationGapV1 | None = None


def resolve_active_contract(
    calendar: FuturesContractCalendarV1,
    at_utc: datetime | str,
) -> ActiveContractResolutionV1:
    observed = parse_utc(at_utc) if isinstance(at_utc, str) else ensure_utc(at_utc)
    observed_text = format_utc(observed)
    for contract in calendar.contracts:
        if contract.active_from <= observed < contract.active_to:
            return ActiveContractResolutionV1(
                status=ActiveContractStatus.ACTIVE,
                instrument_id=calendar.instrument_id,
                observed_at_utc=observed_text,
                contract=contract,
            )
    for gap in calendar.declared_gaps:
        if gap.start <= observed < gap.end:
            return ActiveContractResolutionV1(
                status=ActiveContractStatus.DECLARED_GAP,
                instrument_id=calendar.instrument_id,
                observed_at_utc=observed_text,
                gap=gap,
            )
    return ActiveContractResolutionV1(
        status=ActiveContractStatus.OUTSIDE_CALENDAR,
        instrument_id=calendar.instrument_id,
        observed_at_utc=observed_text,
    )


def require_active_contract(
    calendar: FuturesContractCalendarV1,
    at_utc: datetime | str,
) -> FuturesContractSpecV1:
    resolution = resolve_active_contract(calendar, at_utc)
    if resolution.contract is None:
        raise CatalogError(
            "active futures contract is not available: "
            f"instrument={calendar.instrument_id}, at={resolution.observed_at_utc}, "
            f"status={resolution.status.value}"
        )
    return resolution.contract


def find_registered_contract(
    calendar: FuturesContractCalendarV1,
    *,
    con_id: int | None = None,
    local_symbol: str | None = None,
) -> FuturesContractSpecV1 | None:
    expected_con_id = int(con_id or 0)
    expected_symbol = str(local_symbol or "").strip()
    for contract in calendar.contracts:
        if expected_con_id > 0 and contract.con_id == expected_con_id:
            return contract
        if expected_symbol and contract.local_symbol == expected_symbol:
            return contract
    return None


class SessionPhase(str, Enum):
    TRADING = "TRADING"
    MAINTENANCE = "MAINTENANCE"
    CLOSED = "CLOSED"


@dataclass(frozen=True)
class SessionResolutionV1:
    session_id: str
    observed_at_utc: str
    local_date: str
    local_time: str
    phase: SessionPhase
    reason: str
    production_qualified: bool

    @property
    def is_trading_open(self) -> bool:
        return self.phase == SessionPhase.TRADING


def _contains(intervals: tuple[LocalIntervalV1, ...], seconds: int) -> bool:
    return any(interval.contains_seconds(seconds) for interval in intervals)


def resolve_session(
    calendar: SessionCalendarV1,
    *,
    session_id: str,
    at_utc: datetime | str,
) -> SessionResolutionV1:
    session = calendar.require(session_id)
    observed = parse_utc(at_utc) if isinstance(at_utc, str) else ensure_utc(at_utc)
    local = observed.astimezone(session.zone)
    seconds = local.hour * 3600 + local.minute * 60 + local.second
    local_date = local.date().isoformat()
    local_time = local.time().replace(microsecond=0).isoformat()

    exception = next(
        (item for item in session.exceptions if item.local_date == local_date),
        None,
    )
    if exception is not None:
        if exception.status == SessionExceptionStatus.CLOSED:
            phase = SessionPhase.CLOSED
        elif _contains(exception.maintenance_intervals, seconds):
            phase = SessionPhase.MAINTENANCE
        elif _contains(exception.trading_intervals, seconds):
            phase = SessionPhase.TRADING
        else:
            phase = SessionPhase.CLOSED
        return SessionResolutionV1(
            session_id=session.session_id,
            observed_at_utc=format_utc(observed),
            local_date=local_date,
            local_time=local_time,
            phase=phase,
            reason=f"exception:{exception.reason}",
            production_qualified=session.production_qualified,
        )

    day = session.weekly_days[local.weekday()]
    if _contains(day.maintenance_intervals, seconds):
        phase = SessionPhase.MAINTENANCE
    elif _contains(day.trading_intervals, seconds):
        phase = SessionPhase.TRADING
    else:
        phase = SessionPhase.CLOSED
    return SessionResolutionV1(
        session_id=session.session_id,
        observed_at_utc=format_utc(observed),
        local_date=local_date,
        local_time=local_time,
        phase=phase,
        reason="weekly_template",
        production_qualified=session.production_qualified,
    )


def require_production_qualified_session(session: SessionDefinitionV1) -> None:
    if not session.production_qualified:
        raise CatalogError(
            "session calendar is not production-qualified: "
            f"session={session.session_id}, note={session.qualification_note}"
        )
