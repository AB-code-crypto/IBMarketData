from __future__ import annotations

from dataclasses import dataclass
from datetime import time
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from ibmd.foundation.time import parse_utc


class NewRiskWindowError(ValueError):
    pass


def _seconds_of_day(value: str, *, field_name: str) -> int:
    text = str(value or "").strip()
    try:
        parsed = time.fromisoformat(text)
    except ValueError as exc:
        raise NewRiskWindowError(
            f"{field_name} must be an ISO local time: {value!r}"
        ) from exc
    if parsed.tzinfo is not None:
        raise NewRiskWindowError(
            f"{field_name} must not contain a timezone offset"
        )
    return parsed.hour * 3600 + parsed.minute * 60 + parsed.second


@dataclass(frozen=True)
class NewRiskWindowV1:
    enabled: bool
    timezone_name: str
    liquidation_start_local: str
    risk_blocked_until_local: str

    def __post_init__(self) -> None:
        if not isinstance(self.enabled, bool):
            raise NewRiskWindowError("enabled must be a boolean")
        timezone_name = str(self.timezone_name or "").strip()
        try:
            ZoneInfo(timezone_name)
        except ZoneInfoNotFoundError as exc:
            raise NewRiskWindowError(
                f"unknown new-risk timezone: {timezone_name!r}"
            ) from exc
        object.__setattr__(self, "timezone_name", timezone_name)
        start = _seconds_of_day(
            self.liquidation_start_local,
            field_name="liquidation_start_local",
        )
        end = _seconds_of_day(
            self.risk_blocked_until_local,
            field_name="risk_blocked_until_local",
        )
        if start >= end:
            raise NewRiskWindowError(
                "liquidation_start_local must precede risk_blocked_until_local"
            )

    @property
    def liquidation_start_seconds(self) -> int:
        return _seconds_of_day(
            self.liquidation_start_local,
            field_name="liquidation_start_local",
        )

    @property
    def risk_blocked_until_seconds(self) -> int:
        return _seconds_of_day(
            self.risk_blocked_until_local,
            field_name="risk_blocked_until_local",
        )

    def require_allows_new_risk(
        self,
        *,
        observed_at_utc: str,
        lead_seconds: int = 0,
    ) -> None:
        if not self.enabled:
            return
        lead = int(lead_seconds)
        if lead < 0:
            raise NewRiskWindowError("lead_seconds must be non-negative")
        local = parse_utc(observed_at_utc).astimezone(
            ZoneInfo(self.timezone_name)
        )
        seconds = local.hour * 3600 + local.minute * 60 + local.second
        start = self.liquidation_start_seconds
        end = self.risk_blocked_until_seconds
        already_blocked = start <= seconds < end
        enters_blocked_window = seconds < start <= seconds + lead
        if already_blocked or enters_blocked_window:
            raise NewRiskWindowError(
                "new broker risk is blocked by the daily-flat window: "
                f"local_time={local.isoformat()}, "
                f"blocked={self.liquidation_start_local}.."
                f"{self.risk_blocked_until_local}, "
                f"timezone={self.timezone_name}, lead_seconds={lead}"
            )
