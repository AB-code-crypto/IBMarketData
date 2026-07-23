from __future__ import annotations

import math
import re
from dataclasses import dataclass

from ibmd.foundation.identity import validate_id

_INSTRUMENT_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$")
_HASH_RE = re.compile(r"^[0-9a-f]{64}$")


class MarketDataConfigError(ValueError):
    pass


def _positive_float(value: object, *, field_name: str) -> float:
    try:
        number = float(value)
    except (TypeError, ValueError) as exc:
        raise MarketDataConfigError(
            f"{field_name} must be numeric: {value!r}"
        ) from exc
    if not math.isfinite(number) or number <= 0.0:
        raise MarketDataConfigError(
            f"{field_name} must be finite and positive: {value!r}"
        )
    return number


def _positive_int(value: object, *, field_name: str) -> int:
    if isinstance(value, bool):
        raise MarketDataConfigError(f"{field_name} must be an integer")
    try:
        parsed = int(value)
        exact = float(value)
    except (TypeError, ValueError) as exc:
        raise MarketDataConfigError(
            f"{field_name} must be an integer: {value!r}"
        ) from exc
    if parsed <= 0 or exact != float(parsed):
        raise MarketDataConfigError(
            f"{field_name} must be a positive integer: {value!r}"
        )
    return parsed


@dataclass(frozen=True)
class MarketDataShadowConfig:
    instrument_id: str
    deployment_id: str
    instance_id: str
    application_version: str
    configuration_hash: str
    bar_duration_seconds: int = 5
    history_lookback_seconds: int = 3_600
    bar_max_age_seconds: float = 60.0
    realtime_read_timeout_seconds: float = 1.0
    writer_queue_maxsize: int = 10_000
    use_rth: bool = False
    service_name: str = "market_data"

    def __post_init__(self) -> None:
        instrument = str(self.instrument_id or "").strip()
        if not _INSTRUMENT_RE.fullmatch(instrument):
            raise MarketDataConfigError(
                f"invalid instrument_id: {self.instrument_id!r}"
            )
        object.__setattr__(self, "instrument_id", instrument)
        if not str(self.deployment_id or "").strip():
            raise MarketDataConfigError("deployment_id is required")
        validate_id(self.instance_id, expected_kind="instance")
        if not str(self.application_version or "").strip():
            raise MarketDataConfigError("application_version is required")
        if not _HASH_RE.fullmatch(str(self.configuration_hash or "")):
            raise MarketDataConfigError(
                "configuration_hash must be a lowercase SHA-256 hex string"
            )
        duration = _positive_int(
            self.bar_duration_seconds,
            field_name="bar_duration_seconds",
        )
        if duration != 5:
            raise MarketDataConfigError(
                "market-data shadow v1 supports 5-second bars only"
            )
        object.__setattr__(self, "bar_duration_seconds", duration)
        object.__setattr__(
            self,
            "history_lookback_seconds",
            _positive_int(
                self.history_lookback_seconds,
                field_name="history_lookback_seconds",
            ),
        )
        object.__setattr__(
            self,
            "bar_max_age_seconds",
            _positive_float(
                self.bar_max_age_seconds,
                field_name="bar_max_age_seconds",
            ),
        )
        object.__setattr__(
            self,
            "realtime_read_timeout_seconds",
            _positive_float(
                self.realtime_read_timeout_seconds,
                field_name="realtime_read_timeout_seconds",
            ),
        )
        object.__setattr__(
            self,
            "writer_queue_maxsize",
            _positive_int(
                self.writer_queue_maxsize,
                field_name="writer_queue_maxsize",
            ),
        )
        if not isinstance(self.use_rth, bool):
            raise MarketDataConfigError("use_rth must be boolean")
        if self.service_name != "market_data":
            raise MarketDataConfigError(
                f"service_name must be 'market_data': {self.service_name!r}"
            )
