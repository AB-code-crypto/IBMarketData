from __future__ import annotations

import math
import re
from dataclasses import dataclass

from ibmd.foundation.identity import validate_id

_IDENTIFIER_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$")
_HASH_RE = re.compile(r"^[0-9a-f]{64}$")


class SignalConfigError(ValueError):
    pass


def _positive_int(value: object, *, field_name: str) -> int:
    if isinstance(value, bool):
        raise SignalConfigError(f"{field_name} must be an integer")
    try:
        parsed = int(value)
        exact = float(value)
    except (TypeError, ValueError) as exc:
        raise SignalConfigError(
            f"{field_name} must be an integer: {value!r}"
        ) from exc
    if parsed <= 0 or exact != float(parsed):
        raise SignalConfigError(
            f"{field_name} must be a positive integer: {value!r}"
        )
    return parsed


def _positive_float(value: object, *, field_name: str) -> float:
    try:
        number = float(value)
    except (TypeError, ValueError) as exc:
        raise SignalConfigError(
            f"{field_name} must be numeric: {value!r}"
        ) from exc
    if not math.isfinite(number) or number <= 0.0:
        raise SignalConfigError(
            f"{field_name} must be finite and positive"
        )
    return number


@dataclass(frozen=True)
class SignalShadowConfig:
    deployment_id: str
    instance_id: str
    application_version: str
    service_configuration_hash: str
    strategy_id: str
    strategy_version: int
    signal_configuration_hash: str
    instrument_id: str
    price_precision: int
    source_bar_size_seconds: int
    max_complete_bar_lag_seconds: int
    rolling_step_seconds: int
    pattern_lookback_minutes: int
    potential_horizon_minutes: int
    historical_lookback_days: int
    pearson_minimum: float
    minmax_hard_filter_max_ratio: float
    score_pearson_weight: float
    score_end_delta_weight: float
    score_minmax_weight: float
    potential_candidate_min_count: int
    potential_candidate_max_count: int
    minimum_abs_potential_end_delta_points: float
    candidate_hour_profile: str
    poll_interval_seconds: float = 1.0
    service_name: str = "signal"

    def __post_init__(self) -> None:
        for field_name in (
            "deployment_id",
            "application_version",
            "strategy_id",
            "instrument_id",
            "candidate_hour_profile",
        ):
            value = str(getattr(self, field_name) or "").strip()
            if not _IDENTIFIER_RE.fullmatch(value):
                raise SignalConfigError(
                    f"invalid {field_name}: {value!r}"
                )
            object.__setattr__(self, field_name, value)
        validate_id(self.instance_id, expected_kind="instance")
        for field_name in (
            "service_configuration_hash",
            "signal_configuration_hash",
        ):
            value = str(getattr(self, field_name) or "").strip()
            if not _HASH_RE.fullmatch(value):
                raise SignalConfigError(
                    f"{field_name} must be lowercase SHA-256 hex"
                )
            object.__setattr__(self, field_name, value)

        for field_name in (
            "strategy_version",
            "source_bar_size_seconds",
            "max_complete_bar_lag_seconds",
            "rolling_step_seconds",
            "pattern_lookback_minutes",
            "potential_horizon_minutes",
            "historical_lookback_days",
            "potential_candidate_min_count",
            "potential_candidate_max_count",
        ):
            object.__setattr__(
                self,
                field_name,
                _positive_int(
                    getattr(self, field_name),
                    field_name=field_name,
                ),
            )
        precision = int(self.price_precision)
        if precision < 0:
            raise SignalConfigError(
                "price_precision must be non-negative"
            )
        object.__setattr__(self, "price_precision", precision)
        for field_name in (
            "pearson_minimum",
            "minmax_hard_filter_max_ratio",
            "score_pearson_weight",
            "score_end_delta_weight",
            "score_minmax_weight",
            "minimum_abs_potential_end_delta_points",
            "poll_interval_seconds",
        ):
            object.__setattr__(
                self,
                field_name,
                _positive_float(
                    getattr(self, field_name),
                    field_name=field_name,
                ),
            )
        if not 0.0 < self.pearson_minimum <= 1.0:
            raise SignalConfigError(
                "pearson_minimum must be in (0, 1]"
            )
        if (
            self.potential_candidate_min_count
            > self.potential_candidate_max_count
        ):
            raise SignalConfigError(
                "potential candidate min exceeds max"
            )
        if self.service_name != "signal":
            raise SignalConfigError(
                "service_name must be 'signal'"
            )
