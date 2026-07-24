from __future__ import annotations

import math
import re
from dataclasses import dataclass
from enum import Enum
from typing import Any, ClassVar, Mapping

from ibmd.foundation.identity import validate_id
from ibmd.foundation.time import format_utc, parse_utc

_IDENTIFIER_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$")
_HASH_RE = re.compile(r"^[0-9a-f]{64}$")


class SignalContractError(ValueError):
    pass


class SignalCalculationStatus(str, Enum):
    SIGNAL = "SIGNAL"
    NO_SIGNAL = "NO_SIGNAL"


class SignalDirection(str, Enum):
    LONG = "LONG"
    SHORT = "SHORT"
    NONE = "NONE"


def _exact_keys(
    value: Mapping[str, Any],
    expected: set[str],
    *,
    context: str,
) -> None:
    actual = set(value)
    missing = sorted(expected - actual)
    unknown = sorted(actual - expected)
    if missing or unknown:
        raise SignalContractError(
            f"{context} fields mismatch: missing={missing}, unknown={unknown}"
        )


def _identifier(value: object, *, field_name: str) -> str:
    text = str(value or "").strip()
    if not _IDENTIFIER_RE.fullmatch(text):
        raise SignalContractError(f"invalid {field_name}: {value!r}")
    return text


def _hash(value: object, *, field_name: str) -> str:
    text = str(value or "").strip()
    if not _HASH_RE.fullmatch(text):
        raise SignalContractError(
            f"{field_name} must be lowercase SHA-256 hex: {value!r}"
        )
    return text


def _positive_int(value: object, *, field_name: str) -> int:
    if isinstance(value, bool):
        raise SignalContractError(f"{field_name} must be an integer")
    try:
        parsed = int(value)
        exact = float(value)
    except (TypeError, ValueError) as exc:
        raise SignalContractError(
            f"{field_name} must be an integer: {value!r}"
        ) from exc
    if parsed <= 0 or exact != float(parsed):
        raise SignalContractError(
            f"{field_name} must be a positive integer: {value!r}"
        )
    return parsed


def _non_negative_int(value: object, *, field_name: str) -> int:
    if isinstance(value, bool):
        raise SignalContractError(f"{field_name} must be an integer")
    try:
        parsed = int(value)
        exact = float(value)
    except (TypeError, ValueError) as exc:
        raise SignalContractError(
            f"{field_name} must be an integer: {value!r}"
        ) from exc
    if parsed < 0 or exact != float(parsed):
        raise SignalContractError(
            f"{field_name} must be a non-negative integer: {value!r}"
        )
    return parsed


def _finite(
    value: object,
    *,
    field_name: str,
    non_negative: bool = False,
    positive: bool = False,
) -> float:
    if isinstance(value, bool):
        raise SignalContractError(f"{field_name} must be numeric")
    try:
        number = float(value)
    except (TypeError, ValueError) as exc:
        raise SignalContractError(
            f"{field_name} must be numeric: {value!r}"
        ) from exc
    if not math.isfinite(number):
        raise SignalContractError(f"{field_name} must be finite")
    if positive and number <= 0.0:
        raise SignalContractError(f"{field_name} must be positive")
    if non_negative and number < 0.0:
        raise SignalContractError(f"{field_name} must be non-negative")
    return number


def _optional_finite(
    value: object | None,
    *,
    field_name: str,
) -> float | None:
    if value is None:
        return None
    return _finite(value, field_name=field_name)


@dataclass(frozen=True)
class SignalEventV1:
    event_id: str
    calculation_id: str
    strategy_id: str
    strategy_version: int
    configuration_hash: str
    instrument_id: str
    source_bar_id: str
    source_bar_revision: int
    source_con_id: int
    source_local_symbol: str
    signal_bar_utc: str
    created_at_utc: str
    direction: SignalDirection
    entry_price: float
    best_pearson: float
    candidate_score_best: float | None
    potential_end_delta_points: float
    potential_max_profit_points: float
    potential_max_drawdown_points: float
    potential_used: int

    SCHEMA_NAME: ClassVar[str] = "SignalEvent"
    SCHEMA_VERSION: ClassVar[int] = 1
    KEYS: ClassVar[set[str]] = {
        "schema_name",
        "schema_version",
        "event_id",
        "calculation_id",
        "strategy_id",
        "strategy_version",
        "configuration_hash",
        "instrument_id",
        "source_bar_id",
        "source_bar_revision",
        "source_con_id",
        "source_local_symbol",
        "signal_bar_utc",
        "created_at_utc",
        "direction",
        "entry_price",
        "best_pearson",
        "candidate_score_best",
        "potential_end_delta_points",
        "potential_max_profit_points",
        "potential_max_drawdown_points",
        "potential_used",
    }

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "event_id",
            validate_id(self.event_id, expected_kind="signal_event"),
        )
        object.__setattr__(
            self,
            "calculation_id",
            validate_id(
                self.calculation_id,
                expected_kind="signal_calculation",
            ),
        )
        object.__setattr__(
            self,
            "strategy_id",
            _identifier(self.strategy_id, field_name="strategy_id"),
        )
        object.__setattr__(
            self,
            "strategy_version",
            _positive_int(
                self.strategy_version,
                field_name="strategy_version",
            ),
        )
        object.__setattr__(
            self,
            "configuration_hash",
            _hash(self.configuration_hash, field_name="configuration_hash"),
        )
        object.__setattr__(
            self,
            "instrument_id",
            _identifier(self.instrument_id, field_name="instrument_id"),
        )
        object.__setattr__(
            self,
            "source_bar_id",
            validate_id(self.source_bar_id, expected_kind="market_bar"),
        )
        object.__setattr__(
            self,
            "source_bar_revision",
            _positive_int(
                self.source_bar_revision,
                field_name="source_bar_revision",
            ),
        )
        object.__setattr__(
            self,
            "source_con_id",
            _positive_int(self.source_con_id, field_name="source_con_id"),
        )
        symbol = str(self.source_local_symbol or "").strip()
        if not symbol:
            raise SignalContractError("source_local_symbol is required")
        object.__setattr__(self, "source_local_symbol", symbol)
        signal_time = parse_utc(self.signal_bar_utc)
        created = parse_utc(self.created_at_utc)
        if created < signal_time:
            raise SignalContractError(
                "created_at_utc cannot precede signal_bar_utc"
            )
        object.__setattr__(self, "signal_bar_utc", format_utc(signal_time))
        object.__setattr__(self, "created_at_utc", format_utc(created))
        if not isinstance(self.direction, SignalDirection):
            raise SignalContractError(
                f"invalid signal direction: {self.direction!r}"
            )
        if self.direction == SignalDirection.NONE:
            raise SignalContractError(
                "SignalEventV1 direction must be LONG or SHORT"
            )
        object.__setattr__(
            self,
            "entry_price",
            _finite(
                self.entry_price,
                field_name="entry_price",
                positive=True,
            ),
        )
        best = _finite(self.best_pearson, field_name="best_pearson")
        if best < -1.0 or best > 1.0:
            raise SignalContractError("best_pearson must be in [-1, 1]")
        object.__setattr__(self, "best_pearson", best)
        score = _optional_finite(
            self.candidate_score_best,
            field_name="candidate_score_best",
        )
        if score is not None and not 0.0 <= score <= 1.0:
            raise SignalContractError(
                "candidate_score_best must be in [0, 1]"
            )
        object.__setattr__(self, "candidate_score_best", score)
        for field_name in (
            "potential_end_delta_points",
            "potential_max_profit_points",
            "potential_max_drawdown_points",
        ):
            object.__setattr__(
                self,
                field_name,
                _finite(getattr(self, field_name), field_name=field_name),
            )
        object.__setattr__(
            self,
            "potential_used",
            _positive_int(self.potential_used, field_name="potential_used"),
        )

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> "SignalEventV1":
        _exact_keys(value, cls.KEYS, context="signal event")
        if (
            value["schema_name"] != cls.SCHEMA_NAME
            or value["schema_version"] != cls.SCHEMA_VERSION
        ):
            raise SignalContractError("unsupported signal-event schema")
        try:
            direction = SignalDirection(str(value["direction"]))
        except ValueError as exc:
            raise SignalContractError(
                f"invalid signal direction: {value['direction']!r}"
            ) from exc
        return cls(
            event_id=str(value["event_id"]),
            calculation_id=str(value["calculation_id"]),
            strategy_id=str(value["strategy_id"]),
            strategy_version=value["strategy_version"],
            configuration_hash=str(value["configuration_hash"]),
            instrument_id=str(value["instrument_id"]),
            source_bar_id=str(value["source_bar_id"]),
            source_bar_revision=value["source_bar_revision"],
            source_con_id=value["source_con_id"],
            source_local_symbol=str(value["source_local_symbol"]),
            signal_bar_utc=str(value["signal_bar_utc"]),
            created_at_utc=str(value["created_at_utc"]),
            direction=direction,
            entry_price=value["entry_price"],
            best_pearson=value["best_pearson"],
            candidate_score_best=value["candidate_score_best"],
            potential_end_delta_points=value[
                "potential_end_delta_points"
            ],
            potential_max_profit_points=value[
                "potential_max_profit_points"
            ],
            potential_max_drawdown_points=value[
                "potential_max_drawdown_points"
            ],
            potential_used=value["potential_used"],
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_name": self.SCHEMA_NAME,
            "schema_version": self.SCHEMA_VERSION,
            "event_id": self.event_id,
            "calculation_id": self.calculation_id,
            "strategy_id": self.strategy_id,
            "strategy_version": self.strategy_version,
            "configuration_hash": self.configuration_hash,
            "instrument_id": self.instrument_id,
            "source_bar_id": self.source_bar_id,
            "source_bar_revision": self.source_bar_revision,
            "source_con_id": self.source_con_id,
            "source_local_symbol": self.source_local_symbol,
            "signal_bar_utc": self.signal_bar_utc,
            "created_at_utc": self.created_at_utc,
            "direction": self.direction.value,
            "entry_price": self.entry_price,
            "best_pearson": self.best_pearson,
            "candidate_score_best": self.candidate_score_best,
            "potential_end_delta_points": self.potential_end_delta_points,
            "potential_max_profit_points": self.potential_max_profit_points,
            "potential_max_drawdown_points": (
                self.potential_max_drawdown_points
            ),
            "potential_used": self.potential_used,
        }


@dataclass(frozen=True)
class SignalCalculationV1:
    calculation_id: str
    strategy_id: str
    strategy_version: int
    configuration_hash: str
    instrument_id: str
    source_bar_id: str
    source_bar_revision: int
    source_con_id: int
    source_local_symbol: str
    signal_bar_utc: str
    calculated_at_utc: str
    status: SignalCalculationStatus
    entry_price: float
    raw_candidate_count: int
    valid_pattern_count: int
    pearson_pass_count: int
    minmax_pass_count: int
    skipped_pattern_count: int
    best_raw_pearson: float
    best_signal_pearson: float | None
    best_candidate_score: float | None
    potential_direction: SignalDirection
    potential_end_delta_points: float
    potential_max_profit_points: float
    potential_max_drawdown_points: float
    potential_used: int
    reason: str | None
    event: SignalEventV1 | None

    SCHEMA_NAME: ClassVar[str] = "SignalCalculation"
    SCHEMA_VERSION: ClassVar[int] = 1
    KEYS: ClassVar[set[str]] = {
        "schema_name",
        "schema_version",
        "calculation_id",
        "strategy_id",
        "strategy_version",
        "configuration_hash",
        "instrument_id",
        "source_bar_id",
        "source_bar_revision",
        "source_con_id",
        "source_local_symbol",
        "signal_bar_utc",
        "calculated_at_utc",
        "status",
        "entry_price",
        "raw_candidate_count",
        "valid_pattern_count",
        "pearson_pass_count",
        "minmax_pass_count",
        "skipped_pattern_count",
        "best_raw_pearson",
        "best_signal_pearson",
        "best_candidate_score",
        "potential_direction",
        "potential_end_delta_points",
        "potential_max_profit_points",
        "potential_max_drawdown_points",
        "potential_used",
        "reason",
        "event",
    }

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "calculation_id",
            validate_id(
                self.calculation_id,
                expected_kind="signal_calculation",
            ),
        )
        object.__setattr__(
            self,
            "strategy_id",
            _identifier(self.strategy_id, field_name="strategy_id"),
        )
        object.__setattr__(
            self,
            "strategy_version",
            _positive_int(
                self.strategy_version,
                field_name="strategy_version",
            ),
        )
        object.__setattr__(
            self,
            "configuration_hash",
            _hash(self.configuration_hash, field_name="configuration_hash"),
        )
        object.__setattr__(
            self,
            "instrument_id",
            _identifier(self.instrument_id, field_name="instrument_id"),
        )
        object.__setattr__(
            self,
            "source_bar_id",
            validate_id(self.source_bar_id, expected_kind="market_bar"),
        )
        object.__setattr__(
            self,
            "source_bar_revision",
            _positive_int(
                self.source_bar_revision,
                field_name="source_bar_revision",
            ),
        )
        object.__setattr__(
            self,
            "source_con_id",
            _positive_int(self.source_con_id, field_name="source_con_id"),
        )
        symbol = str(self.source_local_symbol or "").strip()
        if not symbol:
            raise SignalContractError("source_local_symbol is required")
        object.__setattr__(self, "source_local_symbol", symbol)
        signal_time = parse_utc(self.signal_bar_utc)
        calculated = parse_utc(self.calculated_at_utc)
        if calculated < signal_time:
            raise SignalContractError(
                "calculated_at_utc cannot precede signal_bar_utc"
            )
        object.__setattr__(self, "signal_bar_utc", format_utc(signal_time))
        object.__setattr__(
            self,
            "calculated_at_utc",
            format_utc(calculated),
        )
        if not isinstance(self.status, SignalCalculationStatus):
            raise SignalContractError(
                f"invalid calculation status: {self.status!r}"
            )
        object.__setattr__(
            self,
            "entry_price",
            _finite(
                self.entry_price,
                field_name="entry_price",
                positive=True,
            ),
        )
        for field_name in (
            "raw_candidate_count",
            "valid_pattern_count",
            "pearson_pass_count",
            "minmax_pass_count",
            "skipped_pattern_count",
            "potential_used",
        ):
            object.__setattr__(
                self,
                field_name,
                _non_negative_int(
                    getattr(self, field_name),
                    field_name=field_name,
                ),
            )
        if self.valid_pattern_count + self.skipped_pattern_count != (
            self.raw_candidate_count
        ):
            raise SignalContractError(
                "valid_pattern_count + skipped_pattern_count must equal "
                "raw_candidate_count"
            )
        if self.pearson_pass_count > self.valid_pattern_count:
            raise SignalContractError(
                "pearson_pass_count exceeds valid_pattern_count"
            )
        if self.minmax_pass_count > self.pearson_pass_count:
            raise SignalContractError(
                "minmax_pass_count exceeds pearson_pass_count"
            )
        best_raw = _finite(
            self.best_raw_pearson,
            field_name="best_raw_pearson",
        )
        if not -1.0 <= best_raw <= 1.0:
            raise SignalContractError(
                "best_raw_pearson must be in [-1, 1]"
            )
        object.__setattr__(self, "best_raw_pearson", best_raw)
        best_signal = _optional_finite(
            self.best_signal_pearson,
            field_name="best_signal_pearson",
        )
        if best_signal is not None and not -1.0 <= best_signal <= 1.0:
            raise SignalContractError(
                "best_signal_pearson must be in [-1, 1]"
            )
        object.__setattr__(self, "best_signal_pearson", best_signal)
        score = _optional_finite(
            self.best_candidate_score,
            field_name="best_candidate_score",
        )
        if score is not None and not 0.0 <= score <= 1.0:
            raise SignalContractError(
                "best_candidate_score must be in [0, 1]"
            )
        object.__setattr__(self, "best_candidate_score", score)
        if not isinstance(self.potential_direction, SignalDirection):
            raise SignalContractError(
                f"invalid potential direction: {self.potential_direction!r}"
            )
        for field_name in (
            "potential_end_delta_points",
            "potential_max_profit_points",
            "potential_max_drawdown_points",
        ):
            object.__setattr__(
                self,
                field_name,
                _finite(getattr(self, field_name), field_name=field_name),
            )
        reason = None if self.reason is None else str(self.reason).strip()
        object.__setattr__(self, "reason", reason or None)

        if self.status == SignalCalculationStatus.SIGNAL:
            if self.event is None:
                raise SignalContractError(
                    "SIGNAL calculation requires an event"
                )
            if self.event.calculation_id != self.calculation_id:
                raise SignalContractError(
                    "event calculation_id does not match calculation"
                )
            if self.event.direction != self.potential_direction:
                raise SignalContractError(
                    "event direction does not match potential direction"
                )
            if self.potential_direction == SignalDirection.NONE:
                raise SignalContractError(
                    "SIGNAL calculation cannot have NONE direction"
                )
            if self.reason is not None:
                raise SignalContractError(
                    "SIGNAL calculation cannot have a no-signal reason"
                )
        else:
            if self.event is not None:
                raise SignalContractError(
                    "NO_SIGNAL calculation cannot contain an event"
                )
            if self.reason is None:
                raise SignalContractError(
                    "NO_SIGNAL calculation requires a reason"
                )

    @classmethod
    def from_dict(
        cls,
        value: Mapping[str, Any],
    ) -> "SignalCalculationV1":
        _exact_keys(value, cls.KEYS, context="signal calculation")
        if (
            value["schema_name"] != cls.SCHEMA_NAME
            or value["schema_version"] != cls.SCHEMA_VERSION
        ):
            raise SignalContractError(
                "unsupported signal-calculation schema"
            )
        try:
            status = SignalCalculationStatus(str(value["status"]))
            direction = SignalDirection(
                str(value["potential_direction"])
            )
        except ValueError as exc:
            raise SignalContractError(
                "invalid signal calculation enum value"
            ) from exc
        raw_event = value["event"]
        if raw_event is not None and not isinstance(raw_event, Mapping):
            raise SignalContractError("event must be an object or null")
        return cls(
            calculation_id=str(value["calculation_id"]),
            strategy_id=str(value["strategy_id"]),
            strategy_version=value["strategy_version"],
            configuration_hash=str(value["configuration_hash"]),
            instrument_id=str(value["instrument_id"]),
            source_bar_id=str(value["source_bar_id"]),
            source_bar_revision=value["source_bar_revision"],
            source_con_id=value["source_con_id"],
            source_local_symbol=str(value["source_local_symbol"]),
            signal_bar_utc=str(value["signal_bar_utc"]),
            calculated_at_utc=str(value["calculated_at_utc"]),
            status=status,
            entry_price=value["entry_price"],
            raw_candidate_count=value["raw_candidate_count"],
            valid_pattern_count=value["valid_pattern_count"],
            pearson_pass_count=value["pearson_pass_count"],
            minmax_pass_count=value["minmax_pass_count"],
            skipped_pattern_count=value["skipped_pattern_count"],
            best_raw_pearson=value["best_raw_pearson"],
            best_signal_pearson=value["best_signal_pearson"],
            best_candidate_score=value["best_candidate_score"],
            potential_direction=direction,
            potential_end_delta_points=value[
                "potential_end_delta_points"
            ],
            potential_max_profit_points=value[
                "potential_max_profit_points"
            ],
            potential_max_drawdown_points=value[
                "potential_max_drawdown_points"
            ],
            potential_used=value["potential_used"],
            reason=value["reason"],
            event=(
                None
                if raw_event is None
                else SignalEventV1.from_dict(raw_event)
            ),
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_name": self.SCHEMA_NAME,
            "schema_version": self.SCHEMA_VERSION,
            "calculation_id": self.calculation_id,
            "strategy_id": self.strategy_id,
            "strategy_version": self.strategy_version,
            "configuration_hash": self.configuration_hash,
            "instrument_id": self.instrument_id,
            "source_bar_id": self.source_bar_id,
            "source_bar_revision": self.source_bar_revision,
            "source_con_id": self.source_con_id,
            "source_local_symbol": self.source_local_symbol,
            "signal_bar_utc": self.signal_bar_utc,
            "calculated_at_utc": self.calculated_at_utc,
            "status": self.status.value,
            "entry_price": self.entry_price,
            "raw_candidate_count": self.raw_candidate_count,
            "valid_pattern_count": self.valid_pattern_count,
            "pearson_pass_count": self.pearson_pass_count,
            "minmax_pass_count": self.minmax_pass_count,
            "skipped_pattern_count": self.skipped_pattern_count,
            "best_raw_pearson": self.best_raw_pearson,
            "best_signal_pearson": self.best_signal_pearson,
            "best_candidate_score": self.best_candidate_score,
            "potential_direction": self.potential_direction.value,
            "potential_end_delta_points": (
                self.potential_end_delta_points
            ),
            "potential_max_profit_points": (
                self.potential_max_profit_points
            ),
            "potential_max_drawdown_points": (
                self.potential_max_drawdown_points
            ),
            "potential_used": self.potential_used,
            "reason": self.reason,
            "event": None if self.event is None else self.event.to_dict(),
        }
